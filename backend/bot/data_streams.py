from __future__ import annotations

import asyncio
import json
import logging
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import aiohttp
import websockets

from .indicators import update_vwap_accumulators
from .models import DataSource, Direction, MarketOddsSnapshot, MarketResolution, MarketTick
from .state import EngineState

log = logging.getLogger("alpha_z_engine.data_streams")


@dataclass(slots=True, frozen=True)
class DataStreamsConfig:
    symbol: str = "BTCUSDT"
    gamma_api: str = "https://gamma-api.polymarket.com"
    polymarket_crypto_price_api: str = "https://polymarket.com/api/crypto/crypto-price"
    # Official public market-data REST host. This engine only uses public endpoints.
    binance_rest_api: str = "https://data-api.binance.vision"
    binance_ws_api: str = "wss://stream.binance.com:9443/ws"
    kline_interval: str = "1h"
    history_limit: int = 120
    strike_cache_ttl_seconds: float = 300.0
    ws_backoff_initial_seconds: float = 1.0
    ws_backoff_max_seconds: float = 30.0
    ws_backoff_jitter_seconds: float = 0.35
    http_total_timeout_seconds: float = 8.0
    http_connect_timeout_seconds: float = 3.0
    http_sock_read_timeout_seconds: float = 6.0
    http_connector_limit: int = 64
    agg_trade_backfill_batches: int = 3
    agg_trade_backfill_limit: int = 1000
    kline_backfill_limit: int = 20
    websocket_ping_interval_seconds: float = 20.0
    websocket_ping_timeout_seconds: float = 20.0
    websocket_close_timeout_seconds: float = 5.0
    rest_bootstrap_retry_attempts: int = 4

    @property
    def symbol_lower(self) -> str:
        return self.symbol.lower()

    @property
    def kline_stream_url(self) -> str:
        return f"{self.binance_ws_api}/{self.symbol_lower}@kline_{self.kline_interval}"

    @property
    def agg_trade_stream_url(self) -> str:
        return f"{self.binance_ws_api}/{self.symbol_lower}@aggTrade"


def create_http_session(config: DataStreamsConfig) -> aiohttp.ClientSession:
    timeout = aiohttp.ClientTimeout(
        total=config.http_total_timeout_seconds,
        connect=config.http_connect_timeout_seconds,
        sock_read=config.http_sock_read_timeout_seconds,
    )
    connector = aiohttp.TCPConnector(
        limit=config.http_connector_limit,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
    )
    return aiohttp.ClientSession(timeout=timeout, connector=connector)


def _backoff_delay(config: DataStreamsConfig, attempt: int) -> float:
    bounded_attempt = max(attempt, 0)
    delay = min(config.ws_backoff_max_seconds, config.ws_backoff_initial_seconds * (2 ** bounded_attempt))
    return delay + random.uniform(0.0, config.ws_backoff_jitter_seconds)


def _parse_seconds_remaining(end_date_str: str) -> float:
    if not end_date_str:
        return -1.0
    try:
        end_dt = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
        return (end_dt - datetime.now(timezone.utc)).total_seconds()
    except Exception:
        return -1.0


def _parse_end_datetime(end_date_str: str) -> datetime | None:
    if not end_date_str:
        return None
    try:
        return datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
    except Exception:
        return None


def _infer_fee_curve(slug: str, market: dict[str, Any], event: dict[str, Any]) -> tuple[str, bool, float, float]:
    market_category = str(market.get("category") or event.get("category") or "").strip()
    fees_enabled = bool(market.get("feesEnabled", False) or event.get("feesEnabled", False))
    if not fees_enabled:
        return market_category, False, 0.0, 0.0

    fee_text = " ".join(
        [
            slug,
            market_category,
            str(market.get("question") or ""),
            str(event.get("title") or ""),
        ]
    ).lower()
    if any(token in fee_text for token in ("bitcoin", "btc", "ethereum", "eth", "solana", "sol", "crypto")):
        return market_category or "CRYPTO", True, 0.25, 2.0
    if any(token in fee_text for token in ("sport", "nba", "nfl", "mlb", "nhl", "soccer", "epl", "game")):
        return market_category or "SPORTS", True, 0.0175, 1.0
    return market_category or "FEE_ENABLED", True, 0.25, 2.0


def _is_hourly_up_or_down_slug(slug: str) -> bool:
    return bool(re.fullmatch(r"bitcoin-up-or-down-[a-z]+-\d+-\d+(am|pm)-et", slug))


def _infer_market_resolution(slug: str, title: str) -> MarketResolution:
    if re.search(r"\$([\d,]+(?:\.\d+)?)", title):
        return MarketResolution.STRIKE
    if _is_hourly_up_or_down_slug(slug):
        return MarketResolution.CANDLE_OPEN
    return MarketResolution.UNKNOWN


def _decode_json_array(raw: Any) -> list[Any]:
    if isinstance(raw, str):
        try:
            decoded = json.loads(raw)
        except Exception:
            return []
        return list(decoded) if isinstance(decoded, list) else []
    if isinstance(raw, list):
        return list(raw)
    return []


def _normalize_outcome_label(value: Any) -> Direction | None:
    label = str(value or "").strip().upper()
    if label in {"UP", "YES"}:
        return Direction.UP
    if label in {"DOWN", "NO"}:
        return Direction.DOWN
    return None


def _extract_directional_market_data(market: dict[str, Any]) -> tuple[dict[Direction, float], dict[Direction, str]] | None:
    outcomes = _decode_json_array(market.get("outcomes"))
    outcome_prices = _decode_json_array(market.get("outcomePrices"))
    token_ids = [str(item) for item in _decode_json_array(market.get("clobTokenIds"))]
    if len(outcomes) < 2 or len(outcomes) != len(outcome_prices) or len(outcomes) != len(token_ids):
        return None

    prices_by_direction: dict[Direction, float] = {}
    token_ids_by_direction: dict[Direction, str] = {}
    for raw_outcome, raw_price, token_id in zip(outcomes, outcome_prices, token_ids):
        direction = _normalize_outcome_label(raw_outcome)
        if direction is None:
            continue
        try:
            price = float(raw_price)
        except Exception:
            return None
        prices_by_direction[direction] = price * 100.0
        token_ids_by_direction[direction] = str(token_id)

    if Direction.UP not in prices_by_direction or Direction.DOWN not in prices_by_direction:
        return None
    return prices_by_direction, token_ids_by_direction


def _parse_kline_message(raw_payload: dict[str, Any]) -> MarketTick:
    payload = raw_payload.get("data", raw_payload)
    kline = payload["k"]
    return MarketTick(
        timestamp=datetime.fromtimestamp(int(kline["t"]) / 1000.0, tz=timezone.utc),
        source=DataSource.BINANCE,
        price=float(kline["c"]),
        open=float(kline["o"]),
        high=float(kline["h"]),
        low=float(kline["l"]),
        close=float(kline["c"]),
        volume=float(kline["v"]),
        quote_volume=float(kline.get("q", 0.0) or 0.0),
        trade_count=int(kline.get("n", 0) or 0),
        is_closed=bool(kline.get("x", False)),
        event_time_ms=int(payload.get("E", 0) or 0),
        close_time_ms=int(kline.get("T", 0) or 0),
    )


def _parse_rest_kline(row: list[Any]) -> MarketTick:
    return MarketTick(
        timestamp=datetime.fromtimestamp(int(row[0]) / 1000.0, tz=timezone.utc),
        source=DataSource.BINANCE,
        price=float(row[4]),
        open=float(row[1]),
        high=float(row[2]),
        low=float(row[3]),
        close=float(row[4]),
        volume=float(row[5]),
        quote_volume=float(row[7]),
        trade_count=int(row[8]),
        is_closed=True,
        event_time_ms=int(row[0]),
        close_time_ms=int(row[6]),
    )


async def _ensure_intraday_session(state: EngineState, session_date: str) -> None:
    async with state.market_lock:
        if state.vwap_date == session_date:
            return
        state.cvd_total = 0.0
        state.cvd_snapshot_at_candle_open = 0.0
        state.last_cvd_1min = 0.0
        state.cvd_1min_buffer.clear()
        state.vwap_cum_pv = 0.0
        state.vwap_cum_vol = 0.0
        state.vwap_date = session_date


async def _apply_closed_candle(state: EngineState, tick: MarketTick) -> None:
    session_date = tick.timestamp.astimezone(timezone.utc).date().isoformat()
    await _ensure_intraday_session(state, session_date)

    async with state.market_lock:
        state.vwap_cum_pv, state.vwap_cum_vol = update_vwap_accumulators(
            state.vwap_cum_pv,
            state.vwap_cum_vol,
            tick,
        )
        state.cvd_snapshot_at_candle_open = state.cvd_total
        state.candle_history.append(tick)
        state.live_candle = tick
        state.live_price = tick.close
        state.last_closed_kline_ms = max(
            state.last_closed_kline_ms,
            tick.close_time_ms or tick.event_time_ms,
        )


async def _apply_live_kline(state: EngineState, tick: MarketTick) -> None:
    if tick.is_closed:
        await _apply_closed_candle(state, tick)
        return
    await state.set_live_tick(price=tick.close, candle=tick, agg_trade_ms=tick.event_time_ms or None)


async def _apply_agg_trade(state: EngineState, payload: dict[str, Any]) -> None:
    price = float(payload.get("p", 0.0) or 0.0)
    quantity = float(payload.get("q", 0.0) or 0.0)
    is_buyer_maker = bool(payload.get("m", False))
    trade_ts_ms = int(payload.get("T", 0) or 0)
    delta = -quantity if is_buyer_maker else quantity
    event_ts = (trade_ts_ms / 1000.0) if trade_ts_ms > 0 else time.time()

    async with state.market_lock:
        state.cvd_total += delta
        state.live_price = price if price > 0 else state.live_price
        if trade_ts_ms > 0:
            state.last_agg_trade_ms = max(state.last_agg_trade_ms, trade_ts_ms)
        state.cvd_1min_buffer.append((event_ts, delta))
        cutoff = event_ts - 60.0
        while state.cvd_1min_buffer and state.cvd_1min_buffer[0][0] < cutoff:
            state.cvd_1min_buffer.popleft()
        state.last_cvd_1min = sum(item_delta for _, item_delta in state.cvd_1min_buffer)


class BinanceStreamManager:
    __slots__ = ("config",)

    def __init__(self, config: DataStreamsConfig | None = None):
        self.config = config or DataStreamsConfig()

    async def load_initial_history(self, session: aiohttp.ClientSession, state: EngineState) -> None:
        now = datetime.now(timezone.utc)
        session_date = now.date().isoformat()
        await state.reset_intraday_flows(session_date)

        params = {
            "symbol": self.config.symbol,
            "interval": self.config.kline_interval,
            "limit": self.config.history_limit,
        }
        async with session.get(f"{self.config.binance_rest_api}/api/v3/klines", params=params) as response:
            response.raise_for_status()
            rows = await response.json()

        async with state.market_lock:
            state.candle_history.clear()
            state.live_candle = None
            state.live_price = 0.0
            state.last_closed_kline_ms = 0
            state.vwap_cum_pv = 0.0
            state.vwap_cum_vol = 0.0
            state.vwap_date = session_date

            for row in rows:
                tick = _parse_rest_kline(row)
                if tick.timestamp.astimezone(timezone.utc).date().isoformat() == session_date:
                    state.vwap_cum_pv, state.vwap_cum_vol = update_vwap_accumulators(
                        state.vwap_cum_pv,
                        state.vwap_cum_vol,
                        tick,
                    )
                state.candle_history.append(tick)
                state.live_candle = tick
                state.live_price = tick.close
                state.last_closed_kline_ms = max(state.last_closed_kline_ms, tick.close_time_ms)

            state.cvd_snapshot_at_candle_open = state.cvd_total

        log.info(
            "[SYSTEM] Loaded %s candles. VWAP=%s",
            len(rows),
            f"{(state.vwap_cum_pv / state.vwap_cum_vol):,.2f}" if state.vwap_cum_vol > 0 else "0.00",
        )

    async def backfill_missing_klines(self, session: aiohttp.ClientSession, state: EngineState) -> int:
        async with state.market_lock:
            start_ms = state.last_closed_kline_ms
        if start_ms <= 0:
            return 0

        params = {
            "symbol": self.config.symbol,
            "interval": self.config.kline_interval,
            "startTime": start_ms + 1,
            "limit": self.config.kline_backfill_limit,
        }
        async with session.get(f"{self.config.binance_rest_api}/api/v3/klines", params=params) as response:
            if response.status != 200:
                return 0
            rows = await response.json()

        if not rows:
            return 0

        now_ms = int(time.time() * 1000)
        added = 0
        for row in rows:
            tick = _parse_rest_kline(row)
            if tick.close_time_ms <= start_ms or tick.close_time_ms > now_ms:
                continue
            await _apply_closed_candle(state, tick)
            added += 1

        if added > 0:
            log.info("[KLINE REST] Backfilled %s missing closed candles after reconnect.", added)
        return added

    async def backfill_missing_agg_trades(self, session: aiohttp.ClientSession, state: EngineState) -> int:
        async with state.market_lock:
            next_start = state.last_agg_trade_ms + 1
        if next_start <= 1:
            return 0

        total_backfilled = 0
        for _ in range(self.config.agg_trade_backfill_batches):
            params = {
                "symbol": self.config.symbol,
                "startTime": next_start,
                "limit": self.config.agg_trade_backfill_limit,
            }
            async with session.get(f"{self.config.binance_rest_api}/api/v3/aggTrades", params=params) as response:
                if response.status != 200:
                    break
                trades = await response.json()

            if not trades:
                break

            max_seen_ts = next_start
            for trade in trades:
                trade_ts = int(trade.get("T", 0) or 0)
                if trade_ts < next_start:
                    continue
                await _apply_agg_trade(state, trade)
                total_backfilled += 1
                max_seen_ts = max(max_seen_ts, trade_ts)

            if len(trades) < self.config.agg_trade_backfill_limit:
                break
            next_start = max_seen_ts + 1

        if total_backfilled > 0:
            log.info("[TRADE REST] Backfilled %s agg trades after reconnect.", total_backfilled)
        return total_backfilled

    async def stream_klines(self, session: aiohttp.ClientSession, state: EngineState) -> None:
        attempt = 0
        while True:
            try:
                async with websockets.connect(
                    self.config.kline_stream_url,
                    ping_interval=self.config.websocket_ping_interval_seconds,
                    ping_timeout=self.config.websocket_ping_timeout_seconds,
                    close_timeout=self.config.websocket_close_timeout_seconds,
                    max_size=2**20,
                ) as websocket:
                    attempt = 0
                    await self.backfill_missing_klines(session, state)
                    async for raw_message in websocket:
                        payload = json.loads(raw_message)
                        tick = _parse_kline_message(payload)
                        await _apply_live_kline(state, tick)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                delay = _backoff_delay(self.config, attempt)
                attempt += 1
                log.warning("[KLINE WS] Disconnected: %s. Reconnecting in %.2fs...", exc, delay)
                await asyncio.sleep(delay)

    async def stream_agg_trades(self, session: aiohttp.ClientSession, state: EngineState) -> None:
        attempt = 0
        while True:
            try:
                async with websockets.connect(
                    self.config.agg_trade_stream_url,
                    ping_interval=self.config.websocket_ping_interval_seconds,
                    ping_timeout=self.config.websocket_ping_timeout_seconds,
                    close_timeout=self.config.websocket_close_timeout_seconds,
                    max_size=2**20,
                ) as websocket:
                    attempt = 0
                    await self.backfill_missing_agg_trades(session, state)
                    async for raw_message in websocket:
                        payload = json.loads(raw_message)
                        await _apply_agg_trade(state, payload)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                delay = _backoff_delay(self.config, attempt)
                attempt += 1
                log.warning("[TRADE WS] Disconnected: %s. Reconnecting in %.2fs...", exc, delay)
                await asyncio.sleep(delay)

    async def run(self, session: aiohttp.ClientSession, state: EngineState) -> None:
        async with asyncio.TaskGroup() as task_group:
            task_group.create_task(self.stream_klines(session, state))
            task_group.create_task(self.stream_agg_trades(session, state))


class PolymarketFetcher:
    __slots__ = ("config", "_strike_cache")

    def __init__(self, config: DataStreamsConfig | None = None):
        self.config = config or DataStreamsConfig()
        self._strike_cache: dict[str, tuple[float, float, bool]] = {}

    async def fetch_market_meta_from_slug(self, session: aiohttp.ClientSession, slug: str) -> dict[str, Any] | None:
        try:
            async with session.get(f"{self.config.gamma_api}/events/slug/{slug}") as response:
                if response.status != 200:
                    return None
                event = await response.json()
        except Exception:
            return None

        if isinstance(event, list):
            for item in event:
                if not isinstance(item, dict):
                    continue
                if str(item.get("slug") or "") == slug and item.get("clobTokenIds") is not None:
                    return {"event": item, "market": item}
                markets = item.get("markets", [])
                exact_market = next(
                    (
                        market
                        for market in markets
                        if isinstance(market, dict) and str(market.get("slug") or "") == slug
                    ),
                    None,
                )
                if exact_market is not None:
                    return {"event": item, "market": exact_market}
            return None

        if isinstance(event, dict) and str(event.get("slug") or "") == slug and event.get("clobTokenIds") is not None:
            return {"event": event, "market": event}

        markets = event.get("markets", []) if isinstance(event, dict) else []
        exact_market = next(
            (
                market
                for market in markets
                if isinstance(market, dict) and str(market.get("slug") or "") == slug
            ),
            None,
        )
        if exact_market is None:
            return None
        return {"event": event, "market": exact_market}

    async def fetch_price_to_beat_for_market(
        self,
        session: aiohttp.ClientSession,
        slug: str,
        *,
        meta: dict[str, Any] | None = None,
    ) -> float:
        if meta is None:
            meta = await self.fetch_market_meta_from_slug(session, slug)
        if meta is None:
            return 0.0

        market = meta["market"]
        event = meta["event"]
        title = str(market.get("question") or event.get("title") or "")
        match = re.search(r"\$([\d,]+(?:\.\d+)?)", title)
        now_ts = time.time()
        if match:
            strike = float(match.group(1).replace(",", ""))
            self._strike_cache[slug] = (strike, now_ts, True)
            return strike

        end_date_str = str(market.get("endDate") or event.get("endDate") or "")
        end_dt = _parse_end_datetime(end_date_str)
        if end_dt is None:
            return 0.0
        market_start_dt = end_dt - timedelta(hours=1)
        market_started = datetime.now(timezone.utc) >= market_start_dt

        cached = self._strike_cache.get(slug)
        if cached is not None:
            cached_value, cached_at, finalized = cached
            if finalized or ((not market_started) and (now_ts - cached_at) < self.config.strike_cache_ttl_seconds):
                return cached_value

        async def _fetch_binance_hour_open() -> float:
            params = {
                "symbol": self.config.symbol,
                "interval": "1h",
                "startTime": int(market_start_dt.timestamp() * 1000),
                "limit": 1,
            }
            async with session.get(f"{self.config.binance_rest_api}/api/v3/klines", params=params) as response:
                if response.status != 200:
                    return 0.0
                payload = await response.json()
                if not payload:
                    return 0.0
                return float(payload[0][1])

        if market_started:
            try:
                strike = await _fetch_binance_hour_open()
                if strike > 0:
                    self._strike_cache[slug] = (strike, now_ts, True)
                    return strike
            except Exception:
                pass

        try:
            variant = "hourly"
            params = {
                "symbol": "BTC",
                "eventStartTime": market_start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "variant": variant,
                "endDate": end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
            async with session.get(self.config.polymarket_crypto_price_api, params=params) as response:
                if response.status == 200:
                    payload = await response.json()
                    open_price = payload.get("openPrice")
                    if open_price is not None:
                        strike = float(open_price)
                        self._strike_cache[slug] = (strike, now_ts, False)
                        return strike
        except Exception:
            pass

        try:
            strike = await _fetch_binance_hour_open()
            if strike > 0:
                self._strike_cache[slug] = (strike, now_ts, market_started)
                return strike
        except Exception:
            pass

        return 0.0

    async def fetch_market_odds(self, session: aiohttp.ClientSession, slug: str) -> MarketOddsSnapshot:
        if not _is_hourly_up_or_down_slug(slug):
            return MarketOddsSnapshot(slug=slug, market_found=False)

        meta = await self.fetch_market_meta_from_slug(session, slug)
        if meta is None:
            return MarketOddsSnapshot(slug=slug, market_found=False)

        market = meta["market"]
        event = meta["event"]
        title = str(market.get("question") or event.get("title") or "")
        directional_data = _extract_directional_market_data(market)
        if directional_data is None:
            return MarketOddsSnapshot(slug=slug, market_found=False)
        prices_by_direction, token_ids_by_direction = directional_data
        outcome_labels = tuple(
            str(label).strip()
            for label in _decode_json_array(market.get("outcomes"))
            if str(label).strip()
        )

        end_date_str = str(market.get("endDate") or event.get("endDate") or "")
        end_dt = _parse_end_datetime(end_date_str)
        reference_price = await self.fetch_price_to_beat_for_market(session, slug, meta=meta)
        seconds_remaining = _parse_seconds_remaining(end_date_str)
        market_category, fees_enabled, fee_curve_rate, fee_curve_exponent = _infer_fee_curve(slug, market, event)
        market_resolution = _infer_market_resolution(slug, title)

        up_prob = prices_by_direction[Direction.UP]
        down_prob = prices_by_direction[Direction.DOWN]
        up_token_id = token_ids_by_direction[Direction.UP]
        down_token_id = token_ids_by_direction[Direction.DOWN]

        return MarketOddsSnapshot(
            slug=slug,
            market_found=True,
            market_slug=str(market.get("slug") or slug),
            seconds_remaining=seconds_remaining,
            reference_price=reference_price,
            strike_price=reference_price,
            market_resolution=market_resolution,
            market_end_time=end_dt,
            market_category=market_category,
            fees_enabled=fees_enabled,
            fee_curve_rate=fee_curve_rate,
            fee_curve_exponent=fee_curve_exponent,
            up_token_id=up_token_id,
            down_token_id=down_token_id,
            up_public_prob_pct=up_prob,
            down_public_prob_pct=down_prob,
            up_entry_prob_pct=up_prob,
            down_entry_prob_pct=down_prob,
            outcome_labels=outcome_labels,
            fetched_at=datetime.now(timezone.utc),
        )


__all__ = [
    "BinanceStreamManager",
    "DataStreamsConfig",
    "PolymarketFetcher",
    "create_http_session",
]

# Backward-compatible re-exports from connector sub-modules.
# New code should import directly from bot.connectors.binance_connector
# and bot.connectors.gamma_connector respectively.
from .connectors.binance_connector import BinanceStreamManager as BinanceStreamManager  # noqa: F811, E402
from .connectors.gamma_connector import PolymarketFetcher as PolymarketFetcher  # noqa: F811, E402
