"""Binance data connector — WebSocket streams + REST backfill for 1h klines and aggTrades.

Extracted from ``data_streams.py`` as a standalone connector module.  All Binance-specific
parsing, state-application, and WebSocket management lives here.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

import aiohttp
import websockets

from ..indicators import update_vwap_accumulators
from ..models import DataSource, MarketTick
from ..state import EngineState

log = logging.getLogger("alpha_z_engine.binance_connector")


# ---------------------------------------------------------------------------
# Config reference (re-used from data_streams)
# ---------------------------------------------------------------------------

from ..data_streams import DataStreamsConfig, _backoff_delay  # noqa: E402


# ---------------------------------------------------------------------------
# Kline/trade parsing
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# State application helpers
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# BinanceStreamManager
# ---------------------------------------------------------------------------

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


__all__ = ["BinanceStreamManager"]
