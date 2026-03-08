"""Polymarket/Gamma API connector — market metadata, odds, and strike resolution.

Extracted from ``data_streams.py`` as a standalone connector module.  All Polymarket
Gamma API fetching, fee curve inference, and odds snapshot construction lives here.
"""

from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import aiohttp

from ..models import Direction, MarketOddsSnapshot, MarketResolution
from ..data_streams import DataStreamsConfig

log = logging.getLogger("alpha_z_engine.gamma_connector")


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

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
    if re.search(r"\$([\\d,]+(?:\\.\\d+)?)", title):
        return MarketResolution.STRIKE
    if _is_hourly_up_or_down_slug(slug):
        return MarketResolution.CANDLE_OPEN
    return MarketResolution.UNKNOWN


def _decode_json_array(raw: Any) -> list[Any]:
    import json as _json
    if isinstance(raw, str):
        try:
            decoded = _json.loads(raw)
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


# ---------------------------------------------------------------------------
# PolymarketFetcher
# ---------------------------------------------------------------------------

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
        match = re.search(r"\$([\\d,]+(?:\\.\\d+)?)", title)
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


__all__ = ["PolymarketFetcher"]
