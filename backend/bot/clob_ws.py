"""CLOB WebSocket manager and heartbeat loop for Polymarket real-time data.

Provides:
  - ``ClobWebSocketManager``: Maintains a persistent WS connection to
    ``wss://ws-subscriptions-clob.polymarket.com/ws/market`` for real-time
    L2 orderbook updates, eliminating REST polling latency.
  - ``run_heartbeat_loop``: Sends periodic heartbeats to the CLOB. If the
    engine crashes or the network drops, Polymarket auto-cancels ALL open
    orders after 10 s of missed heartbeats (kill switch).
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger("alpha_z_engine.clob_ws")

CLOB_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


@dataclass
class LiveOrderBook:
    """In-memory L2 book maintained from WebSocket deltas."""

    bids: dict[float, float] = field(default_factory=dict)
    asks: dict[float, float] = field(default_factory=dict)
    last_trade_price: float = 0.0
    last_update_ts: float = 0.0

    @property
    def best_bid(self) -> float | None:
        return max(self.bids.keys()) if self.bids else None

    @property
    def best_ask(self) -> float | None:
        return min(self.asks.keys()) if self.asks else None

    @property
    def spread(self) -> float:
        bb, ba = self.best_bid, self.best_ask
        if bb is not None and ba is not None:
            return ba - bb
        return float("inf")

    @property
    def depth_usd(self) -> float:
        return sum(p * s for p, s in self.asks.items()) + sum(p * s for p, s in self.bids.items())

    @property
    def stale(self) -> bool:
        """True if the book has not been updated in >30 s."""
        return (time.time() - self.last_update_ts) > 30.0 if self.last_update_ts > 0 else True


class ClobWebSocketManager:
    """Persistent WS connection to the Polymarket CLOB market channel."""

    def __init__(
        self,
        token_ids: list[str],
        *,
        backoff_initial: float = 1.0,
        backoff_max: float = 30.0,
        ping_interval: float = 20.0,
    ):
        self.token_ids = [token_id for token_id in dict.fromkeys(token_ids) if token_id]
        self.books: dict[str, LiveOrderBook] = defaultdict(LiveOrderBook)
        self._backoff_initial = backoff_initial
        self._backoff_max = backoff_max
        self._ping_interval = ping_interval
        self._running = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._ws: Any | None = None

    def get_book(self, token_id: str) -> LiveOrderBook:
        return self.books[token_id]

    def update_subscriptions(self, token_ids: list[str]) -> None:
        """Update monitored token IDs and reconnect immediately if needed."""
        normalized = [token_id for token_id in dict.fromkeys(token_ids) if token_id]
        if normalized == self.token_ids:
            return
        self.token_ids = normalized
        if self._loop is not None and self._ws is not None:
            self._loop.call_soon_threadsafe(
                lambda: self._loop.create_task(self._close_current_socket("subscription update"))
            )

    async def _close_current_socket(self, reason: str) -> None:
        ws = self._ws
        if ws is None:
            return
        log.info("[CLOB WS] Reconnecting to apply %s.", reason)
        try:
            await ws.close()
        except Exception as exc:
            log.debug("[CLOB WS] Close during %s failed: %s", reason, exc)

    def _apply_snapshot(self, token_id: str, data: dict[str, Any]) -> None:
        book = self.books[token_id]
        book.bids.clear()
        book.asks.clear()
        for bid in data.get("bids", []):
            price, size = float(bid["price"]), float(bid["size"])
            if size > 0:
                book.bids[price] = size
        for ask in data.get("asks", []):
            price, size = float(ask["price"]), float(ask["size"])
            if size > 0:
                book.asks[price] = size
        book.last_update_ts = time.time()

    def _apply_price_change(self, token_id: str, data: dict[str, Any]) -> None:
        book = self.books[token_id]
        side = str(data.get("side", "")).lower()
        price = float(data.get("price", 0))
        size = float(data.get("size", 0))
        target = book.bids if side == "buy" else book.asks
        if size <= 0:
            target.pop(price, None)
        else:
            target[price] = size
        book.last_update_ts = time.time()

    def _apply_last_trade(self, token_id: str, data: dict[str, Any]) -> None:
        self.books[token_id].last_trade_price = float(data.get("price", 0))

    @staticmethod
    def _iter_messages(payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, dict):
            return [payload]
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        return []

    async def run(self, stop_event: asyncio.Event | None = None) -> None:
        """Connect, subscribe, process messages, and reconnect on failure."""
        try:
            import websockets
        except ImportError:
            log.warning("[CLOB WS] websockets not installed - skipping real-time book feed.")
            return

        self._running = True
        self._loop = asyncio.get_running_loop()
        attempt = 0
        while self._running:
            if stop_event is not None and stop_event.is_set():
                break
            if not self.token_ids:
                await asyncio.sleep(0.5)
                continue
            try:
                async with websockets.connect(
                    CLOB_WS_URL,
                    ping_interval=self._ping_interval,
                    max_size=2**21,
                ) as ws:
                    self._ws = ws
                    attempt = 0
                    subscribe_msg = json.dumps(
                        {
                            "type": "subscribe",
                            "channel": "market",
                            "assets_ids": self.token_ids,
                        }
                    )
                    await ws.send(subscribe_msg)
                    log.info("[CLOB WS] Connected - subscribed to %d token(s)", len(self.token_ids))

                    async for raw in ws:
                        if stop_event is not None and stop_event.is_set():
                            break
                        try:
                            payload = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        for msg in self._iter_messages(payload):
                            event_type = str(msg.get("event_type", msg.get("type", ""))).lower()
                            token_id = str(msg.get("asset_id", msg.get("assetId", "")))
                            if not token_id:
                                continue
                            if event_type == "book":
                                self._apply_snapshot(token_id, msg)
                            elif event_type == "price_change":
                                self._apply_price_change(token_id, msg)
                            elif event_type == "last_trade_price":
                                self._apply_last_trade(token_id, msg)
                self._ws = None
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._ws = None
                delay = min(self._backoff_max, self._backoff_initial * (2 ** attempt))
                attempt += 1
                log.warning("[CLOB WS] Disconnected (%s). Reconnecting in %.1fs...", exc, delay)
                await asyncio.sleep(delay)
        self._ws = None
        self._loop = None

    def stop(self) -> None:
        self._running = False


async def run_heartbeat_loop(
    client: Any,
    *,
    heartbeat_id: str = "alpha_z_btc_hourly",
    interval_secs: float = 8.0,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Send periodic heartbeats to the CLOB server."""
    while True:
        if stop_event is not None and stop_event.is_set():
            break
        try:
            await asyncio.to_thread(client.post_heartbeat, heartbeat_id)
        except Exception as exc:
            log.error("[HEARTBEAT] Failed: %s", exc)
        await asyncio.sleep(interval_secs)
