from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Any

from .indicators import StreamingEMA, StreamingRSI
from .models import (
    AIInteraction,
    AISlugState,
    ActivePosition,
    AdaptiveThresholdSnapshot,
    CVDSnapshot,
    Direction,
    DrawdownGuardSnapshot,
    EdgeSnapshot,
    ExecutionFailureState,
    ExecutionTimingSnapshot,
    MarketTick,
    ReentryState,
    RuntimeCounters,
    SignalAlignmentSnapshot,
    TechnicalContext,
)


@dataclass(slots=True)
class EngineState:
    history_maxlen: int = 120
    ml_queue_max: int = 5000
    db_queue_max: int = 10000

    market_lock: asyncio.Lock | None = field(default=None, init=False, repr=False)
    positions_lock: asyncio.Lock | None = field(default=None, init=False, repr=False)
    risk_lock: asyncio.Lock | None = field(default=None, init=False, repr=False)
    ai_lock: asyncio.Lock | None = field(default=None, init=False, repr=False)
    telemetry_lock: asyncio.Lock | None = field(default=None, init=False, repr=False)
    control_lock: asyncio.Lock | None = field(default=None, init=False, repr=False)

    candle_history: deque[MarketTick] = field(default_factory=deque, init=False, repr=False)
    cvd_1min_buffer: deque[tuple[float, float]] = field(default_factory=deque, init=False, repr=False)
    background_tasks: set[asyncio.Task[Any]] = field(default_factory=set, init=False, repr=False)
    ml_queue: asyncio.Queue[dict[str, Any]] | None = field(default=None, init=False, repr=False)
    db_queue: asyncio.Queue[dict[str, Any]] | None = field(default=None, init=False, repr=False)
    ema_fast_9: StreamingEMA | None = field(default=None, init=False, repr=False)
    ema_slow_21: StreamingEMA | None = field(default=None, init=False, repr=False)
    rsi_14: StreamingRSI | None = field(default=None, init=False, repr=False)
    indicator_last_close_ms: int = 0

    target_slug: str = ""
    market_family_prefix: str = ""

    live_price: float = 0.0
    live_candle: MarketTick | None = None
    last_closed_kline_ms: int = 0
    last_agg_trade_ms: int = 0

    cvd_total: float = 0.0
    cvd_snapshot_at_candle_open: float = 0.0
    last_cvd_1min: float = 0.0
    vwap_cum_pv: float = 0.0
    vwap_cum_vol: float = 0.0
    vwap_date: str = ""

    active_positions: dict[str, ActivePosition] = field(default_factory=dict)
    committed_slugs: set[str] = field(default_factory=set)
    soft_skipped_slugs: set[str] = field(default_factory=set)
    best_ev_seen: dict[str, float] = field(default_factory=dict)
    reentry_state: dict[str, ReentryState] = field(default_factory=dict)
    ai_state: dict[str, AISlugState] = field(default_factory=dict)
    execution_failures: dict[str, ExecutionFailureState] = field(default_factory=dict)

    latest_edge_snapshot: EdgeSnapshot = field(default_factory=EdgeSnapshot)
    latest_signal_alignment: SignalAlignmentSnapshot = field(default_factory=SignalAlignmentSnapshot)
    latest_cvd_snapshot: CVDSnapshot = field(default_factory=CVDSnapshot)
    latest_execution_timing: ExecutionTimingSnapshot = field(default_factory=ExecutionTimingSnapshot)
    adaptive_thresholds: AdaptiveThresholdSnapshot = field(default_factory=AdaptiveThresholdSnapshot)
    last_ai_interaction: AIInteraction = field(default_factory=AIInteraction)
    latest_context: TechnicalContext | None = None

    total_wins: int = 0
    total_losses: int = 0
    simulated_balance: float = 5000.0
    current_daily_pnl: float = 0.0
    max_trade_pct: float = 0.05
    max_daily_loss_pct: float = 0.15
    trades_this_hour: int = 0
    current_hour: int = field(default_factory=lambda: datetime.now(timezone.utc).hour)
    current_day: str = field(default_factory=lambda: datetime.now(timezone.utc).date().isoformat())

    ai_call_count: int = 0
    ai_consecutive_failures: int = 0
    ai_circuit_open_until: float = 0.0
    ai_call_in_flight: set[str] = field(default_factory=set)
    last_ai_response_ms: float = 0.0
    ai_response_ema_ms: float = 0.0

    kill_switch_enabled: bool = False
    kill_switch_last_log_ts: float = 0.0

    def __post_init__(self) -> None:
        self.candle_history = deque(maxlen=self.history_maxlen)
        self.cvd_1min_buffer = deque(maxlen=120)

    async def initialize(self) -> None:
        if self.market_lock is None:
            self.market_lock = asyncio.Lock()
        if self.positions_lock is None:
            self.positions_lock = asyncio.Lock()
        if self.risk_lock is None:
            self.risk_lock = asyncio.Lock()
        if self.ai_lock is None:
            self.ai_lock = asyncio.Lock()
        if self.telemetry_lock is None:
            self.telemetry_lock = asyncio.Lock()
        if self.control_lock is None:
            self.control_lock = asyncio.Lock()
        if self.ml_queue is None:
            self.ml_queue = asyncio.Queue(maxsize=self.ml_queue_max)
        if self.db_queue is None:
            self.db_queue = asyncio.Queue(maxsize=self.db_queue_max)
        if self.ema_fast_9 is None:
            self.ema_fast_9 = StreamingEMA(9)
        if self.ema_slow_21 is None:
            self.ema_slow_21 = StreamingEMA(21)
        if self.rsi_14 is None:
            self.rsi_14 = StreamingRSI(14)

    def spawn(self, coro) -> asyncio.Task[Any]:
        task = asyncio.create_task(coro)
        self.background_tasks.add(task)
        task.add_done_callback(self.background_tasks.discard)
        return task

    async def set_target_market(self, slug: str, prefix: str | None = None) -> None:
        async with self.control_lock:
            self.target_slug = slug
            if prefix is not None:
                self.market_family_prefix = prefix

    async def reset_intraday_flows(self, session_date: str | None = None) -> None:
        async with self.market_lock:
            self.cvd_total = 0.0
            self.cvd_snapshot_at_candle_open = 0.0
            self.last_cvd_1min = 0.0
            self.cvd_1min_buffer.clear()
            self.vwap_cum_pv = 0.0
            self.vwap_cum_vol = 0.0
            self.vwap_date = session_date or datetime.now(timezone.utc).date().isoformat()

    async def append_candle(self, candle: MarketTick) -> None:
        async with self.market_lock:
            self.candle_history.append(candle)
            self.live_candle = candle
            if candle.close > 0:
                self.live_price = candle.close
            if candle.close_time_ms > 0:
                self.last_closed_kline_ms = max(self.last_closed_kline_ms, candle.close_time_ms)
            elif candle.event_time_ms > 0:
                self.last_closed_kline_ms = max(self.last_closed_kline_ms, candle.event_time_ms)

    async def set_live_tick(
        self,
        *,
        price: float,
        candle: MarketTick | None = None,
        agg_trade_ms: int | None = None,
    ) -> None:
        async with self.market_lock:
            if price > 0:
                self.live_price = price
            if candle is not None:
                self.live_candle = candle
            if agg_trade_ms is not None and agg_trade_ms > 0:
                self.last_agg_trade_ms = max(self.last_agg_trade_ms, agg_trade_ms)

    async def get_market_snapshot(self) -> dict[str, Any]:
        async with self.market_lock:
            return {
                "target_slug": self.target_slug,
                "market_family_prefix": self.market_family_prefix,
                "live_price": self.live_price,
                "live_candle": self.live_candle.to_dict() if self.live_candle else None,
                "last_closed_kline_ms": self.last_closed_kline_ms,
                "last_agg_trade_ms": self.last_agg_trade_ms,
                "cvd_total": self.cvd_total,
                "cvd_snapshot_at_candle_open": self.cvd_snapshot_at_candle_open,
                "last_cvd_1min": self.last_cvd_1min,
                "vwap_cum_pv": self.vwap_cum_pv,
                "vwap_cum_vol": self.vwap_cum_vol,
                "vwap_date": self.vwap_date,
                "candle_history": [candle.to_dict() for candle in list(self.candle_history)],
                "latest_context": self.latest_context.to_dict() if self.latest_context is not None else None,
            }

    async def update_intraday_metrics(
        self,
        *,
        cvd_total: float | None = None,
        cvd_snapshot_at_open: float | None = None,
        last_cvd_1min: float | None = None,
        cvd_1min_value: tuple[float, float] | None = None,
        vwap_cum_pv: float | None = None,
        vwap_cum_vol: float | None = None,
        vwap_date: str | None = None,
    ) -> None:
        async with self.market_lock:
            if cvd_total is not None:
                self.cvd_total = cvd_total
            if cvd_snapshot_at_open is not None:
                self.cvd_snapshot_at_candle_open = cvd_snapshot_at_open
            if last_cvd_1min is not None:
                self.last_cvd_1min = last_cvd_1min
            if cvd_1min_value is not None:
                self.cvd_1min_buffer.append(cvd_1min_value)
            if vwap_cum_pv is not None:
                self.vwap_cum_pv = vwap_cum_pv
            if vwap_cum_vol is not None:
                self.vwap_cum_vol = vwap_cum_vol
            if vwap_date is not None:
                self.vwap_date = vwap_date

    async def upsert_position(self, position: ActivePosition) -> None:
        async with self.positions_lock:
            self.active_positions[position.slug] = position.clone()

    async def update_position(self, slug: str, **changes: Any) -> ActivePosition | None:
        async with self.positions_lock:
            existing = self.active_positions.get(slug)
            if existing is None:
                return None
            updated = replace(existing, **changes)
            self.active_positions[slug] = updated
            return updated.clone()

    async def get_position(self, slug: str) -> ActivePosition | None:
        async with self.positions_lock:
            position = self.active_positions.get(slug)
            return position.clone() if position is not None else None

    async def pop_position(self, slug: str) -> ActivePosition | None:
        async with self.positions_lock:
            position = self.active_positions.pop(slug, None)
            return position.clone() if position is not None else None

    async def list_positions(self, *, statuses: set[str] | None = None) -> list[ActivePosition]:
        async with self.positions_lock:
            positions = list(self.active_positions.values())
        if statuses is None:
            return [position.clone() for position in positions]
        return [position.clone() for position in positions if position.status.value in statuses]

    async def mark_slug_committed(self, slug: str) -> None:
        async with self.positions_lock:
            self.committed_slugs.add(slug)
            self.soft_skipped_slugs.discard(slug)
            self.best_ev_seen.pop(slug, None)

    async def clear_slug_commit(self, slug: str) -> None:
        async with self.positions_lock:
            self.committed_slugs.discard(slug)

    async def note_soft_skip(self, slug: str, *, best_ev: float | None = None) -> None:
        async with self.positions_lock:
            self.soft_skipped_slugs.add(slug)
            if best_ev is not None:
                self.best_ev_seen[slug] = max(best_ev, self.best_ev_seen.get(slug, 0.0))

    async def clear_soft_skip(self, slug: str) -> None:
        async with self.positions_lock:
            self.soft_skipped_slugs.discard(slug)

    async def record_reentry_state(
        self,
        slug: str,
        *,
        exit_reason: str,
        direction: Direction,
        entry_ev_pct: float,
    ) -> ReentryState:
        async with self.positions_lock:
            current = self.reentry_state.get(slug, ReentryState())
            updated = replace(
                current,
                closed_trades=current.closed_trades + 1,
                last_exit_ts=time.time(),
                last_exit_reason=exit_reason,
                last_exit_direction=direction,
                last_entry_ev_pct=entry_ev_pct,
            )
            self.reentry_state[slug] = updated
            return updated.clone()

    async def clear_reentry_state(self, slug: str) -> None:
        async with self.positions_lock:
            self.reentry_state.pop(slug, None)

    async def record_ai_state(
        self,
        slug: str,
        *,
        ai_calls: int | None = None,
        last_veto_ts: float | None = None,
        last_veto_ev_pct: float | None = None,
    ) -> AISlugState:
        async with self.ai_lock:
            current = self.ai_state.get(slug, AISlugState())
            updated = replace(
                current,
                ai_calls=current.ai_calls if ai_calls is None else ai_calls,
                last_veto_ts=current.last_veto_ts if last_veto_ts is None else last_veto_ts,
                last_veto_ev_pct=current.last_veto_ev_pct if last_veto_ev_pct is None else last_veto_ev_pct,
            )
            self.ai_state[slug] = updated
            return updated.clone()

    async def clear_ai_state(self, slug: str) -> None:
        async with self.ai_lock:
            self.ai_state.pop(slug, None)

    async def record_execution_failure(self, slug: str, *, reason: str, ev_pct: float = 0.0) -> ExecutionFailureState:
        async with self.positions_lock:
            failure = ExecutionFailureState(last_fail_ts=time.time(), reason=reason, ev_pct=ev_pct)
            self.execution_failures[slug] = failure
            return failure.clone()

    async def clear_execution_failure(self, slug: str) -> None:
        async with self.positions_lock:
            self.execution_failures.pop(slug, None)

    async def update_telemetry(
        self,
        *,
        edge_snapshot: EdgeSnapshot | None = None,
        signal_alignment: SignalAlignmentSnapshot | None = None,
        cvd_snapshot: CVDSnapshot | None = None,
        execution_timing: ExecutionTimingSnapshot | None = None,
        adaptive_thresholds: AdaptiveThresholdSnapshot | None = None,
        ai_interaction: AIInteraction | None = None,
        context: TechnicalContext | None = None,
    ) -> None:
        async with self.telemetry_lock:
            if edge_snapshot is not None:
                self.latest_edge_snapshot = edge_snapshot.clone()
            if signal_alignment is not None:
                self.latest_signal_alignment = signal_alignment.clone()
            if cvd_snapshot is not None:
                self.latest_cvd_snapshot = cvd_snapshot.clone()
            if execution_timing is not None:
                self.latest_execution_timing = execution_timing.clone()
            if adaptive_thresholds is not None:
                self.adaptive_thresholds = adaptive_thresholds.clone()
            if ai_interaction is not None:
                self.last_ai_interaction = ai_interaction.clone()
            if context is not None:
                self.latest_context = context.clone()

    async def get_telemetry_snapshot(self) -> dict[str, Any]:
        async with self.telemetry_lock:
            return {
                "edge_snapshot": self.latest_edge_snapshot.to_dict(),
                "signal_alignment": self.latest_signal_alignment.to_dict(),
                "cvd_snapshot": self.latest_cvd_snapshot.to_dict(),
                "execution_timing": self.latest_execution_timing.to_dict(),
                "adaptive_thresholds": self.adaptive_thresholds.to_dict(),
                "last_ai_interaction": self.last_ai_interaction.to_dict(),
                "context": self.latest_context.to_dict() if self.latest_context is not None else None,
            }

    async def update_runtime_counters(
        self,
        *,
        simulated_balance: float | None = None,
        current_daily_pnl: float | None = None,
        total_wins: int | None = None,
        total_losses: int | None = None,
        trades_this_hour: int | None = None,
        current_hour: int | None = None,
        current_day: str | None = None,
        max_trade_pct: float | None = None,
        max_daily_loss_pct: float | None = None,
        ai_call_count: int | None = None,
        ai_consecutive_failures: int | None = None,
        ai_circuit_open_until: float | None = None,
        ai_call_in_flight: set[str] | tuple[str, ...] | list[str] | str | None = None,
        last_ai_response_ms: float | None = None,
        ai_response_ema_ms: float | None = None,
        kill_switch_enabled: bool | None = None,
        kill_switch_last_log_ts: float | None = None,
    ) -> None:
        async with self.risk_lock:
            if simulated_balance is not None:
                self.simulated_balance = simulated_balance
            if current_daily_pnl is not None:
                self.current_daily_pnl = current_daily_pnl
            if total_wins is not None:
                self.total_wins = total_wins
            if total_losses is not None:
                self.total_losses = total_losses
            if trades_this_hour is not None:
                self.trades_this_hour = trades_this_hour
            if current_hour is not None:
                self.current_hour = current_hour
            if current_day is not None:
                self.current_day = current_day
            if max_trade_pct is not None:
                self.max_trade_pct = max_trade_pct
            if max_daily_loss_pct is not None:
                self.max_daily_loss_pct = max_daily_loss_pct
            if ai_call_count is not None:
                self.ai_call_count = ai_call_count
            if ai_consecutive_failures is not None:
                self.ai_consecutive_failures = ai_consecutive_failures
            if ai_circuit_open_until is not None:
                self.ai_circuit_open_until = ai_circuit_open_until
            if ai_call_in_flight is not None:
                if isinstance(ai_call_in_flight, str):
                    self.ai_call_in_flight = {ai_call_in_flight} if ai_call_in_flight else set()
                else:
                    self.ai_call_in_flight = {str(item) for item in ai_call_in_flight if str(item)}
            if last_ai_response_ms is not None:
                self.last_ai_response_ms = last_ai_response_ms
            if ai_response_ema_ms is not None:
                self.ai_response_ema_ms = ai_response_ema_ms
            if kill_switch_enabled is not None:
                self.kill_switch_enabled = kill_switch_enabled
            if kill_switch_last_log_ts is not None:
                self.kill_switch_last_log_ts = kill_switch_last_log_ts

    async def build_runtime_counters(self) -> RuntimeCounters:
        async with self.risk_lock:
            return RuntimeCounters(
                total_wins=self.total_wins,
                total_losses=self.total_losses,
                ai_call_count=self.ai_call_count,
                ai_consecutive_failures=self.ai_consecutive_failures,
                ai_circuit_open_until=self.ai_circuit_open_until,
                ai_call_in_flight=tuple(sorted(self.ai_call_in_flight)),
                last_ai_response_ms=self.last_ai_response_ms,
                ai_response_ema_ms=self.ai_response_ema_ms,
                kill_switch_enabled=self.kill_switch_enabled,
                simulated_balance=self.simulated_balance,
            )

    async def reserve_ai_call(self, slug: str, *, max_calls: int) -> tuple[bool, int]:
        async with self.ai_lock:
            current = self.ai_state.get(slug, AISlugState())
            if current.ai_calls >= max_calls:
                return False, current.ai_calls
            updated = replace(current, ai_calls=current.ai_calls + 1)
            self.ai_state[slug] = updated
            reserved_calls = updated.ai_calls

        async with self.risk_lock:
            self.ai_call_count += 1
            self.ai_call_in_flight.add(slug)

        return True, reserved_calls

    async def release_ai_call(self, slug: str) -> None:
        async with self.risk_lock:
            self.ai_call_in_flight.discard(slug)

    async def increment_ai_failures(self) -> int:
        async with self.risk_lock:
            self.ai_consecutive_failures += 1
            return self.ai_consecutive_failures

    async def set_ai_circuit_open_until(self, opened_until: float) -> float:
        async with self.risk_lock:
            self.ai_circuit_open_until = max(self.ai_circuit_open_until, opened_until)
            return self.ai_circuit_open_until

    async def register_ai_success(self, *, response_ms: float) -> float:
        async with self.risk_lock:
            updated_ema = (
                response_ms
                if self.ai_response_ema_ms <= 0
                else (0.7 * self.ai_response_ema_ms + 0.3 * response_ms)
            )
            self.ai_consecutive_failures = 0
            self.ai_circuit_open_until = 0.0
            self.last_ai_response_ms = response_ms
            self.ai_response_ema_ms = updated_ema
            return updated_ema

    async def build_drawdown_guard(self, current_balance: float | None = None) -> DrawdownGuardSnapshot:
        async with self.risk_lock:
            return DrawdownGuardSnapshot(
                bankroll=self.simulated_balance,
                current_balance=self.simulated_balance if current_balance is None else current_balance,
                daily_pnl=self.current_daily_pnl,
                max_trade_pct=self.max_trade_pct,
                max_daily_loss_pct=self.max_daily_loss_pct,
                trades_this_hour=self.trades_this_hour,
            )

    async def snapshot(self) -> dict[str, Any]:
        market = await self.get_market_snapshot()
        positions = await self.list_positions()
        telemetry = await self.get_telemetry_snapshot()
        runtime = await self.build_runtime_counters()
        drawdown = await self.build_drawdown_guard()
        async with self.positions_lock:
            positions_meta = {
                "committed_slugs": sorted(self.committed_slugs),
                "soft_skipped_slugs": sorted(self.soft_skipped_slugs),
                "best_ev_seen": dict(self.best_ev_seen),
                "reentry_state": {slug: state.to_dict() for slug, state in self.reentry_state.items()},
                "ai_state": {slug: state.to_dict() for slug, state in self.ai_state.items()},
                "execution_failures": {slug: state.to_dict() for slug, state in self.execution_failures.items()},
            }

        return {
            "market": market,
            "positions": [position.to_dict() for position in positions],
            "positions_meta": positions_meta,
            "telemetry": telemetry,
            "runtime": runtime.to_dict(),
            "drawdown_guard": drawdown.to_dict(),
        }
