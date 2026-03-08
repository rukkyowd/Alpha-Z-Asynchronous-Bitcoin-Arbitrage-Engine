from __future__ import annotations

import math
from dataclasses import dataclass, replace
from datetime import datetime, timezone

from .indicators import blended_intraday_volatility
from .models import ActivePosition, Direction, PositionStatus, TechnicalContext
from .state import EngineState

EPSILON = 1e-9


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(value, upper))


def _safe_div(numerator: float, denominator: float, default: float = 0.0) -> float:
    if abs(denominator) <= EPSILON:
        return default
    return numerator / denominator


@dataclass(slots=True, frozen=True)
class RiskConfig:
    max_daily_loss_pct: float = 0.15
    max_trade_pct: float = 0.05
    max_trades_per_hour: int = 3
    min_seconds_remaining: float = 30.0
    fractional_kelly_dampener: float = 0.50
    min_bet_usd: float = 1.00
    max_absolute_bet_usd: float = 50.00
    default_depth_usd: float = 25.0
    market_impact_constant: float = 0.05
    time_decay_tau_seconds: float = 420.0
    time_decay_floor: float = 0.30
    min_token_price: float = 0.01
    max_token_price: float = 0.99


@dataclass(slots=True, frozen=True)
class LiquidityProfile:
    available_depth_usd: float
    estimated_spread_pct: float = 0.0
    best_bid: float | None = None
    best_ask: float | None = None
    levels: int = 0


@dataclass(slots=True, frozen=True)
class EVComputation:
    ev_pct: float
    ev_pct_gross: float
    kelly_bet_usd: float
    raw_kelly_fraction: float
    adjusted_kelly_fraction: float
    token_price: float
    adjusted_token_price: float
    slippage_cost_pct: float
    market_impact_pct: float
    time_decay_multiplier: float
    edge_pct: float
    true_prob_pct: float
    market_prob_pct: float
    approved: bool
    available_depth_usd: float
    fee_cost_pct: float = 0.0


@dataclass(slots=True, frozen=True)
class DrawdownStatus:
    allowed: bool
    reason: str
    daily_pnl: float
    daily_loss_limit: float
    trades_this_hour: int
    max_trade_size_usd: float


@dataclass(slots=True, frozen=True)
class PositionRiskConfig:
    base_tp_token_delta: float = 0.08
    base_sl_token_delta: float = -0.10
    min_tp_token_delta: float = 0.08
    max_sl_token_delta: float = -0.08
    cheap_token_threshold_price: float = 0.30
    cheap_token_soft_sl_max_loss_pct: float = 0.30
    volatility_reference: float = 0.0040
    volatility_floor: float = 0.60
    volatility_ceiling: float = 1.80
    early_phase_seconds: int = 1800
    mid_phase_seconds: int = 600
    near_expiry_seconds: int = 300
    death_zone_seconds: int = 180
    tp_early_exit_window_seconds: int = 900
    sl_loosen_early_multiplier: float = 1.50
    sl_loosen_mid_multiplier: float = 1.20
    sl_widen_late_multiplier: float = 1.50
    tp_late_compress_multiplier: float = 0.70
    settling_window_seconds: int = 180
    settling_sl_breath_multiplier: float = 1.20
    sl_confirm_breach_early: int = 12
    sl_confirm_breach_mid: int = 8
    sl_confirm_breach_late: int = 6
    sl_recovery_reset_buffer: float = 0.01
    hard_sl_extra_cents: float = 0.03
    hard_sl_extra_frac: float = 0.35
    underlying_soft_sl_min_confirms: int = 2
    underlying_vwap_buffer_frac: float = 0.0005
    sl_entry_rel_max_loss_pct: float = 0.55
    sl_entry_rel_min_cents: float = 0.03
    force_tp_roi_pct: float = 0.70
    force_tp_delta_abs: float = 0.35
    tp_retrace_exit_frac: float = 0.45
    tp_retrace_exit_min_delta: float = 0.05
    tp_lock_min_profit_delta: float = 0.02


@dataclass(slots=True, frozen=True)
class PositionRiskSnapshot:
    tp_delta: float
    sl_delta: float
    hard_sl_delta: float
    tp_token_price: float
    sl_token_price: float
    hard_sl_token_price: float
    sl_disabled: bool
    sl_confirms_needed: int
    phase: str
    volatility_modifier: float


@dataclass(slots=True, frozen=True)
class PositionMonitorDecision:
    position: ActivePosition
    should_exit: bool = False
    exit_reason: str = ""
    exit_price: float = 0.0


class RiskManager:
    __slots__ = (
        "config",
        "position_config",
        "current_daily_pnl",
        "trades_this_hour",
        "current_hour",
        "current_day",
    )

    def __init__(self, config: RiskConfig | None = None, position_config: PositionRiskConfig | None = None):
        self.config = config or RiskConfig()
        self.position_config = position_config or PositionRiskConfig()
        now_utc = datetime.now(timezone.utc)
        self.current_daily_pnl = 0.0
        self.trades_this_hour = 0
        self.current_hour = now_utc.hour
        self.current_day = now_utc.date()

    def reset_stats(self, now: datetime | None = None) -> None:
        current = now or datetime.now(timezone.utc)
        self.current_daily_pnl = 0.0
        self.trades_this_hour = 0
        self.current_hour = current.hour
        self.current_day = current.date()

    def _roll_session(self, now: datetime) -> None:
        if now.date() != self.current_day:
            self.reset_stats(now)
        elif now.hour != self.current_hour:
            self.trades_this_hour = 0
            self.current_hour = now.hour

    def record_pnl(self, pnl_impact: float, now: datetime | None = None) -> None:
        current = now or datetime.now(timezone.utc)
        self._roll_session(current)
        self.current_daily_pnl += pnl_impact

    def record_trade(self, now: datetime | None = None) -> None:
        current = now or datetime.now(timezone.utc)
        self._roll_session(current)
        self.trades_this_hour += 1

    def drawdown_status(
        self,
        current_balance: float,
        *,
        trade_size: float = 0.0,
        now: datetime | None = None,
        current_daily_pnl: float | None = None,
        trades_this_hour: int | None = None,
    ) -> DrawdownStatus:
        current = now or datetime.now(timezone.utc)
        self._roll_session(current)
        day_pnl = self.current_daily_pnl if current_daily_pnl is None else current_daily_pnl
        hour_count = self.trades_this_hour if trades_this_hour is None else trades_this_hour
        daily_loss_limit = current_balance * self.config.max_daily_loss_pct
        max_trade_size = min(current_balance * self.config.max_trade_pct, self.config.max_absolute_bet_usd)

        if day_pnl <= -daily_loss_limit:
            return DrawdownStatus(
                allowed=False,
                reason=f"Daily loss limit (-${daily_loss_limit:.2f}) reached.",
                daily_pnl=day_pnl,
                daily_loss_limit=daily_loss_limit,
                trades_this_hour=hour_count,
                max_trade_size_usd=max_trade_size,
            )
        if trade_size > max_trade_size:
            return DrawdownStatus(
                allowed=False,
                reason=f"Trade size ${trade_size:.2f} exceeds max {self.config.max_trade_pct * 100:.1f}% risk.",
                daily_pnl=day_pnl,
                daily_loss_limit=daily_loss_limit,
                trades_this_hour=hour_count,
                max_trade_size_usd=max_trade_size,
            )
        if hour_count >= self.config.max_trades_per_hour:
            return DrawdownStatus(
                allowed=False,
                reason=f"Max trades per hour ({self.config.max_trades_per_hour}) reached.",
                daily_pnl=day_pnl,
                daily_loss_limit=daily_loss_limit,
                trades_this_hour=hour_count,
                max_trade_size_usd=max_trade_size,
            )
        return DrawdownStatus(
            allowed=True,
            reason="Approved",
            daily_pnl=day_pnl,
            daily_loss_limit=daily_loss_limit,
            trades_this_hour=hour_count,
            max_trade_size_usd=max_trade_size,
        )

    def can_trade(
        self,
        current_balance: float,
        trade_size: float,
        *,
        now: datetime | None = None,
        current_daily_pnl: float | None = None,
        trades_this_hour: int | None = None,
    ) -> tuple[bool, str]:
        if trade_size <= 0:
            return False, "Bet size resolved to $0.00 after sizing"
        if trade_size < self.config.min_bet_usd:
            return False, f"Bet size ${trade_size:.2f} below min ${self.config.min_bet_usd:.2f}"
        status = self.drawdown_status(
            current_balance,
            trade_size=trade_size,
            now=now,
            current_daily_pnl=current_daily_pnl,
            trades_this_hour=trades_this_hour,
        )
        return status.allowed, status.reason

    def continuous_time_decay(self, seconds_remaining: float) -> float:
        effective_seconds = max(0.0, seconds_remaining - self.config.min_seconds_remaining)
        if seconds_remaining <= self.config.min_seconds_remaining:
            return _clamp(self.config.time_decay_floor, 0.0, 1.0)
        floor = _clamp(self.config.time_decay_floor, 0.0, 0.95)
        decay = floor + ((1.0 - floor) * math.exp(-effective_seconds / self.config.time_decay_tau_seconds))
        return _clamp(decay, floor, 1.0)

    def square_root_market_impact_pct(self, bet_size_usd: float, available_depth_usd: float) -> float:
        if bet_size_usd <= 0:
            return 0.0
        depth = max(available_depth_usd, self.config.default_depth_usd, self.config.min_bet_usd)
        return _clamp(
            self.config.market_impact_constant * math.sqrt(max(bet_size_usd, 0.0) / depth),
            0.0,
            0.25,
        )

    def estimate_total_slippage_pct(
        self,
        bet_size_usd: float,
        liquidity: LiquidityProfile | None = None,
        *,
        token_price: float | None = None,
        taker_fee_rate: float = 0.0,
    ) -> tuple[float, float]:
        if liquidity is None:
            depth = self.config.default_depth_usd
            spread_abs = 0.0
        else:
            depth = max(liquidity.available_depth_usd, self.config.min_bet_usd)
            spread_abs = max(liquidity.estimated_spread_pct, 0.0)

        reference_price = max(token_price or 0.5, self.config.min_token_price)
        spread_pct = (spread_abs * 0.5) / reference_price
        market_impact_pct = self.square_root_market_impact_pct(bet_size_usd, depth)
        slippage_pct = spread_pct + market_impact_pct + max(taker_fee_rate, 0.0)
        return slippage_pct, market_impact_pct

    @staticmethod
    def _kelly_fraction(true_probability: float, entry_price: float) -> float:
        if entry_price <= 0.0 or entry_price >= 1.0:
            return 0.0
        net_win = 1.0 - entry_price
        if net_win <= 0.0:
            return 0.0
        odds_multiple = net_win / entry_price
        raw_fraction = (odds_multiple * true_probability - (1.0 - true_probability)) / max(odds_multiple, EPSILON)
        return _clamp(raw_fraction, 0.0, 1.0)

    def evaluate_trade(
        self,
        *,
        true_prob_pct: float,
        market_prob_pct: float,
        current_balance: float,
        seconds_remaining: float,
        liquidity: LiquidityProfile | None = None,
        taker_fee_rate: float = 0.0,
    ) -> EVComputation:
        token_price = market_prob_pct / 100.0
        true_probability = true_prob_pct / 100.0
        if not (self.config.min_token_price < token_price < self.config.max_token_price):
            return EVComputation(
                ev_pct=0.0,
                ev_pct_gross=0.0,
                kelly_bet_usd=0.0,
                raw_kelly_fraction=0.0,
                adjusted_kelly_fraction=0.0,
                token_price=round(token_price, 4),
                adjusted_token_price=round(token_price, 4),
                slippage_cost_pct=0.0,
                market_impact_pct=0.0,
                time_decay_multiplier=0.0,
                edge_pct=round(true_prob_pct - market_prob_pct, 2),
                true_prob_pct=round(true_prob_pct, 2),
                market_prob_pct=round(market_prob_pct, 2),
                approved=False,
                available_depth_usd=0.0 if liquidity is None else liquidity.available_depth_usd,
                fee_cost_pct=0.0,
            )

        gross_ev = true_probability * (1.0 - token_price) - ((1.0 - true_probability) * token_price)
        gross_ev_pct = _safe_div(gross_ev, token_price, default=0.0) * 100.0
        time_decay = self.continuous_time_decay(seconds_remaining)
        raw_kelly_fraction = self._kelly_fraction(true_probability, token_price)

        max_risk_cap = min(
            current_balance * self.config.max_trade_pct,
            self.config.max_absolute_bet_usd,
        )
        candidate_bet = raw_kelly_fraction * current_balance * self.config.fractional_kelly_dampener * time_decay
        candidate_bet = min(candidate_bet, max_risk_cap)

        adjusted_fraction = raw_kelly_fraction
        adjusted_token_price = token_price
        total_slippage_pct = 0.0
        market_impact_pct = 0.0

        for _ in range(2):
            total_slippage_pct, market_impact_pct = self.estimate_total_slippage_pct(
                candidate_bet,
                liquidity,
                token_price=token_price,
                taker_fee_rate=taker_fee_rate,
            )
            adjusted_token_price = _clamp(token_price * (1.0 + total_slippage_pct), 0.0001, 0.9999)
            adjusted_fraction = self._kelly_fraction(true_probability, adjusted_token_price)
            recalculated = adjusted_fraction * current_balance * self.config.fractional_kelly_dampener * time_decay
            recalculated = min(recalculated, max_risk_cap)
            if abs(recalculated - candidate_bet) < 0.01:
                candidate_bet = recalculated
                break
            candidate_bet = recalculated

        total_slippage_pct, market_impact_pct = self.estimate_total_slippage_pct(
            candidate_bet,
            liquidity,
            token_price=token_price,
            taker_fee_rate=taker_fee_rate,
        )
        adjusted_token_price = _clamp(token_price * (1.0 + total_slippage_pct), 0.0001, 0.9999)
        adjusted_fraction = self._kelly_fraction(true_probability, adjusted_token_price)
        net_ev = true_probability * (1.0 - adjusted_token_price) - ((1.0 - true_probability) * adjusted_token_price)
        net_ev_pct = _safe_div(net_ev, adjusted_token_price, default=0.0) * 100.0
        fee_cost_pct = max(taker_fee_rate, 0.0) * 100.0

        final_bet = round(min(candidate_bet, max_risk_cap), 2)
        if final_bet < self.config.min_bet_usd:
            final_bet = 0.0

        return EVComputation(
            ev_pct=round(net_ev_pct, 2),
            ev_pct_gross=round(gross_ev_pct, 2),
            kelly_bet_usd=final_bet,
            raw_kelly_fraction=round(raw_kelly_fraction, 6),
            adjusted_kelly_fraction=round(adjusted_fraction, 6),
            token_price=round(token_price, 4),
            adjusted_token_price=round(adjusted_token_price, 4),
            slippage_cost_pct=round(total_slippage_pct * 100.0, 2),
            market_impact_pct=round(market_impact_pct * 100.0, 2),
            time_decay_multiplier=round(time_decay, 6),
            edge_pct=round(true_prob_pct - market_prob_pct, 2),
            true_prob_pct=round(true_prob_pct, 2),
            market_prob_pct=round(market_prob_pct, 2),
            approved=net_ev_pct > 0.0 and time_decay > 0.0,
            available_depth_usd=round(
                self.config.default_depth_usd if liquidity is None else liquidity.available_depth_usd,
                2,
            ),
            fee_cost_pct=round(fee_cost_pct, 3),
        )

    def _volatility_for_position(self, context: TechnicalContext) -> float:
        direct = max(
            context.parkinson_volatility,
            context.garman_klass_volatility,
            context.realized_volatility,
        )
        if direct > 0:
            return direct
        synthetic_tick = 0.0
        if context.price > 0:
            synthetic_tick = max(abs(context.ema_spread_pct), abs(context.price_vs_vwap_pct), 0.001)
        return synthetic_tick

    def _underlying_soft_sl_confirmed(
        self,
        position: ActivePosition,
        context: TechnicalContext,
        *,
        current_underlying_price: float,
    ) -> bool:
        cfg = self.position_config
        underlying_price = current_underlying_price if current_underlying_price > 0 else context.price
        cvd_threshold = max(abs(context.adaptive_cvd_threshold), 1.0)
        confirmations = 0

        if position.decision == Direction.DOWN:
            if position.strike > 0 and underlying_price >= position.strike:
                return True
            if context.vwap > 0 and underlying_price >= (context.vwap * (1.0 + cfg.underlying_vwap_buffer_frac)):
                confirmations += 1
            if context.ema_9 >= context.ema_21:
                confirmations += 1
            if context.cvd_candle_delta >= cvd_threshold:
                confirmations += 1
            return confirmations >= cfg.underlying_soft_sl_min_confirms

        if position.decision == Direction.UP:
            if position.strike > 0 and underlying_price <= position.strike:
                return True
            if context.vwap > 0 and underlying_price <= (context.vwap * (1.0 - cfg.underlying_vwap_buffer_frac)):
                confirmations += 1
            if context.ema_9 <= context.ema_21:
                confirmations += 1
            if context.cvd_candle_delta <= -cvd_threshold:
                confirmations += 1
            return confirmations >= cfg.underlying_soft_sl_min_confirms

        return True

    def position_risk_snapshot(
        self,
        position: ActivePosition,
        context: TechnicalContext,
        *,
        seconds_remaining: float,
        now_ts: float | None = None,
    ) -> PositionRiskSnapshot:
        cfg = self.position_config
        volatility = self._volatility_for_position(context)
        volatility_modifier = _clamp(
            volatility / max(cfg.volatility_reference, EPSILON),
            cfg.volatility_floor,
            cfg.volatility_ceiling,
        )

        tp_delta = max(cfg.base_tp_token_delta * volatility_modifier, cfg.min_tp_token_delta)
        # `max_sl_token_delta` is the widest allowed soft stop in token-price terms.
        sl_delta = max(cfg.base_sl_token_delta * volatility_modifier, cfg.max_sl_token_delta)
        sl_disabled = False
        sl_confirms_needed = cfg.sl_confirm_breach_mid
        phase = "MID"

        if seconds_remaining > cfg.early_phase_seconds:
            sl_delta *= cfg.sl_loosen_early_multiplier
            sl_confirms_needed = cfg.sl_confirm_breach_early
            phase = "EARLY"
        elif seconds_remaining > cfg.mid_phase_seconds:
            sl_delta *= cfg.sl_loosen_mid_multiplier
            sl_confirms_needed = cfg.sl_confirm_breach_mid
            phase = "MID"
        elif seconds_remaining < cfg.death_zone_seconds:
            sl_disabled = True
            sl_confirms_needed = 999
            phase = "DEATH_ZONE"
        elif seconds_remaining < cfg.near_expiry_seconds:
            tp_delta *= cfg.tp_late_compress_multiplier
            sl_delta *= cfg.sl_widen_late_multiplier
            sl_confirms_needed = cfg.sl_confirm_breach_late
            phase = "LATE"

        current_ts = now_ts if now_ts is not None else datetime.now(timezone.utc).timestamp()
        entry_age = current_ts - position.entry_time.timestamp()
        if entry_age < cfg.settling_window_seconds:
            sl_delta *= cfg.settling_sl_breath_multiplier

        sl_delta = max(sl_delta, cfg.max_sl_token_delta)
        if position.bought_price <= cfg.cheap_token_threshold_price:
            cheap_token_sl = -max(
                cfg.sl_entry_rel_min_cents,
                position.bought_price * cfg.cheap_token_soft_sl_max_loss_pct,
            )
            sl_delta = max(sl_delta, cheap_token_sl)
        entry_based_sl = -max(cfg.sl_entry_rel_min_cents, position.bought_price * cfg.sl_entry_rel_max_loss_pct)
        reachable_floor = -(max(position.bought_price - 0.01, 0.0))
        if reachable_floor < 0:
            entry_based_sl = max(entry_based_sl, reachable_floor)
        sl_delta = max(sl_delta, entry_based_sl)
        hard_sl_extra = max(cfg.hard_sl_extra_cents, abs(sl_delta) * cfg.hard_sl_extra_frac)
        hard_sl_delta = max(sl_delta - hard_sl_extra, reachable_floor)

        return PositionRiskSnapshot(
            tp_delta=tp_delta,
            sl_delta=sl_delta,
            hard_sl_delta=hard_sl_delta,
            tp_token_price=position.bought_price + tp_delta,
            sl_token_price=max(0.0, position.bought_price + sl_delta),
            hard_sl_token_price=max(0.0, position.bought_price + hard_sl_delta),
            sl_disabled=sl_disabled,
            sl_confirms_needed=sl_confirms_needed,
            phase=phase,
            volatility_modifier=volatility_modifier,
        )

    def monitor_position(
        self,
        position: ActivePosition,
        context: TechnicalContext,
        *,
        current_token_price: float,
        current_underlying_price: float,
        seconds_remaining: float,
        now_ts: float | None = None,
    ) -> PositionMonitorDecision:
        cfg = self.position_config
        now_ts = now_ts if now_ts is not None else datetime.now(timezone.utc).timestamp()
        snapshot = self.position_risk_snapshot(position, context, seconds_remaining=seconds_remaining, now_ts=now_ts)
        price_delta = current_token_price - position.bought_price

        updated = replace(
            position,
            current_token_price=current_token_price,
            mark_price=current_token_price,
            live_underlying_price=current_underlying_price,
            tp_delta=snapshot.tp_delta,
            sl_delta=snapshot.sl_delta,
            tp_token_price=snapshot.tp_token_price,
            sl_token_price=snapshot.sl_token_price,
            sl_disabled=snapshot.sl_disabled,
            seconds_remaining=int(seconds_remaining),
        )

        if price_delta <= 0 and seconds_remaining > cfg.tp_early_exit_window_seconds and updated.tp_armed:
            peak_delta = float(updated.tp_peak_delta)
            return PositionMonitorDecision(
                position=replace(updated, status=PositionStatus.CLOSING),
                should_exit=True,
                exit_reason=(
                    f"TP_LOCK_BREAKEVEN_GUARD (peak +{peak_delta * 100:.1f}c -> "
                    f"now +{price_delta * 100:.1f}c)"
                ),
                exit_price=current_token_price,
            )

        if price_delta >= snapshot.tp_delta:
            tp_gate_hold = seconds_remaining > cfg.tp_early_exit_window_seconds
            roi_pct_now = _safe_div(current_token_price - position.bought_price, position.bought_price, default=0.0)
            force_tp = (roi_pct_now >= cfg.force_tp_roi_pct) or (price_delta >= cfg.force_tp_delta_abs)
            peak_delta = max(float(updated.tp_peak_delta), price_delta)
            retrace_budget = max(cfg.tp_retrace_exit_min_delta, peak_delta * cfg.tp_retrace_exit_frac)
            lock_floor = max(cfg.tp_lock_min_profit_delta, peak_delta - retrace_budget)
            updated = replace(
                updated,
                sl_breach_count=0,
                tp_armed=True,
                tp_peak_delta=peak_delta,
                tp_lock_floor_delta=max(float(updated.tp_lock_floor_delta), lock_floor),
                tp_gate_logged=tp_gate_hold and not force_tp,
            )
            if (not tp_gate_hold) or force_tp:
                return PositionMonitorDecision(
                    position=replace(updated, status=PositionStatus.CLOSING, tp_gate_logged=False),
                    should_exit=True,
                    exit_reason=f"TAKE_PROFIT (vol-adjusted: +{price_delta * 100:.1f}c)",
                    exit_price=current_token_price,
                )
            return PositionMonitorDecision(position=updated)

        if price_delta < snapshot.tp_delta and seconds_remaining > cfg.tp_early_exit_window_seconds and updated.tp_armed:
            peak_delta = float(updated.tp_peak_delta)
            if peak_delta > 0:
                retrace_budget = max(cfg.tp_retrace_exit_min_delta, peak_delta * cfg.tp_retrace_exit_frac)
                lock_floor = max(cfg.tp_lock_min_profit_delta, peak_delta - retrace_budget)
                lock_floor = max(lock_floor, float(updated.tp_lock_floor_delta))
                updated = replace(updated, tp_lock_floor_delta=lock_floor)
                if price_delta <= lock_floor:
                    return PositionMonitorDecision(
                        position=replace(updated, status=PositionStatus.CLOSING),
                        should_exit=True,
                        exit_reason=(
                            f"TP_LOCK_RETRACE (peak +{peak_delta * 100:.1f}c -> "
                            f"now +{price_delta * 100:.1f}c, floor +{lock_floor * 100:.1f}c)"
                        ),
                        exit_price=current_token_price,
                    )

        if not snapshot.sl_disabled and price_delta <= snapshot.hard_sl_delta:
            return PositionMonitorDecision(
                position=replace(updated, status=PositionStatus.CLOSING),
                should_exit=True,
                exit_reason=(
                    f"HARD_STOP_LOSS ({price_delta * 100:.1f}c <= "
                    f"{snapshot.hard_sl_delta * 100:.1f}c hard floor)"
                ),
                exit_price=current_token_price,
            )

        if not snapshot.sl_disabled and price_delta <= snapshot.sl_delta:
            underlying_confirmed = self._underlying_soft_sl_confirmed(
                position,
                context,
                current_underlying_price=current_underlying_price,
            )
            if not underlying_confirmed:
                if updated.sl_breach_count > 0:
                    updated = replace(updated, sl_breach_count=0)
                return PositionMonitorDecision(position=updated)

            breach_count = int(updated.sl_breach_count) + 1
            updated = replace(updated, sl_breach_count=breach_count)
            if underlying_confirmed and breach_count >= snapshot.sl_confirms_needed:
                return PositionMonitorDecision(
                    position=replace(updated, status=PositionStatus.CLOSING),
                    should_exit=True,
                    exit_reason=(
                        f"STOP_LOSS_CONFIRMED ({price_delta * 100:.1f}c, "
                        f"{breach_count}/{snapshot.sl_confirms_needed}, underlying confirmed)"
                    ),
                    exit_price=current_token_price,
                )
            return PositionMonitorDecision(position=updated)

        if price_delta > (snapshot.sl_delta + cfg.sl_recovery_reset_buffer) and updated.sl_breach_count > 0:
            updated = replace(updated, sl_breach_count=0)

        return PositionMonitorDecision(position=updated)

    async def apply_position_monitoring(
        self,
        state: EngineState,
        slug: str,
        position: ActivePosition,
        context: TechnicalContext,
        *,
        current_token_price: float,
        current_underlying_price: float,
        seconds_remaining: float,
        now_ts: float | None = None,
    ) -> PositionMonitorDecision:
        decision = self.monitor_position(
            position,
            context,
            current_token_price=current_token_price,
            current_underlying_price=current_underlying_price,
            seconds_remaining=seconds_remaining,
            now_ts=now_ts,
        )
        await state.upsert_position(decision.position)
        return decision

    async def sync_runtime_from_state(self, state: EngineState) -> None:
        async with state.risk_lock:
            self.current_daily_pnl = state.current_daily_pnl
            self.trades_this_hour = state.trades_this_hour
            self.current_hour = state.current_hour
            self.current_day = datetime.fromisoformat(f"{state.current_day}T00:00:00+00:00").date()


__all__ = [
    "DrawdownStatus",
    "EVComputation",
    "LiquidityProfile",
    "PositionMonitorDecision",
    "PositionRiskConfig",
    "PositionRiskSnapshot",
    "RiskConfig",
    "RiskManager",
]
