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


def _scaled_score(value: float, soft: float, hard: float, *, invert: bool = False) -> float:
    if invert:
        value = -value
        soft = -soft
        hard = -hard
    width = max(abs(hard - soft), EPSILON)
    return _clamp((value - soft) / width, 0.0, 1.0)


@dataclass(slots=True, frozen=True)
class RiskConfig:
    max_daily_loss_pct: float = 0.15
    max_trade_pct: float = 0.02
    max_trades_per_hour: int = 2
    hourly_trade_limit_drawdown_step1: float = 0.25
    hourly_trade_limit_drawdown_step2: float = 0.50
    min_seconds_remaining: float = 30.0
    fractional_kelly_dampener: float = 0.50
    min_bet_usd: float = 1.00
    max_absolute_bet_usd: float = 0.00
    default_depth_usd: float = 25.0
    estimated_liquidity_depth_confidence: float = 0.15
    unknown_liquidity_depth_usd: float = 1.0
    market_impact_constant: float = 0.05
    time_decay_tau_seconds: float = 420.0
    time_decay_floor: float = 0.30
    min_token_price: float = 0.01
    max_token_price: float = 0.99
    kelly_volatility_soft: float = 0.004
    kelly_volatility_hard: float = 0.012
    kelly_volatility_min_multiplier: float = 0.25
    size_solver_max_iterations: int = 12
    size_solver_tolerance_usd: float = 0.01
    expected_exit_fee_multiplier: float = 0.50
    expected_exit_slippage_multiplier: float = 0.60
    latency_ev_haircut_pct: float = 0.25
    capital_lockup_free_hours: float = 0.35
    capital_lockup_penalty_max_pct: float = 6.0
    late_lottery_window_secs: float = 1800.0
    late_lottery_time_power: float = 1.0
    late_lottery_price_anchor: float = 0.25
    late_lottery_min_true_prob_pct: float = 35.0
    late_lottery_spread_ratio_soft: float = 0.08
    late_lottery_spread_ratio_hard: float = 0.20
    late_lottery_depth_consumption_soft: float = 0.15
    late_lottery_depth_consumption_hard: float = 0.45
    late_lottery_payout_multiple_soft: float = 4.0
    late_lottery_payout_multiple_hard: float = 8.0
    late_lottery_volatility_soft: float = 0.004
    late_lottery_volatility_hard: float = 0.008
    late_lottery_price_weight: float = 0.30
    late_lottery_true_prob_weight: float = 0.20
    late_lottery_payout_weight: float = 0.15
    late_lottery_spread_weight: float = 0.15
    late_lottery_depth_weight: float = 0.10
    late_lottery_volatility_weight: float = 0.10
    late_lottery_ev_penalty_max_pct: float = 45.0
    late_lottery_size_haircut_max: float = 0.75
    late_lottery_elite_boost_cutoff_score: float = 0.12
    late_lottery_size_cap_token_price: float = 0.20
    late_lottery_size_cap_risk_score: float = 0.12
    late_lottery_max_trade_pct_cap: float = 0.02
    elite_ev_soft_pct: float = 12.0
    elite_ev_hard_pct: float = 35.0
    elite_edge_soft_pct: float = 6.0
    elite_edge_hard_pct: float = 20.0
    elite_true_prob_soft_pct: float = 58.0
    elite_true_prob_hard_pct: float = 72.0
    elite_friction_soft_pct: float = 4.0
    elite_friction_hard_pct: float = 12.0
    elite_depth_consumption_soft_pct: float = 8.0
    elite_depth_consumption_hard_pct: float = 30.0
    elite_size_boost_max: float = 0.75
    elite_max_trade_pct: float = 0.06


@dataclass(slots=True, frozen=True)
class LiquidityProfile:
    available_depth_usd: float
    estimated_spread_pct: float = 0.0
    best_bid: float | None = None
    best_ask: float | None = None
    levels: int = 0
    depth_confidence: float = 1.0


@dataclass(slots=True, frozen=True)
class LateLotteryProfile:
    risk_score: float = 0.0
    ev_penalty_pct: float = 0.0
    size_multiplier: float = 1.0
    spread_ratio_pct_of_price: float = 0.0
    depth_consumption_pct: float = 0.0
    payout_multiple: float = 0.0
    time_weight: float = 0.0


@dataclass(slots=True, frozen=True)
class EliteSetupProfile:
    quality_score: float = 0.0
    size_multiplier: float = 1.0
    effective_max_trade_pct: float = 0.0
    capital_lockup_penalty_pct: float = 0.0
    depth_consumption_pct: float = 0.0
    total_friction_pct: float = 0.0


@dataclass(slots=True, frozen=True)
class EVComputation:
    ev_pct: float
    ev_pct_gross: float
    kelly_bet_usd: float
    raw_kelly_fraction: float
    adjusted_kelly_fraction: float
    kelly_confidence_multiplier: float
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
    exit_fee_cost_pct: float = 0.0
    expected_exit_slippage_pct: float = 0.0
    latency_haircut_pct: float = 0.0
    capital_lockup_penalty_pct: float = 0.0
    elite_quality_score: float = 0.0
    elite_size_multiplier: float = 1.0
    effective_max_trade_pct: float = 0.0
    late_lottery_risk_score: float = 0.0
    late_lottery_ev_penalty_pct: float = 0.0
    late_lottery_size_multiplier: float = 1.0
    late_lottery_spread_ratio_pct_of_price: float = 0.0
    late_lottery_depth_consumption_pct: float = 0.0
    late_lottery_payout_multiple: float = 0.0
    late_lottery_time_weight: float = 0.0
    late_lottery_trade_pct_cap: float = 0.0


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
    min_tp_token_delta: float = 0.10
    max_sl_token_delta: float = -0.08
    cheap_token_threshold_price: float = 0.30
    cheap_token_soft_sl_max_loss_pct: float = 0.30
    mid_token_threshold_price: float = 0.75
    mid_token_soft_sl_max_loss_pct: float = 0.22
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
    tp_late_compress_multiplier: float = 0.85
    settling_window_seconds: int = 180
    settling_sl_breath_multiplier: float = 1.20
    sl_confirm_breach_early: int = 4
    sl_confirm_breach_mid: int = 3
    sl_confirm_breach_late: int = 2
    sl_recovery_reset_buffer: float = 0.01
    hard_sl_extra_cents: float = 0.03
    hard_sl_extra_frac: float = 0.35
    hard_sl_min_token_delta: float = -0.18
    underlying_soft_sl_min_confirms: int = 1
    underlying_vwap_buffer_frac: float = 0.0005
    sl_entry_rel_max_loss_pct: float = 0.55
    sl_entry_rel_min_cents: float = 0.03
    force_tp_roi_pct: float = 0.70
    force_tp_delta_abs: float = 0.35
    tp_retrace_exit_frac: float = 0.30
    tp_retrace_exit_min_delta: float = 0.05
    tp_lock_min_profit_delta: float = 0.02
    tp_stall_window_early_seconds: int = 480
    tp_stall_window_mid_seconds: int = 300
    tp_stall_window_late_seconds: int = 150
    tp_stall_band_delta: float = 0.03
    tp_stall_min_profit_delta: float = 0.06
    max_reachable_token_price: float = 0.99


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
        "current_hour_trade_limit_override",
        "current_hour_trade_limit_trigger_frac",
    )

    def __init__(self, config: RiskConfig | None = None, position_config: PositionRiskConfig | None = None):
        self.config = config or RiskConfig()
        self.position_config = position_config or PositionRiskConfig()
        now_utc = datetime.now(timezone.utc)
        self.current_daily_pnl = 0.0
        self.trades_this_hour = 0
        self.current_hour = now_utc.hour
        self.current_day = now_utc.date()
        self.current_hour_trade_limit_override: int | None = None
        self.current_hour_trade_limit_trigger_frac = 0.0

    def reset_stats(self, now: datetime | None = None) -> None:
        current = now or datetime.now(timezone.utc)
        self.current_daily_pnl = 0.0
        self.trades_this_hour = 0
        self.current_hour = current.hour
        self.current_day = current.date()
        self.current_hour_trade_limit_override = None
        self.current_hour_trade_limit_trigger_frac = 0.0

    def _roll_session(self, now: datetime) -> None:
        if now.date() != self.current_day:
            self.reset_stats(now)
        elif now.hour != self.current_hour:
            self.trades_this_hour = 0
            self.current_hour = now.hour
            self.current_hour_trade_limit_override = None
            self.current_hour_trade_limit_trigger_frac = 0.0

    def record_pnl(self, pnl_impact: float, now: datetime | None = None) -> None:
        current = now or datetime.now(timezone.utc)
        self._roll_session(current)
        self.current_daily_pnl += pnl_impact

    def record_trade(self, now: datetime | None = None) -> None:
        current = now or datetime.now(timezone.utc)
        self._roll_session(current)
        self.trades_this_hour += 1

    def _effective_hourly_trade_limit(self, day_pnl: float, daily_loss_limit: float) -> int:
        base_limit = max(1, self.config.max_trades_per_hour)
        computed_limit = base_limit
        drawdown_frac = 0.0

        if daily_loss_limit > 0 and day_pnl < 0:
            drawdown_frac = abs(day_pnl) / daily_loss_limit
            if drawdown_frac >= self.config.hourly_trade_limit_drawdown_step2:
                computed_limit = 1
            elif drawdown_frac >= self.config.hourly_trade_limit_drawdown_step1:
                computed_limit = max(1, base_limit - 1)

        if computed_limit < base_limit:
            pinned_limit = self.current_hour_trade_limit_override
            if pinned_limit is None or computed_limit < pinned_limit:
                self.current_hour_trade_limit_override = computed_limit
                self.current_hour_trade_limit_trigger_frac = drawdown_frac

        if self.current_hour_trade_limit_override is not None:
            return min(self.current_hour_trade_limit_override, computed_limit)
        return computed_limit

    def drawdown_status(
        self,
        current_balance: float,
        *,
        trade_size: float = 0.0,
        now: datetime | None = None,
        current_daily_pnl: float | None = None,
        trades_this_hour: int | None = None,
        max_trade_pct_override: float | None = None,
    ) -> DrawdownStatus:
        current = now or datetime.now(timezone.utc)
        self._roll_session(current)
        day_pnl = self.current_daily_pnl if current_daily_pnl is None else current_daily_pnl
        hour_count = self.trades_this_hour if trades_this_hour is None else trades_this_hour
        daily_loss_limit = current_balance * self.config.max_daily_loss_pct
        effective_trade_pct = max_trade_pct_override if max_trade_pct_override is not None else self.config.max_trade_pct
        pct_cap = current_balance * effective_trade_pct
        raw_trade_cap = pct_cap if self.config.max_absolute_bet_usd <= 0 else min(pct_cap, self.config.max_absolute_bet_usd)
        # Sizing rounds to cents before orders are staged, so the guardrail must compare against
        # the same cent-rounded cap or exact-cap trades will be falsely rejected.
        max_trade_size = round(raw_trade_cap + EPSILON, 2)
        effective_hourly_limit = self._effective_hourly_trade_limit(day_pnl, daily_loss_limit)

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
                reason=f"Trade size ${trade_size:.2f} exceeds max {effective_trade_pct * 100:.1f}% risk.",
                daily_pnl=day_pnl,
                daily_loss_limit=daily_loss_limit,
                trades_this_hour=hour_count,
                max_trade_size_usd=max_trade_size,
            )
        if hour_count >= effective_hourly_limit:
            if effective_hourly_limit < self.config.max_trades_per_hour:
                if (
                    self.current_hour_trade_limit_override is not None
                    and abs(day_pnl) / max(daily_loss_limit, EPSILON) < self.config.hourly_trade_limit_drawdown_step1
                ):
                    reason = (
                        f"Max trades per hour pinned to {effective_hourly_limit} for this hour after drawdown "
                        f"({self.current_hour_trade_limit_trigger_frac:.0%} of daily loss limit) was breached."
                    )
                else:
                    reason = (
                        f"Max trades per hour reduced to {effective_hourly_limit} after drawdown "
                        f"({abs(day_pnl) / max(daily_loss_limit, EPSILON):.0%} of daily loss limit) reached."
                    )
            else:
                reason = f"Max trades per hour ({effective_hourly_limit}) reached."
            return DrawdownStatus(
                allowed=False,
                reason=reason,
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
        max_trade_pct_override: float | None = None,
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
            max_trade_pct_override=max_trade_pct_override,
        )
        return status.allowed, status.reason

    def continuous_time_decay(self, seconds_remaining: float) -> float:
        effective_seconds = max(0.0, seconds_remaining - self.config.min_seconds_remaining)
        if seconds_remaining <= self.config.min_seconds_remaining:
            return _clamp(self.config.time_decay_floor, 0.0, 1.0)
        floor = _clamp(self.config.time_decay_floor, 0.0, 0.95)
        decay = floor + ((1.0 - floor) * math.exp(-effective_seconds / self.config.time_decay_tau_seconds))
        return _clamp(decay, floor, 1.0)

    def _effective_depth_usd(self, liquidity: LiquidityProfile | None) -> float:
        if liquidity is None:
            return max(self.config.unknown_liquidity_depth_usd, self.config.min_bet_usd)

        raw_depth = max(liquidity.available_depth_usd, 0.0)
        if raw_depth <= 0:
            return max(self.config.unknown_liquidity_depth_usd, self.config.min_bet_usd)

        confidence = _clamp(liquidity.depth_confidence, 0.0, 1.0)
        conservative_depth = raw_depth * confidence
        return max(conservative_depth, self.config.min_bet_usd)

    def _kelly_confidence_multiplier(self, underlying_volatility: float) -> float:
        cfg = self.config
        if underlying_volatility <= 0:
            return 1.0
        risk_score = _scaled_score(
            underlying_volatility,
            cfg.kelly_volatility_soft,
            cfg.kelly_volatility_hard,
        )
        return _clamp(
            1.0 - (risk_score * (1.0 - cfg.kelly_volatility_min_multiplier)),
            cfg.kelly_volatility_min_multiplier,
            1.0,
        )

    def square_root_market_impact_pct(self, bet_size_usd: float, available_depth_usd: float) -> float:
        if bet_size_usd <= 0:
            return 0.0
        depth = max(available_depth_usd, self.config.min_bet_usd)
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
        depth = self._effective_depth_usd(liquidity)
        spread_abs = max(liquidity.estimated_spread_pct, 0.0) if liquidity is not None else 0.0

        reference_price = max(token_price or 0.5, self.config.min_token_price)
        spread_pct = (spread_abs * 0.5) / reference_price
        market_impact_pct = self.square_root_market_impact_pct(bet_size_usd, depth)
        slippage_pct = spread_pct + market_impact_pct + max(taker_fee_rate, 0.0)
        return slippage_pct, market_impact_pct

    def _late_lottery_profile(
        self,
        *,
        token_price: float,
        true_prob_pct: float,
        seconds_remaining: float,
        bet_size_usd: float,
        liquidity: LiquidityProfile | None,
        underlying_volatility: float,
    ) -> LateLotteryProfile:
        cfg = self.config
        if (
            seconds_remaining > cfg.late_lottery_window_secs
            or token_price <= 0.0
            or token_price >= 1.0
        ):
            return LateLotteryProfile()

        window = max(cfg.late_lottery_window_secs, 1.0)
        time_progress = _clamp((window - max(seconds_remaining, 0.0)) / window, 0.0, 1.0)
        time_weight = time_progress ** max(cfg.late_lottery_time_power, 0.1)

        price_component = _clamp(
            (cfg.late_lottery_price_anchor - token_price) / max(cfg.late_lottery_price_anchor, EPSILON),
            0.0,
            1.0,
        )
        true_prob_component = _clamp(
            (cfg.late_lottery_min_true_prob_pct - true_prob_pct) / max(cfg.late_lottery_min_true_prob_pct, EPSILON),
            0.0,
            1.0,
        )

        payout_multiple = (1.0 - token_price) / max(token_price, EPSILON)
        payout_component = _clamp(
            (payout_multiple - cfg.late_lottery_payout_multiple_soft)
            / max(cfg.late_lottery_payout_multiple_hard - cfg.late_lottery_payout_multiple_soft, EPSILON),
            0.0,
            1.0,
        )

        spread_abs = max(liquidity.estimated_spread_pct, 0.0) if liquidity is not None else 0.0
        spread_ratio = spread_abs / max(token_price, cfg.min_token_price)
        spread_component = _clamp(
            (spread_ratio - cfg.late_lottery_spread_ratio_soft)
            / max(cfg.late_lottery_spread_ratio_hard - cfg.late_lottery_spread_ratio_soft, EPSILON),
            0.0,
            1.0,
        )

        depth = self._effective_depth_usd(liquidity)
        depth_consumption = bet_size_usd / max(depth, EPSILON)
        depth_component = _clamp(
            (depth_consumption - cfg.late_lottery_depth_consumption_soft)
            / max(cfg.late_lottery_depth_consumption_hard - cfg.late_lottery_depth_consumption_soft, EPSILON),
            0.0,
            1.0,
        )

        volatility_component = _clamp(
            (underlying_volatility - cfg.late_lottery_volatility_soft)
            / max(cfg.late_lottery_volatility_hard - cfg.late_lottery_volatility_soft, EPSILON),
            0.0,
            1.0,
        )

        total_weight = max(
            cfg.late_lottery_price_weight
            + cfg.late_lottery_true_prob_weight
            + cfg.late_lottery_payout_weight
            + cfg.late_lottery_spread_weight
            + cfg.late_lottery_depth_weight
            + cfg.late_lottery_volatility_weight,
            EPSILON,
        )
        weighted_risk = (
            (cfg.late_lottery_price_weight * price_component)
            + (cfg.late_lottery_true_prob_weight * true_prob_component)
            + (cfg.late_lottery_payout_weight * payout_component)
            + (cfg.late_lottery_spread_weight * spread_component)
            + (cfg.late_lottery_depth_weight * depth_component)
            + (cfg.late_lottery_volatility_weight * volatility_component)
        ) / total_weight
        risk_score = _clamp(time_weight * weighted_risk, 0.0, 1.0)
        ev_penalty_pct = risk_score * max(cfg.late_lottery_ev_penalty_max_pct, 0.0)
        size_multiplier = _clamp(
            1.0 - (risk_score * max(cfg.late_lottery_size_haircut_max, 0.0)),
            max(0.0, 1.0 - max(cfg.late_lottery_size_haircut_max, 0.0)),
            1.0,
        )

        return LateLotteryProfile(
            risk_score=round(risk_score, 6),
            ev_penalty_pct=round(ev_penalty_pct, 6),
            size_multiplier=round(size_multiplier, 6),
            spread_ratio_pct_of_price=round(spread_ratio * 100.0, 6),
            depth_consumption_pct=round(depth_consumption * 100.0, 6),
            payout_multiple=round(payout_multiple, 6),
            time_weight=round(time_weight, 6),
        )

    def _elite_setup_profile(
        self,
        *,
        ev_pct: float,
        edge_pct: float,
        true_prob_pct: float,
        total_friction_pct: float,
        depth_consumption_pct: float,
        seconds_remaining: float,
    ) -> EliteSetupProfile:
        cfg = self.config
        ev_component = _scaled_score(ev_pct, cfg.elite_ev_soft_pct, cfg.elite_ev_hard_pct)
        edge_component = _scaled_score(edge_pct, cfg.elite_edge_soft_pct, cfg.elite_edge_hard_pct)
        prob_component = _scaled_score(true_prob_pct, cfg.elite_true_prob_soft_pct, cfg.elite_true_prob_hard_pct)
        friction_component = _scaled_score(total_friction_pct, cfg.elite_friction_soft_pct, cfg.elite_friction_hard_pct, invert=True)
        depth_component = _scaled_score(depth_consumption_pct, cfg.elite_depth_consumption_soft_pct, cfg.elite_depth_consumption_hard_pct, invert=True)
        urgency_component = _clamp(seconds_remaining / max(cfg.min_seconds_remaining * 12.0, 1.0), 0.0, 1.0)

        quality_score = _clamp(
            (0.35 * ev_component)
            + (0.20 * edge_component)
            + (0.20 * prob_component)
            + (0.15 * friction_component)
            + (0.05 * depth_component)
            + (0.05 * urgency_component),
            0.0,
            1.0,
        )
        size_multiplier = 1.0 + (quality_score * max(cfg.elite_size_boost_max, 0.0))
        effective_max_trade_pct = cfg.max_trade_pct + (
            quality_score * max(cfg.elite_max_trade_pct - cfg.max_trade_pct, 0.0)
        )
        lockup_hours = max(seconds_remaining, 0.0) / 3600.0
        excess_lockup = max(0.0, lockup_hours - max(cfg.capital_lockup_free_hours, 0.0))
        capital_lockup_penalty_pct = excess_lockup * max(cfg.capital_lockup_penalty_max_pct, 0.0)

        return EliteSetupProfile(
            quality_score=round(quality_score, 6),
            size_multiplier=round(size_multiplier, 6),
            effective_max_trade_pct=round(effective_max_trade_pct, 6),
            capital_lockup_penalty_pct=round(capital_lockup_penalty_pct, 6),
            depth_consumption_pct=round(depth_consumption_pct, 6),
            total_friction_pct=round(total_friction_pct, 6),
        )

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
        underlying_volatility: float = 0.0,
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
                kelly_confidence_multiplier=round(self._kelly_confidence_multiplier(underlying_volatility), 6),
                token_price=round(token_price, 4),
                adjusted_token_price=round(token_price, 4),
                slippage_cost_pct=0.0,
                market_impact_pct=0.0,
                time_decay_multiplier=0.0,
                edge_pct=round(true_prob_pct - market_prob_pct, 2),
                true_prob_pct=round(true_prob_pct, 2),
                market_prob_pct=round(market_prob_pct, 2),
                approved=False,
                available_depth_usd=0.0 if liquidity is None else self._effective_depth_usd(liquidity),
                fee_cost_pct=0.0,
                exit_fee_cost_pct=0.0,
                expected_exit_slippage_pct=0.0,
                latency_haircut_pct=0.0,
                capital_lockup_penalty_pct=0.0,
                elite_quality_score=0.0,
                elite_size_multiplier=1.0,
                effective_max_trade_pct=self.config.max_trade_pct,
                late_lottery_risk_score=0.0,
                late_lottery_ev_penalty_pct=0.0,
                late_lottery_size_multiplier=1.0,
                late_lottery_spread_ratio_pct_of_price=0.0,
                late_lottery_depth_consumption_pct=0.0,
                late_lottery_payout_multiple=0.0,
                late_lottery_time_weight=0.0,
            )

        gross_ev = true_probability * (1.0 - token_price) - ((1.0 - true_probability) * token_price)
        gross_ev_pct = _safe_div(gross_ev, token_price, default=0.0) * 100.0
        time_decay = self.continuous_time_decay(seconds_remaining)
        raw_kelly_fraction = self._kelly_fraction(true_probability, token_price)
        kelly_confidence_multiplier = self._kelly_confidence_multiplier(underlying_volatility)
        effective_exit_fee_rate = max(taker_fee_rate, 0.0) * max(self.config.expected_exit_fee_multiplier, 0.0)
        adjusted_fraction = raw_kelly_fraction * kelly_confidence_multiplier
        adjusted_token_price = token_price
        total_slippage_pct = 0.0
        market_impact_pct = 0.0
        expected_exit_slippage_pct = 0.0
        latency_haircut_pct = max(self.config.latency_ev_haircut_pct, 0.0)
        late_lottery = LateLotteryProfile()
        effective_depth_usd = self._effective_depth_usd(liquidity)

        base_pct_cap = self.config.max_trade_pct
        base_max_risk_cap = current_balance * base_pct_cap
        if self.config.max_absolute_bet_usd > 0:
            base_max_risk_cap = min(base_max_risk_cap, self.config.max_absolute_bet_usd)
        candidate_bet = (
            raw_kelly_fraction
            * kelly_confidence_multiplier
            * current_balance
            * self.config.fractional_kelly_dampener
            * time_decay
        )
        candidate_bet = min(candidate_bet, base_max_risk_cap)

        elite_profile = EliteSetupProfile(
            effective_max_trade_pct=base_pct_cap,
        )
        effective_max_risk_cap = base_max_risk_cap
        effective_trade_pct_cap = base_pct_cap
        effective_elite_multiplier = 1.0

        def _size_snapshot(
            bet_size_usd: float,
        ) -> tuple[float, float, float, float, float, LateLotteryProfile, EliteSetupProfile, float, float, float, float]:
            size = max(bet_size_usd, 0.0)
            total_slippage_pct, market_impact_pct = self.estimate_total_slippage_pct(
                size,
                liquidity,
                token_price=token_price,
                taker_fee_rate=taker_fee_rate,
            )
            expected_exit_slippage_pct = max(
                0.0,
                total_slippage_pct * max(self.config.expected_exit_slippage_multiplier, 0.0),
            )
            adjusted_token_price = _clamp(token_price * (1.0 + total_slippage_pct), 0.0001, 0.9999)
            adjusted_fraction = (
                self._kelly_fraction(true_probability, adjusted_token_price)
                * kelly_confidence_multiplier
            )
            late_lottery = self._late_lottery_profile(
                token_price=adjusted_token_price,
                true_prob_pct=true_prob_pct,
                seconds_remaining=seconds_remaining,
                bet_size_usd=max(size, self.config.min_bet_usd),
                liquidity=liquidity,
                underlying_volatility=underlying_volatility,
            )
            total_friction_pct = (
                total_slippage_pct + expected_exit_slippage_pct + effective_exit_fee_rate
            ) * 100.0
            depth_consumption_pct = (size / max(effective_depth_usd, EPSILON)) * 100.0
            elite_profile = self._elite_setup_profile(
                ev_pct=gross_ev_pct,
                edge_pct=true_prob_pct - market_prob_pct,
                true_prob_pct=true_prob_pct,
                total_friction_pct=total_friction_pct,
                depth_consumption_pct=depth_consumption_pct,
                seconds_remaining=seconds_remaining,
            )
            effective_trade_pct_cap = elite_profile.effective_max_trade_pct
            if (
                seconds_remaining <= self.config.late_lottery_window_secs
                and adjusted_token_price <= self.config.late_lottery_size_cap_token_price
                and late_lottery.risk_score >= self.config.late_lottery_size_cap_risk_score
            ):
                effective_trade_pct_cap = min(
                    effective_trade_pct_cap,
                    self.config.late_lottery_max_trade_pct_cap,
                )

            effective_max_risk_cap = current_balance * effective_trade_pct_cap
            if self.config.max_absolute_bet_usd > 0:
                effective_max_risk_cap = min(effective_max_risk_cap, self.config.max_absolute_bet_usd)

            elite_boost_cutoff = max(self.config.late_lottery_elite_boost_cutoff_score, EPSILON)
            elite_boost_weight = _clamp(1.0 - (late_lottery.risk_score / elite_boost_cutoff), 0.0, 1.0)
            effective_elite_multiplier = 1.0 + ((elite_profile.size_multiplier - 1.0) * elite_boost_weight)
            candidate_multiplier = effective_elite_multiplier * late_lottery.size_multiplier
            recalculated = (
                adjusted_fraction
                * current_balance
                * self.config.fractional_kelly_dampener
                * time_decay
                * candidate_multiplier
            )
            recalculated = min(recalculated, effective_max_risk_cap)
            return (
                total_slippage_pct,
                market_impact_pct,
                expected_exit_slippage_pct,
                adjusted_token_price,
                adjusted_fraction,
                late_lottery,
                elite_profile,
                effective_max_risk_cap,
                effective_trade_pct_cap,
                effective_elite_multiplier,
                recalculated,
            )

        if candidate_bet > 0 and base_max_risk_cap > 0:
            lower = 0.0
            upper = base_max_risk_cap
            tolerance = max(self.config.size_solver_tolerance_usd, 0.001)
            iterations = max(int(self.config.size_solver_max_iterations), 1)
            for _ in range(iterations):
                (
                    total_slippage_pct,
                    market_impact_pct,
                    expected_exit_slippage_pct,
                    adjusted_token_price,
                    adjusted_fraction,
                    late_lottery,
                    elite_profile,
                    effective_max_risk_cap,
                    effective_trade_pct_cap,
                    effective_elite_multiplier,
                    recalculated,
                ) = _size_snapshot(candidate_bet)
                if abs(recalculated - candidate_bet) <= tolerance:
                    candidate_bet = recalculated
                    break
                if recalculated > candidate_bet:
                    lower = candidate_bet
                else:
                    upper = candidate_bet
                next_candidate = (lower + upper) * 0.5
                if abs(next_candidate - candidate_bet) <= tolerance:
                    candidate_bet = next_candidate
                    break
                candidate_bet = next_candidate

        (
            total_slippage_pct,
            market_impact_pct,
            expected_exit_slippage_pct,
            adjusted_token_price,
            adjusted_fraction,
            late_lottery,
            elite_profile,
            effective_max_risk_cap,
            effective_trade_pct_cap,
            effective_elite_multiplier,
            _recalculated,
        ) = _size_snapshot(candidate_bet)
        expected_win_proceeds = max(0.0, 1.0 - effective_exit_fee_rate - expected_exit_slippage_pct)
        net_ev = true_probability * (expected_win_proceeds - adjusted_token_price) - (
            (1.0 - true_probability) * adjusted_token_price
        )
        net_ev_pct = _safe_div(net_ev, adjusted_token_price, default=0.0) * 100.0
        net_ev_pct_after_haircut = (
            net_ev_pct
            - latency_haircut_pct
            - late_lottery.ev_penalty_pct
            - elite_profile.capital_lockup_penalty_pct
        )
        fee_cost_pct = max(taker_fee_rate, 0.0) * 100.0
        exit_fee_cost_pct = effective_exit_fee_rate * 100.0

        final_bet = round(min(candidate_bet, effective_max_risk_cap), 2)
        if final_bet < self.config.min_bet_usd:
            final_bet = 0.0

        return EVComputation(
            ev_pct=round(net_ev_pct_after_haircut, 2),
            ev_pct_gross=round(gross_ev_pct, 2),
            kelly_bet_usd=final_bet,
            raw_kelly_fraction=round(raw_kelly_fraction, 6),
            adjusted_kelly_fraction=round(adjusted_fraction, 6),
            kelly_confidence_multiplier=round(kelly_confidence_multiplier, 6),
            token_price=round(token_price, 4),
            adjusted_token_price=round(adjusted_token_price, 4),
            slippage_cost_pct=round(total_slippage_pct * 100.0, 2),
            market_impact_pct=round(market_impact_pct * 100.0, 2),
            time_decay_multiplier=round(time_decay, 6),
            edge_pct=round(true_prob_pct - market_prob_pct, 2),
            true_prob_pct=round(true_prob_pct, 2),
            market_prob_pct=round(market_prob_pct, 2),
            approved=net_ev_pct_after_haircut > 0.0 and time_decay > 0.0,
            available_depth_usd=round(effective_depth_usd, 2),
            fee_cost_pct=round(fee_cost_pct, 3),
            exit_fee_cost_pct=round(exit_fee_cost_pct, 3),
            expected_exit_slippage_pct=round(expected_exit_slippage_pct * 100.0, 3),
            latency_haircut_pct=round(latency_haircut_pct, 3),
            capital_lockup_penalty_pct=round(elite_profile.capital_lockup_penalty_pct, 3),
            elite_quality_score=round(elite_profile.quality_score, 4),
            elite_size_multiplier=round(effective_elite_multiplier, 4),
            effective_max_trade_pct=round(effective_trade_pct_cap, 6),
            late_lottery_risk_score=round(late_lottery.risk_score, 4),
            late_lottery_ev_penalty_pct=round(late_lottery.ev_penalty_pct, 3),
            late_lottery_size_multiplier=round(late_lottery.size_multiplier, 4),
            late_lottery_spread_ratio_pct_of_price=round(late_lottery.spread_ratio_pct_of_price, 2),
            late_lottery_depth_consumption_pct=round(late_lottery.depth_consumption_pct, 2),
            late_lottery_payout_multiple=round(late_lottery.payout_multiple, 3),
            late_lottery_time_weight=round(late_lottery.time_weight, 4),
            late_lottery_trade_pct_cap=round(effective_trade_pct_cap, 6),
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

    def _tp_stall_window_seconds(self, seconds_remaining: float) -> int:
        cfg = self.position_config
        if seconds_remaining > cfg.early_phase_seconds:
            return cfg.tp_stall_window_early_seconds
        if seconds_remaining > cfg.mid_phase_seconds:
            return cfg.tp_stall_window_mid_seconds
        return cfg.tp_stall_window_late_seconds

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

        if position.bought_price <= cfg.cheap_token_threshold_price:
            cheap_token_sl = -max(
                cfg.sl_entry_rel_min_cents,
                position.bought_price * cfg.cheap_token_soft_sl_max_loss_pct,
            )
            sl_delta = max(sl_delta, cheap_token_sl)
        elif position.bought_price <= cfg.mid_token_threshold_price:
            mid_token_sl = -max(
                cfg.sl_entry_rel_min_cents,
                position.bought_price * cfg.mid_token_soft_sl_max_loss_pct,
            )
            sl_delta = max(sl_delta, mid_token_sl)
        reachable_tp_delta = max(0.0, cfg.max_reachable_token_price - position.bought_price)
        tp_delta = min(tp_delta, reachable_tp_delta)
        entry_based_sl = -max(cfg.sl_entry_rel_min_cents, position.bought_price * cfg.sl_entry_rel_max_loss_pct)
        reachable_floor = -(max(position.bought_price - 0.01, 0.0))
        if reachable_floor < 0:
            entry_based_sl = max(entry_based_sl, reachable_floor)
        sl_delta = max(sl_delta, entry_based_sl)
        hard_sl_extra = max(cfg.hard_sl_extra_cents, abs(sl_delta) * cfg.hard_sl_extra_frac)
        hard_sl_delta = max(sl_delta - hard_sl_extra, reachable_floor)
        hard_sl_delta = max(min(hard_sl_delta, cfg.hard_sl_min_token_delta), reachable_floor)

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
            previous_peak_delta = float(updated.tp_peak_delta)
            peak_delta = max(previous_peak_delta, price_delta)
            peak_refreshed = peak_delta > (previous_peak_delta + 1e-9)
            retrace_budget = max(cfg.tp_retrace_exit_min_delta, peak_delta * cfg.tp_retrace_exit_frac)
            lock_floor = max(cfg.tp_lock_min_profit_delta, peak_delta - retrace_budget)
            tp_armed_at_ts = float(updated.tp_armed_at_ts) if updated.tp_armed else now_ts
            tp_peak_at_ts = (
                now_ts
                if peak_refreshed or float(updated.tp_peak_at_ts) <= 0
                else float(updated.tp_peak_at_ts)
            )
            updated = replace(
                updated,
                sl_breach_count=0,
                tp_armed=True,
                tp_armed_at_ts=tp_armed_at_ts,
                tp_peak_delta=peak_delta,
                tp_peak_at_ts=tp_peak_at_ts,
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

        if updated.tp_armed:
            peak_delta = float(updated.tp_peak_delta)
            peak_at_ts = float(updated.tp_peak_at_ts) if float(updated.tp_peak_at_ts) > 0 else now_ts
            stall_window = self._tp_stall_window_seconds(seconds_remaining)
            stall_age = max(0.0, now_ts - peak_at_ts)
            stall_floor = max(cfg.tp_lock_min_profit_delta, peak_delta - cfg.tp_stall_band_delta)
            if (
                peak_delta >= cfg.tp_stall_min_profit_delta
                and stall_age >= stall_window
                and price_delta >= stall_floor
            ):
                return PositionMonitorDecision(
                    position=replace(updated, status=PositionStatus.CLOSING),
                    should_exit=True,
                    exit_reason=(
                        f"TP_STALL_EXIT (peak +{peak_delta * 100:.1f}c stalled for "
                        f"{int(stall_age)}s; now +{price_delta * 100:.1f}c)"
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
