from __future__ import annotations

import time
from dataclasses import dataclass, field, replace as _dc_replace

from .calibration import ProbabilityCalibrator
from .indicators import apply_probabilistic_model, directional_probabilities
from .models import (
    ConfidenceLevel,
    Direction,
    EdgeSnapshot,
    MarketOddsSnapshot,
    MarketRegime,
    ReentryState,
    SignalAlignmentSnapshot,
    TechnicalContext,
    TradeSignal,
)
from .risk import EVComputation, LiquidityProfile, RiskConfig, RiskManager
from .state import EngineState

EPSILON = 1e-9


def _safe_div(numerator: float, denominator: float, default: float = 0.0) -> float:
    if abs(denominator) <= EPSILON:
        return default
    return numerator / denominator


def _reference_price(odds: MarketOddsSnapshot) -> float:
    return odds.reference_price if odds.reference_price > 0 else odds.strike_price


@dataclass(slots=True, frozen=True)
class StrategyConfig:
    risk: RiskConfig = field(default_factory=RiskConfig)
    probability_floor_pct: float = 2.0
    probability_ceil_pct: float = 98.0
    degrees_of_freedom: int = 4
    max_indicator_logit_shift: float = 2.0
    close_equals_open_up_bias_prob: float = 0.0005
    ema_squeeze_pct: float = 0.00005
    min_score_to_trade: int = 1
    min_seconds_remaining: float = 30.0
    max_seconds_for_new_bet: float = 3540.0
    max_crowd_prob_to_call: float = 96.0
    min_ev_pct_to_call_ai: float = 1.0
    ev_ai_bypass_threshold: float = 3.0
    score1_min_ev_pct: float = 15.0
    score2_min_ev_pct: float = 6.0
    score0_min_ev_pct: float = 35.0
    score0_max_token_price: float = 0.35
    ev_bypass_min_score: int = 3
    ev_bypass_min_token_price: float = 0.20
    volume_confirmation_ratio: float = 1.05
    trend_lock_enabled: bool = True
    trend_lock_min_ema_spread_pct: float = 0.0012
    trend_lock_min_vwap_dist_pct: float = 0.0010
    trend_lock_cvd_confirm_threshold: float = 15000.0
    countertrend_min_score: int = 3
    countertrend_min_ev_pct: float = 20.0
    countertrend_min_ev_lead_pct: float = 5.0
    countertrend_min_vwap_dist_pct: float = 0.0010
    countertrend_min_strike_dist_pct: float = 0.0010
    countertrend_force_ai: bool = True
    min_directional_ev_lead_pct: float = 2.0
    post_stop_cooldown_secs: float = 600.0
    post_stop_reentry_min_ev_improvement_pct: float = 5.0
    post_stop_reentry_min_score: int = 3
    default_depth_usd: float = 40.0
    default_spread_pct: float = 0.01

def build_signal_alignment(context: TechnicalContext, direction: Direction, *, volume_ratio: float = 1.05) -> SignalAlignmentSnapshot:
    if direction not in (Direction.UP, Direction.DOWN):
        return SignalAlignmentSnapshot(direction=direction)

    vwap_ok = (
        direction == Direction.UP and context.price > context.vwap
    ) or (
        direction == Direction.DOWN and context.price < context.vwap
    )
    rsi_ok = (
        direction == Direction.UP and context.rsi_14 > 50.0
    ) or (
        direction == Direction.DOWN and context.rsi_14 < 50.0
    )
    volume_ok = context.current_volume > (context.vol_sma_20 * volume_ratio)
    threshold = max(abs(context.adaptive_cvd_threshold), 1.0)
    cvd_ok = (
        direction == Direction.UP and context.cvd_candle_delta > threshold
    ) or (
        direction == Direction.DOWN and context.cvd_candle_delta < -threshold
    )
    score = int(vwap_ok) + int(rsi_ok) + int(volume_ok) + int(cvd_ok)
    return SignalAlignmentSnapshot(
        direction=direction,
        score=score,
        max_score=4,
        vwap=vwap_ok,
        rsi=rsi_ok,
        volume=volume_ok,
        cvd=cvd_ok,
    )


def _derive_liquidity_profile(
    odds: MarketOddsSnapshot,
    direction: Direction,
    *,
    config: StrategyConfig,
    explicit_liquidity: LiquidityProfile | None = None,
) -> LiquidityProfile:
    if explicit_liquidity is not None:
        return explicit_liquidity

    if direction == Direction.UP:
        best_bid = odds.up_best_bid
        best_ask = odds.up_best_ask
    else:
        best_bid = odds.down_best_bid
        best_ask = odds.down_best_ask

    spread_pct = config.default_spread_pct
    if best_bid is not None and best_ask is not None and best_ask > 0:
        spread_pct = max(0.0, best_ask - best_bid)

    return LiquidityProfile(
        available_depth_usd=config.default_depth_usd,
        estimated_spread_pct=spread_pct,
        best_bid=best_bid,
        best_ask=best_ask,
        levels=1 if (best_bid is not None or best_ask is not None) else 0,
    )


def _infer_trend_direction(context: TechnicalContext) -> Direction:
    trend_direction = Direction.UNKNOWN
    if context.ema_9 > context.ema_21 and context.vwap_distance > 0:
        trend_direction = Direction.UP
    elif context.ema_9 < context.ema_21 and context.vwap_distance < 0:
        trend_direction = Direction.DOWN
    return trend_direction


def _trend_lock_veto(context: TechnicalContext, direction: Direction, config: StrategyConfig) -> str | None:
    if not config.trend_lock_enabled:
        return None

    price = context.price
    ema_spread_pct = abs(context.ema_spread_pct)
    vwap_dist_pct = abs(_safe_div(context.vwap_distance, price, default=0.0))
    cvd_delta = context.cvd_candle_delta
    trend_direction = _infer_trend_direction(context)

    strong_trend = (
        context.market_regime in (MarketRegime.BULL_TREND, MarketRegime.BEAR_TREND, MarketRegime.BREAKOUT)
        and trend_direction in (Direction.UP, Direction.DOWN)
        and ema_spread_pct >= config.trend_lock_min_ema_spread_pct
        and vwap_dist_pct >= config.trend_lock_min_vwap_dist_pct
    )
    cvd_confirms = (
        trend_direction == Direction.UP and cvd_delta >= config.trend_lock_cvd_confirm_threshold
    ) or (
        trend_direction == Direction.DOWN and cvd_delta <= -config.trend_lock_cvd_confirm_threshold
    )

    if strong_trend and cvd_confirms and direction != trend_direction:
        return (
            f"Trend lock veto: {trend_direction.value} trend confirmed "
            f"(EMA spread {ema_spread_pct * 100:.3f}%, "
            f"VWAP dist {vwap_dist_pct * 100:.3f}%, "
            f"CVD {cvd_delta:+.0f})"
        )
    return None


def _countertrend_reason(
    context: TechnicalContext,
    odds: MarketOddsSnapshot,
    direction: Direction,
    score: int,
    ev_pct: float,
    ev_lead_pct: float,
    config: StrategyConfig,
) -> str | None:
    trend_direction = _infer_trend_direction(context)
    if trend_direction not in (Direction.UP, Direction.DOWN) or direction == trend_direction:
        return None

    price = max(context.price, 1e-9)
    strike_dist_pct = abs(_safe_div(context.price - _reference_price(odds), price, default=0.0))
    vwap_dist_pct = abs(_safe_div(context.vwap_distance, price, default=0.0))

    if score < config.countertrend_min_score:
        return (
            f"Countertrend blocked: score {score}/4 < {config.countertrend_min_score}/4 "
            f"against {trend_direction.value} trend"
        )
    if ev_pct < config.countertrend_min_ev_pct:
        return (
            f"Countertrend blocked: EV {ev_pct:.2f}% < {config.countertrend_min_ev_pct:.2f}% "
            f"against {trend_direction.value} trend"
        )
    if ev_lead_pct < config.countertrend_min_ev_lead_pct:
        return (
            f"Countertrend blocked: EV lead {ev_lead_pct:.2f}% < "
            f"{config.countertrend_min_ev_lead_pct:.2f}% over {trend_direction.value}"
        )
    if vwap_dist_pct < config.countertrend_min_vwap_dist_pct:
        return (
            f"Countertrend blocked: VWAP distance {vwap_dist_pct * 100:.3f}% < "
            f"{config.countertrend_min_vwap_dist_pct * 100:.3f}%"
        )
    if strike_dist_pct < config.countertrend_min_strike_dist_pct:
        return (
            f"Countertrend blocked: strike distance {strike_dist_pct * 100:.3f}% < "
            f"{config.countertrend_min_strike_dist_pct * 100:.3f}%"
        )
    return None


def _skip_signal(slug: str, reason: str, *, score: int = 0) -> TradeSignal:
    return TradeSignal(
        slug=slug,
        direction=Direction.SKIP,
        confidence=ConfidenceLevel.LOW,
        score=score,
        approved=False,
        reasons=(reason,),
    )


def _post_stop_reentry_reason(
    reentry_state: ReentryState | None,
    direction: Direction,
    score: int,
    ev_pct: float,
    config: StrategyConfig,
) -> str | None:
    if reentry_state is None:
        return None

    if reentry_state.last_exit_direction != direction:
        return None

    if "STOP_LOSS" not in reentry_state.last_exit_reason.upper():
        return None

    elapsed = time.time() - reentry_state.last_exit_ts
    if elapsed < config.post_stop_cooldown_secs:
        remaining = max(0, int(config.post_stop_cooldown_secs - elapsed))
        return (
            f"Post-stop cooldown active ({remaining}s left) for {direction.value} "
            f"after {reentry_state.last_exit_reason}"
        )

    required_ev = reentry_state.last_entry_ev_pct + config.post_stop_reentry_min_ev_improvement_pct
    if ev_pct < required_ev:
        return (
            f"Post-stop re-entry blocked: EV {ev_pct:.2f}% < {required_ev:.2f}% "
            f"needed after last {direction.value} stop"
        )

    if score < config.post_stop_reentry_min_score:
        return (
            f"Post-stop re-entry blocked: score {score}/4 < "
            f"{config.post_stop_reentry_min_score}/4 after last {direction.value} stop"
        )

    return None


class StrategyEngine:
    __slots__ = ("config", "risk_manager", "calibrator")

    def __init__(self, config: StrategyConfig | None = None, calibrator: ProbabilityCalibrator | None = None):
        self.config = config or StrategyConfig()
        self.risk_manager = RiskManager(self.config.risk)
        self.calibrator = calibrator

    def compute_expected_value(
        self,
        context: TechnicalContext,
        odds: MarketOddsSnapshot,
        direction: Direction,
        bankroll: float,
        *,
        liquidity: LiquidityProfile | None = None,
    ) -> EVComputation:
        up_probability = max(0.0, min(context.bayesian_probability * 100.0, 100.0))
        down_probability = max(0.0, 100.0 - up_probability)
        true_prob_pct = up_probability if direction == Direction.UP else down_probability
        market_prob_pct = odds.entry_prob_pct(direction)
        taker_fee_rate = odds.effective_taker_fee_rate(direction, market_prob_pct / 100.0)
        profile = _derive_liquidity_profile(odds, direction, config=self.config, explicit_liquidity=liquidity)
        return self.risk_manager.evaluate_trade(
            true_prob_pct=true_prob_pct,
            market_prob_pct=market_prob_pct,
            current_balance=bankroll,
            seconds_remaining=odds.seconds_remaining,
            liquidity=profile,
            taker_fee_rate=taker_fee_rate,
        )

    async def evaluate_trade_signal(
        self,
        context: TechnicalContext,
        odds: MarketOddsSnapshot,
        bankroll: float,
        *,
        slug: str,
        state: EngineState | None = None,
        liquidity_by_direction: dict[Direction, LiquidityProfile] | None = None,
    ) -> TradeSignal:
        if not odds.market_found:
            return _skip_signal(slug, "No Polymarket data")

        if odds.seconds_remaining < self.config.min_seconds_remaining:
            return _skip_signal(
                slug,
                f"Too close to expiry ({int(odds.seconds_remaining)}s < {int(self.config.min_seconds_remaining)}s)",
            )
        if odds.seconds_remaining > self.config.max_seconds_for_new_bet:
            return _skip_signal(
                slug,
                f"Too early ({int(odds.seconds_remaining)}s > {int(self.config.max_seconds_for_new_bet)}s)",
            )
        reference_price = _reference_price(odds)
        if reference_price <= 0:
            return _skip_signal(slug, "Invalid reference price")

        enriched_context = apply_probabilistic_model(
            context,
            strike_price=reference_price,
            seconds_remaining=odds.seconds_remaining,
            degrees_of_freedom=self.config.degrees_of_freedom,
            probability_floor_pct=self.config.probability_floor_pct,
            probability_ceil_pct=self.config.probability_ceil_pct,
            max_indicator_logit_shift=self.config.max_indicator_logit_shift,
            close_equals_open_up_bias_prob=self.config.close_equals_open_up_bias_prob,
        )

        # --- Calibration layer: map raw Bayesian probability to calibrated ---
        raw_bayesian_prob = enriched_context.bayesian_probability
        calibrated_prob = raw_bayesian_prob
        if self.calibrator is not None and self.calibrator.is_fitted:
            calibrated_prob = self.calibrator.calibrate(raw_bayesian_prob)
            floor = self.config.probability_floor_pct / 100.0
            ceil = self.config.probability_ceil_pct / 100.0
            calibrated_prob = max(floor, min(ceil, calibrated_prob))
            enriched_context = _dc_replace(enriched_context, bayesian_probability=calibrated_prob)

        if enriched_context.market_regime == MarketRegime.UNKNOWN:
            return _skip_signal(slug, "Market UNKNOWN - insufficient data")

        if abs(enriched_context.ema_spread_pct) < self.config.ema_squeeze_pct:
            return _skip_signal(
                slug,
                f"EMA Squeeze (spread {abs(enriched_context.ema_spread_pct) * 100:.3f}%)",
            )

        if max(odds.fair_entry_prob_pct(Direction.UP), odds.fair_entry_prob_pct(Direction.DOWN)) > self.config.max_crowd_prob_to_call:
            return _skip_signal(slug, "Crowd skew too high")

        up_ev = self.compute_expected_value(
            enriched_context,
            odds,
            Direction.UP,
            bankroll,
            liquidity=None if liquidity_by_direction is None else liquidity_by_direction.get(Direction.UP),
        )
        down_ev = self.compute_expected_value(
            enriched_context,
            odds,
            Direction.DOWN,
            bankroll,
            liquidity=None if liquidity_by_direction is None else liquidity_by_direction.get(Direction.DOWN),
        )

        if up_ev.ev_pct > down_ev.ev_pct:
            target_direction = Direction.UP
            target_ev = up_ev
            alternate_ev = down_ev
        else:
            target_direction = Direction.DOWN
            target_ev = down_ev
            alternate_ev = up_ev
        signal_alignment = build_signal_alignment(enriched_context, target_direction, volume_ratio=self.config.volume_confirmation_ratio)

        up_probability, down_probability = directional_probabilities(enriched_context)
        fair_up_market_prob = odds.fair_entry_prob_pct(Direction.UP)
        fair_down_market_prob = odds.fair_entry_prob_pct(Direction.DOWN)
        up_edge = up_probability - fair_up_market_prob
        down_edge = down_probability - fair_down_market_prob
        edge_snapshot = EdgeSnapshot(
            slug=slug,
            direction=target_direction,
            up_math_prob=round(up_probability, 2),
            down_math_prob=round(down_probability, 2),
            up_poly_prob=round(fair_up_market_prob, 2),
            down_poly_prob=round(fair_down_market_prob, 2),
            up_public_prob=round(odds.up_public_prob_pct, 2),
            down_public_prob=round(odds.down_public_prob_pct, 2),
            up_edge=round(up_edge, 2),
            down_edge=round(down_edge, 2),
            best_edge=round(up_edge if target_direction == Direction.UP else down_edge, 2),
            best_ev_pct=round(target_ev.ev_pct, 2),
        )

        if state is not None:
            await state.update_telemetry(edge_snapshot=edge_snapshot, signal_alignment=signal_alignment)

        trend_veto_reason = _trend_lock_veto(enriched_context, target_direction, self.config)
        if trend_veto_reason:
            return _skip_signal(slug, trend_veto_reason, score=signal_alignment.score)

        if not target_ev.approved:
            return _skip_signal(slug, f"Net EV {target_ev.ev_pct:.2f}% <= 0 after slippage", score=signal_alignment.score)

        ev_lead_pct = target_ev.ev_pct - alternate_ev.ev_pct
        if ev_lead_pct < self.config.min_directional_ev_lead_pct:
            return _skip_signal(
                slug,
                f"Directional EV lead too weak ({ev_lead_pct:.2f}% < {self.config.min_directional_ev_lead_pct:.2f}%)",
                score=signal_alignment.score,
            )

        score = signal_alignment.score
        token_price = target_ev.adjusted_token_price
        allow_score0_extreme_ev = (
            score == 0
            and target_ev.ev_pct >= self.config.score0_min_ev_pct
            and token_price <= self.config.score0_max_token_price
        )

        if score < self.config.min_score_to_trade and not allow_score0_extreme_ev:
            return _skip_signal(
                slug,
                f"Insufficient technical confirmation ({score}/4 < {self.config.min_score_to_trade}/4)",
                score=score,
            )

        if score == 1 and target_ev.ev_pct < self.config.score1_min_ev_pct:
            return _skip_signal(
                slug,
                f"Score 1 requires EV >= {self.config.score1_min_ev_pct:.2f}% (got {target_ev.ev_pct:.2f}%)",
                score=score,
            )
        if score == 2 and target_ev.ev_pct < self.config.score2_min_ev_pct:
            return _skip_signal(
                slug,
                f"Score 2 requires EV >= {self.config.score2_min_ev_pct:.2f}% (got {target_ev.ev_pct:.2f}%)",
                score=score,
            )

        countertrend_reason = _countertrend_reason(
            enriched_context,
            odds,
            target_direction,
            score,
            target_ev.ev_pct,
            ev_lead_pct,
            self.config,
        )
        if countertrend_reason:
            return _skip_signal(slug, countertrend_reason, score=score)

        reentry_snapshot = None
        if state is not None:
            async with state.positions_lock:
                existing = state.reentry_state.get(slug)
                reentry_snapshot = existing.clone() if existing is not None else None
        post_stop_reason = _post_stop_reentry_reason(
            reentry_snapshot,
            target_direction,
            score,
            target_ev.ev_pct,
            self.config,
        )
        if post_stop_reason:
            return _skip_signal(slug, post_stop_reason, score=score)

        reasons: list[str] = []
        if signal_alignment.vwap:
            reasons.append("VWAP Trend")
        if signal_alignment.rsi:
            reasons.append("RSI Momentum")
        if signal_alignment.volume:
            reasons.append("Vol Spike")
        if signal_alignment.cvd:
            reasons.append("CVD Aligned")

        confidence = ConfidenceLevel.SCOUT
        needs_ai = True
        trend_direction = _infer_trend_direction(enriched_context)
        is_countertrend = trend_direction in (Direction.UP, Direction.DOWN) and trend_direction != target_direction

        if allow_score0_extreme_ev:
            confidence = ConfidenceLevel.SCOUT
            needs_ai = True
            reasons.append(
                f"EXTREME EV OVERRIDE (score=0/4, EV={target_ev.ev_pct:.1f}%, px={token_price:.3f})"
            )
        elif score >= 4:
            confidence = ConfidenceLevel.HIGH
            needs_ai = False
        elif (
            target_ev.ev_pct >= self.config.ev_ai_bypass_threshold
            and score >= self.config.ev_bypass_min_score
            and token_price >= self.config.ev_bypass_min_token_price
        ):
            confidence = ConfidenceLevel.HIGH
            needs_ai = False
            reasons.append(
                f"EV BYPASS ({target_ev.ev_pct:.1f}% >= {self.config.ev_ai_bypass_threshold:.1f}%, "
                f"score={score}/4, px={token_price:.3f})"
            )
        elif target_ev.ev_pct >= self.config.ev_ai_bypass_threshold:
            confidence = ConfidenceLevel.SCOUT
            needs_ai = True
            reasons.append(
                f"HIGH EV requires AI (score={score}/4, px={token_price:.3f}; "
                f"bypass needs score>={self.config.ev_bypass_min_score}, "
                f"px>={self.config.ev_bypass_min_token_price:.2f})"
            )
        elif target_ev.ev_pct >= self.config.min_ev_pct_to_call_ai:
            confidence = ConfidenceLevel.SCOUT
            needs_ai = True
            reasons.append(f"AI VALIDATION REQUIRED (score={score}/4)")
        else:
            return _skip_signal(
                slug,
                f"Net EV {target_ev.ev_pct:.2f}% below AI trigger ({self.config.min_ev_pct_to_call_ai:.2f}%)",
                score=score,
            )

        if is_countertrend:
            reasons.append(f"COUNTERTREND vs {trend_direction.value}")
            if self.config.countertrend_force_ai:
                needs_ai = True
                if confidence == ConfidenceLevel.HIGH:
                    confidence = ConfidenceLevel.SCOUT

        approved, approval_reason = self.risk_manager.can_trade(bankroll, target_ev.kelly_bet_usd)
        if not approved:
            return _skip_signal(slug, approval_reason, score=score)

        fair_target_market_prob = odds.fair_entry_prob_pct(target_direction)
        raw_target_market_prob = odds.entry_prob_pct(target_direction)
        target_vig_pct = odds.entry_vig_pct()
        return TradeSignal(
            slug=slug,
            direction=target_direction,
            confidence=confidence,
            score=score,
            max_score=signal_alignment.max_score,
            bonus_score=0,
            expected_value_pct=target_ev.ev_pct,
            expected_value_gross_pct=target_ev.ev_pct_gross,
            true_probability_pct=target_ev.true_prob_pct,
            market_probability_pct=round(fair_target_market_prob, 2),
            entry_probability_pct=target_ev.market_prob_pct,
            token_price=token_price,
            kelly_bet_usd=target_ev.kelly_bet_usd,
            approved=True,
            needs_ai=needs_ai,
            ai_validated=False,
            expected_slippage_pct=target_ev.slippage_cost_pct,
            market_impact_pct=target_ev.market_impact_pct,
            price_cap=target_ev.adjusted_token_price,
            reasons=tuple(reasons),
            metadata={
                "time_decay_multiplier": target_ev.time_decay_multiplier,
                "adjusted_kelly_fraction": target_ev.adjusted_kelly_fraction,
                "raw_kelly_fraction": target_ev.raw_kelly_fraction,
                "available_depth_usd": target_ev.available_depth_usd,
                "market_regime": enriched_context.market_regime.value,
                "up_ev_pct": up_ev.ev_pct,
                "down_ev_pct": down_ev.ev_pct,
                "ev_lead_pct": ev_lead_pct,
                "up_edge_pct": edge_snapshot.up_edge,
                "down_edge_pct": edge_snapshot.down_edge,
                "fee_cost_pct": target_ev.fee_cost_pct,
                "predicted_win_prob_pct": target_ev.true_prob_pct,
                "base_up_probability_pct": round(enriched_context.base_probability * 100.0, 2),
                "posterior_up_probability_pct": round(enriched_context.bayesian_probability * 100.0, 2),
                "raw_bayesian_prob_pct": round(raw_bayesian_prob * 100.0, 2),
                "calibrated_prob_pct": round(calibrated_prob * 100.0, 2),
                "calibration_active": self.calibrator is not None and self.calibrator.is_fitted,
                "indicator_logit_shift": round(enriched_context.indicator_logit_shift, 4),
                "raw_market_probability_pct": round(raw_target_market_prob, 2),
                "fair_market_probability_pct": round(fair_target_market_prob, 2),
                "entry_vig_pct": round(target_vig_pct, 2),
                "public_vig_pct": round(odds.public_vig_pct(), 2),
                "reference_price": round(reference_price, 6),
                "expected_exit_fee_cost_pct": target_ev.exit_fee_cost_pct,
                "latency_haircut_pct": target_ev.latency_haircut_pct,
            },
        )


__all__ = [
    "StrategyConfig",
    "StrategyEngine",
    "build_signal_alignment",
]
