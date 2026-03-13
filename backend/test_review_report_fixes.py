"""Regression tests for review-report safety and sizing fixes.

Usage:
    py backend/test_review_report_fixes.py
"""

from __future__ import annotations

import asyncio
import math
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

_BACKEND_DIR = Path(__file__).resolve().parent
if str(_BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(_BACKEND_DIR))

from bot.execution import ClobExecutionEngine, ExecutionConfig, _infer_fill_stats
from bot.models import (
    ActivePosition,
    Direction,
    ExitReasonKind,
    FillInferenceStatus,
    MarketResolution,
    MarketOddsSnapshot,
    ReentryState,
    TechnicalContext,
    classify_exit_reason,
)
from bot.risk import LiquidityProfile, PositionRiskConfig, RiskConfig, RiskManager
from bot.strategy import StrategyConfig, _infer_trend_direction, _post_profit_reentry_reason, _post_stop_reentry_reason

PASSED = 0
FAILED = 0


def _assert(condition: bool, name: str, detail: str = "") -> None:
    global PASSED, FAILED
    if condition:
        PASSED += 1
        print(f"  [PASS] {name}")
    else:
        FAILED += 1
        message = f"  [FAIL] {name}"
        if detail:
            message += f" -- {detail}"
        print(message)


def _make_context(**overrides: float) -> TechnicalContext:
    payload: dict[str, object] = {
        "timestamp": datetime.now(timezone.utc),
        "price": 70000.0,
        "vwap": 70010.0,
        "vwap_distance": -10.0,
        "price_vs_vwap_pct": -0.0002,
        "ema_9": 70005.0,
        "ema_21": 70005.0,
        "ema_spread_pct": 0.0,
        "cvd_1min_delta": -50.0,
        "cvd_candle_delta": -100.0,
        "realized_volatility": 0.002,
        "parkinson_volatility": 0.002,
        "garman_klass_volatility": 0.002,
        "bayesian_probability": 0.68,
    }
    payload.update(overrides)
    return TechnicalContext(**payload)


def _make_odds(*, public_prob_pct: float = 55.0) -> MarketOddsSnapshot:
    return MarketOddsSnapshot(
        slug="btc-hourly-test",
        market_found=True,
        seconds_remaining=1800.0,
        reference_price=70000.0,
        strike_price=69950.0,
        up_token_id="token-up",
        down_token_id="token-down",
        up_public_prob_pct=public_prob_pct,
        down_public_prob_pct=100.0 - public_prob_pct,
        up_entry_prob_pct=public_prob_pct,
        down_entry_prob_pct=100.0 - public_prob_pct,
    )


def test_stop_loss_reentry_is_kind_backed() -> None:
    config = StrategyConfig()
    reentry_state = ReentryState(
        last_exit_ts=time.time(),
        last_exit_reason="HARD_STOP_LOSS (-12.0c <= -9.0c hard floor)",
        last_exit_direction=Direction.UP,
        last_entry_ev_pct=12.0,
    )
    reason = _post_stop_reentry_reason(
        reentry_state,
        Direction.UP,
        score=4,
        ev_pct=30.0,
        config=config,
    )
    _assert(
        reason is not None,
        "Hard stop exits still trigger post-stop reentry lockout",
        detail=reason or "",
    )
    _assert(
        classify_exit_reason("LIQUIDATION") == ExitReasonKind.STOP_LOSS,
        "Liquidation-style exits classify as stop losses",
    )


def test_post_win_same_direction_reentry_uses_cooldown_then_quality_gate() -> None:
    config = StrategyConfig(
        post_win_same_direction_lockout_for_slug=True,
        post_win_same_direction_cooldown_secs=120.0,
        post_win_same_direction_min_ev_pct=12.0,
        post_win_same_direction_min_score=3,
    )
    active_cooldown = ReentryState(
        last_exit_ts=time.time(),
        last_exit_reason="TP_LOCK_RETRACE (peak +17.1c -> now +8.4c, floor +12.0c)",
        last_exit_direction=Direction.DOWN,
        last_exit_kind=ExitReasonKind.TAKE_PROFIT,
        last_entry_ev_pct=18.0,
    )
    cooled = ReentryState(
        last_exit_ts=time.time() - 180.0,
        last_exit_reason=active_cooldown.last_exit_reason,
        last_exit_direction=Direction.DOWN,
        last_exit_kind=ExitReasonKind.TAKE_PROFIT,
        last_entry_ev_pct=18.0,
    )

    cooldown_reason = _post_profit_reentry_reason(active_cooldown, Direction.DOWN, score=4, ev_pct=20.0, config=config)
    weak_score_reason = _post_profit_reentry_reason(cooled, Direction.DOWN, score=2, ev_pct=20.0, config=config)
    weak_ev_reason = _post_profit_reentry_reason(cooled, Direction.DOWN, score=4, ev_pct=8.0, config=config)
    allowed_reason = _post_profit_reentry_reason(cooled, Direction.DOWN, score=4, ev_pct=20.0, config=config)

    _assert(
        cooldown_reason is not None and "same-direction cooldown active" in cooldown_reason,
        "Profitable same-direction re-entries now use a cooldown instead of a full-market ban",
        detail=cooldown_reason or "",
    )
    _assert(
        weak_score_reason is not None and "score 2/4 < 3/4" in weak_score_reason,
        "Same-direction re-entry still needs a strong score after the cooldown",
        detail=weak_score_reason or "",
    )
    _assert(
        weak_ev_reason is not None and "EV 8.00% < 12.00%" in weak_ev_reason,
        "Same-direction re-entry still needs enough EV after the cooldown",
        detail=weak_ev_reason or "",
    )
    _assert(
        allowed_reason is None,
        "Strong same-direction setups are allowed again once the post-win cooldown expires",
        detail=allowed_reason or "",
    )


def test_estimated_liquidity_gets_haircut() -> None:
    manager = RiskManager(RiskConfig(max_trade_pct=1.0, elite_max_trade_pct=1.0))
    live = LiquidityProfile(available_depth_usd=40.0, estimated_spread_pct=0.01, depth_confidence=1.0)
    estimated = LiquidityProfile(available_depth_usd=40.0, estimated_spread_pct=0.01, depth_confidence=0.15)
    live_ev = manager.evaluate_trade(
        true_prob_pct=68.0,
        market_prob_pct=40.0,
        current_balance=1000.0,
        seconds_remaining=1800.0,
        liquidity=live,
        underlying_volatility=0.002,
    )
    estimated_ev = manager.evaluate_trade(
        true_prob_pct=68.0,
        market_prob_pct=40.0,
        current_balance=1000.0,
        seconds_remaining=1800.0,
        liquidity=estimated,
        underlying_volatility=0.002,
    )
    _assert(
        estimated_ev.available_depth_usd < live_ev.available_depth_usd,
        "Estimated liquidity reports a lower effective depth budget",
        detail=f"estimated={estimated_ev.available_depth_usd:.2f}, live={live_ev.available_depth_usd:.2f}",
    )
    _assert(
        estimated_ev.kelly_bet_usd < live_ev.kelly_bet_usd,
        "Estimated liquidity reduces Kelly sizing",
        detail=f"estimated={estimated_ev.kelly_bet_usd:.2f}, live={live_ev.kelly_bet_usd:.2f}",
    )


def test_volatility_dampens_kelly() -> None:
    manager = RiskManager()
    low_vol = manager.evaluate_trade(
        true_prob_pct=68.0,
        market_prob_pct=40.0,
        current_balance=1000.0,
        seconds_remaining=1800.0,
        liquidity=LiquidityProfile(available_depth_usd=200.0, estimated_spread_pct=0.01),
        underlying_volatility=0.002,
    )
    high_vol = manager.evaluate_trade(
        true_prob_pct=68.0,
        market_prob_pct=40.0,
        current_balance=1000.0,
        seconds_remaining=1800.0,
        liquidity=LiquidityProfile(available_depth_usd=200.0, estimated_spread_pct=0.01),
        underlying_volatility=0.03,
    )
    _assert(
        high_vol.kelly_confidence_multiplier < low_vol.kelly_confidence_multiplier,
        "Higher volatility lowers the Kelly confidence multiplier",
        detail=f"high={high_vol.kelly_confidence_multiplier:.3f}, low={low_vol.kelly_confidence_multiplier:.3f}",
    )
    _assert(
        high_vol.kelly_bet_usd < low_vol.kelly_bet_usd,
        "Higher volatility reduces final sizing",
        detail=f"high={high_vol.kelly_bet_usd:.2f}, low={low_vol.kelly_bet_usd:.2f}",
    )


def test_flash_crash_filter_rejects_momentum_fallback() -> None:
    config = StrategyConfig()
    flash_crash = _make_context(
        price=68000.0,
        vwap_distance=-2500.0,
        price_vs_vwap_pct=-0.035,
        ema_9=69500.0,
        ema_21=69500.0,
        cvd_1min_delta=-10000.0,
        realized_volatility=0.002,
        parkinson_volatility=0.002,
        garman_klass_volatility=0.002,
    )
    orderly_pullback = _make_context(
        price=69920.0,
        vwap_distance=-80.0,
        price_vs_vwap_pct=-0.0011,
        ema_9=69980.0,
        ema_21=69980.0,
        cvd_1min_delta=-250.0,
        realized_volatility=0.002,
        parkinson_volatility=0.002,
        garman_klass_volatility=0.002,
    )
    _assert(
        _infer_trend_direction(flash_crash, config) == Direction.UNKNOWN,
        "Flash-crash style moves no longer auto-tag a downtrend",
    )
    _assert(
        _infer_trend_direction(orderly_pullback, config) == Direction.DOWN,
        "Orderly pullbacks still qualify for the momentum fallback",
    )


def test_fill_inference_uses_enum_status() -> None:
    _cost, _shares, _avg, status = _infer_fill_stats(
        {"status": "matched", "matchedAmount": "10"},
        intended_cost_usd=5.0,
        fallback_price=0.5,
    )
    _assert(
        status == FillInferenceStatus.FALLBACK,
        "Fill inference returns typed enum statuses",
        detail=f"status={status}",
    )


def test_hard_stop_floor_has_more_breathing_room() -> None:
    manager = RiskManager(position_config=PositionRiskConfig())
    position = ActivePosition(
        slug="btc-hourly-test",
        decision=Direction.DOWN,
        token_id="token-down",
        strike=69950.0,
        bet_size_usd=150.0,
        bought_price=0.48,
        entry_time=datetime.now(timezone.utc),
    )
    snapshot = manager.position_risk_snapshot(
        position,
        _make_context(),
        seconds_remaining=3300.0,
        now_ts=position.entry_time.timestamp() + 60.0,
    )
    _assert(
        math.isclose(snapshot.hard_sl_delta, -0.18, abs_tol=1e-9),
        "Hard stop floor now tightens back to -18.0c for early-hour chop",
        detail=f"hard_sl_delta={snapshot.hard_sl_delta:.4f}",
    )


def test_position_risk_defaults_cut_losers_faster() -> None:
    config = PositionRiskConfig()
    _assert(
        config.underlying_soft_sl_min_confirms == 1,
        "Soft stop now needs only one underlying confirmation",
        detail=f"confirms={config.underlying_soft_sl_min_confirms}",
    )
    _assert(
        (config.sl_confirm_breach_early, config.sl_confirm_breach_mid, config.sl_confirm_breach_late) == (4, 3, 2),
        "Soft stop breach counts are reduced across phases",
        detail=(
            f"early={config.sl_confirm_breach_early}, "
            f"mid={config.sl_confirm_breach_mid}, "
            f"late={config.sl_confirm_breach_late}"
        ),
    )
    _assert(
        math.isclose(config.tp_retrace_exit_frac, 0.30, abs_tol=1e-9),
        "TP retrace giveback is tightened to 30%",
        detail=f"retrace={config.tp_retrace_exit_frac:.2f}",
    )


def test_candle_open_stop_confirmation_needs_structure_beyond_open_retake() -> None:
    manager = RiskManager(position_config=PositionRiskConfig())
    candle_open_position = ActivePosition(
        slug="btc-hourly-test",
        decision=Direction.DOWN,
        token_id="token-down",
        strike=70000.0,
        bet_size_usd=100.0,
        bought_price=0.42,
        ml_features={"market_resolution": MarketResolution.CANDLE_OPEN.value},
    )
    plain_position = ActivePosition(
        slug="btc-hourly-test",
        decision=Direction.DOWN,
        token_id="token-down",
        strike=70000.0,
        bet_size_usd=100.0,
        bought_price=0.42,
    )
    retake_only = _make_context(
        price=70010.0,
        vwap=70050.0,
        ema_9=69980.0,
        ema_21=70020.0,
        cvd_candle_delta=-150.0,
        adaptive_cvd_threshold=100.0,
    )
    retake_with_structure = _make_context(
        price=70010.0,
        vwap=69990.0,
        ema_9=70030.0,
        ema_21=69990.0,
        cvd_candle_delta=150.0,
        adaptive_cvd_threshold=100.0,
    )

    _assert(
        not manager._underlying_soft_sl_confirmed(
            candle_open_position,
            retake_only,
            current_underlying_price=70010.0,
        ),
        "CANDLE_OPEN stops no longer confirm on a bare strike retake",
    )
    _assert(
        manager._underlying_soft_sl_confirmed(
            candle_open_position,
            retake_with_structure,
            current_underlying_price=70010.0,
        ),
        "CANDLE_OPEN stops still confirm once the retake is backed by structure",
    )
    _assert(
        manager._underlying_soft_sl_confirmed(
            plain_position,
            retake_only,
            current_underlying_price=70010.0,
        ),
        "Non-CANDLE_OPEN markets still allow strike retakes to confirm the stop immediately",
    )


def test_crowd_skew_default_is_relaxed() -> None:
    config = StrategyConfig.from_dict({})
    _assert(
        math.isclose(config.max_crowd_prob_to_call, 98.0, abs_tol=1e-9),
        "Crowd skew cap now defaults to 98% instead of overblocking momentum",
        detail=f"cap={config.max_crowd_prob_to_call:.1f}",
    )


def test_strategy_ai_thresholds_are_tighter_by_score() -> None:
    config = StrategyConfig.from_dict({})
    _assert(
        math.isclose(config.ai_score1_min_ev_pct, 18.0, abs_tol=1e-9),
        "Score-1 setups now need 18% EV before AI is consulted",
        detail=f"score1_ai_min_ev={config.ai_score1_min_ev_pct:.1f}",
    )
    _assert(
        math.isclose(config.ai_score2_min_ev_pct, 8.0, abs_tol=1e-9),
        "Score-2 setups now need 8% EV before AI is consulted",
        detail=f"score2_ai_min_ev={config.ai_score2_min_ev_pct:.1f}",
    )
    _assert(
        math.isclose(config.ai_score3_min_ev_pct, 3.0, abs_tol=1e-9),
        "Score-3 setups now need 3% EV before AI is consulted",
        detail=f"score3_ai_min_ev={config.ai_score3_min_ev_pct:.1f}",
    )


def test_strategy_entry_quality_defaults_are_tighter() -> None:
    config = StrategyConfig.from_dict({})
    _assert(
        math.isclose(config.score2_min_ev_pct, 8.0, abs_tol=1e-9),
        "Score-2 setups now need 8% EV before they are considered tradable",
        detail=f"score2_min_ev={config.score2_min_ev_pct:.1f}",
    )
    _assert(
        math.isclose(config.min_raw_edge_cents, 1.0, abs_tol=1e-9),
        "Minimum raw edge defaults to 1.0c to avoid market-maker-grade scraps",
        detail=f"min_raw_edge_cents={config.min_raw_edge_cents:.1f}",
    )


def test_max_trade_pct_default_is_trimmed() -> None:
    config = RiskConfig()
    _assert(
        math.isclose(config.max_trade_pct, 0.02, abs_tol=1e-9),
        "Base max trade size is reduced to 2% while expectancy is repaired",
        detail=f"max_trade_pct={config.max_trade_pct:.2f}",
    )


def test_tiny_amm_fallback_needs_depth_budget() -> None:
    odds = _make_odds(public_prob_pct=55.0)
    disabled = ClobExecutionEngine(
        None,
        config=ExecutionConfig(
            paper_trading=False,
            live_tiny_amm_fallback_depth_usd=0.0,
        ),
    )
    disabled_result = disabled._build_tiny_amm_fallback_result(
        token_id="token-up",
        bet_size_usd=1.0,
        expected_price=0.55,
        odds=odds,
        reason="CLOB unavailable",
    )
    _assert(
        not disabled_result.ok and "disabled" in disabled_result.reason.lower(),
        "Tiny AMM fallback is blocked without an explicit depth budget",
        detail=disabled_result.reason,
    )

    enabled = ClobExecutionEngine(
        None,
        config=ExecutionConfig(
            paper_trading=False,
            live_tiny_amm_fallback_depth_usd=50.0,
            live_tiny_amm_fallback_max_market_impact_pct=0.50,
            live_tiny_amm_fallback_max_total_slippage_pct=0.50,
        ),
    )
    enabled_result = enabled._build_tiny_amm_fallback_result(
        token_id="token-up",
        bet_size_usd=1.0,
        expected_price=0.55,
        odds=odds,
        reason="CLOB unavailable",
    )
    _assert(
        enabled_result.available_depth_usd > 0.0 and enabled_result.market_impact_pct > 0.0,
        "Tiny AMM fallback now models synthetic depth and impact instead of zeroing them out",
        detail=f"depth={enabled_result.available_depth_usd:.2f}, impact={enabled_result.market_impact_pct:.4f}",
    )


async def test_market_params_cache() -> None:
    class FakeClient:
        def __init__(self) -> None:
            self.tick_calls = 0
            self.neg_calls = 0
            self.fee_calls = 0

        def get_tick_size(self, token_id: str) -> str:
            self.tick_calls += 1
            return "0.01"

        def get_neg_risk(self, token_id: str) -> bool:
            self.neg_calls += 1
            return False

        def get_fee_rate_bps(self, token_id: str) -> int:
            self.fee_calls += 1
            return 17

    client = FakeClient()
    engine = ClobExecutionEngine(
        client,
        config=ExecutionConfig(
            paper_trading=False,
            market_params_cache_ttl_secs=300.0,
        ),
    )
    first = await engine._resolve_market_params("token-up")
    second = await engine._resolve_market_params("token-up")
    _assert(
        first == second,
        "Market parameter cache returns stable values",
        detail=f"first={first}, second={second}",
    )
    _assert(
        client.tick_calls == 1 and client.neg_calls == 1 and client.fee_calls == 1,
        "Market parameter cache avoids repeat SDK calls",
        detail=f"tick={client.tick_calls}, neg={client.neg_calls}, fee={client.fee_calls}",
    )


async def run() -> None:
    print("\n" + "=" * 54)
    print("  Alpha-Z Review Report Fixes - Regression Suite")
    print("=" * 54)

    print("\n--- Exit Classification ---")
    test_stop_loss_reentry_is_kind_backed()
    test_post_win_same_direction_reentry_uses_cooldown_then_quality_gate()

    print("\n--- Liquidity & Kelly Sizing ---")
    test_estimated_liquidity_gets_haircut()
    test_volatility_dampens_kelly()

    print("\n--- Trend Shock Filter ---")
    test_flash_crash_filter_rejects_momentum_fallback()

    print("\n--- Execution Guardrails ---")
    test_fill_inference_uses_enum_status()
    test_hard_stop_floor_has_more_breathing_room()
    test_position_risk_defaults_cut_losers_faster()
    test_candle_open_stop_confirmation_needs_structure_beyond_open_retake()
    test_crowd_skew_default_is_relaxed()
    test_strategy_ai_thresholds_are_tighter_by_score()
    test_strategy_entry_quality_defaults_are_tighter()
    test_max_trade_pct_default_is_trimmed()
    test_tiny_amm_fallback_needs_depth_budget()
    await test_market_params_cache()

    print("\n" + "=" * 54)
    print(f"PASSED: {PASSED}")
    print(f"FAILED: {FAILED}")
    print("=" * 54)

    if FAILED:
        raise SystemExit(1)


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
