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
    MarketOddsSnapshot,
    ReentryState,
    TechnicalContext,
    classify_exit_reason,
)
from bot.risk import LiquidityProfile, PositionRiskConfig, RiskConfig, RiskManager
from bot.strategy import StrategyConfig, _infer_trend_direction, _post_stop_reentry_reason

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
        math.isclose(snapshot.hard_sl_delta, -0.20, abs_tol=1e-9),
        "Hard stop floor widens to -20.0c for early-hour chop",
        detail=f"hard_sl_delta={snapshot.hard_sl_delta:.4f}",
    )


def test_crowd_skew_default_is_relaxed() -> None:
    config = StrategyConfig.from_dict({})
    _assert(
        math.isclose(config.max_crowd_prob_to_call, 98.0, abs_tol=1e-9),
        "Crowd skew cap now defaults to 98% instead of overblocking momentum",
        detail=f"cap={config.max_crowd_prob_to_call:.1f}",
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

    print("\n--- Liquidity & Kelly Sizing ---")
    test_estimated_liquidity_gets_haircut()
    test_volatility_dampens_kelly()

    print("\n--- Trend Shock Filter ---")
    test_flash_crash_filter_rejects_momentum_fallback()

    print("\n--- Execution Guardrails ---")
    test_fill_inference_uses_enum_status()
    test_hard_stop_floor_has_more_breathing_room()
    test_crowd_skew_default_is_relaxed()
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
