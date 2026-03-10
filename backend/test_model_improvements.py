"""Self-validation tests for the three model improvements.

Tests:
  1. Adaptive DF: verifies different regimes produce different probabilities
  2. Backtest metrics: validates Brier score / accuracy on synthetic data
  3. Weight optimiser: verifies it returns valid IndicatorWeights

Usage:
    py backend/test_model_improvements.py
"""

from __future__ import annotations

import json
import math
import os
import sqlite3
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

_BACKEND_DIR = Path(__file__).resolve().parent
if str(_BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(_BACKEND_DIR))

from bot.indicators import (
    DEFAULT_INDICATOR_WEIGHTS,
    REGIME_DF_MAP,
    IndicatorWeights,
    apply_probabilistic_model,
)
from bot.models import MarketRegime, TechnicalContext
from backtest import (
    ResolvedTrade,
    brier_score,
    binary_accuracy,
    calibration_curve,
    optimise_weights,
    pnl_by_bucket,
    WEIGHT_NAMES,
)

PASSED = 0
FAILED = 0


def _assert(condition: bool, name: str, detail: str = "") -> None:
    global PASSED, FAILED
    if condition:
        PASSED += 1
        print(f"  [PASS] {name}")
    else:
        FAILED += 1
        msg = f"  [FAIL] {name}"
        if detail:
            msg += f" -- {detail}"
        print(msg)


# ---------------------------------------------------------------------------
# 1. Adaptive DF Tests
# ---------------------------------------------------------------------------

def test_adaptive_df() -> None:
    print("\n--- Adaptive Degrees of Freedom ---")

    base_ctx = TechnicalContext(
        timestamp=datetime.now(timezone.utc),
        price=70000.0,
        realized_volatility=0.005,
        garman_klass_volatility=0.005,
        parkinson_volatility=0.005,
        market_regime=MarketRegime.RANGE,
    )

    # Test 1: regime_df_map=None should use the global df (backward compat)
    ctx_no_map = apply_probabilistic_model(
        base_ctx,
        strike_price=69900.0,
        seconds_remaining=1800.0,
        degrees_of_freedom=4,
        regime_df_map=None,
    )
    ctx_with_map = apply_probabilistic_model(
        base_ctx,
        strike_price=69900.0,
        seconds_remaining=1800.0,
        degrees_of_freedom=4,
        regime_df_map=REGIME_DF_MAP,
    )
    # RANGE maps to df=8 in REGIME_DF_MAP, which differs from global df=4,
    # so the probabilities will differ.
    _assert(
        abs(ctx_no_map.bayesian_probability - ctx_with_map.bayesian_probability) > 1e-6,
        "RANGE regime uses df=8 (different from global df=4)",
        f"no_map={ctx_no_map.bayesian_probability:.6f} vs with_map={ctx_with_map.bayesian_probability:.6f}",
    )

    # Test 2: Different regimes produce different base probabilities
    results: dict[str, float] = {}
    for regime in [MarketRegime.BULL_TREND, MarketRegime.VOLATILE_RANGE, MarketRegime.BREAKOUT, MarketRegime.RANGE]:
        ctx = TechnicalContext(
            timestamp=datetime.now(timezone.utc),
            price=70000.0,
            realized_volatility=0.005,
            garman_klass_volatility=0.005,
            parkinson_volatility=0.005,
            market_regime=regime,
        )
        enriched = apply_probabilistic_model(
            ctx,
            strike_price=69900.0,
            seconds_remaining=1800.0,
            degrees_of_freedom=4,
            regime_df_map=REGIME_DF_MAP,
        )
        results[regime.value] = enriched.base_probability

    # RANGE (df=8) and BREAKOUT (df=3) should differ
    _assert(
        abs(results["RANGE"] - results["BREAKOUT"]) > 1e-6,
        "RANGE (df=8) vs BREAKOUT (df=3) produce different probabilities",
        f"RANGE={results['RANGE']:.6f}, BREAKOUT={results['BREAKOUT']:.6f}",
    )

    # Test 3: REGIME_DF_MAP has all expected keys
    expected_keys = {"BULL_TREND", "BEAR_TREND", "RANGE", "VOLATILE_RANGE", "BREAKOUT", "UNKNOWN"}
    _assert(
        set(REGIME_DF_MAP.keys()) == expected_keys,
        "REGIME_DF_MAP contains all MarketRegime values",
    )

    # Test 4: All df values are reasonable (>= 3)
    _assert(
        all(v >= 3 for v in REGIME_DF_MAP.values()),
        "All regime DF values >= 3",
    )


# ---------------------------------------------------------------------------
# 2. Backtest Metric Tests
# ---------------------------------------------------------------------------

def _make_trade(prob_pct: float, result: str, **kwargs: Any) -> ResolvedTrade:
    from typing import Any
    defaults = dict(
        timestamp="2026-03-09T12:00:00",
        slug="test",
        decision="UP",
        pnl_impact=1.0 if result == "WIN" else -1.0,
        score=2,
        base_up_probability_pct=50.0,
        posterior_up_probability_pct=prob_pct,
        indicator_logit_shift=0.5,
        market_regime="RANGE",
        entry_price=0.50,
        bet_size_usd=10.0,
        hour_of_day=12,
    )
    defaults.update(kwargs)
    return ResolvedTrade(predicted_win_prob_pct=prob_pct, result=result, **defaults)


def test_backtest_metrics() -> None:
    print("\n--- Backtest Metrics ---")

    # Perfect predictions: prob=90% for WIN, prob=10% for LOSS
    perfect = [
        _make_trade(90.0, "WIN"),
        _make_trade(10.0, "LOSS"),
    ]
    bs = brier_score(perfect)
    _assert(
        abs(bs - 0.01) < 1e-6,
        f"Brier score for perfect predictions = 0.01 (got {bs:.6f})",
    )

    acc = binary_accuracy(perfect)
    _assert(
        abs(acc - 1.0) < 1e-6,
        f"Accuracy for perfect predictions = 100% (got {acc * 100:.1f}%)",
    )

    # Worst case: prob=90% for LOSS, prob=10% for WIN
    worst = [
        _make_trade(90.0, "LOSS"),
        _make_trade(10.0, "WIN"),
    ]
    bs_worst = brier_score(worst)
    _assert(
        abs(bs_worst - 0.81) < 1e-6,
        f"Brier score for worst predictions = 0.81 (got {bs_worst:.6f})",
    )

    acc_worst = binary_accuracy(worst)
    _assert(
        abs(acc_worst - 0.0) < 1e-6,
        f"Accuracy for worst predictions = 0% (got {acc_worst * 100:.1f}%)",
    )

    # PnL by score
    trades = [
        _make_trade(70.0, "WIN", score=3, pnl_impact=5.0),
        _make_trade(60.0, "LOSS", score=1, pnl_impact=-3.0),
        _make_trade(80.0, "WIN", score=3, pnl_impact=7.0),
    ]
    by_score = pnl_by_bucket(trades, "score")
    _assert("3" in by_score and by_score["3"]["trades"] == 2, "PnL by score groups correctly")
    _assert(
        by_score["3"]["total_pnl"] == 12.0,
        f"Score-3 PnL = $12.00 (got ${by_score['3']['total_pnl']:.2f})",
    )

    # Calibration curve
    curve = calibration_curve(trades, n_bins=5)
    _assert(len(curve) > 0, "Calibration curve produces non-empty output")


# ---------------------------------------------------------------------------
# 3. Weight Optimizer Tests
# ---------------------------------------------------------------------------

def test_weight_optimiser() -> None:
    print("\n--- Weight Optimizer ---")

    # Too few trades -> should return None
    few_trades = [_make_trade(70.0, "WIN") for _ in range(5)]
    result = optimise_weights(few_trades, min_trades=10)
    _assert(result is None, "Returns None with insufficient data (5 < 10)")

    # Enough trades → should return valid weights
    trades = []
    for i in range(30):
        # Alternate WIN/LOSS to create a dataset
        prob = 55.0 + (i % 10)
        win = i % 3 != 0  # ~67% win rate
        trades.append(_make_trade(
            prob,
            "WIN" if win else "LOSS",
            base_up_probability_pct=50.0,
            indicator_logit_shift=0.3 + (i % 5) * 0.1,
        ))

    result = optimise_weights(trades, min_trades=10)
    _assert(result is not None, "Returns IndicatorWeights with 30 trades")

    if result is not None:
        for name in WEIGHT_NAMES:
            val = getattr(result, name)
            _assert(val >= 0.0, f"Weight '{name}' is non-negative ({val:.4f})")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("\n" + "=" * 50)
    print("  Alpha-Z Model Improvements — Test Suite")
    print("=" * 50)

    test_adaptive_df()
    test_backtest_metrics()
    test_weight_optimiser()

    print(f"\n{'=' * 50}")
    print(f"  Results: {PASSED} passed, {FAILED} failed")
    print(f"{'=' * 50}\n")

    sys.exit(1 if FAILED > 0 else 0)


if __name__ == "__main__":
    main()
