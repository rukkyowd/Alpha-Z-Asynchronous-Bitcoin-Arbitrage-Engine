"""Alpha-Z Backtesting & Weight Optimization Script.

Reads resolved trades from alpha_z_history.db, computes diagnostic metrics
(Brier score, accuracy, PnL by bucket), and optionally optimises
IndicatorWeights via scipy to minimise Brier score.

Usage:
    py backend/backtest.py                     # full diagnostics + optimisation
    py backend/backtest.py --db path/to/db     # custom db path
    py backend/backtest.py --no-optimize       # diagnostics only
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sqlite3
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Ensure the backend package is importable when running as a standalone script
# ---------------------------------------------------------------------------
_BACKEND_DIR = Path(__file__).resolve().parent
if str(_BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(_BACKEND_DIR))

from scipy.optimize import minimize  # noqa: E402

from bot.indicators import (  # noqa: E402
    DEFAULT_INDICATOR_WEIGHTS,
    REGIME_DF_MAP,
    IndicatorWeights,
    logit,
    sigmoid,
)

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

@dataclass
class ResolvedTrade:
    """One resolved trade with the fields we need for analysis."""
    timestamp: str
    slug: str
    decision: str
    result: str  # "WIN" or "LOSS"
    pnl_impact: float
    score: int
    predicted_win_prob_pct: float
    base_up_probability_pct: float
    posterior_up_probability_pct: float
    indicator_logit_shift: float
    market_regime: str
    entry_price: float
    bet_size_usd: float
    hour_of_day: int = 0


def _parse_meta(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def load_resolved_trades(db_path: str) -> list[ResolvedTrade]:
    """Load WIN/LOSS trades from the database."""
    if not os.path.exists(db_path):
        print(f"[BACKTEST] Database not found: {db_path}")
        return []

    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(
            "SELECT timestamp, slug, decision, result, pnl_impact, score, "
            "entry_price, bet_size_usd, metadata_json "
            "FROM trades WHERE result IN ('WIN', 'LOSS')"
        )
        rows = cursor.fetchall()
    except sqlite3.OperationalError as exc:
        print(f"[BACKTEST] DB error: {exc}")
        return []
    finally:
        conn.close()

    trades: list[ResolvedTrade] = []
    for ts, slug, decision, result, pnl, score, entry_px, bet_usd, meta_raw in rows:
        meta = _parse_meta(meta_raw)
        pred_prob = meta.get("predicted_win_prob_pct")
        if pred_prob is None:
            continue
        try:
            pred_prob_val = float(pred_prob)
        except (ValueError, TypeError):
            continue

        # Parse hour from timestamp
        hour = 0
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            hour = dt.hour
        except Exception:
            pass

        trades.append(ResolvedTrade(
            timestamp=ts,
            slug=slug,
            decision=decision,
            result=result,
            pnl_impact=float(pnl or 0),
            score=int(score or 0),
            predicted_win_prob_pct=pred_prob_val,
            base_up_probability_pct=float(meta.get("base_up_probability_pct", 50.0)),
            posterior_up_probability_pct=float(meta.get("posterior_up_probability_pct", 50.0)),
            indicator_logit_shift=float(meta.get("indicator_logit_shift", 0.0)),
            market_regime=str(meta.get("market_regime", "UNKNOWN")),
            entry_price=float(entry_px or 0),
            bet_size_usd=float(bet_usd or 0),
            hour_of_day=hour,
        ))
    return trades

# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

def brier_score(trades: list[ResolvedTrade]) -> float:
    """Mean squared error between predicted probability and outcome."""
    if not trades:
        return float("nan")
    total = 0.0
    for t in trades:
        predicted = t.predicted_win_prob_pct / 100.0
        outcome = 1.0 if t.result == "WIN" else 0.0
        total += (predicted - outcome) ** 2
    return total / len(trades)


def binary_accuracy(trades: list[ResolvedTrade]) -> float:
    """Fraction of trades where predicted direction matched outcome."""
    if not trades:
        return float("nan")
    correct = 0
    for t in trades:
        predicted_win = t.predicted_win_prob_pct > 50.0
        actual_win = t.result == "WIN"
        if predicted_win == actual_win:
            correct += 1
    return correct / len(trades)


def calibration_curve(trades: list[ResolvedTrade], n_bins: int = 5) -> list[dict[str, Any]]:
    """Group trades into probability bins and compare predicted vs actual win rate."""
    if not trades:
        return []

    bin_width = 100.0 / n_bins
    bins: dict[int, list[ResolvedTrade]] = {i: [] for i in range(n_bins)}
    for t in trades:
        idx = min(int(t.predicted_win_prob_pct / bin_width), n_bins - 1)
        bins[idx].append(t)

    curve = []
    for i in range(n_bins):
        bucket = bins[i]
        if not bucket:
            continue
        avg_predicted = sum(t.predicted_win_prob_pct for t in bucket) / len(bucket)
        actual_win_rate = sum(1 for t in bucket if t.result == "WIN") / len(bucket)
        curve.append({
            "bin": f"{i * bin_width:.0f}-{(i + 1) * bin_width:.0f}%",
            "count": len(bucket),
            "avg_predicted_pct": round(avg_predicted, 2),
            "actual_win_rate_pct": round(actual_win_rate * 100, 2),
            "gap_pct": round(avg_predicted - actual_win_rate * 100, 2),
        })
    return curve


def pnl_by_bucket(trades: list[ResolvedTrade], key: str) -> dict[str, dict[str, Any]]:
    """Group PnL by a trade attribute (score, market_regime, hour_of_day)."""
    buckets: dict[str, list[ResolvedTrade]] = {}
    for t in trades:
        bucket_key = str(getattr(t, key, "UNKNOWN"))
        buckets.setdefault(bucket_key, []).append(t)

    result = {}
    for k, bucket in sorted(buckets.items()):
        wins = sum(1 for t in bucket if t.result == "WIN")
        total_pnl = sum(t.pnl_impact for t in bucket)
        avg_prob = sum(t.predicted_win_prob_pct for t in bucket) / len(bucket)
        result[k] = {
            "trades": len(bucket),
            "wins": wins,
            "losses": len(bucket) - wins,
            "win_rate_pct": round(wins / len(bucket) * 100, 1),
            "total_pnl": round(total_pnl, 2),
            "avg_predicted_prob_pct": round(avg_prob, 1),
        }
    return result

# ---------------------------------------------------------------------------
# Weight Optimization
# ---------------------------------------------------------------------------

# The 7 weights in IndicatorWeights, in order
WEIGHT_NAMES = ("rsi", "ema_spread", "vwap_distance", "cvd_candle", "cvd_micro", "volume", "regime")


def _reconstruct_brier_for_weights(
    weights_array: list[float],
    trades: list[ResolvedTrade],
    default_weights: IndicatorWeights,
) -> float:
    """Approximate Brier score under a hypothetical set of weights.

    We can't perfectly replay the full model (we don't have all raw indicator
    values stored), but we CAN approximate the effect of weight changes:

    The stored `indicator_logit_shift` was computed as a weighted sum of
    normalised indicator features.  We know the *current* weights and the
    total shift.  Under a scale factor α for each weight, the new shift is:

        new_shift = sum(α_i * original_contribution_i)

    Since we only store the total shift (not per-indicator), we approximate
    by scaling the total shift proportionally to the ratio of new total
    weight magnitude vs old total weight magnitude.  This is a first-order
    approximation that works well when the optimiser moves weights smoothly.

    For a full replay, we'd need to store raw indicator features per trade,
    which we recommend adding in a future iteration.
    """
    default_arr = [getattr(default_weights, name) for name in WEIGHT_NAMES]
    old_total = sum(abs(w) for w in default_arr)
    new_total = sum(abs(w) for w in weights_array)

    if old_total < 1e-9:
        scale = 1.0
    else:
        scale = new_total / old_total

    total_sq_error = 0.0
    for t in trades:
        # Reconstruct: base_logit + scaled_shift → new_posterior
        base_prob = t.base_up_probability_pct / 100.0
        base_prob = max(1e-6, min(1.0 - 1e-6, base_prob))
        base_log = logit(base_prob)

        old_shift = t.indicator_logit_shift
        new_shift = old_shift * scale
        # Clamp to same bounds as the model
        new_shift = max(-2.0, min(2.0, new_shift))

        new_posterior = sigmoid(base_log + new_shift)

        # The predicted win probability depends on the direction
        if t.decision == "UP":
            pred_win_prob = new_posterior
        else:
            pred_win_prob = 1.0 - new_posterior

        outcome = 1.0 if t.result == "WIN" else 0.0
        total_sq_error += (pred_win_prob - outcome) ** 2

    return total_sq_error / max(len(trades), 1)


def optimise_weights(
    trades: list[ResolvedTrade],
    min_trades: int = 10,
) -> IndicatorWeights | None:
    """Find weights that minimise Brier score via Nelder-Mead."""
    if len(trades) < min_trades:
        print(f"\n[OPTIMIZER] Skipping: {len(trades)} trades < {min_trades} minimum for optimisation.")
        print("[OPTIMIZER] Collect more resolved trades and re-run.")
        return None

    default = DEFAULT_INDICATOR_WEIGHTS
    x0 = [getattr(default, name) for name in WEIGHT_NAMES]

    def objective(x: list[float]) -> float:
        # Keep weights non-negative
        clamped = [max(0.0, w) for w in x]
        return _reconstruct_brier_for_weights(clamped, trades, default)

    result = minimize(
        objective,
        x0,
        method="Nelder-Mead",
        options={"maxiter": 2000, "xatol": 1e-4, "fatol": 1e-6, "adaptive": True},
    )

    optimized = [max(0.0, round(w, 4)) for w in result.x]
    return IndicatorWeights(**dict(zip(WEIGHT_NAMES, optimized)))

# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def print_separator(title: str = "") -> None:
    if title:
        print(f"\n{'=' * 60}")
        print(f"  {title}")
        print(f"{'=' * 60}")
    else:
        print(f"\n{'-' * 60}")


def print_diagnostics(trades: list[ResolvedTrade]) -> None:
    print_separator("ALPHA-Z MODEL DIAGNOSTICS")

    print(f"\n  Resolved trades:  {len(trades)}")
    wins = sum(1 for t in trades if t.result == "WIN")
    losses = len(trades) - wins
    print(f"  Wins / Losses:    {wins} / {losses}")
    print(f"  Total PnL:        ${sum(t.pnl_impact for t in trades):+.2f}")
    print(f"  Brier Score:      {brier_score(trades):.4f}")
    print(f"  Binary Accuracy:  {binary_accuracy(trades) * 100:.1f}%")

    # Calibration curve
    curve = calibration_curve(trades)
    if curve:
        print_separator("CALIBRATION CURVE")
        print(f"  {'Bin':<12} {'Count':>5} {'Avg Pred':>10} {'Actual WR':>10} {'Gap':>8}")
        for row in curve:
            print(f"  {row['bin']:<12} {row['count']:>5} {row['avg_predicted_pct']:>9.1f}% {row['actual_win_rate_pct']:>9.1f}% {row['gap_pct']:>+7.1f}%")

    # PnL by score
    by_score = pnl_by_bucket(trades, "score")
    if by_score:
        print_separator("PnL BY SCORE")
        print(f"  {'Score':<8} {'Trades':>6} {'Win%':>6} {'PnL':>10}")
        for k, v in by_score.items():
            print(f"  {k:<8} {v['trades']:>6} {v['win_rate_pct']:>5.1f}% ${v['total_pnl']:>+9.2f}")

    # PnL by regime
    by_regime = pnl_by_bucket(trades, "market_regime")
    if by_regime:
        print_separator("PnL BY REGIME")
        print(f"  {'Regime':<16} {'Trades':>6} {'Win%':>6} {'PnL':>10}")
        for k, v in by_regime.items():
            print(f"  {k:<16} {v['trades']:>6} {v['win_rate_pct']:>5.1f}% ${v['total_pnl']:>+9.2f}")

    # PnL by hour
    by_hour = pnl_by_bucket(trades, "hour_of_day")
    if by_hour:
        print_separator("PnL BY HOUR (UTC)")
        print(f"  {'Hour':<8} {'Trades':>6} {'Win%':>6} {'PnL':>10}")
        for k, v in sorted(by_hour.items(), key=lambda x: int(x[0])):
            print(f"  {k + 'h':<8} {v['trades']:>6} {v['win_rate_pct']:>5.1f}% ${v['total_pnl']:>+9.2f}")


def print_weight_comparison(
    current: IndicatorWeights,
    optimized: IndicatorWeights,
    trades: list[ResolvedTrade],
) -> None:
    print_separator("WEIGHT OPTIMIZATION RESULTS")

    current_brier = _reconstruct_brier_for_weights(
        [getattr(current, n) for n in WEIGHT_NAMES], trades, current
    )
    new_brier = _reconstruct_brier_for_weights(
        [getattr(optimized, n) for n in WEIGHT_NAMES], trades, current
    )

    print(f"\n  Brier score (current weights):   {current_brier:.4f}")
    print(f"  Brier score (optimized weights): {new_brier:.4f}")
    improvement = (current_brier - new_brier) / max(current_brier, 1e-9) * 100
    print(f"  Improvement:                     {improvement:+.2f}%")

    print(f"\n  {'Weight':<16} {'Current':>10} {'Optimized':>10} {'Change':>10}")
    print(f"  {'-' * 46}")
    for name in WEIGHT_NAMES:
        cur = getattr(current, name)
        opt = getattr(optimized, name)
        diff = opt - cur
        print(f"  {name:<16} {cur:>10.4f} {opt:>10.4f} {diff:>+10.4f}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Alpha-Z Model Backtest & Weight Optimizer")
    parser.add_argument(
        "--db",
        default=str(_BACKEND_DIR / "alpha_z_history.db"),
        help="Path to the SQLite database (default: backend/alpha_z_history.db)",
    )
    parser.add_argument(
        "--no-optimize",
        action="store_true",
        help="Skip weight optimization, only print diagnostics",
    )
    parser.add_argument(
        "--min-trades",
        type=int,
        default=10,
        help="Minimum resolved trades required for optimization (default: 10)",
    )
    parser.add_argument(
        "--output",
        default=str(_BACKEND_DIR / "optimized_weights.json"),
        help="Output path for optimized weights JSON",
    )
    args = parser.parse_args()

    # Load data
    trades = load_resolved_trades(args.db)
    if not trades:
        print("\n[BACKTEST] No resolved trades found. Run the engine to accumulate trade history.")
        print("[BACKTEST] The backtester will produce meaningful results once you have ≥10 resolved trades.")
        return

    # Print diagnostics
    print_diagnostics(trades)

    # Optimise weights
    if not args.no_optimize:
        print_separator("RUNNING WEIGHT OPTIMIZER")
        optimized = optimise_weights(trades, min_trades=args.min_trades)

        if optimized is not None:
            print_weight_comparison(DEFAULT_INDICATOR_WEIGHTS, optimized, trades)

            # Save to JSON
            output = {
                "optimized_weights": asdict(optimized),
                "current_weights": asdict(DEFAULT_INDICATOR_WEIGHTS),
                "sample_count": len(trades),
                "brier_score_current": round(brier_score(trades), 6),
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "note": "Copy optimized_weights values into IndicatorWeights defaults in bot/indicators.py to adopt.",
            }
            with open(args.output, "w") as f:
                json.dump(output, f, indent=2)
            print(f"\n  Optimized weights saved to: {args.output}")

    print_separator()
    print("  Done.\n")


if __name__ == "__main__":
    main()
