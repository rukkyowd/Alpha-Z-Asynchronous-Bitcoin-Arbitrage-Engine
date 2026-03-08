"""Probability calibration layer for Alpha-Z.

Transforms raw Bayesian probabilities from the indicator model into
calibrated probabilities that map more accurately to real-world outcomes.

Two calibration methods are supported:
  - **Platt scaling**: logistic regression on logit-transformed probabilities
    (fast, interpretable, 2 parameters).
  - **Isotonic regression**: nonparametric monotone mapping (better for
    heavily skewed distributions, but needs more data).

Usage:
    calibrator = ProbabilityCalibrator(db_path="backend/alpha_z_history.db")
    calibrator.fit_from_history()  # Load resolved trades and fit
    calibrated = calibrator.calibrate(0.62)  # raw prob → calibrated prob
"""

from __future__ import annotations

import json
import logging
import math
import os
import sqlite3
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger("alpha_z_engine.calibration")

# ---------------------------------------------------------------------------
# Platt scaling (logistic calibration) — pure Python, no sklearn dependency
# ---------------------------------------------------------------------------

def _logit(p: float) -> float:
    """Log-odds transform, clamped to avoid ±inf."""
    p = max(1e-7, min(1.0 - 1e-7, p))
    return math.log(p / (1.0 - p))


def _sigmoid(x: float) -> float:
    """Inverse logit."""
    if x >= 0:
        return 1.0 / (1.0 + math.exp(-x))
    ex = math.exp(x)
    return ex / (1.0 + ex)


def _fit_platt(probs: list[float], outcomes: list[float], *, lr: float = 0.05, epochs: int = 500) -> tuple[float, float]:
    """Fit Platt scaling parameters (A, B) via gradient descent.

    Model: calibrated_prob = sigmoid(A * logit(raw_prob) + B)

    Returns (A, B). A=1, B=0 is the identity (no calibration).
    """
    a = 1.0
    b = 0.0
    n = len(probs)
    if n == 0:
        return a, b

    logits = [_logit(p) for p in probs]

    for _ in range(epochs):
        grad_a = 0.0
        grad_b = 0.0
        for logit_val, y in zip(logits, outcomes):
            z = a * logit_val + b
            pred = _sigmoid(z)
            error = pred - y  # d(loss)/d(pred) for cross-entropy
            grad_a += error * logit_val
            grad_b += error
        # Average gradients
        a -= lr * (grad_a / n)
        b -= lr * (grad_b / n)

    return a, b


def _apply_platt(raw_prob: float, a: float, b: float) -> float:
    """Apply fitted Platt parameters to a raw probability."""
    return _sigmoid(a * _logit(raw_prob) + b)


# ---------------------------------------------------------------------------
# Isotonic regression — pure Python implementation
# ---------------------------------------------------------------------------

def _fit_isotonic(probs: list[float], outcomes: list[float]) -> list[tuple[float, float]]:
    """Fit isotonic regression via the pool-adjacent-violators algorithm (PAVA).

    Returns a sorted list of (raw_prob_centroid, calibrated_value) pairs
    that define the piecewise-constant isotonic mapping.
    """
    if not probs:
        return []

    # Sort by raw probability
    paired = sorted(zip(probs, outcomes), key=lambda x: x[0])
    xs = [p[0] for p in paired]
    ys = [p[1] for p in paired]

    n = len(xs)
    # PAVA: merge adjacent blocks that violate monotonicity
    blocks: list[list[int]] = [[i] for i in range(n)]
    block_means: list[float] = list(ys)

    merged = True
    while merged:
        merged = False
        new_blocks: list[list[int]] = []
        new_means: list[float] = []
        i = 0
        while i < len(blocks):
            if i + 1 < len(blocks) and block_means[i] > block_means[i + 1]:
                # Merge blocks i and i+1
                combined = blocks[i] + blocks[i + 1]
                combined_mean = sum(ys[idx] for idx in combined) / len(combined)
                new_blocks.append(combined)
                new_means.append(combined_mean)
                merged = True
                i += 2
            else:
                new_blocks.append(blocks[i])
                new_means.append(block_means[i])
                i += 1
        blocks = new_blocks
        block_means = new_means

    # Build the isotonic mapping: (centroid_x, calibrated_y)
    mapping: list[tuple[float, float]] = []
    for block, mean_val in zip(blocks, block_means):
        centroid_x = sum(xs[idx] for idx in block) / len(block)
        mapping.append((centroid_x, mean_val))

    return mapping


def _apply_isotonic(raw_prob: float, mapping: list[tuple[float, float]]) -> float:
    """Apply fitted isotonic mapping via linear interpolation."""
    if not mapping:
        return raw_prob
    if len(mapping) == 1:
        return mapping[0][1]

    # Clamp to mapping range
    if raw_prob <= mapping[0][0]:
        return mapping[0][1]
    if raw_prob >= mapping[-1][0]:
        return mapping[-1][1]

    # Linear interpolation between surrounding knots
    for i in range(len(mapping) - 1):
        x0, y0 = mapping[i]
        x1, y1 = mapping[i + 1]
        if x0 <= raw_prob <= x1:
            if abs(x1 - x0) < 1e-12:
                return y0
            t = (raw_prob - x0) / (x1 - x0)
            return y0 + t * (y1 - y0)

    return raw_prob


# ---------------------------------------------------------------------------
# Calibrator class
# ---------------------------------------------------------------------------

@dataclass
class CalibrationConfig:
    """Configuration for the probability calibrator."""
    method: str = "platt"  # "platt" or "isotonic"
    min_samples: int = 30   # Minimum resolved trades before fitting
    refit_interval: int = 50  # Refit after this many new resolved trades
    db_path: str = ""
    platt_lr: float = 0.05
    platt_epochs: int = 500


@dataclass
class ProbabilityCalibrator:
    """Calibrates raw model probabilities to improve Brier score.

    Gracefully degrades: returns raw probability if not enough data to fit.
    """
    config: CalibrationConfig = field(default_factory=CalibrationConfig)

    # Platt parameters
    _platt_a: float = field(default=1.0, init=False, repr=False)
    _platt_b: float = field(default=0.0, init=False, repr=False)

    # Isotonic mapping
    _isotonic_map: list[tuple[float, float]] = field(default_factory=list, init=False, repr=False)

    # Tracking
    _is_fitted: bool = field(default=False, init=False)
    _fitted_on_n: int = field(default=0, init=False)
    _last_resolved_count: int = field(default=0, init=False)

    @property
    def is_fitted(self) -> bool:
        return self._is_fitted

    @property
    def fitted_sample_count(self) -> int:
        return self._fitted_on_n

    def calibrate(self, raw_probability: float) -> float:
        """Transform a raw probability [0, 1] through the calibration model.

        Returns the raw probability unchanged if the model is not yet fitted.
        """
        if not self._is_fitted:
            return raw_probability

        clamped = max(0.001, min(0.999, raw_probability))

        if self.config.method == "isotonic":
            result = _apply_isotonic(clamped, self._isotonic_map)
        else:
            result = _apply_platt(clamped, self._platt_a, self._platt_b)

        return max(0.001, min(0.999, result))

    def fit(self, probabilities: list[float], outcomes: list[float]) -> bool:
        """Fit the calibrator from probability/outcome pairs.

        Args:
            probabilities: Raw model probabilities in [0, 1].
            outcomes: Binary outcomes (1.0 = win, 0.0 = loss).

        Returns:
            True if the fit succeeded, False if insufficient data.
        """
        n = len(probabilities)
        if n < self.config.min_samples:
            log.info(
                "[CALIBRATION] Skipping fit: %d samples < %d minimum.",
                n, self.config.min_samples,
            )
            return False

        if len(probabilities) != len(outcomes):
            log.warning("[CALIBRATION] probabilities and outcomes length mismatch.")
            return False

        if self.config.method == "isotonic":
            self._isotonic_map = _fit_isotonic(probabilities, outcomes)
            log.info(
                "[CALIBRATION] Isotonic fit complete with %d knots from %d samples.",
                len(self._isotonic_map), n,
            )
        else:
            self._platt_a, self._platt_b = _fit_platt(
                probabilities, outcomes,
                lr=self.config.platt_lr,
                epochs=self.config.platt_epochs,
            )
            log.info(
                "[CALIBRATION] Platt fit complete: A=%.4f, B=%.4f from %d samples.",
                self._platt_a, self._platt_b, n,
            )

        self._is_fitted = True
        self._fitted_on_n = n
        return True

    def fit_from_history(self, db_path: str | None = None) -> bool:
        """Load resolved trades from SQLite and fit the calibration model.

        Reads `predicted_win_prob_pct` from `metadata_json` and `result`
        from the `trades` table.
        """
        path = db_path or self.config.db_path
        if not path or not os.path.exists(path):
            log.info("[CALIBRATION] No database at %s, skipping fit.", path)
            return False

        try:
            conn = sqlite3.connect(path)
            cursor = conn.execute(
                "SELECT result, metadata_json FROM trades WHERE result IN ('WIN', 'LOSS')"
            )
            rows = cursor.fetchall()
            conn.close()
        except Exception as exc:
            log.warning("[CALIBRATION] Failed to read trades: %s", exc)
            return False

        probabilities: list[float] = []
        outcomes: list[float] = []

        for result, meta_raw in rows:
            meta = _parse_meta(meta_raw)
            prob = meta.get("predicted_win_prob_pct")
            if prob is None:
                continue
            try:
                prob_val = float(prob) / 100.0
            except (ValueError, TypeError):
                continue
            if not (0.0 < prob_val < 1.0):
                continue
            probabilities.append(prob_val)
            outcomes.append(1.0 if result == "WIN" else 0.0)

        self._last_resolved_count = len(probabilities)
        return self.fit(probabilities, outcomes)

    def should_refit(self, current_resolved_count: int) -> bool:
        """Check if enough new resolved trades have accumulated to justify a refit."""
        if not self._is_fitted:
            return current_resolved_count >= self.config.min_samples
        return (current_resolved_count - self._fitted_on_n) >= self.config.refit_interval

    def diagnostics(self) -> dict[str, Any]:
        """Return diagnostic info for logging/telemetry."""
        return {
            "method": self.config.method,
            "is_fitted": self._is_fitted,
            "fitted_on_n": self._fitted_on_n,
            "platt_a": round(self._platt_a, 4) if self.config.method == "platt" else None,
            "platt_b": round(self._platt_b, 4) if self.config.method == "platt" else None,
            "isotonic_knots": len(self._isotonic_map) if self.config.method == "isotonic" else None,
        }


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


__all__ = [
    "CalibrationConfig",
    "ProbabilityCalibrator",
]
