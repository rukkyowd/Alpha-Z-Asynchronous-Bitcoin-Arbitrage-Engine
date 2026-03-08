from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, replace
from statistics import median
from typing import Sequence

from scipy.special import stdtr

from .models import MarketRegime, MarketTick, TechnicalContext

EPSILON = 1e-9
DEFAULT_BAR_SECONDS = 3600.0


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(value, upper))


def _safe_div(numerator: float, denominator: float, default: float = 0.0) -> float:
    if abs(denominator) <= EPSILON:
        return default
    return numerator / denominator


def sigmoid(value: float) -> float:
    if value >= 0:
        exp_term = math.exp(-value)
        return 1.0 / (1.0 + exp_term)
    exp_term = math.exp(value)
    return exp_term / (1.0 + exp_term)


def logit(probability: float) -> float:
    bounded = _clamp(probability, 1e-6, 1.0 - 1e-6)
    return math.log(bounded / (1.0 - bounded))


@dataclass(slots=True, frozen=True)
class IndicatorWeights:
    rsi: float = 0.35
    ema_spread: float = 0.55
    vwap_distance: float = 0.25
    cvd_candle: float = 0.65
    cvd_micro: float = 0.30
    volume: float = 0.18
    regime: float = 0.20


DEFAULT_INDICATOR_WEIGHTS = IndicatorWeights()


class StreamingEMA:
    __slots__ = ("period", "k", "ema", "history")

    def __init__(self, period: int):
        if period <= 0:
            raise ValueError("EMA period must be positive.")
        self.period = period
        self.k = 2.0 / (period + 1)
        self.ema: float | None = None
        self.history: list[float] = []

    def update(self, price: float) -> float:
        if self.ema is None:
            self.history.append(price)
            if len(self.history) == self.period:
                self.ema = sum(self.history) / self.period
                self.history.clear()
            return self.ema or (sum(self.history) / len(self.history))
        self.ema = (price * self.k) + (self.ema * (1.0 - self.k))
        return self.ema

    def peek(self, price: float) -> float:
        if self.ema is None:
            if not self.history:
                return price
            return (sum(self.history) + price) / (len(self.history) + 1)
        return (price * self.k) + (self.ema * (1.0 - self.k))


class StreamingRSI:
    __slots__ = ("period", "seed_gains", "seed_losses", "avg_gain", "avg_loss", "last_price")

    def __init__(self, period: int = 14):
        if period <= 0:
            raise ValueError("RSI period must be positive.")
        self.period = period
        self.seed_gains: deque[float] = deque(maxlen=period)
        self.seed_losses: deque[float] = deque(maxlen=period)
        self.avg_gain: float | None = None
        self.avg_loss: float | None = None
        self.last_price: float | None = None

    @staticmethod
    def _rsi_from_avgs(avg_gain: float, avg_loss: float) -> float:
        if avg_gain == 0 and avg_loss == 0:
            return 50.0
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    def update(self, price: float) -> float:
        if self.last_price is None:
            self.last_price = price
            return 50.0

        change = price - self.last_price
        gain = max(change, 0.0)
        loss = max(-change, 0.0)
        self.last_price = price

        if self.avg_gain is None or self.avg_loss is None:
            self.seed_gains.append(gain)
            self.seed_losses.append(loss)
            if len(self.seed_gains) < self.period:
                return 50.0
            self.avg_gain = sum(self.seed_gains) / self.period
            self.avg_loss = sum(self.seed_losses) / self.period
            return self._rsi_from_avgs(self.avg_gain, self.avg_loss)

        self.avg_gain = ((self.avg_gain * (self.period - 1)) + gain) / self.period
        self.avg_loss = ((self.avg_loss * (self.period - 1)) + loss) / self.period
        return self._rsi_from_avgs(self.avg_gain, self.avg_loss)

    def peek(self, price: float) -> float:
        if self.last_price is None:
            return 50.0

        change = price - self.last_price
        gain = max(change, 0.0)
        loss = max(-change, 0.0)

        if self.avg_gain is None or self.avg_loss is None:
            temp_gains = list(self.seed_gains) + [gain]
            temp_losses = list(self.seed_losses) + [loss]
            if len(temp_gains) < self.period:
                return 50.0
            temp_avg_gain = sum(temp_gains[-self.period:]) / self.period
            temp_avg_loss = sum(temp_losses[-self.period:]) / self.period
            return self._rsi_from_avgs(temp_avg_gain, temp_avg_loss)

        temp_avg_gain = ((self.avg_gain * (self.period - 1)) + gain) / self.period
        temp_avg_loss = ((self.avg_loss * (self.period - 1)) + loss) / self.period
        return self._rsi_from_avgs(temp_avg_gain, temp_avg_loss)


def compute_vwap(cumulative_price_volume: float, cumulative_volume: float) -> float:
    return _safe_div(cumulative_price_volume, cumulative_volume, default=0.0)


def update_vwap_accumulators(
    cumulative_price_volume: float,
    cumulative_volume: float,
    candle: MarketTick,
) -> tuple[float, float]:
    typical_price = (candle.high + candle.low + candle.close) / 3.0
    return (
        cumulative_price_volume + (typical_price * candle.volume),
        cumulative_volume + candle.volume,
    )


def infer_bar_seconds(history: Sequence[MarketTick]) -> float:
    if len(history) < 2:
        return DEFAULT_BAR_SECONDS

    diffs: list[float] = []
    for previous, current in zip(history[-6:-1], history[-5:]):
        if previous.close_time_ms > 0 and current.close_time_ms > 0:
            delta_ms = current.close_time_ms - previous.close_time_ms
        elif previous.event_time_ms > 0 and current.event_time_ms > 0:
            delta_ms = current.event_time_ms - previous.event_time_ms
        else:
            continue

        if delta_ms > 0:
            diffs.append(delta_ms / 1000.0)

    if not diffs:
        return DEFAULT_BAR_SECONDS
    return max(1.0, float(median(diffs)))


def parkinson_volatility(candles: Sequence[MarketTick], *, min_periods: int = 5) -> float:
    samples = []
    for candle in candles:
        if candle.high <= 0 or candle.low <= 0 or candle.high <= candle.low:
            continue
        samples.append(math.log(candle.high / candle.low) ** 2)

    if len(samples) < min_periods:
        return 0.0

    variance = sum(samples) / (4.0 * len(samples) * math.log(2.0))
    return math.sqrt(max(variance, 0.0))


def garman_klass_volatility(candles: Sequence[MarketTick], *, min_periods: int = 5) -> float:
    samples = []
    log_constant = 2.0 * math.log(2.0) - 1.0
    for candle in candles:
        if candle.open <= 0 or candle.high <= 0 or candle.low <= 0 or candle.close <= 0:
            continue
        if candle.high <= candle.low:
            continue
        high_low = math.log(candle.high / candle.low)
        close_open = math.log(candle.close / candle.open)
        samples.append((0.5 * high_low * high_low) - (log_constant * close_open * close_open))

    if len(samples) < min_periods:
        return 0.0

    variance = sum(samples) / len(samples)
    return math.sqrt(max(variance, 0.0))


def close_to_close_volatility(candles: Sequence[MarketTick], *, min_periods: int = 5) -> float:
    if len(candles) < (min_periods + 1):
        return 0.0

    returns = []
    for previous, current in zip(candles[:-1], candles[1:]):
        prev_close = previous.close if previous.close > 0 else previous.resolved_price
        curr_close = current.close if current.close > 0 else current.resolved_price
        if prev_close <= 0 or curr_close <= 0:
            continue
        returns.append(math.log(curr_close / prev_close))

    if len(returns) < min_periods:
        return 0.0

    variance = sum(ret * ret for ret in returns) / len(returns)
    return math.sqrt(max(variance, 0.0))


def blended_intraday_volatility(candles: Sequence[MarketTick]) -> float:
    gk_vol = garman_klass_volatility(candles)
    park_vol = parkinson_volatility(candles)
    cc_vol = close_to_close_volatility(candles)

    if gk_vol > 0 and park_vol > 0:
        blended = (0.6 * gk_vol) + (0.4 * park_vol)
    else:
        blended = max(gk_vol, park_vol)

    return max(blended, cc_vol)


def _ema_from_prices(prices: Sequence[float], period: int) -> float:
    if not prices:
        return 0.0
    ema = prices[0]
    alpha = 2.0 / (period + 1)
    for price in prices[1:]:
        ema = (price * alpha) + (ema * (1.0 - alpha))
    return ema


def detect_market_regime(history: Sequence[MarketTick]) -> MarketRegime:
    if len(history) < 20:
        return MarketRegime.UNKNOWN

    window = list(history[-30:])
    closes = [candle.close if candle.close > 0 else candle.resolved_price for candle in window]
    if not closes or closes[-1] <= 0:
        return MarketRegime.UNKNOWN

    current_price = closes[-1]
    ema_short = _ema_from_prices(closes[-12:], period=8)
    ema_long = _ema_from_prices(closes[-30:], period=21)
    spread_pct = _safe_div(ema_short - ema_long, current_price, default=0.0)
    volatility = blended_intraday_volatility(window)

    previous_window = window[:-1]
    last_candle = window[-1]
    if previous_window:
        prior_high = max(candle.high for candle in previous_window[-6:])
        prior_low = min(candle.low for candle in previous_window[-6:])
        breakout_scale = current_price * max(volatility, 0.0015)
        if last_candle.close > prior_high and last_candle.body_size > breakout_scale:
            return MarketRegime.BREAKOUT
        if last_candle.close < prior_low and last_candle.body_size > breakout_scale:
            return MarketRegime.BREAKOUT

    if abs(spread_pct) < max(volatility * 0.40, 0.0005):
        if volatility > 0.006:
            return MarketRegime.VOLATILE_RANGE
        return MarketRegime.RANGE

    if spread_pct > 0:
        return MarketRegime.BULL_TREND
    if spread_pct < 0:
        return MarketRegime.BEAR_TREND
    return MarketRegime.UNKNOWN


def build_technical_context(
    current_candle: MarketTick,
    history: Sequence[MarketTick],
    *,
    ema_fast: StreamingEMA,
    ema_slow: StreamingEMA,
    rsi: StreamingRSI,
    vwap: float,
    cvd_total: float,
    cvd_snapshot_at_candle_open: float,
    cvd_1min_delta: float,
    adaptive_cvd_threshold: float,
) -> TechnicalContext:
    price = current_candle.resolved_price
    fast_ema = ema_fast.peek(price)
    slow_ema = ema_slow.peek(price)
    live_rsi = rsi.peek(price)
    recent_history = list(history[-30:]) if history else [current_candle]
    current_volume = current_candle.volume
    volume_window = recent_history[-20:] if len(recent_history) >= 20 else recent_history
    volume_average = sum(candle.volume for candle in volume_window) / max(len(volume_window), 1)
    parkinson = parkinson_volatility(recent_history)
    garman_klass = garman_klass_volatility(recent_history)
    blended_volatility = blended_intraday_volatility(recent_history)
    price_vs_vwap_pct = _safe_div(price - vwap, price, default=0.0) if vwap > 0 else 0.0

    notes = (
        f"bar_seconds={int(infer_bar_seconds(recent_history))}",
        f"candle_structure={current_candle.structure}",
    )

    return TechnicalContext(
        timestamp=current_candle.timestamp,
        price=price,
        vwap=vwap,
        vwap_distance=price - vwap if vwap > 0 else 0.0,
        price_vs_vwap_pct=price_vs_vwap_pct,
        ema_9=fast_ema,
        ema_21=slow_ema,
        ema_spread_pct=_safe_div(fast_ema - slow_ema, price, default=0.0),
        rsi_14=live_rsi,
        cvd_total=cvd_total,
        cvd_candle_delta=cvd_total - cvd_snapshot_at_candle_open,
        cvd_1min_delta=cvd_1min_delta,
        current_volume=current_volume,
        vol_sma_20=volume_average,
        parkinson_volatility=parkinson,
        garman_klass_volatility=garman_klass,
        realized_volatility=blended_volatility,
        adaptive_atr_floor=price * blended_volatility,
        adaptive_cvd_threshold=adaptive_cvd_threshold,
        market_regime=detect_market_regime(recent_history),
        expected_move_sigma=price * blended_volatility,
        notes=notes,
    )


def _regime_feature(regime: MarketRegime, ema_spread_pct: float, price_vs_vwap_pct: float) -> float:
    directional_hint = math.copysign(1.0, ema_spread_pct or price_vs_vwap_pct or 1.0)
    if regime == MarketRegime.BULL_TREND:
        return 0.75
    if regime == MarketRegime.BEAR_TREND:
        return -0.75
    if regime == MarketRegime.BREAKOUT:
        return 0.50 * directional_hint
    return 0.0


def apply_probabilistic_model(
    context: TechnicalContext,
    *,
    strike_price: float,
    seconds_remaining: float,
    degrees_of_freedom: int = 4,
    weights: IndicatorWeights = DEFAULT_INDICATOR_WEIGHTS,
    probability_floor_pct: float = 2.0,
    probability_ceil_pct: float = 98.0,
    max_indicator_logit_shift: float = 2.0,
    close_equals_open_up_bias_prob: float = 0.0005,
    bar_seconds: float | None = None,
) -> TechnicalContext:
    if context.price <= 0 or strike_price <= 0:
        return replace(
            context,
            strike_price=strike_price,
            base_probability=0.5,
            bayesian_logit=0.0,
            bayesian_probability=0.5,
            indicator_logit_shift=0.0,
            expected_move_sigma=0.0,
            expected_move_t=0.0,
        )

    effective_bar_seconds = bar_seconds or DEFAULT_BAR_SECONDS
    horizon_bars = max(seconds_remaining / max(effective_bar_seconds, 1.0), 1e-6)
    sigma_bar = max(context.realized_volatility, context.garman_klass_volatility, context.parkinson_volatility, 1e-5)
    expected_move_sigma = max(context.price * sigma_bar * math.sqrt(horizon_bars), context.price * 1e-4)
    t_score = (context.price - strike_price) / expected_move_sigma
    base_probability = float(stdtr(max(3, degrees_of_freedom), t_score))
    if close_equals_open_up_bias_prob > 0:
        base_probability = _clamp(
            base_probability + (close_equals_open_up_bias_prob * (1.0 - base_probability)),
            1e-6,
            1.0 - 1e-6,
        )

    threshold = max(abs(context.adaptive_cvd_threshold), 1.0)
    volume_ratio = _safe_div(context.current_volume, context.vol_sma_20, default=1.0) - 1.0
    base_logit = logit(base_probability)
    indicator_logit_shift = 0.0
    indicator_logit_shift += weights.rsi * ((context.rsi_14 - 50.0) / 12.5)
    indicator_logit_shift += weights.ema_spread * math.tanh(context.ema_spread_pct / 0.0015)
    indicator_logit_shift += weights.vwap_distance * math.tanh(context.price_vs_vwap_pct / 0.0025)
    indicator_logit_shift += weights.cvd_candle * math.tanh(context.cvd_candle_delta / threshold)
    indicator_logit_shift += weights.cvd_micro * math.tanh(context.cvd_1min_delta / max(threshold * 0.35, 1.0))
    indicator_logit_shift += weights.volume * math.tanh(volume_ratio)
    indicator_logit_shift += weights.regime * _regime_feature(
        context.market_regime,
        context.ema_spread_pct,
        context.price_vs_vwap_pct,
    )
    bounded_shift = _clamp(indicator_logit_shift, -abs(max_indicator_logit_shift), abs(max_indicator_logit_shift))
    posterior_logit = base_logit + bounded_shift

    bounded_probability = _clamp(
        sigmoid(posterior_logit),
        probability_floor_pct / 100.0,
        probability_ceil_pct / 100.0,
    )

    return replace(
        context,
        strike_price=strike_price,
        base_probability=base_probability,
        bayesian_logit=posterior_logit,
        bayesian_probability=bounded_probability,
        indicator_logit_shift=bounded_shift,
        expected_move_sigma=expected_move_sigma,
        expected_move_t=t_score,
    )


def directional_probabilities(context: TechnicalContext) -> tuple[float, float]:
    up_probability = _clamp(context.bayesian_probability * 100.0, 0.0, 100.0)
    return round(up_probability, 2), round(100.0 - up_probability, 2)


__all__ = [
    "DEFAULT_BAR_SECONDS",
    "DEFAULT_INDICATOR_WEIGHTS",
    "IndicatorWeights",
    "StreamingEMA",
    "StreamingRSI",
    "apply_probabilistic_model",
    "blended_intraday_volatility",
    "build_technical_context",
    "close_to_close_volatility",
    "compute_vwap",
    "detect_market_regime",
    "directional_probabilities",
    "garman_klass_volatility",
    "infer_bar_seconds",
    "logit",
    "parkinson_volatility",
    "sigmoid",
    "update_vwap_accumulators",
]
