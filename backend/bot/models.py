from __future__ import annotations

"""
Target bot package split:
- bot/models.py
- bot/state.py
- bot/data_streams.py
- bot/indicators.py
- bot/strategy.py
- bot/ai_agent.py
- bot/execution.py
- bot/risk.py
"""

from collections import deque
from dataclasses import asdict, dataclass, field, is_dataclass, replace
from datetime import date, datetime, timezone
from enum import StrEnum
from typing import Any, Mapping


def _serialize(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, deque):
        return [_serialize(item) for item in value]
    if isinstance(value, tuple):
        return [_serialize(item) for item in value]
    if isinstance(value, list):
        return [_serialize(item) for item in value]
    if isinstance(value, set):
        return sorted(_serialize(item) for item in value)
    if isinstance(value, dict):
        return {str(key): _serialize(item) for key, item in value.items()}
    if is_dataclass(value):
        return {key: _serialize(item) for key, item in asdict(value).items()}
    return value


class SerializableModel:
    def to_dict(self) -> dict[str, Any]:
        return _serialize(self)

    def clone(self):
        return replace(self)


class Direction(StrEnum):
    UP = "UP"
    DOWN = "DOWN"
    SKIP = "SKIP"
    UNKNOWN = "UNKNOWN"


class ConfidenceLevel(StrEnum):
    LOW = "Low"
    MEDIUM = "Medium"
    SCOUT = "Scout"
    HIGH = "High"
    CRITICAL = "Critical"


class PositionStatus(StrEnum):
    ENTERING = "ENTERING"
    OPEN = "OPEN"
    CLOSING = "CLOSING"
    RESOLVING = "RESOLVING"
    UNCERTAIN = "UNCERTAIN"
    CLOSED = "CLOSED"


class MarketRegime(StrEnum):
    BULL_TREND = "BULL_TREND"
    BEAR_TREND = "BEAR_TREND"
    RANGE = "RANGE"
    VOLATILE_RANGE = "VOLATILE_RANGE"
    BREAKOUT = "BREAKOUT"
    UNKNOWN = "UNKNOWN"


class DataSource(StrEnum):
    BINANCE = "BINANCE"
    POLYMARKET_GAMMA = "POLYMARKET_GAMMA"
    POLYMARKET_CLOB = "POLYMARKET_CLOB"
    INTERNAL = "INTERNAL"


class MarketResolution(StrEnum):
    STRIKE = "STRIKE"
    CANDLE_OPEN = "CANDLE_OPEN"
    UNKNOWN = "UNKNOWN"


@dataclass(slots=True, kw_only=True)
class MarketTick(SerializableModel):
    timestamp: datetime
    symbol: str = "BTCUSDT"
    source: DataSource = DataSource.BINANCE
    price: float = 0.0
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    close: float = 0.0
    volume: float = 0.0
    quote_volume: float = 0.0
    trade_count: int = 0
    is_closed: bool = False
    trade_id: int = 0
    event_time_ms: int = 0
    close_time_ms: int = 0
    taker_is_buyer_maker: bool | None = None

    @property
    def resolved_price(self) -> float:
        for candidate in (self.price, self.close, self.open):
            if candidate > 0:
                return candidate
        return 0.0

    @property
    def body_size(self) -> float:
        return abs(self.close - self.open)

    @property
    def upper_wick(self) -> float:
        return self.high - max(self.open, self.close)

    @property
    def lower_wick(self) -> float:
        return min(self.open, self.close) - self.low

    @property
    def structure(self) -> str:
        return "BULLISH" if self.close >= self.open else "BEARISH"


@dataclass(slots=True, kw_only=True)
class MarketOddsSnapshot(SerializableModel):
    slug: str
    market_found: bool = False
    market_slug: str = ""
    seconds_remaining: float = 0.0
    reference_price: float = 0.0
    strike_price: float = 0.0
    market_resolution: MarketResolution = MarketResolution.UNKNOWN
    market_end_time: datetime | None = None
    market_category: str = ""
    fees_enabled: bool = False
    fee_curve_rate: float = 0.0
    fee_curve_exponent: float = 0.0
    sdk_fee_rate_bps: int = 0
    up_token_id: str = ""
    down_token_id: str = ""
    up_public_prob_pct: float = 0.0
    down_public_prob_pct: float = 0.0
    up_entry_prob_pct: float = 0.0
    down_entry_prob_pct: float = 0.0
    up_best_bid: float | None = None
    up_best_ask: float | None = None
    down_best_bid: float | None = None
    down_best_ask: float | None = None
    outcome_labels: tuple[str, ...] = field(default_factory=tuple)
    fetched_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __post_init__(self) -> None:
        """Runtime validation — clamp probabilities and sanitize inputs."""
        self.up_public_prob_pct = max(0.0, min(100.0, self.up_public_prob_pct))
        self.down_public_prob_pct = max(0.0, min(100.0, self.down_public_prob_pct))
        self.up_entry_prob_pct = max(0.0, min(100.0, self.up_entry_prob_pct))
        self.down_entry_prob_pct = max(0.0, min(100.0, self.down_entry_prob_pct))
        self.seconds_remaining = max(0.0, self.seconds_remaining)

    def public_prob_pct(self, direction: Direction) -> float:
        if direction == Direction.UP:
            return self.up_public_prob_pct
        if direction == Direction.DOWN:
            return self.down_public_prob_pct
        return 0.0

    def entry_prob_pct(self, direction: Direction) -> float:
        if direction == Direction.UP:
            return self.up_entry_prob_pct or self.up_public_prob_pct
        if direction == Direction.DOWN:
            return self.down_entry_prob_pct or self.down_public_prob_pct
        return 0.0

    def fair_public_prob_pct(self, direction: Direction) -> float:
        total = self.up_public_prob_pct + self.down_public_prob_pct
        if total <= 0:
            return self.public_prob_pct(direction)
        if direction == Direction.UP:
            return (self.up_public_prob_pct / total) * 100.0
        if direction == Direction.DOWN:
            return (self.down_public_prob_pct / total) * 100.0
        return 0.0

    def fair_entry_prob_pct(self, direction: Direction) -> float:
        total = self.up_entry_prob_pct + self.down_entry_prob_pct
        if total <= 0:
            return self.entry_prob_pct(direction)
        if direction == Direction.UP:
            return (self.up_entry_prob_pct / total) * 100.0
        if direction == Direction.DOWN:
            return (self.down_entry_prob_pct / total) * 100.0
        return 0.0

    def public_vig_pct(self) -> float:
        return max(0.0, (self.up_public_prob_pct + self.down_public_prob_pct) - 100.0)

    def entry_vig_pct(self) -> float:
        return max(0.0, (self.up_entry_prob_pct + self.down_entry_prob_pct) - 100.0)

    def token_id(self, direction: Direction) -> str:
        if direction == Direction.UP:
            return self.up_token_id
        if direction == Direction.DOWN:
            return self.down_token_id
        return ""

    def effective_taker_fee_rate(self, direction: Direction, entry_price: float | None = None) -> float:
        # Prefer SDK-authoritative fee rate if available
        if self.sdk_fee_rate_bps > 0:
            reference_price = entry_price
            if reference_price is None:
                reference_price = self.entry_prob_pct(direction) / 100.0
            bounded_price = min(max(reference_price, 0.001), 0.999)
            probability_term = bounded_price * (1.0 - bounded_price)
            return max(0.0, (self.sdk_fee_rate_bps / 10000.0) * (probability_term ** max(self.fee_curve_exponent, 1.0)))

        # Fallback to heuristic fee curve
        if not self.fees_enabled or self.fee_curve_rate <= 0 or self.fee_curve_exponent <= 0:
            return 0.0

        reference_price = entry_price
        if reference_price is None:
            reference_price = self.entry_prob_pct(direction) / 100.0

        bounded_price = min(max(reference_price, 0.001), 0.999)
        probability_term = bounded_price * (1.0 - bounded_price)
        return max(0.0, self.fee_curve_rate * (probability_term ** self.fee_curve_exponent))


@dataclass(slots=True, kw_only=True)
class TechnicalContext(SerializableModel):
    timestamp: datetime
    price: float
    strike_price: float = 0.0
    vwap: float = 0.0
    vwap_distance: float = 0.0
    price_vs_vwap_pct: float = 0.0
    ema_9: float = 0.0
    ema_21: float = 0.0
    ema_spread_pct: float = 0.0
    rsi_14: float = 50.0
    cvd_total: float = 0.0
    cvd_candle_delta: float = 0.0
    cvd_1min_delta: float = 0.0
    current_volume: float = 0.0
    vol_sma_20: float = 0.0
    parkinson_volatility: float = 0.0
    garman_klass_volatility: float = 0.0
    realized_volatility: float = 0.0
    adaptive_atr_floor: float = 0.0
    adaptive_cvd_threshold: float = 0.0
    market_regime: MarketRegime = MarketRegime.UNKNOWN
    base_probability: float = 0.5
    bayesian_logit: float = 0.0
    bayesian_probability: float = 0.5
    indicator_logit_shift: float = 0.0
    expected_move_sigma: float = 0.0
    expected_move_t: float = 0.0
    notes: tuple[str, ...] = field(default_factory=tuple)


@dataclass(slots=True, kw_only=True)
class TradeSignal(SerializableModel):
    slug: str
    direction: Direction
    confidence: ConfidenceLevel
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    score: int = 0
    max_score: int = 4
    bonus_score: int = 0
    expected_value_pct: float = 0.0
    expected_value_gross_pct: float = 0.0
    true_probability_pct: float = 0.0
    market_probability_pct: float = 0.0
    entry_probability_pct: float = 0.0
    token_price: float = 0.0
    kelly_bet_usd: float = 0.0
    approved: bool = False
    needs_ai: bool = False
    ai_validated: bool = False
    expected_slippage_pct: float = 0.0
    market_impact_pct: float = 0.0
    price_cap: float = 0.0
    reasons: tuple[str, ...] = field(default_factory=tuple)
    metadata: dict[str, float | str | bool] = field(default_factory=dict)


@dataclass(slots=True, kw_only=True)
class ActivePosition(SerializableModel):
    slug: str
    decision: Direction
    token_id: str
    strike: float
    bet_size_usd: float
    bought_price: float
    status: PositionStatus = PositionStatus.ENTERING
    score: int = 0
    bonus_score: int = 0
    entry_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    mark_price: float = 0.0
    current_token_price: float = 0.0
    live_underlying_price: float = 0.0
    entry_underlying_price: float = 0.0
    tp_delta: float = 0.0
    sl_delta: float = 0.0
    tp_token_price: float = 0.0
    sl_token_price: float = 0.0
    seconds_remaining: int = 0
    sl_disabled: bool = False
    sl_breach_count: int = 0
    tp_gate_logged: bool = False
    tp_armed: bool = False
    tp_peak_delta: float = 0.0
    tp_lock_floor_delta: float = 0.0
    signals: tuple[str, ...] = field(default_factory=tuple)
    notes: tuple[str, ...] = field(default_factory=tuple)
    ml_features: dict[str, float | str | int | bool] = field(default_factory=dict)

    @property
    def shares_owned(self) -> float:
        if self.bought_price <= 0:
            return 0.0
        return self.bet_size_usd / self.bought_price

    @property
    def unrealized_pnl(self) -> float:
        if self.bought_price <= 0 or self.current_token_price <= 0:
            return 0.0
        return self.bet_size_usd * ((self.current_token_price / self.bought_price) - 1.0)


@dataclass(slots=True, kw_only=True)
class ReentryState(SerializableModel):
    closed_trades: int = 0
    last_exit_ts: float = 0.0
    last_exit_reason: str = ""
    last_exit_direction: Direction = Direction.UNKNOWN
    last_entry_ev_pct: float = 0.0


@dataclass(slots=True, kw_only=True)
class AISlugState(SerializableModel):
    ai_calls: int = 0
    last_veto_ts: float = 0.0
    last_veto_ev_pct: float = 0.0


@dataclass(slots=True, kw_only=True)
class ExecutionFailureState(SerializableModel):
    last_fail_ts: float = 0.0
    reason: str = ""
    ev_pct: float = 0.0


@dataclass(slots=True, kw_only=True)
class EdgeSnapshot(SerializableModel):
    slug: str = ""
    direction: Direction = Direction.UNKNOWN
    up_math_prob: float = 0.0
    down_math_prob: float = 0.0
    up_poly_prob: float = 0.0
    down_poly_prob: float = 0.0
    up_public_prob: float = 0.0
    down_public_prob: float = 0.0
    up_edge: float = 0.0
    down_edge: float = 0.0
    best_edge: float = 0.0
    best_ev_pct: float = 0.0


@dataclass(slots=True, kw_only=True)
class SignalAlignmentSnapshot(SerializableModel):
    direction: Direction = Direction.UNKNOWN
    score: int = 0
    max_score: int = 4
    vwap: bool = False
    rsi: bool = False
    volume: bool = False
    cvd: bool = False


@dataclass(slots=True, kw_only=True)
class CVDSnapshot(SerializableModel):
    delta: float = 0.0
    one_min_delta: float = 0.0
    threshold: float = 0.0
    divergence: str = "NONE"
    divergence_strength: float = 0.0


@dataclass(slots=True, kw_only=True)
class ExecutionTimingSnapshot(SerializableModel):
    signal_generation_ms: float = 0.0
    ai_inference_ms: float = 0.0
    clob_request_ms: float = 0.0
    confirmation_ms: float = 0.0
    total_ms: float = 0.0
    updated_at: float = 0.0


@dataclass(slots=True, kw_only=True)
class AdaptiveThresholdSnapshot(SerializableModel):
    atr_min: float = 0.0
    cvd_threshold: float = 0.0
    volatility_lookback: int = 0


@dataclass(slots=True, kw_only=True)
class AIInteraction(SerializableModel):
    prompt: str = "No AI calls yet."
    response: str = "N/A"
    timestamp: str = ""


@dataclass(slots=True, kw_only=True)
class RuntimeCounters(SerializableModel):
    total_wins: int = 0
    total_losses: int = 0
    ai_call_count: int = 0
    ai_consecutive_failures: int = 0
    ai_circuit_open_until: float = 0.0
    ai_call_in_flight: tuple[str, ...] = field(default_factory=tuple)
    last_ai_response_ms: float = 0.0
    ai_response_ema_ms: float = 0.0
    kill_switch_enabled: bool = False
    simulated_balance: float = 0.0


@dataclass(slots=True, kw_only=True)
class DrawdownGuardSnapshot(SerializableModel):
    bankroll: float = 0.0
    current_balance: float = 0.0
    daily_pnl: float = 0.0
    max_trade_pct: float = 0.0
    max_daily_loss_pct: float = 0.0
    trades_this_hour: int = 0


def trade_signal_from_mapping(payload: Mapping[str, Any], *, slug: str) -> TradeSignal:
    direction_raw = str(payload.get("decision", Direction.UNKNOWN.value)).upper()
    direction = Direction(direction_raw) if direction_raw in Direction._value2member_map_ else Direction.UNKNOWN

    confidence_raw = str(payload.get("confidence", ConfidenceLevel.MEDIUM.value))
    confidence = (
        ConfidenceLevel(confidence_raw)
        if confidence_raw in ConfidenceLevel._value2member_map_
        else ConfidenceLevel.MEDIUM
    )

    reasons_raw = payload.get("reason", "")
    if isinstance(reasons_raw, str):
        reasons = tuple(part.strip() for part in reasons_raw.split("|") if part.strip())
    elif isinstance(reasons_raw, (list, tuple)):
        reasons = tuple(str(part).strip() for part in reasons_raw if str(part).strip())
    else:
        reasons = ()

    return TradeSignal(
        slug=slug,
        direction=direction,
        confidence=confidence,
        score=int(payload.get("score", 0) or 0),
        bonus_score=int(payload.get("bonus", 0) or 0),
        expected_value_pct=float(payload.get("ev_pct", 0.0) or 0.0),
        expected_value_gross_pct=float(payload.get("ev_pct_gross", 0.0) or 0.0),
        true_probability_pct=float(payload.get("true_prob_pct", 0.0) or 0.0),
        market_probability_pct=float(payload.get("market_prob_pct", 0.0) or 0.0),
        entry_probability_pct=float(payload.get("entry_prob_pct", 0.0) or 0.0),
        token_price=float(payload.get("token_price", 0.0) or 0.0),
        kelly_bet_usd=float(payload.get("bet_size", payload.get("kelly_bet", 0.0)) or 0.0),
        approved=bool(payload.get("approved", False)),
        needs_ai=bool(payload.get("needs_ai", False)),
        ai_validated="AI confirmed" in " | ".join(reasons),
        expected_slippage_pct=float(payload.get("slippage_cost_pct", 0.0) or 0.0),
        market_impact_pct=float(payload.get("market_impact_pct", 0.0) or 0.0),
        price_cap=float(payload.get("price_cap", 0.0) or 0.0),
        reasons=reasons,
    )
