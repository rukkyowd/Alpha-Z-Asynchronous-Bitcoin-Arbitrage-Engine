"""Regression tests for trading-path execution and AI wiring fixes.

Usage:
    py backend/test_trade_path_fixes.py
"""

from __future__ import annotations

import asyncio
import logging
import math
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

_BACKEND_DIR = Path(__file__).resolve().parent
if str(_BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(_BACKEND_DIR))

import main as main_module
import bot.strategy as strategy_module
from bot.ai_agent import AIConfig, AIDecision, LocalAIAgent
from bot.execution import ClobExecutionEngine, ExecutionConfig, LiquidityCheckResult
from bot.models import (
    ActivePosition,
    ConfidenceLevel,
    DataSource,
    Direction,
    MarketOddsSnapshot,
    MarketRegime,
    MarketTick,
    PositionStatus,
    TechnicalContext,
    TradeSignal,
)
from bot.risk import EVComputation, RiskManager
from bot.state import EngineState
from bot.strategy import StrategyConfig, StrategyEngine

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


def _make_context(*, bayesian_probability: float = 0.62) -> TechnicalContext:
    return TechnicalContext(
        timestamp=datetime.now(timezone.utc),
        price=70000.0,
        strike_price=69950.0,
        bayesian_probability=bayesian_probability,
        bayesian_logit=0.5,
        expected_move_t=0.8,
    )


def _make_odds(*, entry_prob_pct: float = 40.0) -> MarketOddsSnapshot:
    return MarketOddsSnapshot(
        slug="btc-hourly-test",
        market_found=True,
        seconds_remaining=1800.0,
        reference_price=70000.0,
        strike_price=69950.0,
        up_token_id="token-up",
        down_token_id="token-down",
        up_public_prob_pct=entry_prob_pct,
        down_public_prob_pct=100.0 - entry_prob_pct,
        up_entry_prob_pct=entry_prob_pct,
        down_entry_prob_pct=100.0 - entry_prob_pct,
    )


def _make_signal(
    *,
    bet_size_usd: float = 100.0,
    token_price: float = 0.42,
    expected_value_pct: float = 12.0,
    model_context: TechnicalContext | None = None,
) -> TradeSignal:
    return TradeSignal(
        slug="btc-hourly-test",
        direction=Direction.UP,
        confidence=ConfidenceLevel.SCOUT,
        score=2,
        expected_value_pct=expected_value_pct,
        expected_value_gross_pct=15.0,
        true_probability_pct=68.0,
        market_probability_pct=40.0,
        entry_probability_pct=40.0,
        token_price=token_price,
        kelly_bet_usd=bet_size_usd,
        approved=True,
        needs_ai=model_context is not None,
        reasons=("Regression test",),
        model_context=model_context,
        price_cap=token_price,
        metadata={"effective_max_trade_pct": 0.05},
    )


def _make_liquidity(*, entry_price: float) -> LiquidityCheckResult:
    return LiquidityCheckResult(
        ok=True,
        mode="CLOB",
        reason="OK",
        entry_price=entry_price,
        best_bid=max(0.01, entry_price - 0.01),
        best_ask=entry_price,
        spread_pct=0.01,
        available_depth_usd=5000.0,
        expected_slippage_pct=0.0,
        market_impact_pct=0.0,
        levels=5,
    )


async def _build_state() -> EngineState:
    state = EngineState()
    await state.initialize()
    state.simulated_balance = 5000.0
    return state


class TestExecutionEngine(ClobExecutionEngine):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.check_liquidity_hook = None
        self.resolve_market_params_hook = None
        self.submit_limit_hook = None
        self.get_order_status_hook = None
        self.cancel_order_hook = None
        self.submit_market_hook = None

    def _is_ny_session(self) -> bool:
        return False

    async def check_liquidity_and_spread(self, *args, **kwargs):
        assert self.check_liquidity_hook is not None
        return await self.check_liquidity_hook(*args, **kwargs)

    async def _resolve_market_params(self, token_id: str):
        assert self.resolve_market_params_hook is not None
        return await self.resolve_market_params_hook(token_id)

    async def _submit_limit_order(self, *args, **kwargs):
        assert self.submit_limit_hook is not None
        return await self.submit_limit_hook(*args, **kwargs)

    async def _get_order_status(self, order_id: str):
        assert self.get_order_status_hook is not None
        return await self.get_order_status_hook(order_id)

    async def _cancel_order(self, order_id: str):
        assert self.cancel_order_hook is not None
        return await self.cancel_order_hook(order_id)

    async def _submit_market_order(self, *args, **kwargs):
        assert self.submit_market_hook is not None
        return await self.submit_market_hook(*args, **kwargs)


class NySessionExecutionEngine(TestExecutionEngine):
    def _is_ny_session(self) -> bool:
        return True


class RecordingRiskManager(RiskManager):
    def __init__(self):
        super().__init__()
        self.observed_market_prob_pct: list[float] = []

    def evaluate_trade(self, **kwargs):
        self.observed_market_prob_pct.append(float(kwargs["market_prob_pct"]))
        return super().evaluate_trade(**kwargs)


class StubStrategyEngine(StrategyEngine):
    def __init__(self, *, config: StrategyConfig | None = None, up_ev: EVComputation, down_ev: EVComputation):
        super().__init__(config=config)
        self._up_ev = up_ev
        self._down_ev = down_ev

    def compute_expected_value(
        self,
        context: TechnicalContext,
        odds: MarketOddsSnapshot,
        direction: Direction,
        bankroll: float,
        *,
        liquidity=None,
    ) -> EVComputation:
        if direction == Direction.UP:
            return self._up_ev
        return self._down_ev


def _make_ev(
    *,
    ev_pct: float,
    adjusted_token_price: float,
    true_prob_pct: float,
    market_prob_pct: float,
    approved: bool = True,
    kelly_bet_usd: float = 50.0,
) -> EVComputation:
    return EVComputation(
        ev_pct=ev_pct,
        ev_pct_gross=ev_pct + 2.0,
        kelly_bet_usd=kelly_bet_usd,
        raw_kelly_fraction=0.02,
        adjusted_kelly_fraction=0.02,
        kelly_confidence_multiplier=1.0,
        token_price=adjusted_token_price,
        adjusted_token_price=adjusted_token_price,
        slippage_cost_pct=0.2,
        market_impact_pct=0.1,
        time_decay_multiplier=1.0,
        edge_pct=ev_pct,
        true_prob_pct=true_prob_pct,
        market_prob_pct=market_prob_pct,
        approved=approved,
        available_depth_usd=1000.0,
        effective_max_trade_pct=0.02,
    )


async def test_maker_partial_fill_sizes_taker_remainder() -> None:
    state = await _build_state()
    risk_manager = RiskManager()
    engine = TestExecutionEngine(
        object(),
        config=ExecutionConfig(
            paper_trading=False,
            dry_run=False,
            maker_enabled=True,
            maker_window_secs=0.015,
            maker_poll_interval_secs=0.01,
            order_submit_timeout_secs=0.5,
        ),
        risk_manager=risk_manager,
    )
    engine.check_liquidity_hook = lambda *args, **kwargs: asyncio.sleep(0, result=_make_liquidity(entry_price=0.40))
    engine.resolve_market_params_hook = lambda token_id: asyncio.sleep(0, result=("0.01", False, 0))
    engine.submit_limit_hook = lambda *args, **kwargs: asyncio.sleep(0, result={"orderID": "maker-1", "status": "live"})

    taker_amounts: list[float] = []
    taker_limit_prices: list[float] = []
    status_calls = 0

    async def fake_get_order_status(order_id: str) -> dict[str, object]:
        nonlocal status_calls
        status_calls += 1
        if status_calls == 1:
            return {
                "status": "live",
                "transactions": [{"size": "100", "price": "0.40"}],
            }
        return {
            "status": "cancelled",
            "transactions": [{"size": "100", "price": "0.40"}],
        }

    async def fake_cancel_order(order_id: str) -> dict[str, object]:
        return {
            "status": "cancelled",
            "transactions": [{"size": "100", "price": "0.40"}],
        }

    async def fake_submit_market_order(
        token_id: str,
        *,
        amount: float,
        limit_price: float,
        order_type,
        tick_size: str,
        neg_risk: bool,
    ) -> dict[str, object]:
        taker_amounts.append(amount)
        taker_limit_prices.append(limit_price)
        taker_shares = amount / max(limit_price, 0.01)
        return {
            "status": "matched",
            "transactions": [{"size": f"{taker_shares:.6f}", "price": f"{limit_price:.4f}"}],
        }

    engine.get_order_status_hook = fake_get_order_status
    engine.cancel_order_hook = fake_cancel_order
    engine.submit_market_hook = fake_submit_market_order

    position = await engine.submit_entry_order(
        state,
        _make_signal(),
        _make_odds(entry_prob_pct=40.0),
        _make_context(),
    )

    expected_avg_price = 0.0
    if taker_limit_prices:
        expected_avg_price = 100.0 / (100.0 + (60.0 / taker_limit_prices[0]))
    _assert(position is not None, "Partial maker fill still opens a position")
    _assert(len(taker_amounts) == 1 and math.isclose(taker_amounts[0], 60.0, abs_tol=1e-6), "Taker only submits maker remainder", f"amounts={taker_amounts}")
    if position is not None:
        _assert(position.bet_size_usd <= 100.0 + 1e-6, "Combined fill cost stays within approved size", f"bet={position.bet_size_usd:.4f}")
        _assert(math.isclose(position.bought_price, expected_avg_price, rel_tol=1e-6), "Average entry price blends maker and taker fills", f"price={position.bought_price:.6f}")


async def test_authoritative_ev_uses_executable_price() -> None:
    state = await _build_state()
    risk_manager = RecordingRiskManager()
    engine = TestExecutionEngine(
        None,
        config=ExecutionConfig(
            paper_trading=True,
            dry_run=False,
            maker_enabled=False,
        ),
        risk_manager=risk_manager,
    )
    engine.check_liquidity_hook = lambda *args, **kwargs: asyncio.sleep(0, result=_make_liquidity(entry_price=0.55))
    engine.resolve_market_params_hook = lambda token_id: asyncio.sleep(0, result=("0.01", False, 0))

    position = await engine.submit_entry_order(
        state,
        _make_signal(bet_size_usd=75.0, token_price=0.58),
        _make_odds(entry_prob_pct=40.0),
        _make_context(),
    )

    _assert(position is not None, "Paper entry still succeeds after authoritative EV recheck")
    _assert(
        len(risk_manager.observed_market_prob_pct) == 1 and math.isclose(risk_manager.observed_market_prob_pct[0], 55.0, abs_tol=1e-6),
        "Authoritative EV is recomputed from size-adjusted executable price",
        f"market_prob_pct={risk_manager.observed_market_prob_pct}",
    )


async def test_high_ev_entry_premium_override_keeps_tradeable_edges() -> None:
    low_ev_state = await _build_state()
    high_ev_state = await _build_state()
    engine = TestExecutionEngine(
        None,
        config=ExecutionConfig(
            paper_trading=True,
            dry_run=False,
            maker_enabled=False,
            live_entry_slippage_cents=0.0,
            max_entry_premium_cents=0.018,
            high_ev_entry_premium_cents=0.02,
            high_ev_entry_premium_min_ev_pct=15.0,
        ),
        risk_manager=RiskManager(),
    )
    engine.check_liquidity_hook = lambda *args, **kwargs: asyncio.sleep(0, result=_make_liquidity(entry_price=0.421))
    engine.resolve_market_params_hook = lambda token_id: asyncio.sleep(0, result=("0.01", False, 0))

    low_ev_position = await engine.submit_entry_order(
        low_ev_state,
        _make_signal(token_price=0.411, expected_value_pct=12.0),
        _make_odds(entry_prob_pct=40.0),
        _make_context(),
    )
    high_ev_position = await engine.submit_entry_order(
        high_ev_state,
        _make_signal(token_price=0.411, expected_value_pct=18.0),
        _make_odds(entry_prob_pct=40.0),
        _make_context(),
    )

    _assert(low_ev_position is None, "Base entry premium cap still blocks low-EV overpays")
    _assert(high_ev_position is not None, "High-EV setups can use the widened ~2.0c premium cap")


def test_ny_session_premium_relaxes_once_ev_is_real() -> None:
    engine = NySessionExecutionEngine(
        None,
        config=ExecutionConfig(
            ny_session_max_entry_premium_cents=0.015,
            ny_session_relaxed_entry_min_ev_pct=6.0,
            max_entry_premium_cents=0.018,
            high_ev_entry_premium_cents=0.02,
            high_ev_entry_premium_min_ev_pct=15.0,
        ),
    )

    low_ev_cap = engine._active_entry_premium_cap(_make_signal(expected_value_pct=5.5))
    mid_ev_cap = engine._active_entry_premium_cap(_make_signal(expected_value_pct=8.0))
    high_ev_cap = engine._active_entry_premium_cap(_make_signal(expected_value_pct=18.0))

    _assert(math.isclose(low_ev_cap, 0.015, abs_tol=1e-9), "NY session keeps the tighter 1.5c cap for marginal EV")
    _assert(math.isclose(mid_ev_cap, 0.018, abs_tol=1e-9), "NY session relaxes back to the base 1.8c cap once EV clears 6%")
    _assert(math.isclose(high_ev_cap, 0.02, abs_tol=1e-9), "High-EV setups still unlock the 2.0c premium override in NY")


async def test_strategy_skips_marginal_score2_ai_calls() -> None:
    original_apply_probabilistic_model = strategy_module.apply_probabilistic_model
    strategy_module.apply_probabilistic_model = lambda context, **kwargs: context
    try:
        engine = StubStrategyEngine(
            config=StrategyConfig(
                score2_min_ev_pct=6.0,
                min_ev_pct_to_call_ai=1.0,
                ai_score1_min_ev_pct=18.0,
                ai_score2_min_ev_pct=8.0,
                ai_score3_min_ev_pct=3.0,
            ),
            up_ev=_make_ev(ev_pct=7.0, adjusted_token_price=0.42, true_prob_pct=62.0, market_prob_pct=40.0),
            down_ev=_make_ev(ev_pct=1.0, adjusted_token_price=0.58, true_prob_pct=38.0, market_prob_pct=60.0),
        )
        context = TechnicalContext(
            timestamp=datetime.now(timezone.utc),
            price=70000.0,
            strike_price=69950.0,
            vwap=69990.0,
            vwap_distance=10.0,
            ema_9=70005.0,
            ema_21=69995.0,
            ema_spread_pct=0.0001,
            rsi_14=55.0,
            current_volume=100.0,
            vol_sma_20=100.0,
            cvd_candle_delta=50.0,
            adaptive_cvd_threshold=100.0,
            market_regime=MarketRegime.RANGE,
            bayesian_probability=0.62,
            bayesian_logit=0.5,
            expected_move_t=0.7,
            realized_volatility=0.002,
            parkinson_volatility=0.002,
            garman_klass_volatility=0.002,
        )

        signal = await engine.evaluate_trade_signal(
            context,
            _make_odds(entry_prob_pct=40.0),
            5000.0,
            slug="btc-hourly-test",
        )
    finally:
        strategy_module.apply_probabilistic_model = original_apply_probabilistic_model

    _assert(signal.direction == Direction.SKIP, "Score-2 setups below the 8% AI floor are skipped before AI is called", f"signal={signal}")
    _assert(
        bool(signal.reasons) and "below AI trigger (8.00%)" in signal.reasons[0],
        "Skip reason reports the tighter score-aware AI threshold",
        f"reasons={signal.reasons}",
    )


async def test_strategy_skips_thin_raw_edge() -> None:
    original_apply_probabilistic_model = strategy_module.apply_probabilistic_model
    strategy_module.apply_probabilistic_model = lambda context, **kwargs: context
    try:
        engine = StubStrategyEngine(
            config=StrategyConfig(
                score2_min_ev_pct=8.0,
                min_raw_edge_cents=1.0,
            ),
            up_ev=_make_ev(ev_pct=10.0, adjusted_token_price=0.615, true_prob_pct=62.0, market_prob_pct=40.0),
            down_ev=_make_ev(ev_pct=1.0, adjusted_token_price=0.58, true_prob_pct=38.0, market_prob_pct=60.0),
        )
        context = TechnicalContext(
            timestamp=datetime.now(timezone.utc),
            price=70000.0,
            strike_price=69950.0,
            vwap=69990.0,
            vwap_distance=10.0,
            ema_9=70005.0,
            ema_21=69995.0,
            ema_spread_pct=0.0001,
            rsi_14=55.0,
            current_volume=100.0,
            vol_sma_20=100.0,
            cvd_candle_delta=50.0,
            adaptive_cvd_threshold=100.0,
            market_regime=MarketRegime.RANGE,
            bayesian_probability=0.62,
            bayesian_logit=0.5,
            expected_move_t=0.7,
            realized_volatility=0.002,
            parkinson_volatility=0.002,
            garman_klass_volatility=0.002,
        )

        signal = await engine.evaluate_trade_signal(
            context,
            _make_odds(entry_prob_pct=40.0),
            5000.0,
            slug="btc-hourly-test",
        )
    finally:
        strategy_module.apply_probabilistic_model = original_apply_probabilistic_model

    _assert(signal.direction == Direction.SKIP, "Thin raw-edge setups are skipped before the engine leans on AI", f"signal={signal}")
    _assert(
        bool(signal.reasons) and "Raw edge too thin (0.50c < 1.0c)" in signal.reasons[0],
        "Skip reason reports the new minimum raw-edge filter",
        f"reasons={signal.reasons}",
    )


async def test_ai_validation_uses_strategy_context() -> None:
    state = await _build_state()
    raw_context = _make_context(bayesian_probability=0.41)
    model_context = _make_context(bayesian_probability=0.83)
    signal = _make_signal(model_context=model_context)
    odds = _make_odds(entry_prob_pct=40.0)
    runtime = SimpleNamespace(
        http_session=None,
        state=state,
        ai_agent=object(),
        ai_config=AIConfig(),
        paper_trading=False,
    )

    captured_probability = 0.0
    original_call_local_ai = main_module.call_local_ai

    async def fake_call_local_ai(session, state_obj, signal_obj, context_obj, odds_obj, **kwargs):
        nonlocal captured_probability
        captured_probability = context_obj.bayesian_probability
        return AIDecision(decision=Direction.UP, raw_response="FINAL:UP", reason="AI confirmed favored direction")

    main_module.call_local_ai = fake_call_local_ai
    try:
        validated_signal, reason = await main_module.maybe_validate_with_ai(runtime, signal, raw_context, odds)
    finally:
        main_module.call_local_ai = original_call_local_ai

    _assert(math.isclose(captured_probability, model_context.bayesian_probability, abs_tol=1e-9), "AI gate uses the strategy's prepared model context", f"captured={captured_probability:.4f}")
    _assert(validated_signal.ai_validated and validated_signal.approved and not validated_signal.needs_ai, "AI approval preserves validated signal state", f"signal={validated_signal}")
    _assert(reason == "", "AI approval returns no rejection reason", f"reason={reason}")


async def test_build_context_prefers_live_price_over_last_closed_candle() -> None:
    state = await _build_state()
    closed_tick = MarketTick(
        timestamp=datetime.now(timezone.utc),
        source=DataSource.BINANCE,
        price=70000.0,
        open=69950.0,
        high=70020.0,
        low=69920.0,
        close=70000.0,
        volume=100.0,
        quote_volume=7000000.0,
        trade_count=50,
        is_closed=True,
        event_time_ms=1_000,
        close_time_ms=2_000,
    )
    await state.append_candle(closed_tick)
    await state.set_live_tick(price=70525.0)

    runtime = SimpleNamespace(state=state, latest_context=None)
    context = await main_module.build_context_from_state(runtime)

    _assert(context is not None, "Fresh context can be rebuilt from state after a closed bar")
    if context is not None:
        _assert(
            math.isclose(context.price, 70525.0, abs_tol=1e-9),
            "Context prefers the latest live price even when the last candle is closed",
            f"price={context.price:.2f}",
        )


async def test_ai_validation_respects_recent_veto_cooldown() -> None:
    state = await _build_state()
    await state.record_ai_state(
        "btc-hourly-test",
        ai_calls=1,
        last_veto_ts=time.time(),
        last_veto_ev_pct=12.0,
        last_veto_direction=Direction.UP,
        last_veto_score=2,
        last_veto_token_price=0.42,
    )
    signal = _make_signal(
        token_price=0.43,
        expected_value_pct=13.0,
        model_context=_make_context(bayesian_probability=0.79),
    )
    runtime = SimpleNamespace(
        http_session=None,
        state=state,
        ai_agent=object(),
        ai_config=AIConfig(
            veto_cooldown_seconds=45.0,
            veto_min_ev_improvement_pct=2.0,
            veto_min_score_improvement=1,
            veto_min_token_price_move_cents=2.0,
        ),
        paper_trading=False,
    )

    ai_called = False
    original_call_local_ai = main_module.call_local_ai

    async def fake_call_local_ai(*args, **kwargs):
        nonlocal ai_called
        ai_called = True
        return AIDecision(decision=Direction.UP, raw_response="FINAL:UP", reason="AI confirmed favored direction")

    main_module.call_local_ai = fake_call_local_ai
    try:
        validated_signal, reason = await main_module.maybe_validate_with_ai(runtime, signal, _make_context(), _make_odds())
    finally:
        main_module.call_local_ai = original_call_local_ai

    _assert(not ai_called, "Recent AI vetoes suppress near-identical retries during cooldown")
    _assert(
        not validated_signal.approved and "AI veto cooldown active" in reason,
        "Cooldown rejection explains the required improvement before re-querying AI",
        f"reason={reason}",
    )


async def test_strategy_blocks_expensive_entries_without_extra_edge() -> None:
    original_apply_probabilistic_model = strategy_module.apply_probabilistic_model
    strategy_module.apply_probabilistic_model = lambda context, **kwargs: context
    try:
        engine = StubStrategyEngine(
            config=StrategyConfig(
                expensive_entry_price_floor=0.65,
                expensive_entry_min_score=3,
                expensive_entry_min_ev_pct=12.0,
                expensive_entry_min_ev_lead_pct=4.0,
                expensive_entry_min_true_prob_pct=68.0,
            ),
            up_ev=_make_ev(ev_pct=10.0, adjusted_token_price=0.70, true_prob_pct=72.0, market_prob_pct=40.0),
            down_ev=_make_ev(ev_pct=1.0, adjusted_token_price=0.30, true_prob_pct=31.0, market_prob_pct=60.0),
        )
        context = TechnicalContext(
            timestamp=datetime.now(timezone.utc),
            price=70020.0,
            strike_price=69950.0,
            vwap=70000.0,
            vwap_distance=20.0,
            ema_9=70025.0,
            ema_21=70005.0,
            ema_spread_pct=0.0003,
            rsi_14=56.0,
            current_volume=120.0,
            vol_sma_20=100.0,
            cvd_candle_delta=50.0,
            adaptive_cvd_threshold=100.0,
            market_regime=MarketRegime.BULL_TREND,
            bayesian_probability=0.64,
            bayesian_logit=0.58,
            expected_move_t=0.8,
            realized_volatility=0.002,
            parkinson_volatility=0.002,
            garman_klass_volatility=0.002,
        )

        signal = await engine.evaluate_trade_signal(
            context,
            _make_odds(entry_prob_pct=40.0),
            5000.0,
            slug="btc-hourly-test",
        )
    finally:
        strategy_module.apply_probabilistic_model = original_apply_probabilistic_model

    _assert(signal.direction == Direction.SKIP, "Expensive entries need stronger EV before the engine will buy them", f"signal={signal}")
    _assert(
        bool(signal.reasons) and "Expensive entry blocked: px 0.700 needs EV >= 12.00%" in signal.reasons[0],
        "Expensive-entry rejection reports the tighter EV threshold",
        f"reasons={signal.reasons}",
    )


def test_position_heartbeat_reports_soft_and_hard_stops() -> None:
    messages: list[str] = []

    class _Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            messages.append(record.getMessage())

    handler = _Capture(level=logging.INFO)
    runtime = SimpleNamespace(position_heartbeat_ts={})
    position = ActivePosition(
        slug="btc-hourly-test",
        decision=Direction.UP,
        token_id="token-up",
        strike=69950.0,
        bet_size_usd=100.0,
        bought_price=0.70,
        status=PositionStatus.OPEN,
        current_token_price=0.68,
        mark_price=0.68,
        tp_delta=0.13,
        sl_delta=-0.12,
        hard_sl_delta=-0.18,
        tp_token_price=0.83,
        sl_token_price=0.58,
        hard_sl_token_price=0.52,
        seconds_remaining=2400,
    )

    main_module.logger.addHandler(handler)
    previous_level = main_module.logger.level
    main_module.logger.setLevel(logging.INFO)
    try:
        main_module.log_position_heartbeat(runtime, position, min_repeat_secs=0.0)
    finally:
        main_module.logger.setLevel(previous_level)
        main_module.logger.removeHandler(handler)

    _assert(
        any("Soft SL:" in message and "Hard SL:" in message for message in messages),
        "Heartbeat logs show both soft and hard stop levels",
        detail=messages[-1] if messages else "",
    )


class _SlowResponse:
    def __init__(self, delay_seconds: float):
        self.delay_seconds = delay_seconds

    async def __aenter__(self):
        await asyncio.sleep(self.delay_seconds)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def raise_for_status(self) -> None:
        return None

    async def json(self) -> dict[str, object]:
        return {"choices": [{"message": {"content": "FINAL:UP"}}]}


class _SlowSession:
    def __init__(self, delay_seconds: float):
        self.delay_seconds = delay_seconds

    def post(self, *args, **kwargs):
        return _SlowResponse(self.delay_seconds)


async def test_ai_budget_limits_inline_latency() -> None:
    state = await _build_state()
    agent = LocalAIAgent(
        AIConfig(
            timeout_total_seconds=1.0,
            decision_budget_seconds=0.05,
            max_retries=3,
            retry_delay_seconds=0.02,
        )
    )

    started = time.perf_counter()
    decision = await agent.call_local_ai(
        _SlowSession(delay_seconds=0.20),
        state,
        _make_signal(),
        _make_context(),
        _make_odds(),
    )
    elapsed = time.perf_counter() - started

    _assert(decision.decision == Direction.SKIP and decision.transport_error, "Slow AI calls fail closed under the decision budget", f"decision={decision}")
    _assert(elapsed < 0.20, "AI decision budget caps wall-clock latency", f"elapsed={elapsed:.3f}s")


def test_live_ws_sender_is_throttled() -> None:
    _assert(
        main_module.WS_PUSH_INTERVAL_SECS >= 0.25,
        "Live websocket cadence is throttled away from micro-tick pushes",
        f"interval={main_module.WS_PUSH_INTERVAL_SECS:.3f}s",
    )
    _assert(
        main_module.WS_SEND_TIMEOUT_SECS >= 1.0,
        "Live websocket sender has a longer timeout than the push interval",
        f"timeout={main_module.WS_SEND_TIMEOUT_SECS:.3f}s",
    )


async def run() -> None:
    print("\n" + "=" * 52)
    print("  Alpha-Z Trading Path Fixes - Regression Suite")
    print("=" * 52)

    print("\n--- Maker Partial Fill ---")
    await test_maker_partial_fill_sizes_taker_remainder()

    print("\n--- Executable EV Recheck ---")
    await test_authoritative_ev_uses_executable_price()
    await test_high_ev_entry_premium_override_keeps_tradeable_edges()
    test_ny_session_premium_relaxes_once_ev_is_real()
    await test_strategy_skips_marginal_score2_ai_calls()
    await test_strategy_skips_thin_raw_edge()

    print("\n--- AI Context Wiring ---")
    await test_ai_validation_uses_strategy_context()
    await test_ai_validation_respects_recent_veto_cooldown()
    await test_build_context_prefers_live_price_over_last_closed_candle()
    await test_strategy_blocks_expensive_entries_without_extra_edge()

    print("\n--- AI Latency Budget ---")
    await test_ai_budget_limits_inline_latency()

    print("\n--- Live Runtime Pacing ---")
    test_live_ws_sender_is_throttled()
    test_position_heartbeat_reports_soft_and_hard_stops()

    print("\n" + "=" * 52)
    print(f"PASSED: {PASSED}")
    print(f"FAILED: {FAILED}")
    print("=" * 52)

    if FAILED:
        raise SystemExit(1)


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
