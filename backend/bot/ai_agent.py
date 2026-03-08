from __future__ import annotations

import asyncio
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone

import aiohttp

from .models import AIInteraction, Direction, MarketOddsSnapshot, TechnicalContext, TradeSignal
from .state import EngineState


@dataclass(slots=True, frozen=True)
class AIConfig:
    url: str = "http://localhost:11434/v1/chat/completions"
    model: str = "llama3.2:3b-instruct-q4_K_M"
    timeout_total_seconds: float = 30.0
    max_retries: int = 2
    retry_delay_seconds: float = 2.0
    max_calls_per_slug: int = 6
    max_calls_per_slug_paper: int = 18
    circuit_breaker_threshold: int = 5
    circuit_breaker_base_cooldown_seconds: float = 30.0
    circuit_breaker_max_cooldown_seconds: float = 300.0
    temperature: float = 0.0
    max_tokens: int = 12


@dataclass(slots=True, frozen=True)
class AIDecision:
    decision: Direction
    raw_response: str
    reason: str
    response_ms: float = 0.0
    circuit_open: bool = False
    transport_error: bool = False
    malformed: bool = False

    @property
    def approved(self) -> bool:
        return self.decision in (Direction.UP, Direction.DOWN)


class LocalAIAgent:
    __slots__ = ("config",)

    def __init__(self, config: AIConfig | None = None):
        self.config = config or AIConfig()

    def build_prompt(
        self,
        signal: TradeSignal,
        context: TechnicalContext,
        odds_snapshot: MarketOddsSnapshot,
        *,
        counter_signal: TradeSignal | None = None,
    ) -> str:
        favored = signal.direction.value
        entry_price = signal.token_price or (odds_snapshot.entry_prob_pct(signal.direction) / 100.0)
        opposite = Direction.DOWN if signal.direction == Direction.UP else Direction.UP
        opposite_prob = odds_snapshot.entry_prob_pct(opposite)
        up_entry_prob = odds_snapshot.entry_prob_pct(Direction.UP)
        down_entry_prob = odds_snapshot.entry_prob_pct(Direction.DOWN)
        counter_line = ""
        if counter_signal is not None:
            counter_line = (
                f"Counter-hypothesis: {counter_signal.direction.value} | "
                f"EV={counter_signal.expected_value_pct:.2f}% | score={counter_signal.score}/{counter_signal.max_score}\n"
            )

        return (
            "You are validating a BTC Polymarket binary trade for a fail-closed execution engine.\n"
            "Decide if the favored direction is supported by the quantitative context.\n"
            "If the evidence is mixed, contradictory, or low quality, output SKIP.\n\n"
            f"Favored direction: {favored}\n"
            f"Trade EV (net): {signal.expected_value_pct:.2f}%\n"
            f"Trade EV (gross): {signal.expected_value_gross_pct:.2f}%\n"
            f"Technical score: {signal.score}/{signal.max_score}\n"
            f"Confidence: {signal.confidence.value}\n"
            f"Entry token price: {entry_price:.4f}\n"
            f"Market probability: {signal.market_probability_pct:.2f}%\n"
            f"Bayesian probability: {context.bayesian_probability * 100.0:.2f}%\n"
            f"Bayesian log-odds: {context.bayesian_logit:+.4f}\n"
            f"Expected move t-score: {context.expected_move_t:+.3f}\n"
            f"Strike price: {odds_snapshot.strike_price:,.2f}\n"
            f"Underlying BTC price: {context.price:,.2f}\n"
            f"VWAP distance: {context.vwap_distance:+.2f}\n"
            f"EMA spread pct: {context.ema_spread_pct * 100.0:+.4f}%\n"
            f"RSI(14): {context.rsi_14:.2f}\n"
            f"CVD candle delta: {context.cvd_candle_delta:+.0f}\n"
            f"CVD 1m delta: {context.cvd_1min_delta:+.0f}\n"
            f"Adaptive CVD threshold: {context.adaptive_cvd_threshold:.2f}\n"
            f"Parkinson volatility: {context.parkinson_volatility:.6f}\n"
            f"Garman-Klass volatility: {context.garman_klass_volatility:.6f}\n"
            f"Realized volatility: {context.realized_volatility:.6f}\n"
            f"Market regime: {context.market_regime.value}\n"
            f"Seconds remaining: {odds_snapshot.seconds_remaining:.0f}\n"
            f"Public UP/DOWN probabilities: {odds_snapshot.up_public_prob_pct:.2f}% / {odds_snapshot.down_public_prob_pct:.2f}%\n"
            f"Executable UP/DOWN entry probabilities: {up_entry_prob:.2f}% / {down_entry_prob:.2f}%\n"
            f"Opposite-side executable probability: {opposite_prob:.2f}%\n"
            f"Reasons: {' | '.join(signal.reasons) if signal.reasons else 'None'}\n"
            f"{counter_line}"
            "Hard rules:\n"
            "1. Approve only the favored direction or SKIP.\n"
            "2. If evidence is contradictory, output SKIP.\n"
            "3. If volatility or regime implies unstable directionality, prefer SKIP.\n"
            "4. Do not output explanations.\n\n"
            f"OUTPUT FORMAT: FINAL:{favored} or FINAL:SKIP\n"
            "Return exactly one line."
        )

    @staticmethod
    def parse_decision(raw_text: str, favored_direction: Direction) -> Direction | None:
        text_up = (raw_text or "").upper()
        match = re.search(r"\bFINAL\s*[:=]\s*(UP|DOWN|SKIP)\b", text_up)
        if match:
            token = match.group(1)
            if token == "SKIP":
                return Direction.SKIP
            if token == favored_direction.value:
                return favored_direction
            return Direction.SKIP

        tokens = re.findall(r"\b(UP|DOWN|SKIP)\b", text_up)
        if not tokens:
            return None
        first = tokens[0]
        if first == "SKIP":
            return Direction.SKIP
        if first == favored_direction.value:
            return favored_direction
        return Direction.SKIP

    def _cooldown_seconds(self, consecutive_failures: int) -> float:
        if consecutive_failures < self.config.circuit_breaker_threshold:
            return 0.0
        exponent = consecutive_failures - self.config.circuit_breaker_threshold
        return min(
            self.config.circuit_breaker_max_cooldown_seconds,
            self.config.circuit_breaker_base_cooldown_seconds * (2 ** max(exponent, 0)),
        )
    async def call_local_ai(
        self,
        session: aiohttp.ClientSession | None,
        state: EngineState,
        signal: TradeSignal,
        context: TechnicalContext,
        odds_snapshot: MarketOddsSnapshot,
        *,
        counter_signal: TradeSignal | None = None,
        max_calls_override: int | None = None,
    ) -> AIDecision:
        now_ts = time.time()
        runtime = await state.build_runtime_counters()
        if runtime.ai_circuit_open_until > now_ts:
            return AIDecision(
                decision=Direction.SKIP,
                raw_response="",
                reason=f"AI circuit breaker open for {int(runtime.ai_circuit_open_until - now_ts)}s",
                circuit_open=True,
            )

        max_calls = max_calls_override if max_calls_override is not None else self.config.max_calls_per_slug
        reserved, slug_calls = await state.reserve_ai_call(signal.slug, max_calls=max_calls)
        if not reserved:
            return AIDecision(
                decision=Direction.SKIP,
                raw_response="",
                reason=f"AI cap reached ({slug_calls}/{max_calls})",
            )

        prompt = self.build_prompt(signal, context, odds_snapshot, counter_signal=counter_signal)
        timestamp_text = datetime.now(timezone.utc).strftime("%H:%M:%S")
        await state.update_telemetry(ai_interaction=AIInteraction(prompt=prompt, response="PENDING", timestamp=timestamp_text))

        payload = {
            "model": self.config.model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": self.config.temperature,
            "max_tokens": self.config.max_tokens,
            "keep_alive": -1,
            "stream": False,
        }

        own_session = False
        if session is None:
            session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.config.timeout_total_seconds))
            own_session = True

        raw_response = ""
        last_error = ""
        decision: Direction | None = None
        response_ms = 0.0
        transport_error = False

        try:
            for attempt in range(1, self.config.max_retries + 1):
                try:
                    started = time.perf_counter()
                    async with session.post(
                        self.config.url,
                        json=payload,
                        timeout=aiohttp.ClientTimeout(total=self.config.timeout_total_seconds),
                    ) as response:
                        response.raise_for_status()
                        body = await response.json()
                    raw_response = str(body["choices"][0]["message"]["content"]).strip()
                    response_ms = (time.perf_counter() - started) * 1000.0
                    decision = self.parse_decision(raw_response, signal.direction)
                    transport_error = False
                    last_error = ""
                    break
                except Exception as exc:
                    transport_error = True
                    last_error = f"{type(exc).__name__}: {exc}"
                    if attempt < self.config.max_retries:
                        await asyncio.sleep(self.config.retry_delay_seconds)

            if decision is not None:
                updated_ema = await state.register_ai_success(response_ms=response_ms)
                await state.release_ai_call(signal.slug)
                await state.update_runtime_counters(
                    last_ai_response_ms=response_ms,
                    ai_response_ema_ms=updated_ema,
                )
                await state.update_telemetry(
                    ai_interaction=AIInteraction(prompt=prompt, response=raw_response or decision.value, timestamp=timestamp_text)
                )
                if decision == Direction.SKIP:
                    await state.record_ai_state(
                        signal.slug,
                        last_veto_ts=time.time(),
                        last_veto_ev_pct=signal.expected_value_pct,
                    )
                    return AIDecision(
                        decision=Direction.SKIP,
                        raw_response=raw_response,
                        reason="AI vetoed borderline signal",
                        response_ms=response_ms,
                    )
                return AIDecision(
                    decision=decision,
                    raw_response=raw_response,
                    reason="AI confirmed favored direction",
                    response_ms=response_ms,
                )

            current_failures = await state.increment_ai_failures()
            cooldown = self._cooldown_seconds(current_failures)
            circuit_until = time.time() + cooldown if cooldown > 0 else 0.0
            if circuit_until > 0:
                circuit_until = await state.set_ai_circuit_open_until(circuit_until)
            await state.release_ai_call(signal.slug)
            await state.record_ai_state(
                signal.slug,
                last_veto_ts=time.time(),
                last_veto_ev_pct=signal.expected_value_pct,
            )
            await state.update_runtime_counters(
                ai_consecutive_failures=current_failures,
                ai_circuit_open_until=circuit_until,
            )
            await state.update_telemetry(
                ai_interaction=AIInteraction(prompt=prompt, response=raw_response or last_error or "MALFORMED", timestamp=timestamp_text)
            )

            if transport_error:
                return AIDecision(
                    decision=Direction.SKIP,
                    raw_response=raw_response,
                    reason=f"AI unavailable (timeout/network). Last error: {last_error}",
                    response_ms=response_ms,
                    circuit_open=cooldown > 0,
                    transport_error=True,
                )
            return AIDecision(
                decision=Direction.SKIP,
                raw_response=raw_response,
                reason="AI response malformed; fail closed to SKIP",
                response_ms=response_ms,
                malformed=True,
            )
        finally:
            if own_session:
                await session.close()
            await state.release_ai_call(signal.slug)


_DEFAULT_AGENT = LocalAIAgent()


async def call_local_ai(
    session: aiohttp.ClientSession | None,
    state: EngineState,
    signal: TradeSignal,
    context: TechnicalContext,
    odds_snapshot: MarketOddsSnapshot,
    *,
    counter_signal: TradeSignal | None = None,
    agent: LocalAIAgent | None = None,
    max_calls_override: int | None = None,
) -> AIDecision:
    runtime_agent = agent or _DEFAULT_AGENT
    return await runtime_agent.call_local_ai(
        session,
        state,
        signal,
        context,
        odds_snapshot,
        counter_signal=counter_signal,
        max_calls_override=max_calls_override,
    )


__all__ = [
    "AIConfig",
    "AIDecision",
    "LocalAIAgent",
    "call_local_ai",
]
