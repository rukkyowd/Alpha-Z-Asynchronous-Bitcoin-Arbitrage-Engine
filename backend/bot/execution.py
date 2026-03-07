from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from typing import Any

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import MarketOrderArgs, OrderType, PartialCreateOrderOptions
from py_clob_client.order_builder.constants import BUY, SELL

from .models import (
    ActivePosition,
    Direction,
    MarketOddsSnapshot,
    PositionStatus,
    TechnicalContext,
    TradeSignal,
)
from .risk import LiquidityProfile, PositionRiskSnapshot, RiskManager
from .state import EngineState

log = logging.getLogger("alpha_z_engine.execution")

EPSILON = 1e-9


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(value, upper))


def _quantize(value: float, tick_size: str, rounding: str) -> float:
    tick = Decimal(str(tick_size))
    return float(Decimal(str(value)).quantize(tick, rounding=rounding))


def _extract_levels(entries: list[Any] | None, *, reverse: bool) -> list[tuple[float, float]]:
    levels: list[tuple[float, float]] = []
    for entry in entries or []:
        try:
            price = float(entry.price)
            size = float(entry.size)
        except Exception:
            continue
        if price <= 0 or size <= 0:
            continue
        levels.append((price, size))
    levels.sort(key=lambda item: item[0], reverse=reverse)
    return levels


def _simulate_buy_fill(ask_levels: list[tuple[float, float]], spend_usd: float) -> tuple[float, float, float]:
    if spend_usd <= 0:
        return 0.0, 0.0, 0.0

    remaining = spend_usd
    total_cost = 0.0
    total_shares = 0.0
    top_ask = ask_levels[0][0] if ask_levels else 0.0

    for price, shares in ask_levels:
        level_notional = price * shares
        if level_notional <= 0:
            continue
        take_notional = min(level_notional, remaining)
        take_shares = take_notional / price
        total_cost += take_notional
        total_shares += take_shares
        remaining -= take_notional
        if remaining <= 1e-9:
            break

    avg_price = (total_cost / total_shares) if total_shares > 0 else top_ask
    return total_cost, total_shares, avg_price


def _simulate_sell_fill(bid_levels: list[tuple[float, float]], shares_to_sell: float) -> tuple[float, float, float]:
    if shares_to_sell <= 0:
        return 0.0, 0.0, 0.0

    remaining = shares_to_sell
    total_proceeds = 0.0
    sold_shares = 0.0
    top_bid = bid_levels[0][0] if bid_levels else 0.0

    for price, size in bid_levels:
        take_shares = min(size, remaining)
        total_proceeds += take_shares * price
        sold_shares += take_shares
        remaining -= take_shares
        if remaining <= 1e-9:
            break

    avg_price = (total_proceeds / sold_shares) if sold_shares > 0 else top_bid
    return total_proceeds, sold_shares, avg_price


def _parse_numeric(value: Any) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def _infer_fill_stats(response: dict[str, Any], intended_cost_usd: float, fallback_price: float) -> tuple[float, float, float]:
    transactions = response.get("transactions") or []
    if transactions:
        total_cost = 0.0
        total_shares = 0.0
        for tx in transactions:
            size = _parse_numeric(tx.get("size"))
            price = _parse_numeric(tx.get("price")) or fallback_price
            if size > 0 and price > 0:
                total_shares += size
                total_cost += size * price
        if total_shares > 0:
            avg_price = total_cost / total_shares
            return total_cost, total_shares, avg_price

    matched_amount = _parse_numeric(response.get("matchedAmount"))
    making_amount = _parse_numeric(response.get("makingAmount"))
    taking_amount = _parse_numeric(response.get("takingAmount"))
    taker_amount = _parse_numeric(response.get("takerAmount"))

    candidates = [value for value in (matched_amount, making_amount, taking_amount, taker_amount) if value > 0]
    if len(candidates) >= 2 and fallback_price > 0:
        best_pair: tuple[float, float] | None = None
        best_error = float("inf")
        for idx, first in enumerate(candidates):
            for jdx, second in enumerate(candidates):
                if idx == jdx:
                    continue
                modeled_cost = first * second
                error = abs(modeled_cost - intended_cost_usd)
                if error < best_error:
                    best_error = error
                    best_pair = (first, second)
        if best_pair is not None:
            a, b = best_pair
            if abs(a - intended_cost_usd) <= abs(b - intended_cost_usd):
                total_cost = a
                total_shares = b
            else:
                total_cost = b
                total_shares = a
            if total_cost > 0 and total_shares > 0:
                avg_price = total_cost / total_shares
                if 0.01 <= avg_price <= 0.99:
                    return total_cost, total_shares, avg_price

    if matched_amount > 0 and fallback_price > 0:
        if matched_amount <= (intended_cost_usd / max(fallback_price, 0.01)) * 2.0:
            total_shares = matched_amount
            total_cost = total_shares * fallback_price
            return total_cost, total_shares, fallback_price
        total_cost = matched_amount
        total_shares = total_cost / fallback_price
        return total_cost, total_shares, fallback_price

    status = str(response.get("status", "")).lower()
    if status == "matched" and fallback_price > 0 and intended_cost_usd > 0:
        total_shares = intended_cost_usd / fallback_price
        return intended_cost_usd, total_shares, fallback_price

    return 0.0, 0.0, fallback_price


@dataclass(slots=True, frozen=True)
class ExecutionConfig:
    paper_trading: bool = True
    dry_run: bool = False
    live_require_clob_liquidity: bool = True
    live_tiny_amm_fallback_max_bet_usd: float = 2.0
    live_tiny_amm_fallback_max_spread_pct: float = 0.015
    live_clob_recovery_wait_secs: float = 12.0
    live_clob_recovery_poll_secs: float = 1.0
    live_entry_slippage_cents: float = 0.02
    live_tiny_entry_slippage_cents: float = 0.01
    live_tiny_reprice_buffer_cents: float = 0.005
    live_tiny_reprice_max_extra_cents: float = 0.01
    max_spread_pct: float = 0.05
    min_liquidity_multiplier: float = 1.15
    paper_sim_estimated_depth_usd: float = 1000.0
    paper_sim_fallback_spread_pct: float = 0.01
    paper_sim_scale_haircut: float = 0.95
    order_submit_timeout_secs: float = 4.0
    min_live_fill_shares: float = 0.01
    exit_floor_first: float = 0.98
    exit_floor_second: float = 0.96


@dataclass(slots=True, frozen=True)
class LiquidityCheckResult:
    ok: bool
    mode: str
    reason: str
    entry_price: float
    best_bid: float | None
    best_ask: float | None
    spread_pct: float
    available_depth_usd: float
    expected_slippage_pct: float
    market_impact_pct: float
    levels: int
    allow_tiny_amm_fallback: bool = False

    def as_profile(self) -> LiquidityProfile:
        return LiquidityProfile(
            available_depth_usd=self.available_depth_usd,
            estimated_spread_pct=self.spread_pct,
            best_bid=self.best_bid,
            best_ask=self.best_ask,
            levels=self.levels,
        )


@dataclass(slots=True, frozen=True)
class FillResult:
    success: bool
    order_type: str
    response: dict[str, Any]
    filled_cost_usd: float
    filled_shares: float
    average_price: float
    requested_price: float
    requested_bet_usd: float
    status: str
    reason: str = ""


class ClobExecutionEngine:
    __slots__ = ("client", "config", "risk_manager")

    def __init__(
        self,
        client: ClobClient | None,
        *,
        config: ExecutionConfig | None = None,
        risk_manager: RiskManager | None = None,
    ):
        self.client = client
        self.config = config or ExecutionConfig()
        self.risk_manager = risk_manager or RiskManager()

    async def _fetch_tick_and_neg_risk(self, token_id: str) -> tuple[str, bool]:
        tick_size = "0.01"
        neg_risk = False
        if self.client is None:
            return tick_size, neg_risk

        try:
            tick_size = str(await asyncio.to_thread(self.client.get_tick_size, token_id))
        except Exception as exc:
            log.debug("[ENTRY ORDER] Tick size fetch failed for %s: %s", token_id, exc)
        try:
            neg_risk = bool(await asyncio.to_thread(self.client.get_neg_risk, token_id))
        except Exception as exc:
            log.debug("[ENTRY ORDER] Neg-risk fetch failed for %s: %s", token_id, exc)
        return tick_size, neg_risk
    async def check_liquidity_and_spread(
        self,
        token_id: str,
        *,
        bet_size_usd: float,
        expected_price: float,
        side: str = "buy",
        odds: MarketOddsSnapshot | None = None,
        allow_tiny_amm_fallback: bool = True,
    ) -> LiquidityCheckResult:
        if self.config.paper_trading or self.config.dry_run:
            spread = self.config.paper_sim_fallback_spread_pct
            depth = max(bet_size_usd * self.config.paper_sim_scale_haircut, self.config.paper_sim_estimated_depth_usd)
            slippage, impact = self.risk_manager.estimate_total_slippage_pct(
                bet_size_usd,
                LiquidityProfile(available_depth_usd=depth, estimated_spread_pct=spread),
            )
            return LiquidityCheckResult(
                ok=True,
                mode="PAPER_SIM",
                reason="Paper simulation liquidity",
                entry_price=expected_price,
                best_bid=expected_price - (spread * 0.5),
                best_ask=expected_price + (spread * 0.5),
                spread_pct=spread,
                available_depth_usd=depth,
                expected_slippage_pct=slippage,
                market_impact_pct=impact,
                levels=1,
            )

        if not token_id or self.client is None:
            return LiquidityCheckResult(
                ok=False,
                mode="NO_CLOB",
                reason="CLOB client unavailable",
                entry_price=expected_price,
                best_bid=None,
                best_ask=None,
                spread_pct=1.0,
                available_depth_usd=0.0,
                expected_slippage_pct=1.0,
                market_impact_pct=1.0,
                levels=0,
            )

        try:
            book = await asyncio.to_thread(self.client.get_order_book, token_id)
        except Exception as exc:
            if allow_tiny_amm_fallback and bet_size_usd <= self.config.live_tiny_amm_fallback_max_bet_usd:
                fallback_spread = 0.0
                if odds is not None:
                    direction = Direction.UP if token_id == odds.up_token_id else Direction.DOWN
                    public_prob = odds.entry_prob_pct(direction) / 100.0
                    fallback_spread = abs(public_prob - expected_price)
                allow = fallback_spread <= self.config.live_tiny_amm_fallback_max_spread_pct
                return LiquidityCheckResult(
                    ok=allow,
                    mode="AMM_TINY_LIVE_FALLBACK",
                    reason=f"CLOB unavailable: {exc}",
                    entry_price=expected_price,
                    best_bid=None,
                    best_ask=None,
                    spread_pct=fallback_spread,
                    available_depth_usd=0.0,
                    expected_slippage_pct=fallback_spread,
                    market_impact_pct=0.0,
                    levels=0,
                    allow_tiny_amm_fallback=allow,
                )
            return LiquidityCheckResult(
                ok=False,
                mode="NO_CLOB",
                reason=f"CLOB unavailable: {exc}",
                entry_price=expected_price,
                best_bid=None,
                best_ask=None,
                spread_pct=1.0,
                available_depth_usd=0.0,
                expected_slippage_pct=1.0,
                market_impact_pct=1.0,
                levels=0,
            )

        bid_levels = _extract_levels(getattr(book, "bids", None), reverse=True)
        ask_levels = _extract_levels(getattr(book, "asks", None), reverse=False)
        best_bid = bid_levels[0][0] if bid_levels else None
        best_ask = ask_levels[0][0] if ask_levels else None

        if best_bid is not None and best_ask is not None and best_bid <= 0.002 and best_ask >= 0.998:
            if allow_tiny_amm_fallback and bet_size_usd <= self.config.live_tiny_amm_fallback_max_bet_usd:
                return LiquidityCheckResult(
                    ok=True,
                    mode="AMM_TINY_LIVE_FALLBACK",
                    reason="CLOB shows stub quotes",
                    entry_price=expected_price,
                    best_bid=best_bid,
                    best_ask=best_ask,
                    spread_pct=0.0,
                    available_depth_usd=0.0,
                    expected_slippage_pct=0.0,
                    market_impact_pct=0.0,
                    levels=0,
                    allow_tiny_amm_fallback=True,
                )
            return LiquidityCheckResult(
                ok=False,
                mode="STUB_QUOTES",
                reason="CLOB shows stub quotes",
                entry_price=expected_price,
                best_bid=best_bid,
                best_ask=best_ask,
                spread_pct=1.0,
                available_depth_usd=0.0,
                expected_slippage_pct=1.0,
                market_impact_pct=1.0,
                levels=0,
            )

        if side.lower() == "buy":
            if not ask_levels:
                return LiquidityCheckResult(
                    ok=False,
                    mode="NO_ASKS",
                    reason="No executable asks on CLOB",
                    entry_price=expected_price,
                    best_bid=best_bid,
                    best_ask=best_ask,
                    spread_pct=1.0,
                    available_depth_usd=0.0,
                    expected_slippage_pct=1.0,
                    market_impact_pct=1.0,
                    levels=0,
                )
            depth_usd = sum(price * size for price, size in ask_levels)
            _, filled_shares, avg_price = _simulate_buy_fill(ask_levels, bet_size_usd)
            entry_price = avg_price if filled_shares > 0 else ask_levels[0][0]
            spread_pct = max(0.0, (best_ask - best_bid) if best_bid is not None and best_ask is not None else 0.0)
        else:
            if not bid_levels:
                return LiquidityCheckResult(
                    ok=False,
                    mode="NO_BIDS",
                    reason="No executable bids on CLOB",
                    entry_price=expected_price,
                    best_bid=best_bid,
                    best_ask=best_ask,
                    spread_pct=1.0,
                    available_depth_usd=0.0,
                    expected_slippage_pct=1.0,
                    market_impact_pct=1.0,
                    levels=0,
                )
            shares_to_sell = max(bet_size_usd / max(expected_price, 0.01), 0.0)
            _, filled_shares, avg_price = _simulate_sell_fill(bid_levels, shares_to_sell)
            entry_price = avg_price if filled_shares > 0 else bid_levels[0][0]
            depth_usd = sum(price * size for price, size in bid_levels)
            spread_pct = max(0.0, (best_ask - best_bid) if best_bid is not None and best_ask is not None else 0.0)

        profile = LiquidityProfile(
            available_depth_usd=depth_usd,
            estimated_spread_pct=spread_pct,
            best_bid=best_bid,
            best_ask=best_ask,
            levels=max(len(bid_levels), len(ask_levels)),
        )
        expected_slippage_pct, market_impact_pct = self.risk_manager.estimate_total_slippage_pct(bet_size_usd, profile)

        if depth_usd < (bet_size_usd * self.config.min_liquidity_multiplier):
            return LiquidityCheckResult(
                ok=False,
                mode="THIN_BOOK",
                reason=f"Insufficient depth (${depth_usd:.2f} < ${bet_size_usd * self.config.min_liquidity_multiplier:.2f})",
                entry_price=entry_price,
                best_bid=best_bid,
                best_ask=best_ask,
                spread_pct=spread_pct,
                available_depth_usd=depth_usd,
                expected_slippage_pct=expected_slippage_pct,
                market_impact_pct=market_impact_pct,
                levels=profile.levels,
            )
        if spread_pct > self.config.max_spread_pct:
            return LiquidityCheckResult(
                ok=False,
                mode="WIDE_SPREAD",
                reason=f"Spread too wide ({spread_pct * 100:.2f}c > {self.config.max_spread_pct * 100:.2f}c)",
                entry_price=entry_price,
                best_bid=best_bid,
                best_ask=best_ask,
                spread_pct=spread_pct,
                available_depth_usd=depth_usd,
                expected_slippage_pct=expected_slippage_pct,
                market_impact_pct=market_impact_pct,
                levels=profile.levels,
            )

        return LiquidityCheckResult(
            ok=True,
            mode="CLOB",
            reason="Executable CLOB depth available",
            entry_price=entry_price,
            best_bid=best_bid,
            best_ask=best_ask,
            spread_pct=spread_pct,
            available_depth_usd=depth_usd,
            expected_slippage_pct=expected_slippage_pct,
            market_impact_pct=market_impact_pct,
            levels=profile.levels,
        )

    async def wait_for_live_clob_recovery(
        self,
        token_id: str,
        *,
        bet_size_usd: float,
        expected_price: float,
        side: str = "buy",
    ) -> LiquidityCheckResult:
        deadline = time.monotonic() + self.config.live_clob_recovery_wait_secs
        last_result = LiquidityCheckResult(
            ok=False,
            mode="WAIT_TIMEOUT",
            reason="No recovery attempts made",
            entry_price=expected_price,
            best_bid=None,
            best_ask=None,
            spread_pct=1.0,
            available_depth_usd=0.0,
            expected_slippage_pct=1.0,
            market_impact_pct=1.0,
            levels=0,
        )

        while time.monotonic() < deadline:
            last_result = await self.check_liquidity_and_spread(
                token_id,
                bet_size_usd=bet_size_usd,
                expected_price=expected_price,
                side=side,
                odds=None,
                allow_tiny_amm_fallback=False,
            )
            if last_result.ok and last_result.mode == "CLOB":
                return last_result
            await asyncio.sleep(self.config.live_clob_recovery_poll_secs)

        return last_result

    async def _submit_market_order(
        self,
        token_id: str,
        *,
        amount: float,
        limit_price: float,
        order_type: OrderType,
        tick_size: str,
        neg_risk: bool,
    ) -> dict[str, Any]:
        if self.client is None:
            raise RuntimeError("CLOB client is unavailable.")

        def _sign_and_post() -> dict[str, Any]:
            order_args = MarketOrderArgs(
                token_id=token_id,
                amount=float(Decimal(str(amount)).quantize(Decimal("0.01"), rounding=ROUND_DOWN)),
                side=BUY,
                price=float(limit_price),
                order_type=order_type,
            )
            options = PartialCreateOrderOptions(tick_size=tick_size, neg_risk=neg_risk)
            signed = self.client.create_market_order(order_args, options)
            return self.client.post_order(signed, order_type)

        response = await asyncio.to_thread(_sign_and_post)
        if isinstance(response, dict):
            response["_entry_order_type"] = str(order_type)
            response["_entry_limit_price"] = float(limit_price)
        return response

    def _build_active_position(
        self,
        signal: TradeSignal,
        odds: MarketOddsSnapshot,
        context: TechnicalContext,
        *,
        token_id: str,
        fill_cost_usd: float,
        average_price: float,
        fill_shares: float,
        risk_snapshot: PositionRiskSnapshot,
        liquidity: LiquidityCheckResult,
        order_type: str,
    ) -> ActivePosition:
        return ActivePosition(
            slug=signal.slug,
            decision=signal.direction,
            token_id=token_id,
            strike=odds.strike_price,
            bet_size_usd=fill_cost_usd,
            bought_price=average_price,
            status=PositionStatus.OPEN,
            score=signal.score,
            bonus_score=signal.bonus_score,
            mark_price=average_price,
            current_token_price=average_price,
            live_underlying_price=context.price,
            entry_underlying_price=context.price,
            tp_delta=risk_snapshot.tp_delta,
            sl_delta=risk_snapshot.sl_delta,
            tp_token_price=risk_snapshot.tp_token_price,
            sl_token_price=risk_snapshot.sl_token_price,
            seconds_remaining=int(max(0.0, odds.seconds_remaining)),
            sl_disabled=risk_snapshot.sl_disabled,
            sl_breach_count=0,
            tp_gate_logged=False,
            tp_armed=False,
            tp_peak_delta=0.0,
            tp_lock_floor_delta=0.0,
            signals=signal.reasons,
            notes=(f"liq_mode={liquidity.mode}", f"entry_order_type={order_type}", liquidity.reason),
            ml_features={
                "filled_cost_usd": round(fill_cost_usd, 6),
                "filled_shares": round(fill_shares, 6),
                "entry_avg_price": round(average_price, 6),
                "expected_slippage_pct": signal.expected_slippage_pct,
                "market_impact_pct": signal.market_impact_pct,
                "available_depth_usd": liquidity.available_depth_usd,
                "clob_levels": liquidity.levels,
            },
        )
    async def submit_entry_order(
        self,
        state: EngineState,
        signal: TradeSignal,
        odds: MarketOddsSnapshot,
        context: TechnicalContext,
    ) -> ActivePosition | None:
        if signal.direction not in (Direction.UP, Direction.DOWN):
            return None

        token_id = odds.token_id(signal.direction)
        if not token_id:
            await state.record_execution_failure(signal.slug, reason="Missing token_id", ev_pct=signal.expected_value_pct)
            return None

        bankroll = state.simulated_balance
        approved, reason = self.risk_manager.can_trade(
            bankroll,
            signal.kelly_bet_usd,
            current_daily_pnl=state.current_daily_pnl,
            trades_this_hour=state.trades_this_hour,
        )
        if not approved:
            await state.record_execution_failure(signal.slug, reason=reason, ev_pct=signal.expected_value_pct)
            return None

        expected_price = signal.token_price or (odds.entry_prob_pct(signal.direction) / 100.0)
        liquidity = await self.check_liquidity_and_spread(
            token_id,
            bet_size_usd=signal.kelly_bet_usd,
            expected_price=expected_price,
            side="buy",
            odds=odds,
            allow_tiny_amm_fallback=True,
        )

        if not liquidity.ok:
            await state.record_execution_failure(signal.slug, reason=liquidity.reason, ev_pct=signal.expected_value_pct)
            return None

        if liquidity.mode == "AMM_TINY_LIVE_FALLBACK":
            log.info(
                "[ENTRY WAIT] %s: tiny fallback path active; waiting up to %.0fs for CLOB recovery...",
                signal.slug,
                self.config.live_clob_recovery_wait_secs,
            )
            recovered = await self.wait_for_live_clob_recovery(
                token_id,
                bet_size_usd=signal.kelly_bet_usd,
                expected_price=expected_price,
                side="buy",
            )
            if recovered.ok and recovered.mode == "CLOB":
                liquidity = recovered
            else:
                log.warning(
                    "[ENTRY WAIT] %s: CLOB did not recover within %.0fs; routing tiny live fallback.",
                    signal.slug,
                    self.config.live_clob_recovery_wait_secs,
                )

        tick_size, neg_risk = await self._fetch_tick_and_neg_risk(token_id)
        slippage_cents = (
            self.config.live_tiny_entry_slippage_cents
            if signal.kelly_bet_usd <= self.config.live_tiny_amm_fallback_max_bet_usd
            else self.config.live_entry_slippage_cents
        )
        target_price = max(liquidity.entry_price, expected_price, signal.price_cap or 0.0)
        limit_price = _quantize(_clamp(target_price + slippage_cents, 0.01, 0.99), tick_size, ROUND_UP)

        if self.config.paper_trading or self.config.dry_run:
            synthetic_fill_cost = round(signal.kelly_bet_usd, 2)
            synthetic_shares = synthetic_fill_cost / max(limit_price, 0.01)
            risk_snapshot = self.risk_manager.position_risk_snapshot(
                ActivePosition(
                    slug=signal.slug,
                    decision=signal.direction,
                    token_id=token_id,
                    strike=odds.strike_price,
                    bet_size_usd=synthetic_fill_cost,
                    bought_price=limit_price,
                ),
                context,
                seconds_remaining=odds.seconds_remaining,
            )
            position = self._build_active_position(
                signal,
                odds,
                context,
                token_id=token_id,
                fill_cost_usd=synthetic_fill_cost,
                average_price=limit_price,
                fill_shares=synthetic_shares,
                risk_snapshot=risk_snapshot,
                liquidity=liquidity,
                order_type="PAPER",
            )
            await state.upsert_position(position)
            await state.update_runtime_counters(trades_this_hour=state.trades_this_hour + 1)
            return position

        log.info(
            "[BET ATTEMPT] LIVE %s on %s | Bet: $%.2f | Expected: %.4f | Liq: OK (%s) | spread=%.2fc | impact=%.2f%% | levels=%s",
            signal.direction.value,
            signal.slug,
            signal.kelly_bet_usd,
            expected_price,
            liquidity.mode,
            liquidity.spread_pct * 100.0,
            liquidity.market_impact_pct * 100.0,
            liquidity.levels,
        )
        log.info(
            "[ENTRY ORDER] Max Entry @ %.4f (expected %.4f + max %.1fc slip)",
            limit_price,
            expected_price,
            slippage_cents * 100.0,
        )

        order_response: dict[str, Any] = {}
        order_type_used = "FOK"

        try:
            try:
                order_response = await asyncio.wait_for(
                    self._submit_market_order(
                        token_id,
                        amount=signal.kelly_bet_usd,
                        limit_price=limit_price,
                        order_type=OrderType.FOK,
                        tick_size=tick_size,
                        neg_risk=neg_risk,
                    ),
                    timeout=self.config.order_submit_timeout_secs,
                )
                order_type_used = "FOK"
            except Exception as exc:
                if "fully filled or killed" not in str(exc).lower():
                    raise
                log.warning("[ENTRY ORDER] FOK unfilled for %s; retrying once as FAK at same max price.", signal.slug)
                try:
                    order_response = await asyncio.wait_for(
                        self._submit_market_order(
                            token_id,
                            amount=signal.kelly_bet_usd,
                            limit_price=limit_price,
                            order_type=OrderType.FAK,
                            tick_size=tick_size,
                            neg_risk=neg_risk,
                        ),
                        timeout=self.config.order_submit_timeout_secs,
                    )
                    order_type_used = "FAK"
                except Exception as fak_exc:
                    if (
                        signal.kelly_bet_usd <= self.config.live_tiny_amm_fallback_max_bet_usd
                        and "no orders found to match with fak order" in str(fak_exc).lower()
                    ):
                        recovered = await self.check_liquidity_and_spread(
                            token_id,
                            bet_size_usd=signal.kelly_bet_usd,
                            expected_price=expected_price,
                            side="buy",
                            odds=odds,
                            allow_tiny_amm_fallback=False,
                        )
                        if recovered.best_ask is not None:
                            retry_candidate = recovered.best_ask + self.config.live_tiny_reprice_buffer_cents
                            retry_cap = min(0.99, limit_price + self.config.live_tiny_reprice_max_extra_cents)
                            retry_price = _quantize(min(retry_candidate, retry_cap), tick_size, ROUND_UP)
                            if retry_price > limit_price:
                                log.warning(
                                    "[ENTRY RETRY] FAK no-match on %s; repricing %.4f -> %.4f (best_ask=%.4f) and retrying once.",
                                    signal.slug,
                                    limit_price,
                                    retry_price,
                                    recovered.best_ask,
                                )
                                limit_price = retry_price
                                order_response = await asyncio.wait_for(
                                    self._submit_market_order(
                                        token_id,
                                        amount=signal.kelly_bet_usd,
                                        limit_price=limit_price,
                                        order_type=OrderType.FAK,
                                        tick_size=tick_size,
                                        neg_risk=neg_risk,
                                    ),
                                    timeout=self.config.order_submit_timeout_secs,
                                )
                                order_type_used = "FAK_REPRICE"
                            else:
                                raise fak_exc
                        else:
                            raise fak_exc
                    else:
                        raise fak_exc
        except Exception as exc:
            await state.record_execution_failure(signal.slug, reason=str(exc), ev_pct=signal.expected_value_pct)
            return None

        status = str(order_response.get("status", "") or "")
        fill_cost_usd, fill_shares, average_price = _infer_fill_stats(order_response, signal.kelly_bet_usd, limit_price)
        if fill_shares < self.config.min_live_fill_shares or fill_cost_usd <= 0:
            await state.record_execution_failure(
                signal.slug,
                reason=f"ENTRY_UNFILLED_{status or 'UNKNOWN'}",
                ev_pct=signal.expected_value_pct,
            )
            return None

        proto_position = ActivePosition(
            slug=signal.slug,
            decision=signal.direction,
            token_id=token_id,
            strike=odds.strike_price,
            bet_size_usd=fill_cost_usd,
            bought_price=average_price,
        )
        risk_snapshot = self.risk_manager.position_risk_snapshot(
            proto_position,
            context,
            seconds_remaining=odds.seconds_remaining,
        )
        position = self._build_active_position(
            signal,
            odds,
            context,
            token_id=token_id,
            fill_cost_usd=fill_cost_usd,
            average_price=average_price,
            fill_shares=fill_shares,
            risk_snapshot=risk_snapshot,
            liquidity=liquidity,
            order_type=order_type_used,
        )
        await state.upsert_position(position)
        await state.update_runtime_counters(trades_this_hour=state.trades_this_hour + 1)
        await state.clear_execution_failure(signal.slug)
        log.info(
            "[BET] BET PLACED [LIVE] %s on %s | Filled: $%.2f | Avg Px: %.4f | Shares: %.4f | Type: %s",
            signal.direction.value,
            signal.slug,
            fill_cost_usd,
            average_price,
            fill_shares,
            order_type_used,
        )
        return position

    async def submit_exit_order(
        self,
        state: EngineState,
        position: ActivePosition,
        *,
        exit_reason: str,
        current_token_price: float,
    ) -> FillResult:
        shares_owned = position.shares_owned
        if shares_owned <= 0:
            return FillResult(
                success=False,
                order_type="NONE",
                response={},
                filled_cost_usd=0.0,
                filled_shares=0.0,
                average_price=0.0,
                requested_price=0.0,
                requested_bet_usd=0.0,
                status="NO_POSITION",
                reason="Position has zero shares.",
            )

        if self.config.paper_trading or self.config.dry_run or self.client is None:
            exit_notional = shares_owned * current_token_price
            pnl = exit_notional - position.bet_size_usd
            self.risk_manager.record_pnl(pnl)
            await state.update_runtime_counters(current_daily_pnl=state.current_daily_pnl + pnl)
            await state.pop_position(position.slug)
            return FillResult(
                success=True,
                order_type="PAPER",
                response={"status": "matched"},
                filled_cost_usd=exit_notional,
                filled_shares=shares_owned,
                average_price=current_token_price,
                requested_price=current_token_price,
                requested_bet_usd=position.bet_size_usd,
                status="matched",
                reason=exit_reason,
            )

        tick_size, neg_risk = await self._fetch_tick_and_neg_risk(position.token_id)
        floors = [
            _quantize(_clamp(current_token_price * self.config.exit_floor_first, 0.01, 0.99), tick_size, ROUND_DOWN),
            _quantize(_clamp(current_token_price * self.config.exit_floor_second, 0.01, 0.99), tick_size, ROUND_DOWN),
        ]

        for idx, floor_price in enumerate(floors, start=1):
            try:
                order_args = MarketOrderArgs(
                    token_id=position.token_id,
                    amount=float(Decimal(str(shares_owned)).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)),
                    side=SELL,
                    price=floor_price,
                    order_type=OrderType.FAK,
                )
                options = PartialCreateOrderOptions(tick_size=tick_size, neg_risk=neg_risk)
                signed = await asyncio.to_thread(self.client.create_market_order, order_args, options)
                response = await asyncio.wait_for(
                    asyncio.to_thread(self.client.post_order, signed, OrderType.FAK),
                    timeout=self.config.order_submit_timeout_secs,
                )
                proceeds_usd, sold_shares, avg_price = _infer_fill_stats(
                    response if isinstance(response, dict) else {},
                    position.bet_size_usd,
                    floor_price,
                )
                if sold_shares > 0:
                    fraction = min(sold_shares / shares_owned, 1.0)
                    realized_cost = position.bet_size_usd * fraction
                    pnl = proceeds_usd - realized_cost
                    self.risk_manager.record_pnl(pnl)
                    await state.update_runtime_counters(current_daily_pnl=state.current_daily_pnl + pnl)
                    if sold_shares >= shares_owned - 1e-4:
                        await state.pop_position(position.slug)
                    else:
                        await state.update_position(
                            position.slug,
                            bet_size_usd=max(position.bet_size_usd - realized_cost, 0.0),
                            status=PositionStatus.OPEN,
                            current_token_price=current_token_price,
                            mark_price=current_token_price,
                            sl_breach_count=0,
                        )
                    return FillResult(
                        success=True,
                        order_type=f"FAK_EXIT_{idx}",
                        response=response if isinstance(response, dict) else {},
                        filled_cost_usd=proceeds_usd,
                        filled_shares=sold_shares,
                        average_price=avg_price,
                        requested_price=floor_price,
                        requested_bet_usd=position.bet_size_usd,
                        status=str((response or {}).get("status", "matched")),
                        reason=exit_reason,
                    )
            except Exception as exc:
                if idx == len(floors):
                    await state.record_execution_failure(position.slug, reason=f"EXIT_FAILED: {exc}")
                    return FillResult(
                        success=False,
                        order_type=f"FAK_EXIT_{idx}",
                        response={},
                        filled_cost_usd=0.0,
                        filled_shares=0.0,
                        average_price=0.0,
                        requested_price=floor_price,
                        requested_bet_usd=position.bet_size_usd,
                        status="FAILED",
                        reason=str(exc),
                    )
                await asyncio.sleep(0.5)

        return FillResult(
            success=False,
            order_type="FAK_EXIT",
            response={},
            filled_cost_usd=0.0,
            filled_shares=0.0,
            average_price=0.0,
            requested_price=current_token_price,
            requested_bet_usd=position.bet_size_usd,
            status="FAILED",
            reason="Exit order attempts exhausted.",
        )


__all__ = [
    "ClobExecutionEngine",
    "ExecutionConfig",
    "FillResult",
    "LiquidityCheckResult",
]
