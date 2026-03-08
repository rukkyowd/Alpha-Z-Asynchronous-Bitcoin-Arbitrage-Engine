from __future__ import annotations

import asyncio
import csv
import json
import logging
import math
import os
import random
import sqlite3
import time
import tracemalloc
from collections import defaultdict, deque
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import mean, pstdev
from typing import Any
from zoneinfo import ZoneInfo

import aiohttp
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Request, WebSocket
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import AssetType, BalanceAllowanceParams
from starlette.websockets import WebSocketDisconnect, WebSocketState

from bot.ai_agent import AIConfig, LocalAIAgent, call_local_ai
from bot.data_streams import BinanceStreamManager, DataStreamsConfig, PolymarketFetcher, create_http_session
from bot.execution import ClobExecutionEngine, ExecutionConfig, FillResult
from bot.clob_ws import ClobWebSocketManager, run_heartbeat_loop
from bot.indicators import apply_probabilistic_model, blended_intraday_volatility, build_technical_context, compute_vwap
from bot.models import (
    ActivePosition,
    AdaptiveThresholdSnapshot,
    CVDSnapshot,
    ConfidenceLevel,
    Direction,
    ExecutionTimingSnapshot,
    MarketOddsSnapshot,
    MarketResolution,
    MarketRegime,
    MarketTick,
    PositionStatus,
    TechnicalContext,
    TradeSignal,
)
from bot.risk import LiquidityProfile, RiskConfig, RiskManager
from bot.state import EngineState
from bot.strategy import StrategyConfig, StrategyEngine
from bot.calibration import CalibrationConfig, ProbabilityCalibrator

BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"
DB_PATH = BASE_DIR / "alpha_z_history.db"
ML_CSV_PATH = BASE_DIR / "ai_training_data.csv"
TRADING_LOG_PATH = BASE_DIR / "trading_log.txt"
ET_TZ = ZoneInfo("America/New_York")

WS_PUSH_INTERVAL_SECS = 0.35
WS_PORTFOLIO_PUSH_INTERVAL_SECS = 2.0
METRICS_CACHE_TTL_SECS = 2.0
BALANCE_REFRESH_SECS = 15.0
EVALUATION_IDLE_SLEEP_SECS = 0.40
POSITION_MONITOR_SLEEP_SECS = 1.00
POSITION_HEARTBEAT_SECS = 12.0
SOFT_EVAL_REFRESH_SECS = 8.0
RESOLVING_SETTLEMENT_RETRY_SECS = 30.0

logger = logging.getLogger("alpha_z_engine")
load_dotenv(ENV_PATH)
if os.getenv("TRACE_MALLOC", os.getenv("ENABLE_TRACEMALLOC", "false")).strip().lower() in {"1", "true", "yes", "on"}:
    tracemalloc.start()


def _env_bool(name: str, default: bool) -> bool:
    return os.getenv(name, str(default)).strip().lower() in {"1", "true", "yes", "on"}


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


PAPER_TRADING = _env_bool("PAPER_TRADING", True)
DRY_RUN = _env_bool("DRY_RUN", False)
PAPER_USE_LIVE_CLOB = _env_bool("PAPER_USE_LIVE_CLOB", True)
BASE_BANKROLL = _env_float("BANKROLL", 5000.0)
POLY_PRIVATE_KEY = os.getenv("POLY_PRIVATE_KEY", "").strip()
POLY_FUNDER = os.getenv("POLY_FUNDER", "").strip()
POLY_SIG_TYPE = _env_int("POLY_SIG_TYPE", 2)
POLY_HOST = os.getenv("POLY_HOST", "https://clob.polymarket.com").strip()
POLY_CHAIN_ID = _env_int("POLY_CHAIN_ID", 137)
ENGINE_CONTROL_API_KEY = os.getenv("ENGINE_CONTROL_API_KEY", "").strip()
ALLOWED_ORIGINS = [
    origin.strip()
    for origin in os.getenv(
        "ALLOWED_ORIGINS",
        "http://localhost:3000,http://127.0.0.1:3000,http://localhost:3001,http://127.0.0.1:3001",
    ).split(",")
    if origin.strip()
]


class EngineControlUpdate(BaseModel):
    kill_switch: bool | None = None
    paper_trading: bool | None = None
    max_trade_pct: float | None = None
    max_daily_loss_pct: float | None = None


class RecentLogHandler(logging.Handler):
    def __init__(self, sink: deque[str]):
        super().__init__(level=logging.INFO)
        self.sink = sink

    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = self.format(record)
        except Exception:
            message = record.getMessage()
        self.sink.append(message)


@dataclass(slots=True)
class EngineServices:
    state: EngineState
    http_session: aiohttp.ClientSession
    stream_manager: BinanceStreamManager
    polymarket_fetcher: PolymarketFetcher
    strategy_engine: StrategyEngine
    risk_manager: RiskManager
    execution_engine: ClobExecutionEngine
    ai_agent: LocalAIAgent
    data_config: DataStreamsConfig
    strategy_config: StrategyConfig
    risk_config: RiskConfig
    execution_config: ExecutionConfig
    ai_config: AIConfig
    clob_client: ClobClient | None
    bankroll: float
    paper_trading: bool
    dry_run: bool
    stop_event: asyncio.Event = field(default_factory=asyncio.Event)
    recent_logs: deque[str] = field(default_factory=lambda: deque(maxlen=200))
    tasks: list[asyncio.Task[Any]] = field(default_factory=list)
    clob_ws_manager: ClobWebSocketManager | None = None
    task_group: asyncio.TaskGroup | None = None
    log_handler: RecentLogHandler | None = None
    latest_context: TechnicalContext | None = None
    latest_odds: MarketOddsSnapshot | None = None
    latest_signal: TradeSignal | None = None
    latest_rejected_reason: str = ""
    latest_locks: list[str] = field(default_factory=list)
    last_logged_rejection_reason: str = ""
    last_logged_rejection_ts: float = 0.0
    market_audit_logged: set[str] = field(default_factory=set)
    position_heartbeat_ts: dict[str, float] = field(default_factory=dict)
    resolving_retry_ts: dict[str, float] = field(default_factory=dict)
    trade_history: list[dict[str, Any]] = field(default_factory=list)
    execution_history: list[dict[str, Any]] = field(default_factory=list)
    equity_curve: list[dict[str, Any]] = field(default_factory=list)
    daily_pnl_series: list[dict[str, Any]] = field(default_factory=list)
    current_balance: float = 0.0
    metrics_cache: dict[str, Any] | None = None
    metrics_cache_ts: float = 0.0
    metrics_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    live_payload_cache: dict[str, Any] | None = None
    live_payload_cache_ts: float = 0.0
    live_payload_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    api_semaphore: asyncio.Semaphore = field(default_factory=lambda: asyncio.Semaphore(8))
    api_capacity: int = 8
    api_inflight: int = 0
    api_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    balance_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


RUNTIME: EngineServices | None = None


DEFAULT_MARKET_DATA = {
    "price": 0.0,
    "vwap": 0.0,
    "history": [],
    "active_trades": {},
    "candle": None,
    "is_paper": True,
    "logs": [],
    "regime": "UNKNOWN",
    "atr": 0.0,
    "strategy": {
        "edge_tracker": {
            "slug": "",
            "direction": "UNKNOWN",
            "up_math_prob": 0.0,
            "down_math_prob": 0.0,
            "up_poly_prob": 0.0,
            "down_poly_prob": 0.0,
            "up_public_prob": 0.0,
            "down_public_prob": 0.0,
            "up_edge": 0.0,
            "down_edge": 0.0,
            "best_edge": 0.0,
            "best_ev_pct": 0.0,
        },
        "signal_alignment": {
            "direction": "UNKNOWN",
            "score": 0,
            "max_score": 4,
            "vwap": False,
            "rsi": False,
            "volume": False,
            "cvd": False,
        },
        "execution_latency": {
            "signal_generation_ms": 0.0,
            "ai_inference_ms": 0.0,
            "clob_request_ms": 0.0,
            "confirmation_ms": 0.0,
            "total_ms": 0.0,
            "updated_at": 0.0,
        },
        "cvd_gauge": {
            "delta": 0.0,
            "one_min_delta": 0.0,
            "threshold": 0.0,
            "divergence": "NONE",
            "divergence_strength": 0.0,
        },
        "adaptive_thresholds": {
            "atr_min": 0.0,
            "cvd_threshold": 0.0,
        },
        "system_locks": {
            "locks": [],
            "ai_circuit_open": False,
            "ai_circuit_remaining_secs": 0,
            "ai_failures": 0,
            "ai_in_flight": False,
        },
        "drawdown_guard": {
            "regime": "NORMAL",
            "text": "",
            "bankroll": 0.0,
            "max_bet_cap": 0.0,
            "drawdown_used": 0.0,
            "drawdown_room_left": 0.0,
            "max_trade_pct": 0.05,
            "max_daily_loss_pct": 0.15,
        },
    },
}


def log_skip_reason(runtime: EngineServices, slug: str, reason: str, *, min_repeat_secs: float = 15.0) -> None:
    now_ts = time.time()
    if reason == "Position already active":
        last_position_log = runtime.position_heartbeat_ts.get(slug, 0.0)
        if (now_ts - last_position_log) <= (POSITION_HEARTBEAT_SECS + 2.0):
            return
    should_log = (
        reason != runtime.last_logged_rejection_reason
        or (now_ts - runtime.last_logged_rejection_ts) >= min_repeat_secs
    )
    if not should_log:
        return
    logger.info("[SKIP] %s: %s", slug, reason)
    runtime.last_logged_rejection_reason = reason
    runtime.last_logged_rejection_ts = now_ts


def _format_countdown(seconds_remaining: float) -> str:
    total = max(0, int(seconds_remaining))
    hours, remainder = divmod(total, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    return f"{minutes:02d}:{seconds:02d}"


def log_position_heartbeat(
    runtime: EngineServices,
    position: ActivePosition,
    *,
    min_repeat_secs: float = POSITION_HEARTBEAT_SECS,
) -> None:
    now_ts = time.time()
    last_logged = runtime.position_heartbeat_ts.get(position.slug, 0.0)
    if (now_ts - last_logged) < min_repeat_secs:
        return

    mark_price = _safe_float(position.current_token_price, position.mark_price)
    unrealized_pnl = position.unrealized_pnl
    unrealized_pct = 0.0
    if position.bet_size_usd > 0:
        unrealized_pct = (unrealized_pnl / position.bet_size_usd) * 100.0

    logger.info(
        "[POSITION] %s %s | Mark: %.4f | U-PnL: $%+.2f (%+.2f%%) | TP: %+.1fc @ %.4f | SL: %+.1fc @ %.4f | T-%s | TP Armed: %s | SL Disabled: %s",
        position.status.value,
        position.slug,
        mark_price,
        unrealized_pnl,
        unrealized_pct,
        position.tp_delta * 100.0,
        position.tp_token_price,
        position.sl_delta * 100.0,
        position.sl_token_price,
        _format_countdown(float(position.seconds_remaining)),
        "Y" if position.tp_armed else "N",
        "Y" if position.sl_disabled else "N",
    )
    runtime.position_heartbeat_ts[position.slug] = now_ts


def sanitize_data(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: sanitize_data(item) for key, item in value.items()}
    if isinstance(value, list):
        return [sanitize_data(item) for item in value]
    if isinstance(value, tuple):
        return [sanitize_data(item) for item in value]
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return 0.0
    return value


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_timestamp_text(dt: datetime | None = None) -> str:
    value = dt or utc_now()
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _market_slug_for_et_datetime(now_et: datetime) -> str:
    month = now_et.strftime("%B").lower()
    day = now_et.day
    hour_24 = now_et.hour
    hour_12 = hour_24 % 12 or 12
    ampm = "am" if hour_24 < 12 else "pm"
    return f"bitcoin-up-or-down-{month}-{day}-{hour_12}{ampm}-et"


def current_market_slug(now_utc: datetime | None = None) -> str:
    now_et = (now_utc or utc_now()).astimezone(ET_TZ)
    return _market_slug_for_et_datetime(now_et)


def candidate_market_slugs(
    now_utc: datetime | None = None,
    *,
    hour_offsets: tuple[int, ...] = (-3, -2, -1, 0, 1, 2, 3),
) -> list[str]:
    base_time = now_utc or utc_now()
    candidates: list[str] = []
    seen: set[str] = set()
    for offset_hours in hour_offsets:
        candidate = current_market_slug(base_time + timedelta(hours=offset_hours))
        if candidate in seen:
            continue
        seen.add(candidate)
        candidates.append(candidate)
    return candidates


def market_family_prefix(slug: str) -> str:
    parts = slug.split("-")
    if len(parts) <= 1:
        return slug
    return "-".join(parts[:-1])


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _safe_timestamp(raw: Any) -> datetime:
    if isinstance(raw, datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
    raw_text = str(raw or "").strip()
    if not raw_text:
        return utc_now()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S"):
        try:
            parsed = datetime.strptime(raw_text, fmt)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(raw_text.replace("Z", "+00:00"))
    except ValueError:
        return utc_now()


def _epoch_seconds(dt: datetime | None) -> int:
    if dt is None:
        return 0
    return int(dt.astimezone(timezone.utc).timestamp())


def _format_candle(tick: MarketTick | None) -> dict[str, Any] | None:
    if tick is None:
        return None
    return {
        "time": _epoch_seconds(tick.timestamp),
        "open": tick.open,
        "high": tick.high,
        "low": tick.low,
        "close": tick.close if tick.close > 0 else tick.resolved_price,
    }


def _setup_logging(runtime: EngineServices) -> None:
    logger.setLevel(logging.INFO)
    logger.propagate = False
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    file_handler = logging.FileHandler(TRADING_LOG_PATH, encoding="utf-8")
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    recent_handler = RecentLogHandler(runtime.recent_logs)
    recent_handler.setFormatter(formatter)

    logger.handlers.clear()
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.addHandler(recent_handler)
    runtime.log_handler = recent_handler


@asynccontextmanager
async def api_slot(runtime: EngineServices):
    await runtime.api_semaphore.acquire()
    async with runtime.api_lock:
        runtime.api_inflight += 1
    try:
        yield
    finally:
        async with runtime.api_lock:
            runtime.api_inflight = max(0, runtime.api_inflight - 1)
        runtime.api_semaphore.release()


def _init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH, timeout=30.0) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                slug TEXT NOT NULL,
                decision TEXT NOT NULL,
                strike REAL DEFAULT 0,
                final_price REAL DEFAULT 0,
                actual_outcome TEXT DEFAULT '',
                result TEXT DEFAULT '',
                pnl_impact REAL DEFAULT 0,
                entry_price REAL DEFAULT 0,
                exit_price REAL DEFAULT 0,
                entry_underlying REAL DEFAULT 0,
                exit_underlying REAL DEFAULT 0,
                bet_size_usd REAL DEFAULT 0,
                confidence TEXT DEFAULT '',
                score INTEGER DEFAULT 0,
                bonus_score INTEGER DEFAULT 0,
                ai_validated INTEGER DEFAULT 0,
                trigger_reason TEXT DEFAULT '',
                local_calc_outcome TEXT DEFAULT '',
                official_outcome TEXT DEFAULT '',
                match_status TEXT DEFAULT '',
                metadata_json TEXT DEFAULT '{}'
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS execution_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                slug TEXT NOT NULL,
                direction TEXT NOT NULL,
                expected_price REAL DEFAULT 0,
                actual_price REAL DEFAULT 0,
                slippage_bps REAL DEFAULT 0,
                spread_cents REAL DEFAULT 0,
                liquidity_check TEXT DEFAULT '',
                order_type TEXT DEFAULT '',
                status TEXT DEFAULT '',
                filled_cost_usd REAL DEFAULT 0,
                filled_shares REAL DEFAULT 0,
                reason TEXT DEFAULT '',
                response_json TEXT DEFAULT '{}'
            )
            """
        )
        conn.commit()



def _normalize_trade_record(raw: dict[str, Any]) -> dict[str, Any]:
    timestamp = utc_timestamp_text(_safe_timestamp(raw.get("timestamp") or raw.get("Timestamp (UTC)")))
    decision = str(raw.get("decision") or raw.get("AI Decision") or Direction.UNKNOWN.value).upper()
    score = _safe_int(raw.get("score"), _safe_int(raw.get("Score")))
    bonus_score = _safe_int(raw.get("bonus_score"), _safe_int(raw.get("Bonus Score")))
    pnl = _safe_float(raw.get("pnl_impact"), _safe_float(raw.get("PnL")))
    trigger_reason = str(raw.get("trigger_reason") or raw.get("Trigger Reason") or "")

    return {
        "timestamp": timestamp,
        "slug": str(raw.get("slug") or raw.get("Market Slug") or ""),
        "decision": decision,
        "strike_price": _safe_float(raw.get("strike"), _safe_float(raw.get("Strike Price"))),
        "final_price": _safe_float(raw.get("final_price"), _safe_float(raw.get("Final Price"))),
        "actual_outcome": str(raw.get("actual_outcome") or raw.get("Actual Outcome") or ""),
        "result": str(raw.get("result") or raw.get("Result") or ""),
        "pnl_impact": pnl,
        "entry_price": _safe_float(raw.get("entry_price"), _safe_float(raw.get("Entry Price"))),
        "exit_price": _safe_float(raw.get("exit_price"), _safe_float(raw.get("Exit Price"))),
        "entry_underlying": _safe_float(raw.get("entry_underlying"), _safe_float(raw.get("Entry Underlying"))),
        "exit_underlying": _safe_float(raw.get("exit_underlying"), _safe_float(raw.get("Exit Underlying"))),
        "bet_size_usd": _safe_float(raw.get("bet_size_usd"), _safe_float(raw.get("Bet Size (USD)"))),
        "confidence": str(raw.get("confidence") or raw.get("Confidence") or ""),
        "score": score,
        "bonus_score": bonus_score,
        "ai_validated": bool(raw.get("ai_validated", False)),
        "trigger_reason": trigger_reason,
        "local_calc_outcome": str(raw.get("local_calc_outcome") or raw.get("Local Calc") or trigger_reason),
        "official_outcome": str(raw.get("official_outcome") or raw.get("Poly Official") or raw.get("actual_outcome") or ""),
        "match_status": str(raw.get("match_status") or raw.get("Match Status") or ""),
        "metadata_json": raw.get("metadata_json") or json.dumps(raw.get("metadata", {})),
    }



def _normalize_execution_record(raw: dict[str, Any]) -> dict[str, Any]:
    timestamp = utc_timestamp_text(_safe_timestamp(raw.get("timestamp")))
    return {
        "timestamp": timestamp,
        "slug": str(raw.get("slug") or ""),
        "direction": str(raw.get("direction") or ""),
        "expected_price": _safe_float(raw.get("expected_price")),
        "actual_price": _safe_float(raw.get("actual_price")),
        "slippage_bps": _safe_float(raw.get("slippage_bps")),
        "spread_cents": _safe_float(raw.get("spread_cents")),
        "liquidity_check": str(raw.get("liquidity_check") or ""),
        "order_type": str(raw.get("order_type") or ""),
        "status": str(raw.get("status") or ""),
        "filled_cost_usd": _safe_float(raw.get("filled_cost_usd")),
        "filled_shares": _safe_float(raw.get("filled_shares")),
        "reason": str(raw.get("reason") or ""),
        "response_json": str(raw.get("response_json") or "{}"),
    }



def _journal_row(record: dict[str, Any], running_win_rate_pct: float) -> dict[str, Any]:
    return {
        "Timestamp (UTC)": record["timestamp"],
        "Market Slug": record["slug"],
        "AI Decision": record["decision"],
        "Strike Price": round(record["strike_price"], 2),
        "Final Price": round(record["final_price"], 2),
        "Actual Outcome": record["actual_outcome"],
        "Result": record["result"],
        "Running Win Rate (%)": round(running_win_rate_pct, 2),
        "PnL": round(record["pnl_impact"], 2),
        "Local Calc": record["local_calc_outcome"],
        "Poly Official": record["official_outcome"],
        "Match Status": record["match_status"],
        "Trigger Reason": record["trigger_reason"],
    }



def _refresh_analytics(runtime: EngineServices) -> None:
    sorted_trades = sorted(runtime.trade_history, key=lambda item: item["timestamp"])
    balance = runtime.bankroll
    equity_curve: list[dict[str, Any]] = []
    daily: dict[str, dict[str, Any]] = {}
    wins = 0
    losses = 0

    for record in sorted_trades:
        pnl = _safe_float(record.get("pnl_impact"))
        balance += pnl
        timestamp = _safe_timestamp(record["timestamp"])
        equity_curve.append({"time": _epoch_seconds(timestamp), "value": round(balance, 2)})

        day_key = timestamp.date().isoformat()
        bucket = daily.setdefault(day_key, {"date": day_key, "day": timestamp.strftime("%a"), "pnl": 0.0, "trades": 0})
        bucket["pnl"] += pnl
        bucket["trades"] += 1

        result = str(record.get("result") or "").upper()
        if result == "WIN":
            wins += 1
        elif result == "LOSS":
            losses += 1

    runtime.equity_curve = equity_curve[-500:]
    runtime.daily_pnl_series = [
        {
            "date": item["date"],
            "day": item["day"],
            "pnl": round(item["pnl"], 2),
            "trades": int(item["trades"]),
        }
        for item in sorted(daily.values(), key=lambda value: value["date"])[-21:]
    ]
    runtime.metrics_cache = None
    runtime.metrics_cache_ts = 0.0



def _sync_write_db_batch(items: list[dict[str, Any]]) -> None:
    if not items:
        return
    with sqlite3.connect(DB_PATH, timeout=30.0) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        for item in items:
            table = item.get("table")
            row = item.get("row", {})
            if table == "trades":
                record = _normalize_trade_record(row)
                conn.execute(
                    """
                    INSERT INTO trades (
                        timestamp, slug, decision, strike, final_price, actual_outcome, result, pnl_impact,
                        entry_price, exit_price, entry_underlying, exit_underlying, bet_size_usd,
                        confidence, score, bonus_score, ai_validated, trigger_reason, local_calc_outcome,
                        official_outcome, match_status, metadata_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        record["timestamp"],
                        record["slug"],
                        record["decision"],
                        record["strike_price"],
                        record["final_price"],
                        record["actual_outcome"],
                        record["result"],
                        record["pnl_impact"],
                        record["entry_price"],
                        record["exit_price"],
                        record["entry_underlying"],
                        record["exit_underlying"],
                        record["bet_size_usd"],
                        record["confidence"],
                        record["score"],
                        record["bonus_score"],
                        int(record["ai_validated"]),
                        record["trigger_reason"],
                        record["local_calc_outcome"],
                        record["official_outcome"],
                        record["match_status"],
                        str(record["metadata_json"]),
                    ),
                )
            elif table == "execution_metrics":
                record = _normalize_execution_record(row)
                conn.execute(
                    """
                    INSERT INTO execution_metrics (
                        timestamp, slug, direction, expected_price, actual_price, slippage_bps, spread_cents,
                        liquidity_check, order_type, status, filled_cost_usd, filled_shares, reason, response_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        record["timestamp"],
                        record["slug"],
                        record["direction"],
                        record["expected_price"],
                        record["actual_price"],
                        record["slippage_bps"],
                        record["spread_cents"],
                        record["liquidity_check"],
                        record["order_type"],
                        record["status"],
                        record["filled_cost_usd"],
                        record["filled_shares"],
                        record["reason"],
                        record["response_json"],
                    ),
                )
        conn.commit()



def _sync_write_ml_batch(items: list[dict[str, Any]]) -> None:
    if not items:
        return
    ML_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    existing_header: list[str] | None = None
    if ML_CSV_PATH.exists() and ML_CSV_PATH.stat().st_size > 0:
        with ML_CSV_PATH.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.reader(handle)
            existing_header = next(reader, None)

    fieldnames = existing_header or sorted({key for item in items for key in item.keys()})

    with ML_CSV_PATH.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        if handle.tell() == 0:
            writer.writeheader()
        for item in items:
            row = {key: item.get(key, "") for key in fieldnames}
            writer.writerow(row)


async def db_writer_worker(runtime: EngineServices) -> None:
    state = runtime.state
    while True:
        item = await state.db_queue.get()
        shutdown = bool(item.get("__shutdown__"))
        batch = [] if shutdown else [item]
        if not shutdown:
            while len(batch) < 100:
                try:
                    queued = state.db_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                if queued.get("__shutdown__"):
                    shutdown = True
                    state.db_queue.task_done()
                    break
                batch.append(queued)
        try:
            if batch:
                await asyncio.to_thread(_sync_write_db_batch, batch)
        finally:
            state.db_queue.task_done()
            for _ in batch[1:]:
                state.db_queue.task_done()
        if shutdown:
            break


async def ml_writer_worker(runtime: EngineServices) -> None:
    state = runtime.state
    while True:
        item = await state.ml_queue.get()
        shutdown = bool(item.get("__shutdown__"))
        batch = [] if shutdown else [item]
        if not shutdown:
            while len(batch) < 100:
                try:
                    queued = state.ml_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                if queued.get("__shutdown__"):
                    shutdown = True
                    state.ml_queue.task_done()
                    break
                batch.append(queued)
        try:
            if batch:
                await asyncio.to_thread(_sync_write_ml_batch, batch)
        finally:
            state.ml_queue.task_done()
            for _ in batch[1:]:
                state.ml_queue.task_done()
        if shutdown:
            break



def _sqlite_table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


async def load_persisted_history(runtime: EngineServices) -> None:
    if not DB_PATH.exists():
        runtime.current_balance = runtime.bankroll
        await runtime.state.update_runtime_counters(
            simulated_balance=runtime.current_balance,
            current_daily_pnl=0.0,
            total_wins=0,
            total_losses=0,
            trades_this_hour=0,
            current_hour=utc_now().hour,
            current_day=utc_now().date().isoformat(),
        )
        return

    def _load() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        with sqlite3.connect(DB_PATH, timeout=30.0) as conn:
            conn.row_factory = sqlite3.Row
            loaded_trades: list[dict[str, Any]] = []
            loaded_execs: list[dict[str, Any]] = []
            if _sqlite_table_exists(conn, "trades"):
                loaded_trades = [dict(row) for row in conn.execute("SELECT * FROM trades ORDER BY id ASC")]
            if _sqlite_table_exists(conn, "execution_metrics"):
                loaded_execs = [dict(row) for row in conn.execute("SELECT * FROM execution_metrics ORDER BY id ASC")]
            return loaded_trades, loaded_execs

    trade_rows, execution_rows = await asyncio.to_thread(_load)

    runtime.trade_history = [_normalize_trade_record(row) for row in trade_rows]
    runtime.execution_history = [_normalize_execution_record(row) for row in execution_rows]
    _refresh_analytics(runtime)

    total_historical_pnl = sum(item["pnl_impact"] for item in runtime.trade_history)
    today = utc_now()
    today_key = today.date().isoformat()
    today_pnl = sum(item["pnl_impact"] for item in runtime.trade_history if item["timestamp"].startswith(today_key))
    this_hour = today.hour
    trades_this_hour = sum(
        1
        for item in runtime.trade_history
        if _safe_timestamp(item["timestamp"]).hour == this_hour and _safe_timestamp(item["timestamp"]).date() == today.date()
    )
    wins = sum(1 for item in runtime.trade_history if str(item.get("result", "")).upper() == "WIN")
    losses = sum(1 for item in runtime.trade_history if str(item.get("result", "")).upper() == "LOSS")
    runtime.current_balance = runtime.bankroll + total_historical_pnl
    runtime.risk_manager.current_daily_pnl = today_pnl
    runtime.risk_manager.trades_this_hour = trades_this_hour
    runtime.risk_manager.current_hour = this_hour
    runtime.risk_manager.current_day = today.date()

    await runtime.state.update_runtime_counters(
        simulated_balance=runtime.current_balance,
        current_daily_pnl=today_pnl,
        total_wins=wins,
        total_losses=losses,
        trades_this_hour=trades_this_hour,
        current_hour=this_hour,
        current_day=today_key,
        max_trade_pct=runtime.risk_manager.config.max_trade_pct,
        max_daily_loss_pct=runtime.risk_manager.config.max_daily_loss_pct,
        kill_switch_enabled=False,
    )


def _paper_balance(runtime: EngineServices) -> float:
    return runtime.bankroll + sum(item["pnl_impact"] for item in runtime.trade_history)



def _extract_balance_value(payload: Any) -> float:
    if payload is None:
        return 0.0
    if isinstance(payload, (int, float)):
        return max(0.0, float(payload))
    if isinstance(payload, str):
        try:
            return max(0.0, float(payload))
        except ValueError:
            return 0.0
    if isinstance(payload, list):
        for item in payload:
            value = _extract_balance_value(item)
            if value > 0:
                return value
        return 0.0
    if isinstance(payload, dict):
        preferred_keys = (
            "available_balance",
            "availableBalance",
            "balance",
            "amount",
            "value",
            "balance_usdc",
            "balanceUsd",
            "usdc",
            "available",
        )
        for key in preferred_keys:
            if key in payload:
                value = _extract_balance_value(payload[key])
                if value > 0:
                    return value
        for key, value in payload.items():
            if "allowance" in str(key).lower():
                continue
            parsed = _extract_balance_value(value)
            if parsed > 0:
                return parsed
    return 0.0


def _parse_collateral_balance_usd(payload: Any) -> float:
    raw_balance: Any = None
    if isinstance(payload, dict):
        raw_balance = payload.get("balance")
    elif isinstance(payload, (int, float, str)):
        raw_balance = payload

    if raw_balance is None:
        return 0.0

    raw_text = str(raw_balance).strip()
    if not raw_text:
        return 0.0

    try:
        value = float(raw_text)
    except ValueError:
        return 0.0

    if value <= 0:
        return 0.0

    # Polymarket collateral balances are returned in 6-decimal USDC.e base units.
    if "." not in raw_text and "e" not in raw_text.lower():
        return value / 1_000_000.0
    return value


async def build_clob_client() -> ClobClient | None:
    if not POLY_PRIVATE_KEY:
        return None
    client = ClobClient(
        host=POLY_HOST,
        chain_id=POLY_CHAIN_ID,
        key=POLY_PRIVATE_KEY,
        signature_type=POLY_SIG_TYPE,
        funder=POLY_FUNDER or None,
    )
    creds = await asyncio.to_thread(client.create_or_derive_api_creds)
    await asyncio.to_thread(client.set_api_creds, creds)
    return client


async def fetch_live_balance(runtime: EngineServices, *, force: bool = False) -> float:
    if runtime.paper_trading or runtime.clob_client is None:
        return runtime.current_balance or runtime.state.simulated_balance

    async with runtime.balance_lock:
        existing = runtime.current_balance or runtime.state.simulated_balance
        if existing > 0 and not force:
            async with runtime.state.risk_lock:
                current_ts = time.time()
                last_log = runtime.state.kill_switch_last_log_ts
            if current_ts - last_log < BALANCE_REFRESH_SECS:
                return existing

        params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, signature_type=POLY_SIG_TYPE)
        try:
            balance_payload = await asyncio.to_thread(runtime.clob_client.get_balance_allowance, params)
            balance = _parse_collateral_balance_usd(balance_payload)
            if balance > 0:
                runtime.current_balance = balance
                await runtime.state.update_runtime_counters(simulated_balance=balance, kill_switch_last_log_ts=time.time())
                return balance
            logger.warning("[BALANCE] Live balance payload returned no usable collateral balance: %s", balance_payload)
        except Exception as exc:
            logger.warning("[BALANCE] Failed to fetch live balance: %s", exc)
        return existing if existing > 0 else runtime.bankroll


async def resolve_active_market_slug(runtime: EngineServices, *, force: bool = False) -> str:
    current_target = runtime.state.target_slug.strip()
    latest_odds = runtime.latest_odds
    if (
        not force
        and current_target
        and latest_odds is not None
        and latest_odds.slug == current_target
        and latest_odds.market_found
        and latest_odds.seconds_remaining > runtime.strategy_config.risk.min_seconds_remaining
    ):
        return current_target

    candidate_slugs: list[str] = []
    for candidate in [current_target, *candidate_market_slugs()]:
        candidate = candidate.strip()
        if not candidate or candidate in candidate_slugs:
            continue
        candidate_slugs.append(candidate)

    best_slug = ""
    best_seconds_remaining: float | None = None
    for candidate in candidate_slugs:
        try:
            async with api_slot(runtime):
                odds = await runtime.polymarket_fetcher.fetch_market_odds(runtime.http_session, candidate)
        except Exception:
            continue
        if not odds.market_found or odds.seconds_remaining <= 0:
            continue
        if best_seconds_remaining is None or odds.seconds_remaining < best_seconds_remaining:
            best_slug = odds.slug or candidate
            best_seconds_remaining = odds.seconds_remaining

    return best_slug or current_market_slug()


async def sync_target_market(runtime: EngineServices, *, force: bool = False) -> str:
    slug = await resolve_active_market_slug(runtime, force=force)
    prefix = market_family_prefix(slug)
    if slug != runtime.state.target_slug:
        await runtime.state.set_target_market(slug, prefix)
        logger.info("Starting Quant Engine: %s | Prefix: %s", slug, prefix)
    return slug


async def roll_state_clock(runtime: EngineServices) -> None:
    now_utc = utc_now()
    day = now_utc.date().isoformat()
    hour = now_utc.hour
    current_daily_pnl = runtime.state.current_daily_pnl
    trades_this_hour = runtime.state.trades_this_hour
    if runtime.state.current_day != day:
        current_daily_pnl = 0.0
    if runtime.state.current_hour != hour:
        trades_this_hour = 0
    await runtime.state.update_runtime_counters(current_day=day, current_hour=hour, current_daily_pnl=current_daily_pnl, trades_this_hour=trades_this_hour)
    runtime.risk_manager.current_day = now_utc.date()
    runtime.risk_manager.current_hour = hour
    runtime.risk_manager.current_daily_pnl = current_daily_pnl
    runtime.risk_manager.trades_this_hour = trades_this_hour


def _require_control_auth(request: Request) -> None:
    if not ENGINE_CONTROL_API_KEY:
        return
    supplied = request.headers.get("x-api-key", "").strip()
    if supplied != ENGINE_CONTROL_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


async def build_context_from_state(runtime: EngineServices) -> TechnicalContext | None:
    async with runtime.state.market_lock:
        history = list(runtime.state.candle_history)
        live_candle = runtime.state.live_candle.clone() if runtime.state.live_candle is not None else None
        live_price = runtime.state.live_price
        vwap_cum_pv = runtime.state.vwap_cum_pv
        vwap_cum_vol = runtime.state.vwap_cum_vol
        raw_cvd_total = runtime.state.cvd_total
        raw_snapshot = runtime.state.cvd_snapshot_at_candle_open
        raw_cvd_1min = runtime.state.last_cvd_1min
        cvd_buffer = list(runtime.state.cvd_1min_buffer)
        ema_fast = runtime.state.ema_fast_9
        ema_slow = runtime.state.ema_slow_21
        rsi = runtime.state.rsi_14
        indicator_last_close_ms = runtime.state.indicator_last_close_ms

    if live_candle is None:
        if not history:
            return None
        live_candle = history[-1].clone()
    if live_price > 0 and (live_candle.close <= 0 or not live_candle.is_closed):
        live_candle = replace(live_candle, price=live_price, close=live_price)

    price = live_candle.resolved_price
    if price <= 0:
        return None

    if ema_fast is None or ema_slow is None or rsi is None:
        raise RuntimeError("EngineState indicators are not initialized. Call await state.initialize() first.")

    committed_history = history
    if history and live_candle.is_closed:
        latest_history_marker = history[-1].close_time_ms or history[-1].event_time_ms
        live_marker = live_candle.close_time_ms or live_candle.event_time_ms
        if latest_history_marker > 0 and latest_history_marker == live_marker:
            committed_history = history[:-1]

    next_indicator_close_ms = indicator_last_close_ms
    for candle in committed_history:
        close_marker = candle.close_time_ms or candle.event_time_ms
        if close_marker <= indicator_last_close_ms:
            continue
        close_price = candle.close if candle.close > 0 else candle.resolved_price
        if close_price <= 0:
            continue
        ema_fast.update(close_price)
        ema_slow.update(close_price)
        rsi.update(close_price)
        next_indicator_close_ms = max(next_indicator_close_ms, close_marker)

    if next_indicator_close_ms != indicator_last_close_ms:
        async with runtime.state.market_lock:
            runtime.state.indicator_last_close_ms = max(runtime.state.indicator_last_close_ms, next_indicator_close_ms)

    recent_candles = history[-30:] if history else [live_candle]
    volume_baseline = mean([max(candle.volume, 0.0) for candle in recent_candles[-20:]]) if recent_candles else 0.0
    blended_vol = blended_intraday_volatility(recent_candles)

    recent_micro_flow = 0.0
    for item in cvd_buffer:
        if isinstance(item, tuple) and len(item) == 2:
            recent_micro_flow += abs(_safe_float(item[1]))
        else:
            recent_micro_flow += abs(_safe_float(item))

    cvd_total_notional = raw_cvd_total * price
    cvd_snapshot_notional = raw_snapshot * price
    cvd_1min_notional = raw_cvd_1min * price
    adaptive_cvd_threshold = max(
        5000.0,
        recent_micro_flow * price * 0.35,
        volume_baseline * price * max(blended_vol, 0.001) * 0.12,
    )

    context = build_technical_context(
        live_candle,
        history,
        ema_fast=ema_fast,
        ema_slow=ema_slow,
        rsi=rsi,
        vwap=compute_vwap(vwap_cum_pv, vwap_cum_vol),
        cvd_total=cvd_total_notional,
        cvd_snapshot_at_candle_open=cvd_snapshot_notional,
        cvd_1min_delta=cvd_1min_notional,
        adaptive_cvd_threshold=adaptive_cvd_threshold,
    )

    await runtime.state.update_telemetry(
        cvd_snapshot=CVDSnapshot(
            delta=context.cvd_candle_delta,
            one_min_delta=context.cvd_1min_delta,
            threshold=context.adaptive_cvd_threshold,
            divergence="NONE",
            divergence_strength=0.0,
        ),
        adaptive_thresholds=AdaptiveThresholdSnapshot(
            atr_min=context.adaptive_atr_floor,
            cvd_threshold=context.adaptive_cvd_threshold,
            volatility_lookback=min(len(history), 30),
        ),
        context=context,
    )
    runtime.latest_context = context
    return context


async def _live_binance_reference_price_from_state(runtime: EngineServices, odds: MarketOddsSnapshot) -> float:
    if odds.market_resolution != MarketResolution.CANDLE_OPEN or odds.market_end_time is None:
        return 0.0

    market_start_time = odds.market_end_time.astimezone(timezone.utc) - timedelta(hours=1)
    if utc_now() < market_start_time:
        return 0.0

    async with runtime.state.market_lock:
        candidates = []
        if runtime.state.live_candle is not None:
            candidates.append(runtime.state.live_candle.clone())
        candidates.extend(candle.clone() for candle in list(runtime.state.candle_history)[-4:])

    for candle in candidates:
        candle_start = candle.timestamp.astimezone(timezone.utc)
        if abs((candle_start - market_start_time).total_seconds()) > 1.0:
            continue
        live_open = candle.open if candle.open > 0 else candle.resolved_price
        if live_open > 0:
            return live_open

    return 0.0


async def fetch_market_odds(runtime: EngineServices, slug: str) -> tuple[MarketOddsSnapshot, dict[Direction, LiquidityProfile]]:
    async with api_slot(runtime):
        odds = await runtime.polymarket_fetcher.fetch_market_odds(runtime.http_session, slug)

    liquidity_by_direction: dict[Direction, LiquidityProfile] = {}
    if not odds.market_found:
        runtime.latest_odds = odds
        return odds, liquidity_by_direction

    live_reference_price = await _live_binance_reference_price_from_state(runtime, odds)
    if live_reference_price > 0:
        odds = replace(
            odds,
            reference_price=live_reference_price,
            strike_price=live_reference_price,
        )

    probe_size = min(2.0, max(runtime.execution_config.live_tiny_amm_fallback_max_bet_usd, 1.0))
    if odds.up_token_id:
        up_check = await runtime.execution_engine.check_liquidity_and_spread(
            odds.up_token_id,
            bet_size_usd=probe_size,
            expected_price=odds.up_public_prob_pct / 100.0,
            side="buy",
            odds=odds,
        )
        liquidity_by_direction[Direction.UP] = up_check.as_profile()
        odds = replace(
            odds,
            up_best_bid=up_check.best_bid,
            up_best_ask=up_check.best_ask,
            up_entry_prob_pct=round(up_check.entry_price * 100.0, 2) if up_check.ok and up_check.entry_price > 0 else None,
        )
    if odds.down_token_id:
        down_check = await runtime.execution_engine.check_liquidity_and_spread(
            odds.down_token_id,
            bet_size_usd=probe_size,
            expected_price=odds.down_public_prob_pct / 100.0,
            side="buy",
            odds=odds,
        )
        liquidity_by_direction[Direction.DOWN] = down_check.as_profile()
        odds = replace(
            odds,
            down_best_bid=down_check.best_bid,
            down_best_ask=down_check.best_ask,
            down_entry_prob_pct=round(down_check.entry_price * 100.0, 2) if down_check.ok and down_check.entry_price > 0 else None,
        )

    if slug not in runtime.market_audit_logged:
        logger.info(
            "[MARKET AUDIT] requested_slug=%s | matched_slug=%s | outcomes=%s | resolution=%s | up_token=%s | down_token=%s",
            slug,
            odds.market_slug or odds.slug,
            ", ".join(odds.outcome_labels) if odds.outcome_labels else "UNKNOWN",
            odds.market_resolution.value,
            odds.up_token_id or "N/A",
            odds.down_token_id or "N/A",
        )
        runtime.market_audit_logged.add(slug)
        # Dynamically update CLOB WS subscriptions for the new market's token IDs
        if runtime.clob_ws_manager is not None:
            ws_tokens = [t for t in (odds.up_token_id, odds.down_token_id) if t]
            if ws_tokens:
                runtime.clob_ws_manager.update_subscriptions(ws_tokens)
                logger.info("[CLOB WS] Updated subscriptions: %d token(s) for %s", len(ws_tokens), slug)

    runtime.latest_odds = odds
    return odds, liquidity_by_direction


def _position_has_ai_validation(position: dict[str, Any]) -> bool:
    ml_features = position.get("ml_features", {})
    return bool(ml_features.get("ai_validated") or ml_features.get("ai_confirmed"))


def _build_execution_metric(
    *,
    slug: str,
    direction: Direction,
    expected_price: float,
    actual_price: float,
    spread_cents: float,
    liquidity_check: str,
    order_type: str,
    status: str,
    filled_cost_usd: float,
    filled_shares: float,
    reason: str,
    response_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    expected = max(expected_price, 0.0001)
    slippage_bps = ((actual_price - expected) / expected) * 10000.0 if actual_price > 0 else 0.0
    return {
        "timestamp": utc_timestamp_text(),
        "slug": slug,
        "direction": direction.value,
        "expected_price": round(expected_price, 6),
        "actual_price": round(actual_price, 6),
        "slippage_bps": round(slippage_bps, 2),
        "spread_cents": round(spread_cents * 100.0, 3),
        "liquidity_check": liquidity_check,
        "order_type": order_type,
        "status": status,
        "filled_cost_usd": round(filled_cost_usd, 6),
        "filled_shares": round(filled_shares, 6),
        "reason": reason,
        "response_json": json.dumps(response_payload or {}, default=str),
    }


async def _record_execution(runtime: EngineServices, record: dict[str, Any]) -> None:
    runtime.execution_history.append(_normalize_execution_record(record))
    runtime.execution_history = runtime.execution_history[-500:]
    runtime.metrics_cache = None
    runtime.metrics_cache_ts = 0.0
    await runtime.state.db_queue.put({"table": "execution_metrics", "row": record})


async def _record_trade(runtime: EngineServices, record: dict[str, Any]) -> None:
    normalized = _normalize_trade_record(record)
    runtime.trade_history.append(normalized)
    runtime.trade_history = runtime.trade_history[-5000:]
    _refresh_analytics(runtime)
    await runtime.state.db_queue.put({"table": "trades", "row": record})
    await runtime.state.ml_queue.put(
        {
            "timestamp": normalized["timestamp"],
            "slug": normalized["slug"],
            "decision": normalized["decision"],
            "pnl_impact": normalized["pnl_impact"],
            "result": normalized["result"],
            "ai_validated": int(bool(normalized["ai_validated"])),
            "score": normalized["score"],
            "bonus_score": normalized["bonus_score"],
            "entry_price": normalized["entry_price"],
            "exit_price": normalized["exit_price"],
            "entry_underlying": normalized["entry_underlying"],
            "exit_underlying": normalized["exit_underlying"],
        }
    )


def _resolve_outcome(final_underlying: float, strike_price: float) -> str:
    if final_underlying >= strike_price:
        return Direction.UP.value
    return Direction.DOWN.value


def _decode_json_array(raw: Any) -> list[Any]:
    if isinstance(raw, list):
        return raw
    if isinstance(raw, tuple):
        return list(raw)
    if isinstance(raw, str):
        with suppress(Exception):
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return parsed
    return []


def _opposite_direction(direction: str) -> str:
    if direction == Direction.UP.value:
        return Direction.DOWN.value
    if direction == Direction.DOWN.value:
        return Direction.UP.value
    return "TIE"


def _official_outcome_to_underlying(outcome: str, strike_price: float, fallback_underlying: float) -> float:
    if strike_price <= 0:
        return fallback_underlying
    if outcome == Direction.UP.value:
        return strike_price + 0.01
    if outcome == Direction.DOWN.value:
        return strike_price - 0.01
    return strike_price


def _parse_iso_datetime(raw: str) -> datetime | None:
    if not raw:
        return None
    with suppress(Exception):
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    return None


async def _fetch_binance_hourly_resolution_candle(
    runtime: EngineServices,
    market_end_time: datetime,
) -> tuple[float, float] | None:
    candle_end_utc = market_end_time.astimezone(timezone.utc)
    candle_start_utc = candle_end_utc - timedelta(hours=1)
    params = {
        "symbol": runtime.data_config.symbol,
        "interval": "1h",
        "startTime": int(candle_start_utc.timestamp() * 1000),
        "limit": 1,
    }
    try:
        async with api_slot(runtime):
            async with runtime.http_session.get(f"{runtime.data_config.binance_rest_api}/api/v3/klines", params=params) as response:
                if response.status != 200:
                    return None
                payload = await response.json()
        if not payload:
            return None
        row = payload[0]
        return float(row[1]), float(row[4])
    except Exception:
        return None


def _extract_market_from_gamma_payload(payload: Any, slug: str) -> dict[str, Any] | None:
    if isinstance(payload, list) and payload:
        return _extract_market_from_gamma_payload(payload[0], slug)
    if not isinstance(payload, dict):
        return None
    if str(payload.get("slug") or "") == slug and payload.get("clobTokenIds") is not None:
        return payload

    markets = payload.get("markets")
    if isinstance(markets, list):
        for market in markets:
            if isinstance(market, dict) and str(market.get("slug") or "") == slug:
                return market
        for market in markets:
            if isinstance(market, dict) and market.get("clobTokenIds") is not None:
                return market
    return None


def _extract_official_market_outcome(position: dict[str, Any], market: dict[str, Any]) -> tuple[str | None, str]:
    decision = str(position.get("decision") or Direction.UNKNOWN.value)
    opposite = _opposite_direction(decision)
    token_id = str(position.get("token_id") or "")
    token_ids = [str(item) for item in _decode_json_array(market.get("clobTokenIds"))]
    outcome_prices = [_safe_float(item) for item in _decode_json_array(market.get("outcomePrices"))]

    for winner_key in ("winningToken", "winningTokenId", "winnerToken", "winning_token"):
        winner_token = str(market.get(winner_key) or "").strip()
        if not winner_token:
            continue
        if winner_token == token_id:
            return decision, winner_key
        if winner_token in token_ids:
            return opposite, winner_key

    if token_id and token_ids and outcome_prices and len(token_ids) == len(outcome_prices):
        max_price = max(outcome_prices)
        min_price = min(outcome_prices)
        if max_price >= 0.99 and min_price <= 0.01:
            winner_index = outcome_prices.index(max_price)
            winner_token = token_ids[winner_index]
            if winner_token == token_id:
                return decision, "outcomePrices"
            return opposite, "outcomePrices"

    resolved_text = str(market.get("resolution") or market.get("resolvedBy") or "").upper()
    if "TIE" in resolved_text or "DRAW" in resolved_text:
        return "TIE", "resolution_text"

    return None, "pending"


async def _poll_official_market_outcome(
    runtime: EngineServices,
    slug: str,
    position: dict[str, Any],
    *,
    max_wait_seconds: float = 30.0,
    poll_interval_seconds: float = 5.0,
) -> tuple[str | None, str]:
    deadline = time.monotonic() + max_wait_seconds
    last_reason = "official outcome pending"
    urls = [
        f"{runtime.data_config.gamma_api}/events/{slug}",
        f"{runtime.data_config.gamma_api}/events/slug/{slug}",
    ]

    while time.monotonic() < deadline and not runtime.stop_event.is_set():
        for url in urls:
            try:
                async with api_slot(runtime):
                    async with runtime.http_session.get(url) as response:
                        if response.status != 200:
                            last_reason = f"gamma_http_{response.status}"
                            continue
                        payload = await response.json()
                market = _extract_market_from_gamma_payload(payload, slug)
                if market is None:
                    last_reason = "gamma_market_missing"
                    continue
                outcome, source = _extract_official_market_outcome(position, market)
                if outcome is not None:
                    return outcome, source
                last_reason = source
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                last_reason = str(exc)

        if (deadline - time.monotonic()) <= poll_interval_seconds:
            break
        await asyncio.sleep(poll_interval_seconds)

    return None, last_reason


async def finalize_closed_position(
    runtime: EngineServices,
    position: dict[str, Any],
    *,
    exit_reason: str,
    exit_price: float,
    exit_underlying: float,
    exit_result: FillResult | None = None,
    settled_at_expiry: bool = False,
    official_outcome: str | None = None,
    settlement_source: str | None = None,
) -> None:
    bet_size_usd = _safe_float(position.get("bet_size_usd"))
    bought_price = _safe_float(position.get("bought_price"))
    shares_owned = bet_size_usd / max(bought_price, 0.0001)
    proceeds = exit_result.filled_cost_usd if exit_result is not None else shares_owned * exit_price
    pnl = proceeds - bet_size_usd
    outcome = _resolve_outcome(exit_underlying, _safe_float(position.get("strike")))
    resolved_official_outcome = official_outcome or (outcome if settled_at_expiry else "EARLY_EXIT")
    resolved_match_status = settlement_source or ("LOCAL_SETTLED" if settled_at_expiry else "EARLY_EXIT")
    result = "WIN" if pnl > 0 else "LOSS" if pnl < 0 else "TIE"
    ai_validated = _position_has_ai_validation(position)

    runtime.risk_manager.record_pnl(pnl)
    wins = runtime.state.total_wins + (1 if result == "WIN" else 0)
    losses = runtime.state.total_losses + (1 if result == "LOSS" else 0)
    next_balance = runtime.state.simulated_balance + pnl
    runtime.current_balance = next_balance if runtime.paper_trading else runtime.current_balance
    await runtime.state.update_runtime_counters(
        simulated_balance=next_balance,
        current_daily_pnl=runtime.state.current_daily_pnl + pnl,
        total_wins=wins,
        total_losses=losses,
    )
    if not runtime.paper_trading:
        runtime.current_balance = runtime.state.simulated_balance

    trade_record = {
        "timestamp": utc_timestamp_text(),
        "slug": position.get("slug", ""),
        "decision": str(position.get("decision", Direction.UNKNOWN.value)),
        "strike": _safe_float(position.get("strike")),
        "final_price": round(exit_underlying, 2),
        "actual_outcome": outcome,
        "result": result,
        "pnl_impact": round(pnl, 2),
        "entry_price": _safe_float(position.get("bought_price")),
        "exit_price": round(exit_price, 4),
        "entry_underlying": _safe_float(position.get("entry_underlying_price")),
        "exit_underlying": round(exit_underlying, 2),
        "bet_size_usd": bet_size_usd,
        "confidence": str(position.get("ml_features", {}).get("confidence", "")),
        "score": _safe_int(position.get("score")),
        "bonus_score": _safe_int(position.get("bonus_score")),
        "ai_validated": ai_validated,
        "trigger_reason": exit_reason,
        "local_calc_outcome": exit_reason,
        "official_outcome": resolved_official_outcome,
        "match_status": resolved_match_status,
        "metadata_json": json.dumps(position.get("ml_features", {}), default=str),
    }
    await _record_trade(runtime, trade_record)
    await runtime.state.clear_slug_commit(trade_record["slug"])
    await runtime.state.clear_ai_state(trade_record["slug"])
    if "STOP_LOSS" in exit_reason.upper():
        await runtime.state.record_reentry_state(
            trade_record["slug"],
            exit_reason=exit_reason,
            direction=Direction(trade_record["decision"]) if trade_record["decision"] in Direction._value2member_map_ else Direction.UNKNOWN,
            entry_ev_pct=_safe_float(position.get("ml_features", {}).get("expected_ev_pct")),
        )
    else:
        await runtime.state.clear_reentry_state(trade_record["slug"])
    runtime.position_heartbeat_ts.pop(trade_record["slug"], None)
    logger.info(
        "[TRADE CLOSED] %s | %s | PnL: $%+.2f | Exit: %.4f | Underlying: %.2f | Reason: %s",
        trade_record["slug"],
        trade_record["decision"],
        pnl,
        exit_price,
        exit_underlying,
        exit_reason,
    )

    # --- Auto-refit calibration layer after trade resolution ---
    calibrator = getattr(runtime.strategy_engine, "calibrator", None)
    if calibrator is not None:
        resolved_n = wins + losses
        if calibrator.should_refit(resolved_n):
            refitted = calibrator.fit_from_history()
            if refitted:
                diag = calibrator.diagnostics()
                logger.info(
                    "[CALIBRATION] Refit triggered at %d resolved trades: A=%.4f B=%.4f",
                    diag["fitted_on_n"], diag.get("platt_a") or 0.0, diag.get("platt_b") or 0.0,
                )


async def settle_expired_position(
    runtime: EngineServices,
    position: dict[str, Any],
    *,
    final_underlying: float,
) -> FillResult:
    bet_size_usd = _safe_float(position.get("bet_size_usd"))
    bought_price = _safe_float(position.get("bought_price"))
    shares_owned = bet_size_usd / max(bought_price, 0.0001)
    strike_price = _safe_float(position.get("strike"))
    position_features = position.get("ml_features", {})
    market_resolution = str(position_features.get("market_resolution") or MarketResolution.UNKNOWN.value)
    market_end_time = _parse_iso_datetime(str(position_features.get("market_end_iso") or ""))
    official_outcome, source = await _poll_official_market_outcome(runtime, str(position.get("slug") or ""), position)
    settlement_source = f"POLYMARKET_GAMMA:{source}" if official_outcome is not None else f"LOCAL_BINANCE_FALLBACK:{source}"
    if official_outcome is not None:
        outcome = official_outcome
        resolved_underlying = _official_outcome_to_underlying(official_outcome, strike_price, final_underlying)
    else:
        hourly_candle = None
        if market_resolution == MarketResolution.CANDLE_OPEN.value and market_end_time is not None:
            hourly_candle = await _fetch_binance_hourly_resolution_candle(runtime, market_end_time)
        if hourly_candle is not None:
            candle_open, candle_close = hourly_candle
            outcome = Direction.UP.value if candle_close >= candle_open else Direction.DOWN.value
            resolved_underlying = candle_close
            settlement_source = f"LOCAL_BINANCE_1H_CANDLE_FALLBACK:{source}"
            logger.warning(
                "[SETTLEMENT] %s: official Polymarket outcome unavailable (%s). Falling back to Binance BTCUSDT 1H candle O=%.2f C=%.2f.",
                position.get("slug"),
                source,
                candle_open,
                candle_close,
            )
        else:
            await runtime.state.update_position(
                str(position.get("slug")),
                status=PositionStatus.RESOLVING,
                current_token_price=position.get("current_token_price", 0.0) or 0.0,
                mark_price=position.get("mark_price", 0.0) or 0.0,
            )
            await runtime.state.record_execution_failure(
                str(position.get("slug")),
                reason="EXPIRY_SETTLEMENT_UNAVAILABLE: finalized Binance BTCUSDT 1H candle unavailable",
            )
            logger.error(
                "[SETTLEMENT] %s: official Polymarket outcome unavailable (%s) and finalized Binance BTCUSDT 1H candle could not be fetched. Position moved to RESOLVING.",
                position.get("slug"),
                source,
            )
            return FillResult(
                success=False,
                order_type="SETTLE",
                response={"status": "unsettled"},
                filled_cost_usd=0.0,
                filled_shares=0.0,
                average_price=0.0,
                requested_price=0.0,
                requested_bet_usd=bet_size_usd,
                status="UNSETTLED",
                reason="EXPIRY_SETTLEMENT_UNAVAILABLE",
            )
    settlement_price = 0.0
    if outcome == str(position.get("decision")):
        settlement_price = 1.0
    elif outcome == "TIE":
        settlement_price = bought_price
    proceeds = shares_owned * settlement_price
    popped = await runtime.state.pop_position(str(position.get("slug")))
    if popped is not None:
        await finalize_closed_position(
            runtime,
            popped.to_dict(),
            exit_reason="EXPIRY_SETTLEMENT",
            exit_price=settlement_price,
            exit_underlying=resolved_underlying,
            exit_result=FillResult(
                success=True,
                order_type="SETTLE",
                response={"status": "settled"},
                filled_cost_usd=proceeds,
                filled_shares=shares_owned,
                average_price=settlement_price,
                requested_price=settlement_price,
                requested_bet_usd=bet_size_usd,
                status="settled",
                reason=f"EXPIRY_SETTLEMENT:{settlement_source}",
            ),
            settled_at_expiry=True,
            official_outcome=outcome,
            settlement_source=settlement_source,
        )
    return FillResult(
        success=True,
        order_type="SETTLE",
        response={"status": "settled"},
        filled_cost_usd=proceeds,
        filled_shares=shares_owned,
        average_price=settlement_price,
        requested_price=settlement_price,
        requested_bet_usd=bet_size_usd,
        status="settled",
        reason=f"EXPIRY_SETTLEMENT:{settlement_source}",
    )


async def maybe_validate_with_ai(
    runtime: EngineServices,
    signal: TradeSignal,
    context: TechnicalContext,
    odds: MarketOddsSnapshot,
) -> tuple[TradeSignal, str]:
    if not signal.needs_ai:
        return signal, ""

    ai_decision = await call_local_ai(
        runtime.http_session,
        runtime.state,
        signal,
        context,
        odds,
        agent=runtime.ai_agent,
        max_calls_override=(
            runtime.ai_config.max_calls_per_slug_paper
            if runtime.paper_trading
            else runtime.ai_config.max_calls_per_slug
        ),
    )
    if ai_decision.approved:
        return replace(signal, approved=True, ai_validated=True, needs_ai=False), ""
    return replace(signal, approved=False), ai_decision.reason


async def evaluation_loop(runtime: EngineServices) -> None:
    last_closed_kline_ms = 0
    last_cvd_1min = 0.0
    last_soft_eval_ts = 0.0

    while not runtime.stop_event.is_set():
        try:
            await roll_state_clock(runtime)
            slug = await sync_target_market(runtime)

            async with runtime.state.market_lock:
                closed_kline_ms = runtime.state.last_closed_kline_ms
                cvd_1min = runtime.state.last_cvd_1min
                history_len = len(runtime.state.candle_history)
                live_price = runtime.state.live_price

            if history_len < 20 or live_price <= 0:
                await asyncio.sleep(EVALUATION_IDLE_SLEEP_SECS)
                continue

            should_evaluate = False
            if closed_kline_ms != last_closed_kline_ms:
                last_closed_kline_ms = closed_kline_ms
                should_evaluate = True

            flow_threshold = max(
                2500.0,
                _safe_float(runtime.state.latest_cvd_snapshot.threshold, 0.0) * 0.35,
            )
            if abs(cvd_1min - last_cvd_1min) >= flow_threshold:
                last_cvd_1min = cvd_1min
                should_evaluate = True

            if not should_evaluate and (time.monotonic() - last_soft_eval_ts) >= SOFT_EVAL_REFRESH_SECS:
                should_evaluate = True

            if not should_evaluate:
                await asyncio.sleep(EVALUATION_IDLE_SLEEP_SECS)
                continue

            last_soft_eval_ts = time.monotonic()
            cycle_started = time.perf_counter()
            context = await build_context_from_state(runtime)
            if context is None:
                await asyncio.sleep(EVALUATION_IDLE_SLEEP_SECS)
                continue

            odds, liquidity_by_direction = await fetch_market_odds(runtime, slug)
            if not odds.market_found or odds.seconds_remaining <= 0:
                refreshed_slug = await sync_target_market(runtime, force=True)
                if refreshed_slug != slug:
                    await asyncio.sleep(EVALUATION_IDLE_SLEEP_SECS)
                    continue
            if not odds.market_found:
                runtime.latest_rejected_reason = "No active market found"
                runtime.latest_locks = [runtime.latest_rejected_reason]
                log_skip_reason(runtime, slug, runtime.latest_rejected_reason)
                await asyncio.sleep(EVALUATION_IDLE_SLEEP_SECS)
                continue
            if odds.seconds_remaining <= 0:
                runtime.latest_rejected_reason = f"Expired market ({int(odds.seconds_remaining)}s)"
                runtime.latest_locks = [runtime.latest_rejected_reason]
                log_skip_reason(runtime, slug, runtime.latest_rejected_reason)
                await asyncio.sleep(EVALUATION_IDLE_SLEEP_SECS)
                continue

            runtime.latest_context = context
            runtime.latest_odds = odds
            enriched_context = apply_probabilistic_model(
                context,
                strike_price=odds.reference_price or odds.strike_price,
                seconds_remaining=odds.seconds_remaining,
                degrees_of_freedom=runtime.strategy_config.degrees_of_freedom,
                probability_floor_pct=runtime.strategy_config.probability_floor_pct,
                probability_ceil_pct=runtime.strategy_config.probability_ceil_pct,
                max_indicator_logit_shift=runtime.strategy_config.max_indicator_logit_shift,
                close_equals_open_up_bias_prob=runtime.strategy_config.close_equals_open_up_bias_prob,
            )
            runtime.latest_context = enriched_context
            await runtime.state.update_telemetry(context=enriched_context)

            bankroll = runtime.current_balance or runtime.state.simulated_balance or runtime.bankroll
            signal_started = time.perf_counter()
            signal = await runtime.strategy_engine.evaluate_trade_signal(
                context,
                odds,
                bankroll,
                slug=slug,
                state=runtime.state,
                liquidity_by_direction=liquidity_by_direction,
            )
            signal_ms = (time.perf_counter() - signal_started) * 1000.0
            runtime.latest_signal = signal

            if runtime.state.kill_switch_enabled:
                runtime.latest_rejected_reason = "Kill switch enabled"
                runtime.latest_locks = [runtime.latest_rejected_reason]
                log_skip_reason(runtime, slug, runtime.latest_rejected_reason)
                await runtime.state.update_telemetry(
                    execution_timing=ExecutionTimingSnapshot(
                        signal_generation_ms=signal_ms,
                        total_ms=(time.perf_counter() - cycle_started) * 1000.0,
                        updated_at=time.time(),
                    )
                )
                await asyncio.sleep(EVALUATION_IDLE_SLEEP_SECS)
                continue

            if not signal.approved or signal.direction == Direction.SKIP:
                runtime.latest_rejected_reason = signal.reasons[0] if signal.reasons else "Signal rejected"
                runtime.latest_locks = [runtime.latest_rejected_reason]
                log_skip_reason(runtime, slug, runtime.latest_rejected_reason)
                await runtime.state.note_soft_skip(slug, best_ev=signal.expected_value_pct)
                await runtime.state.update_telemetry(
                    execution_timing=ExecutionTimingSnapshot(
                        signal_generation_ms=signal_ms,
                        total_ms=(time.perf_counter() - cycle_started) * 1000.0,
                        updated_at=time.time(),
                    )
                )
                await asyncio.sleep(EVALUATION_IDLE_SLEEP_SECS)
                continue

            async with runtime.state.positions_lock:
                already_open = slug in runtime.state.active_positions or slug in runtime.state.committed_slugs
            if already_open:
                runtime.latest_rejected_reason = "Position already active"
                runtime.latest_locks = [runtime.latest_rejected_reason]
                log_skip_reason(runtime, slug, runtime.latest_rejected_reason, min_repeat_secs=30.0)
                await asyncio.sleep(EVALUATION_IDLE_SLEEP_SECS)
                continue

            ai_ms = 0.0
            ai_reason = ""
            if signal.needs_ai:
                ai_started = time.perf_counter()
                signal, ai_reason = await maybe_validate_with_ai(runtime, signal, enriched_context, odds)
                ai_ms = (time.perf_counter() - ai_started) * 1000.0
                runtime.latest_signal = signal

            if not signal.approved:
                runtime.latest_rejected_reason = ai_reason or "AI rejected signal"
                runtime.latest_locks = [runtime.latest_rejected_reason]
                log_skip_reason(runtime, slug, runtime.latest_rejected_reason)
                await runtime.state.update_telemetry(
                    execution_timing=ExecutionTimingSnapshot(
                        signal_generation_ms=signal_ms,
                        ai_inference_ms=ai_ms,
                        total_ms=(time.perf_counter() - cycle_started) * 1000.0,
                        updated_at=time.time(),
                    )
                )
                await asyncio.sleep(EVALUATION_IDLE_SLEEP_SECS)
                continue

            reserved_slug = False
            position = None
            try:
                async with runtime.state.positions_lock:
                    already_open = slug in runtime.state.active_positions or slug in runtime.state.committed_slugs
                    if not already_open:
                        runtime.state.committed_slugs.add(slug)
                        runtime.state.soft_skipped_slugs.discard(slug)
                        runtime.state.best_ev_seen.pop(slug, None)
                        reserved_slug = True
                if already_open:
                    runtime.latest_rejected_reason = "Position already active"
                    runtime.latest_locks = [runtime.latest_rejected_reason]
                    log_skip_reason(runtime, slug, runtime.latest_rejected_reason, min_repeat_secs=30.0)
                    await asyncio.sleep(EVALUATION_IDLE_SLEEP_SECS)
                    continue

                execution_started = time.perf_counter()
                position = await runtime.execution_engine.submit_entry_order(runtime.state, signal, odds, context)
            except Exception:
                if reserved_slug:
                    async with runtime.state.positions_lock:
                        if slug not in runtime.state.active_positions:
                            runtime.state.committed_slugs.discard(slug)
                raise
            execution_ms = (time.perf_counter() - execution_started) * 1000.0

            await runtime.state.update_telemetry(
                execution_timing=ExecutionTimingSnapshot(
                    signal_generation_ms=signal_ms,
                    ai_inference_ms=ai_ms,
                    clob_request_ms=execution_ms,
                    confirmation_ms=0.0,
                    total_ms=(time.perf_counter() - cycle_started) * 1000.0,
                    updated_at=time.time(),
                )
            )

            if position is None:
                async with runtime.state.positions_lock:
                    failure = runtime.state.execution_failures.get(slug)
                    if slug not in runtime.state.active_positions:
                        runtime.state.committed_slugs.discard(slug)
                runtime.latest_rejected_reason = failure.reason if failure is not None else "Entry failed"
                runtime.latest_locks = [runtime.latest_rejected_reason]
                log_skip_reason(runtime, slug, runtime.latest_rejected_reason)
                await asyncio.sleep(EVALUATION_IDLE_SLEEP_SECS)
                continue

            runtime.latest_rejected_reason = ""
            runtime.latest_locks = []
            runtime.last_logged_rejection_reason = ""
            runtime.last_logged_rejection_ts = 0.0
            merged_features = dict(position.ml_features)
            merged_features.update(
                {
                    "confidence": signal.confidence.value,
                    "ai_validated": signal.ai_validated,
                    "expected_ev_pct": signal.expected_value_pct,
                    "expected_price": signal.token_price,
                }
            )
            updated_position = await runtime.state.update_position(
                slug,
                signals=signal.reasons,
                ml_features=merged_features,
                status=PositionStatus.OPEN,
            )
            current_position = updated_position.to_dict() if updated_position is not None else position.to_dict()
            liquidity_mode = next((note.split("=", 1)[1] for note in current_position.get("notes", []) if note.startswith("liq_mode=")), "")
            order_type = next((note.split("=", 1)[1] for note in current_position.get("notes", []) if note.startswith("entry_order_type=")), "")
            fill_status = next((note.split("=", 1)[1] for note in current_position.get("notes", []) if note.startswith("fill_status=")), "matched")
            execution_record = _build_execution_metric(
                slug=slug,
                direction=signal.direction,
                expected_price=signal.token_price,
                actual_price=_safe_float(current_position.get("bought_price")),
                spread_cents=(liquidity_by_direction.get(signal.direction).estimated_spread_pct if signal.direction in liquidity_by_direction else 0.0),
                liquidity_check=liquidity_mode or "ENTRY",
                order_type=order_type or "ENTRY",
                status=fill_status,
                filled_cost_usd=_safe_float(current_position.get("bet_size_usd")),
                filled_shares=_safe_float(current_position.get("bet_size_usd")) / max(_safe_float(current_position.get("bought_price")), 0.0001),
                reason="ENTRY_FILLED",
            )
            await _record_execution(runtime, execution_record)
        except asyncio.CancelledError:
            with suppress(Exception):
                async with runtime.state.positions_lock:
                    if slug in runtime.state.committed_slugs and slug not in runtime.state.active_positions:
                        runtime.state.committed_slugs.discard(slug)
            raise
        except Exception as exc:
            runtime.latest_rejected_reason = f"Evaluation loop error: {exc}"
            runtime.latest_locks = [runtime.latest_rejected_reason]
            logger.exception("[LOOP] evaluation_loop failed: %s", exc)
            await asyncio.sleep(1.0)


async def position_monitor_loop(runtime: EngineServices) -> None:
    while not runtime.stop_event.is_set():
        try:
            positions = await runtime.state.list_positions(
                statuses={
                    PositionStatus.OPEN.value,
                    PositionStatus.CLOSING.value,
                    PositionStatus.RESOLVING.value,
                }
            )
            if not positions:
                await asyncio.sleep(POSITION_MONITOR_SLEEP_SECS)
                continue

            context = runtime.latest_context or await build_context_from_state(runtime)
            if context is None:
                await asyncio.sleep(POSITION_MONITOR_SLEEP_SECS)
                continue

            live_underlying = context.price
            for position in positions:
                slug = position.slug
                odds, _ = await fetch_market_odds(runtime, slug)
                seconds_remaining = odds.seconds_remaining if odds.market_found else float(position.seconds_remaining)
                retry_now = time.monotonic()

                if position.status == PositionStatus.RESOLVING:
                    last_retry = runtime.resolving_retry_ts.get(slug, 0.0)
                    if (retry_now - last_retry) < RESOLVING_SETTLEMENT_RETRY_SECS:
                        continue

                if position.status == PositionStatus.RESOLVING or seconds_remaining <= 0:
                    settlement = await settle_expired_position(runtime, position.to_dict(), final_underlying=live_underlying)
                    if settlement.status == "UNSETTLED":
                        runtime.resolving_retry_ts[slug] = retry_now
                    else:
                        runtime.resolving_retry_ts.pop(slug, None)
                    continue

                public_price = odds.entry_prob_pct(position.decision) / 100.0 if odds.market_found else position.current_token_price
                liquidity = await runtime.execution_engine.check_liquidity_and_spread(
                    position.token_id,
                    bet_size_usd=position.bet_size_usd,
                    expected_price=public_price,
                    side="sell",
                    shares_to_sell=position.shares_owned,
                    odds=odds,
                    allow_tiny_amm_fallback=False,
                )
                mark_price = liquidity.entry_price if liquidity.ok and liquidity.entry_price > 0 else public_price

                monitor_decision = await runtime.risk_manager.apply_position_monitoring(
                    runtime.state,
                    slug,
                    position,
                    context,
                    current_token_price=mark_price,
                    current_underlying_price=live_underlying,
                    seconds_remaining=seconds_remaining,
                )

                if not monitor_decision.should_exit:
                    log_position_heartbeat(runtime, monitor_decision.position)
                    continue

                exit_fill = await runtime.execution_engine.submit_exit_order(
                    runtime.state,
                    monitor_decision.position,
                    exit_reason=monitor_decision.exit_reason,
                    current_token_price=monitor_decision.exit_price or mark_price,
                )
                await _record_execution(
                    runtime,
                    _build_execution_metric(
                        slug=slug,
                        direction=position.decision,
                        expected_price=mark_price,
                        actual_price=exit_fill.average_price,
                        spread_cents=liquidity.spread_pct,
                        liquidity_check=liquidity.mode,
                        order_type=exit_fill.order_type,
                        status=exit_fill.status,
                        filled_cost_usd=exit_fill.filled_cost_usd,
                        filled_shares=exit_fill.filled_shares,
                        reason=monitor_decision.exit_reason,
                        response_payload=exit_fill.response,
                    ),
                )

                remaining = await runtime.state.get_position(slug)
                if exit_fill.success and remaining is None:
                    runtime.resolving_retry_ts.pop(slug, None)
                    await finalize_closed_position(
                        runtime,
                        monitor_decision.position.to_dict(),
                        exit_reason=monitor_decision.exit_reason,
                        exit_price=exit_fill.average_price or mark_price,
                        exit_underlying=live_underlying,
                        exit_result=exit_fill,
                    )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("[LOOP] position_monitor_loop failed: %s", exc)
            await asyncio.sleep(1.0)

        await asyncio.sleep(POSITION_MONITOR_SLEEP_SECS)


async def balance_refresh_loop(runtime: EngineServices) -> None:
    while not runtime.stop_event.is_set():
        try:
            if not runtime.paper_trading and runtime.clob_client is not None:
                balance = await fetch_live_balance(runtime, force=True)
                if balance > 0:
                    runtime.current_balance = balance
                    await runtime.state.update_runtime_counters(simulated_balance=balance)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("[BALANCE] Refresh loop error: %s", exc)
        await asyncio.sleep(BALANCE_REFRESH_SECS)


def generate_ai_insights(
    *,
    win_rate: float,
    expectancy: float,
    current_streak: int,
    is_winning_streak: bool,
    max_drawdown: float,
) -> list[dict[str, Any]]:
    insights: list[dict[str, Any]] = []
    if expectancy > 0.05:
        insights.append({"type": "positive", "text": f"Strong positive expectancy (${expectancy:.2f} per trade)."})
    elif expectancy < -0.05:
        insights.append({"type": "warning", "text": f"Negative expectancy (${expectancy:.2f})."})

    if win_rate >= 55.0:
        insights.append({"type": "positive", "text": f"Solid win rate ({win_rate:.1f}%)."})
    elif 0 < win_rate <= 45.0:
        insights.append({"type": "warning", "text": f"Win rate softened to {win_rate:.1f}%."})

    if current_streak >= 3:
        if is_winning_streak:
            insights.append({"type": "positive", "text": f"Active {current_streak}-trade winning streak."})
        else:
            insights.append({"type": "warning", "text": f"Active {current_streak}-trade losing streak."})

    if max_drawdown <= -3.50:
        insights.append({"type": "warning", "text": f"Deep drawdown observed: ${max_drawdown:.2f}."})

    if not insights:
        insights.append({"type": "default", "text": "Market conditions are neutral."})
    return insights[:3]


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(round((len(ordered) - 1) * percentile))))
    return ordered[index]


def _monte_carlo_paths(pnls: list[float], current_balance: float, days: int = 180, paths: int = 20) -> list[list[float]]:
    if not pnls or current_balance <= 0:
        return []
    rng = random.Random(42)
    results: list[list[float]] = []
    for _ in range(paths):
        balance = current_balance
        path = [round(balance, 2)]
        for _ in range(days):
            balance = max(0.0, balance + rng.choice(pnls))
            path.append(round(balance, 2))
        results.append(path)
    return results


async def build_metrics_snapshot(runtime: EngineServices, *, force: bool = False) -> dict[str, Any]:
    async with runtime.metrics_lock:
        now = time.time()
        if not force and runtime.metrics_cache is not None and (now - runtime.metrics_cache_ts) < METRICS_CACHE_TTL_SECS:
            return runtime.metrics_cache

        trades = list(runtime.trade_history)
        execution_metrics = list(runtime.execution_history[-300:])
        current_balance = runtime.current_balance or runtime.state.simulated_balance or runtime.bankroll

        if not trades:
            snapshot = {
                "metrics": {
                    "win_rate": 0.0,
                "total_trades": 0,
                "total_wins": 0,
                "total_losses": 0,
                "max_drawdown": 0.0,
                "current_pnl": 0.0,
                "current_balance": round(current_balance, 2),
                "sharpe": 0.0,
                    "var_95": 0.0,
                    "expectancy": 0.0,
                    "kelly_optimal": "0.0%",
                    "current_streak": 0,
                    "is_winning_streak": False,
                },
                "projections": {"median_180d": 0.0, "paths": []},
                "equity_curve": runtime.equity_curve,
                "heatmap": [],
                "insights": generate_ai_insights(
                    win_rate=0.0,
                    expectancy=0.0,
                    current_streak=0,
                    is_winning_streak=False,
                    max_drawdown=0.0,
                ),
                "journal": [],
                "daily_pnl": runtime.daily_pnl_series,
                "execution_metrics": execution_metrics,
                "signals": {},
                "attribution": {
                    "ai_confirmed": {"trades": 0, "wins": 0, "losses": 0, "win_rate": 0.0, "pnl": 0.0},
                    "system_only": {"trades": 0, "wins": 0, "losses": 0, "win_rate": 0.0, "pnl": 0.0},
                },
            }
            runtime.metrics_cache = snapshot
            runtime.metrics_cache_ts = now
            return snapshot

        resolved = [item for item in trades if str(item.get("result", "")).upper() in {"WIN", "LOSS"}]
        total_pnl = sum(item["pnl_impact"] for item in trades)
        total_trades = len(resolved)
        total_wins = sum(1 for item in resolved if str(item["result"]).upper() == "WIN")
        total_losses = sum(1 for item in resolved if str(item["result"]).upper() == "LOSS")
        win_rate = (total_wins / total_trades) if total_trades > 0 else 0.0

        win_pnls = [item["pnl_impact"] for item in trades if item["pnl_impact"] > 0]
        loss_pnls = [abs(item["pnl_impact"]) for item in trades if item["pnl_impact"] < 0]
        avg_win = mean(win_pnls) if win_pnls else 0.0
        avg_loss = mean(loss_pnls) if loss_pnls else 0.0
        expectancy = (win_rate * avg_win) - ((1.0 - win_rate) * avg_loss) if total_trades > 0 else 0.0

        equity_values = [point["value"] for point in runtime.equity_curve]
        max_drawdown = 0.0
        running_peak = 0.0
        for value in equity_values:
            running_peak = max(running_peak, value)
            max_drawdown = min(max_drawdown, value - running_peak)

        pnl_samples = [item["pnl_impact"] for item in trades]
        sharpe = 0.0
        if len(pnl_samples) > 1:
            std = pstdev(pnl_samples)
            if std > 0:
                sharpe = mean(pnl_samples) / std * math.sqrt(len(pnl_samples))

        var_95 = _percentile(pnl_samples, 0.05)
        current_streak = 0
        is_winning_streak = False
        if resolved:
            last_result = str(resolved[-1]["result"]).upper()
            is_winning_streak = last_result == "WIN"
            for item in reversed(resolved):
                if str(item["result"]).upper() == last_result:
                    current_streak += 1
                else:
                    break

        win_loss_ratio = (avg_win / avg_loss) if avg_loss > 0 else 0.0
        kelly_fraction = 0.0
        if win_loss_ratio > 0:
            kelly_fraction = max(0.0, win_rate - ((1.0 - win_rate) / win_loss_ratio))

        journal_rows: list[dict[str, Any]] = []
        running_wins = 0
        running_resolved = 0
        for item in trades:
            if str(item.get("result", "")).upper() == "WIN":
                running_wins += 1
                running_resolved += 1
            elif str(item.get("result", "")).upper() == "LOSS":
                running_resolved += 1
            running_wr = (running_wins / max(running_resolved, 1)) * 100.0
            journal_rows.append(_journal_row(item, running_wr))
        journal_rows = list(reversed(journal_rows))[:200]

        hourly_buckets: dict[int, dict[str, Any]] = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0})
        for item in resolved:
            hour = _safe_timestamp(item["timestamp"]).hour
            if str(item["result"]).upper() == "WIN":    
                hourly_buckets[hour]["wins"] += 1
            else:
                hourly_buckets[hour]["losses"] += 1
            hourly_buckets[hour]["pnl"] += item.get("pnl_impact", 0.0)

        heatmap = []
        for hour in range(24):
            wins = hourly_buckets[hour]["wins"]
            losses = hourly_buckets[hour]["losses"]
            total_pnl_hour = hourly_buckets[hour]["pnl"]
            trades_count = wins + losses
            heatmap.append(
                {
                    "hour": hour,
                    "win_rate": round((wins / trades_count) * 100.0, 1) if trades_count else 0.0,
                    "trades": trades_count,
                    "avg_pnl": round(total_pnl_hour / trades_count, 2) if trades_count > 0 else 0.0,
                    "total_pnl": round(total_pnl_hour, 2),
                }
            )

        attribution = {
            "ai_confirmed": {"trades": 0, "wins": 0, "losses": 0, "win_rate": 0.0, "pnl": 0.0},
            "system_only": {"trades": 0, "wins": 0, "losses": 0, "win_rate": 0.0, "pnl": 0.0},
        }
        for item in trades:
            bucket_name = "ai_confirmed" if item.get("ai_validated") else "system_only"
            bucket = attribution[bucket_name]
            bucket["trades"] += 1
            bucket["pnl"] += item["pnl_impact"]
            result = str(item.get("result", "")).upper()
            if result == "WIN":
                bucket["wins"] += 1
            elif result == "LOSS":
                bucket["losses"] += 1
        for bucket in attribution.values():
            resolved_count = bucket["wins"] + bucket["losses"]
            bucket["win_rate"] = round((bucket["wins"] / resolved_count) * 100.0, 2) if resolved_count else 0.0
            bucket["pnl"] = round(bucket["pnl"], 2)

        signal_stats: dict[str, dict[str, Any]] = {}
        for label in ("UP", "DOWN"):
            subset = [item for item in trades if item["decision"] == label]
            resolved_subset = [item for item in subset if str(item.get("result", "")).upper() in {"WIN", "LOSS"}]
            wins = sum(1 for item in resolved_subset if str(item["result"]).upper() == "WIN")
            signal_stats[label] = {
                "trades": len(subset),
                "resolved": len(resolved_subset),
                "win_rate": round((wins / len(resolved_subset)) * 100.0, 2) if resolved_subset else 0.0,
                "pnl": round(sum(item["pnl_impact"] for item in subset), 2),
            }

        current_effective_balance = current_balance if current_balance > 0 else runtime.bankroll + total_pnl
        paths = _monte_carlo_paths(pnl_samples or [0.0], current_effective_balance)
        median_180d = 0.0
        if paths:
            endings = sorted(path[-1] for path in paths)
            median_180d = endings[len(endings) // 2]

        snapshot = {
            "metrics": {
                "win_rate": round(win_rate * 100.0, 2),
                "total_trades": total_trades,
                "total_wins": total_wins,
                "total_losses": total_losses,
                "max_drawdown": round(max_drawdown, 2),
                "current_pnl": round(total_pnl, 2),
                "current_balance": round(current_effective_balance, 2),
                "sharpe": round(sharpe, 2),
                "var_95": round(var_95, 2),
                "expectancy": round(expectancy, 2),
                "kelly_optimal": f"{kelly_fraction * 100.0:.1f}%",
                "current_streak": current_streak,
                "is_winning_streak": is_winning_streak,
            },
            "projections": {
                "median_180d": round(median_180d, 2),
                "paths": paths,
            },
            "equity_curve": runtime.equity_curve,
            "heatmap": heatmap,
            "insights": generate_ai_insights(
                win_rate=win_rate * 100.0,
                expectancy=expectancy,
                current_streak=current_streak,
                is_winning_streak=is_winning_streak,
                max_drawdown=max_drawdown,
            ),
            "journal": journal_rows,
            "daily_pnl": runtime.daily_pnl_series,
            "execution_metrics": execution_metrics,
            "signals": signal_stats,
            "attribution": attribution,
        }
        runtime.metrics_cache = snapshot
        runtime.metrics_cache_ts = now
        return snapshot


async def build_engine_health_snapshot(runtime: EngineServices) -> dict[str, Any]:
    now_ms = int(time.time() * 1000)
    async with runtime.state.market_lock:
        last_feed_ms = max(int(runtime.state.last_agg_trade_ms or 0), int(runtime.state.last_closed_kline_ms or 0))
    feed_stale_ms = max(0, now_ms - last_feed_ms) if last_feed_ms > 0 else 0
    ws_latency_ms = min(5000, feed_stale_ms) if feed_stale_ms > 0 else 0
    mem_current, mem_peak = tracemalloc.get_traced_memory()
    runtime_counters = await runtime.state.build_runtime_counters()

    ai_status = "ACTIVE"
    if runtime_counters.ai_circuit_open_until > time.time():
        ai_status = "OPEN"
    elif runtime_counters.ai_response_ema_ms > 5000:
        ai_status = "DEGRADED"
    elif runtime_counters.ai_response_ema_ms > 2500:
        ai_status = "SLOW"

    async with runtime.api_lock:
        api_inflight = runtime.api_inflight

    return {
        "ws_latency_ms": int(ws_latency_ms),
        "feed_stale_ms": int(feed_stale_ms),
        "memory_mb": round(mem_current / (1024 * 1024), 2),
        "memory_peak_mb": round(mem_peak / (1024 * 1024), 2),
        "ai_response_ms": round(runtime_counters.last_ai_response_ms, 1),
        "ai_response_ema_ms": round(runtime_counters.ai_response_ema_ms, 1),
        "ai_status": ai_status,
        "ai_in_flight": bool(runtime_counters.ai_call_in_flight),
        "ai_failures": int(runtime_counters.ai_consecutive_failures),
        "api_inflight": api_inflight,
        "api_capacity": runtime.api_capacity,
        "api_saturation_pct": round((api_inflight / runtime.api_capacity) * 100.0, 1),
        "kill_switch": bool(runtime.state.kill_switch_enabled),
    }


async def build_engine_control_snapshot(runtime: EngineServices) -> dict[str, Any]:
    return {
        "kill_switch": bool(runtime.state.kill_switch_enabled),
        "paper_trading": bool(runtime.paper_trading),
        "max_trade_pct": float(runtime.risk_manager.config.max_trade_pct),
        "max_daily_loss_pct": float(runtime.risk_manager.config.max_daily_loss_pct),
    }


async def build_live_payload(runtime: EngineServices) -> dict[str, Any]:
    market_snapshot = await runtime.state.get_market_snapshot()
    telemetry = await runtime.state.get_telemetry_snapshot()
    positions = await runtime.state.list_positions()
    runtime_counters = await runtime.state.build_runtime_counters()
    drawdown_guard = await runtime.state.build_drawdown_guard(runtime.current_balance or runtime.state.simulated_balance or runtime.bankroll)
    current_balance = runtime.current_balance or runtime.state.simulated_balance or runtime.bankroll
    health = await build_engine_health_snapshot(runtime)
    control = await build_engine_control_snapshot(runtime)
    portfolio = await build_metrics_snapshot(runtime)

    locks = list(runtime.latest_locks)
    if runtime.latest_rejected_reason and runtime.latest_rejected_reason not in locks:
        locks.append(runtime.latest_rejected_reason)
    if runtime.state.kill_switch_enabled and "Kill switch enabled" not in locks:
        locks.append("Kill switch enabled")

    drawdown_limit = current_balance * runtime.risk_manager.config.max_daily_loss_pct
    drawdown_used = max(0.0, -runtime.state.current_daily_pnl)
    drawdown_room_left = max(0.0, drawdown_limit - drawdown_used)
    drawdown_regime = "PAUSED" if runtime.state.kill_switch_enabled or drawdown_room_left <= 0 else "NORMAL"
    drawdown_text = "Daily loss limit reached" if drawdown_room_left <= 0 else "Within risk limits"

    latest_context = runtime.latest_context
    latest_odds = runtime.latest_odds
    context_dict = telemetry.get("context") or market_snapshot.get("latest_context")
    if context_dict is None and latest_context is not None:
        context_dict = latest_context.to_dict()

    market_payload = dict(market_snapshot)
    market_payload["live_price"] = _safe_float(market_snapshot.get("live_price"), 0.0)
    market_payload["latest_context"] = context_dict

    runtime_payload = runtime_counters.to_dict()
    runtime_payload.update(
        {
            "latest_rejected_reason": runtime.latest_rejected_reason,
            "paper_trading": bool(runtime.paper_trading or runtime.dry_run),
            "logs": list(runtime.recent_logs)[-120:],
            "locks": locks,
        }
    )

    drawdown_payload = drawdown_guard.to_dict()
    drawdown_payload.update(
        {
            "regime": drawdown_regime,
            "text": drawdown_text,
            "drawdown_used": round(drawdown_used, 2),
            "drawdown_room_left": round(drawdown_room_left, 2),
            "max_bet_cap": round(current_balance * runtime.risk_manager.config.max_trade_pct, 2),
        }
    )

    telemetry_payload = dict(telemetry)
    telemetry_payload["context"] = context_dict

    payload = {
        "market": market_payload,
        "positions": {position.slug: position.to_dict() for position in positions},
        "telemetry": telemetry_payload,
        "runtime": runtime_payload,
        "drawdown_guard": drawdown_payload,
        "engine_health": health,
        "engine_control": control,
        "portfolio": portfolio,
        "meta": {
            "seconds_remaining": round(latest_odds.seconds_remaining, 2) if latest_odds is not None else 0.0,
            "balance": round(current_balance, 2),
            "regime": latest_context.market_regime.value if latest_context is not None else MarketRegime.UNKNOWN.value,
        },
    }
    return sanitize_data(payload)


async def get_cached_live_payload(runtime: EngineServices) -> dict[str, Any]:
    now = time.monotonic()
    async with runtime.live_payload_lock:
        if (
            runtime.live_payload_cache is not None
            and (now - runtime.live_payload_cache_ts) < WS_PUSH_INTERVAL_SECS
        ):
            return dict(runtime.live_payload_cache)
        payload = await build_live_payload(runtime)
        runtime.live_payload_cache = payload
        runtime.live_payload_cache_ts = now
        return dict(payload)


async def bootstrap_runtime() -> EngineServices:
    state = EngineState()
    await state.initialize()
    data_config = DataStreamsConfig()
    risk_config = RiskConfig(
        max_daily_loss_pct=_env_float("MAX_DAILY_LOSS_PCT", 0.15),
        max_trade_pct=_env_float("MAX_TRADE_PCT", 0.05),
        max_trades_per_hour=_env_int("MAX_TRADES_PER_HOUR", 2),
        hourly_trade_limit_drawdown_step1=_env_float("HOURLY_TRADE_LIMIT_DRAWDOWN_STEP1", 0.25),
        hourly_trade_limit_drawdown_step2=_env_float("HOURLY_TRADE_LIMIT_DRAWDOWN_STEP2", 0.50),
        min_bet_usd=_env_float("MIN_BET_USD", 1.0),
        max_absolute_bet_usd=_env_float("MAX_BET_USD", 0.0),
        expected_exit_fee_multiplier=_env_float("EXPECTED_EXIT_FEE_MULTIPLIER", 0.50),
        latency_ev_haircut_pct=_env_float("LATENCY_EV_HAIRCUT_PCT", 0.25),
    )
    strategy_config = StrategyConfig(
        risk=risk_config,
        close_equals_open_up_bias_prob=_env_float("CLOSE_EQUALS_OPEN_UP_BIAS_PROB", 0.0005),
    )
    execution_config = ExecutionConfig(
        paper_trading=PAPER_TRADING,
        dry_run=DRY_RUN,
        paper_use_live_clob=PAPER_USE_LIVE_CLOB,
    )
    ai_config = AIConfig(model=os.getenv("LOCAL_AI_MODEL", AIConfig().model))

    http_session = create_http_session(data_config)

    clob_client: ClobClient | None = None
    live_enabled = not PAPER_TRADING and not DRY_RUN
    should_initialize_clob = live_enabled or (PAPER_TRADING and PAPER_USE_LIVE_CLOB)
    if should_initialize_clob:
        try:
            clob_client = await build_clob_client()
        except Exception as exc:
            if live_enabled:
                live_enabled = False
                logger.warning("[CLOB] Initialization failed. Falling back to paper mode: %s", exc)
            else:
                logger.warning("[CLOB] Paper shadow CLOB unavailable. Falling back to PAPER_SIM: %s", exc)

    # Create CLOB WS manager (token IDs populated dynamically at runtime via sync_target_market)
    clob_ws_mgr: ClobWebSocketManager | None = None
    if clob_client is not None:
        clob_ws_mgr = ClobWebSocketManager(token_ids=[])

    # --- Calibration layer ---
    calibrator = ProbabilityCalibrator(
        config=CalibrationConfig(
            method="platt",
            min_samples=30,
            refit_interval=50,
            db_path=str(DB_PATH),
        )
    )
    calibrator.fit_from_history()
    diag = calibrator.diagnostics()
    logger.info(
        "[CALIBRATION] Startup fit: method=%s fitted=%s n=%d A=%.4f B=%.4f",
        diag["method"], diag["is_fitted"], diag["fitted_on_n"],
        diag.get("platt_a") or 0.0, diag.get("platt_b") or 0.0,
    )

    risk_manager = RiskManager(risk_config)
    runtime = EngineServices(
        state=state,
        http_session=http_session,
        stream_manager=BinanceStreamManager(data_config),
        polymarket_fetcher=PolymarketFetcher(data_config),
        strategy_engine=StrategyEngine(strategy_config, calibrator=calibrator),
        risk_manager=risk_manager,
        execution_engine=ClobExecutionEngine(
            clob_client,
            config=replace(execution_config, paper_trading=not live_enabled, dry_run=DRY_RUN),
            risk_manager=risk_manager,
            clob_ws=clob_ws_mgr,
        ),
        ai_agent=LocalAIAgent(ai_config),
        data_config=data_config,
        strategy_config=strategy_config,
        risk_config=risk_config,
        execution_config=replace(execution_config, paper_trading=not live_enabled, dry_run=DRY_RUN),
        ai_config=ai_config,
        clob_client=clob_client,
        bankroll=BASE_BANKROLL,
        paper_trading=not live_enabled,
        dry_run=DRY_RUN,
        current_balance=BASE_BANKROLL,
    )
    runtime.clob_ws_manager = clob_ws_mgr
    runtime.strategy_engine.risk_manager = runtime.risk_manager
    _setup_logging(runtime)
    _init_db()
    logger.info("Elite Quant Engine v2 Initialized. Monitoring 1h WebSocket...")

    await load_persisted_history(runtime)
    if runtime.paper_trading:
        logger.info(
            "[INIT] Simulated balance initialized: $%.2f (base: $%.1f + historical: $%.2f)",
            runtime.current_balance,
            runtime.bankroll,
            runtime.current_balance - runtime.bankroll,
        )
        if runtime.execution_config.paper_use_live_clob and runtime.clob_client is not None:
            logger.info("[INIT] Paper mode using live Polymarket CLOB for shadow pricing/liquidity.")
        elif runtime.execution_config.paper_use_live_clob:
            logger.info("[INIT] Paper mode falling back to PAPER_SIM liquidity (no CLOB client).")
    else:
        runtime.current_balance = await fetch_live_balance(runtime, force=True)
        logger.info("[INIT] Live balance initialized: $%.2f", runtime.current_balance)

    await sync_target_market(runtime, force=True)
    await runtime.stream_manager.load_initial_history(runtime.http_session, runtime.state)
    initial_context = await build_context_from_state(runtime)
    if initial_context is not None and runtime.state.target_slug:
        with suppress(Exception):
            seed_odds, _ = await fetch_market_odds(runtime, runtime.state.target_slug)
            if seed_odds.market_found:
                seeded_context = apply_probabilistic_model(
                    initial_context,
                    strike_price=seed_odds.reference_price or seed_odds.strike_price,
                    seconds_remaining=seed_odds.seconds_remaining,
                    degrees_of_freedom=runtime.strategy_config.degrees_of_freedom,
                    probability_floor_pct=runtime.strategy_config.probability_floor_pct,
                    probability_ceil_pct=runtime.strategy_config.probability_ceil_pct,
                    close_equals_open_up_bias_prob=runtime.strategy_config.close_equals_open_up_bias_prob,
                )
                runtime.latest_context = seeded_context
                await runtime.state.update_telemetry(context=seeded_context)
    logger.info("[SYSTEM] Warming up local AI (%s) into RAM...", runtime.ai_config.model)
    warm_signal = TradeSignal(
        slug="warmup",
        direction=Direction.UP,
        confidence=ConfidenceLevel.LOW,
    )
    warm_context = TechnicalContext(timestamp=utc_now(), price=runtime.state.live_price or 0.0)
    warm_odds = MarketOddsSnapshot(slug="warmup")
    with suppress(Exception):
        await call_local_ai(runtime.http_session, runtime.state, warm_signal, warm_context, warm_odds, agent=runtime.ai_agent)
    logger.info("[SYSTEM] AI Warmup complete. Engine is hot and ready.")

    task_group = asyncio.TaskGroup()
    await task_group.__aenter__()
    runtime.task_group = task_group
    runtime.tasks = [
        task_group.create_task(runtime.stream_manager.run(runtime.http_session, runtime.state)),
        task_group.create_task(evaluation_loop(runtime)),
        task_group.create_task(position_monitor_loop(runtime)),
        task_group.create_task(balance_refresh_loop(runtime)),
        task_group.create_task(db_writer_worker(runtime)),
        task_group.create_task(ml_writer_worker(runtime)),
    ]
    # --- Polymarket SDK heartbeat kill-switch + CLOB WS book ---
    if live_enabled and clob_client is not None:
        runtime.tasks.append(
            task_group.create_task(
                run_heartbeat_loop(
                    clob_client,
                    heartbeat_id="alpha_z_btc_hourly",
                    interval_secs=8.0,
                    stop_event=runtime.stop_event,
                )
            )
        )
        logger.info("[SYSTEM] CLOB heartbeat kill-switch ARMED (8s interval).")
    if clob_ws_mgr is not None:
        runtime.tasks.append(
            task_group.create_task(
                clob_ws_mgr.run(stop_event=runtime.stop_event)
            )
        )
        if runtime.paper_trading:
            logger.info("[SYSTEM] CLOB WebSocket book manager started in paper shadow mode (heartbeat disabled).")
        else:
            logger.info("[SYSTEM] CLOB WebSocket book manager started.")
    return runtime


async def shutdown_runtime(runtime: EngineServices | None) -> None:
    if runtime is None:
        return
    runtime.stop_event.set()
    with suppress(Exception):
        await runtime.state.db_queue.put({"__shutdown__": True})
    with suppress(Exception):
        await runtime.state.ml_queue.put({"__shutdown__": True})

    for task in runtime.tasks:
        task.cancel()

    with suppress(Exception):
        await asyncio.wait_for(runtime.state.db_queue.join(), timeout=5.0)
    with suppress(Exception):
        await asyncio.wait_for(runtime.state.ml_queue.join(), timeout=5.0)

    if runtime.task_group is not None:
        try:
            await runtime.task_group.__aexit__(None, None, None)
        except* asyncio.CancelledError:
            pass
        except* Exception as exc_group:
            logger.error("[SHUTDOWN] Task group exit raised: %s", exc_group)

    with suppress(Exception):
        await runtime.http_session.close()
    if runtime.log_handler is not None:
        with suppress(Exception):
            logger.removeHandler(runtime.log_handler)


def get_runtime() -> EngineServices:
    if RUNTIME is None:
        raise HTTPException(status_code=503, detail="Engine not initialized")
    return RUNTIME


@asynccontextmanager
async def lifespan(_: FastAPI):
    global RUNTIME
    runtime = await bootstrap_runtime()
    RUNTIME = runtime
    try:
        yield
    finally:
        await shutdown_runtime(runtime)
        RUNTIME = None


app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


@app.get("/api/engine/health")
async def get_engine_health() -> dict[str, Any]:
    runtime = get_runtime()
    return sanitize_data(await build_engine_health_snapshot(runtime))


@app.get("/api/engine/control")
async def get_engine_control() -> dict[str, Any]:
    runtime = get_runtime()
    return sanitize_data(await build_engine_control_snapshot(runtime))


@app.post("/api/engine/control")
async def update_engine_control(update: EngineControlUpdate, request: Request) -> dict[str, Any]:
    runtime = get_runtime()
    _require_control_auth(request)

    if update.paper_trading is False and runtime.clob_client is None:
        raise HTTPException(status_code=400, detail="Cannot switch to live mode: CLOB client is unavailable.")

    if update.kill_switch is not None:
        await runtime.state.update_runtime_counters(kill_switch_enabled=bool(update.kill_switch))
        runtime.latest_rejected_reason = "Kill switch enabled" if update.kill_switch else ""

    if update.paper_trading is not None:
        runtime.paper_trading = bool(update.paper_trading)
        runtime.execution_config = replace(runtime.execution_config, paper_trading=runtime.paper_trading)
        runtime.execution_engine.config = replace(runtime.execution_engine.config, paper_trading=runtime.paper_trading)
        if runtime.paper_trading:
            runtime.current_balance = _paper_balance(runtime)
            await runtime.state.update_runtime_counters(simulated_balance=runtime.current_balance)
        else:
            runtime.current_balance = await fetch_live_balance(runtime, force=True)

    if update.max_trade_pct is not None:
        value = max(0.005, min(float(update.max_trade_pct), 0.50))
        runtime.risk_config = replace(runtime.risk_config, max_trade_pct=value)
        runtime.risk_manager.config = runtime.risk_config
        runtime.execution_engine.risk_manager.config = runtime.risk_config
        runtime.strategy_config = replace(runtime.strategy_config, risk=replace(runtime.strategy_config.risk, max_trade_pct=value))
        runtime.strategy_engine.config = runtime.strategy_config
        runtime.strategy_engine.risk_manager.config = runtime.risk_config
        await runtime.state.update_runtime_counters(max_trade_pct=value)

    if update.max_daily_loss_pct is not None:
        value = max(0.01, min(float(update.max_daily_loss_pct), 0.80))
        runtime.risk_config = replace(runtime.risk_config, max_daily_loss_pct=value)
        runtime.risk_manager.config = runtime.risk_config
        runtime.execution_engine.risk_manager.config = runtime.risk_config
        runtime.strategy_config = replace(runtime.strategy_config, risk=replace(runtime.strategy_config.risk, max_daily_loss_pct=value))
        runtime.strategy_engine.config = runtime.strategy_config
        runtime.strategy_engine.risk_manager.config = runtime.risk_config
        await runtime.state.update_runtime_counters(max_daily_loss_pct=value)

    return sanitize_data({"ok": True, "control": await build_engine_control_snapshot(runtime)})


@app.get("/api/metrics")
async def get_metrics() -> dict[str, Any]:
    runtime = get_runtime()
    return sanitize_data(await build_metrics_snapshot(runtime, force=True))


@app.get("/api/history")
async def get_history(limit: int = Query(default=240, ge=20, le=2000)) -> dict[str, Any]:
    runtime = get_runtime()
    market_snapshot = await runtime.state.get_market_snapshot()
    positions = await runtime.state.list_positions()

    candle_history = market_snapshot.get("candle_history", [])
    formatted_history = [
        {
            "time": _epoch_seconds(_safe_timestamp(item.get("timestamp"))),
            "open": _safe_float(item.get("open")),
            "high": _safe_float(item.get("high")),
            "low": _safe_float(item.get("low")),
            "close": _safe_float(item.get("close")),
            "volume": _safe_float(item.get("volume")),
        }
        for item in candle_history[-limit:]
        if item.get("timestamp")
    ]

    return sanitize_data(
        {
            "slug": runtime.state.target_slug,
            "price": _safe_float(market_snapshot.get("live_price")),
            "history": formatted_history,
            "active_trades": [position.to_dict() for position in positions],
            "recent_trades": list(reversed(runtime.trade_history[-100:])),
            "recent_executions": list(reversed(runtime.execution_history[-100:])),
        }
    )


@app.get("/api/history/replay")
async def get_history_replay(
    timestamp: int = Query(..., ge=1),
    minutes: int = Query(default=30, ge=5, le=180),
) -> dict[str, Any]:
    runtime = get_runtime()
    start_dt = datetime.fromtimestamp(timestamp, tz=timezone.utc) - timedelta(minutes=minutes)
    end_dt = datetime.fromtimestamp(timestamp, tz=timezone.utc) + timedelta(minutes=minutes)

    kline_params = {
        "symbol": runtime.data_config.symbol,
        "interval": "1m",
        "startTime": int(start_dt.timestamp() * 1000),
        "endTime": int(end_dt.timestamp() * 1000),
        "limit": min((minutes * 2) + 10, 1000),
    }
    trade_params = {
        "symbol": runtime.data_config.symbol,
        "startTime": int(start_dt.timestamp() * 1000),
        "endTime": int(end_dt.timestamp() * 1000),
        "limit": 1000,
    }

    async with api_slot(runtime):
        async with runtime.http_session.get(f"{runtime.data_config.binance_rest_api}/api/v3/klines", params=kline_params) as response:
            response.raise_for_status()
            klines = await response.json()

        async with runtime.http_session.get(f"{runtime.data_config.binance_rest_api}/api/v3/aggTrades", params=trade_params) as response:
            response.raise_for_status()
            agg_trades = await response.json()

    replay_klines = [
        {
            "time": int(row[0] // 1000),
            "open": _safe_float(row[1]),
            "high": _safe_float(row[2]),
            "low": _safe_float(row[3]),
            "close": _safe_float(row[4]),
            "volume": _safe_float(row[5]),
        }
        for row in klines
    ]
    replay_trades = [
        {
            "time": int(_safe_int(item.get("T")) // 1000),
            "price": _safe_float(item.get("p")),
            "qty": _safe_float(item.get("q")),
            "is_buyer_maker": bool(item.get("m", False)),
        }
        for item in agg_trades
    ]

    return sanitize_data(
        {
            "symbol": runtime.data_config.symbol,
            "requested_timestamp": timestamp,
            "window_minutes": minutes,
            "klines": replay_klines,
            "agg_trades": replay_trades,
        }
    )


@app.get("/api/analysis/post-mortem")
async def get_post_mortem(limit: int = Query(default=50, ge=10, le=250)) -> dict[str, Any]:
    runtime = get_runtime()
    recent_executions = list(reversed(runtime.execution_history[-limit:]))
    recent_trades = list(reversed(runtime.trade_history[-limit:]))

    rejection_buckets: dict[str, int] = defaultdict(int)
    status_buckets: dict[str, int] = defaultdict(int)
    for record in recent_executions:
        reason = str(record.get("reason") or "UNKNOWN")
        status = str(record.get("status") or "UNKNOWN")
        rejection_buckets[reason] += 1
        status_buckets[status] += 1

    recent_failures = [
        record
        for record in recent_executions
        if str(record.get("status", "")).lower() not in {"matched", "settled", "paper"}
    ][:20]

    recent_resolved = [
        record for record in recent_trades if str(record.get("result", "")).upper() in {"WIN", "LOSS"}
    ]
    wins = sum(1 for record in recent_resolved if str(record.get("result")).upper() == "WIN")
    losses = sum(1 for record in recent_resolved if str(record.get("result")).upper() == "LOSS")
    total_pnl = sum(_safe_float(record.get("pnl_impact")) for record in recent_trades)

    summary = {
        "resolved_trades": len(recent_resolved),
        "wins": wins,
        "losses": losses,
        "win_rate": round((wins / len(recent_resolved)) * 100.0, 2) if recent_resolved else 0.0,
        "net_pnl": round(total_pnl, 2),
        "top_rejections": sorted(
            [{"reason": key, "count": count} for key, count in rejection_buckets.items()],
            key=lambda item: item["count"],
            reverse=True,
        )[:10],
        "status_counts": sorted(
            [{"status": key, "count": count} for key, count in status_buckets.items()],
            key=lambda item: item["count"],
            reverse=True,
        )[:10],
    }

    return sanitize_data(
        {
            "summary": summary,
            "recent_failures": recent_failures,
            "recent_trades": recent_trades[:20],
        }
    )


@app.websocket("/ws/live")
async def websocket_live(websocket: WebSocket) -> None:
    runtime = get_runtime()
    await websocket.accept()

    last_metrics_push = 0.0
    try:
        while True:
            loop_started = time.monotonic()
            if runtime.stop_event.is_set():
                break
            if websocket.client_state != WebSocketState.CONNECTED:
                break

            payload = await get_cached_live_payload(runtime)
            now = time.monotonic()
            if (now - last_metrics_push) < WS_PORTFOLIO_PUSH_INTERVAL_SECS:
                payload["portfolio"] = runtime.metrics_cache or DEFAULT_MARKET_DATA.get("portfolio", {})
            else:
                last_metrics_push = now

            await asyncio.wait_for(
                websocket.send_json(jsonable_encoder(sanitize_data(payload))),
                timeout=WS_PUSH_INTERVAL_SECS,
            )
            sleep_for = max(0.0, WS_PUSH_INTERVAL_SECS - (time.monotonic() - loop_started))
            await asyncio.sleep(sleep_for)
    except WebSocketDisconnect:
        return
    except asyncio.TimeoutError:
        logger.warning("[WS] /ws/live send timed out; closing slow client.")
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.warning("[WS] /ws/live disconnected: %s", exc)
    finally:
        if websocket.client_state == WebSocketState.CONNECTED:
            with suppress(Exception):
                await websocket.close()
