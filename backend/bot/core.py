import asyncio
import aiohttp
import websockets
import json
import logging
import sys
import re
import os
import sqlite3
import datetime
import time
import math
import io
import csv
from collections import deque
from urllib.parse import urlparse
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from typing import Optional, Tuple
from decimal import Decimal, ROUND_DOWN, ROUND_UP

load_dotenv()

# ============================================================
# CONFIGURATION & CONSTANTS
# ============================================================
SOCKET_KLINE    = "wss://stream.binance.com:9443/ws/btcusdt@kline_15m"
SOCKET_TRADE    = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"

LOCAL_AI_URL    = "http://localhost:11434/v1/chat/completions"
LOCAL_AI_MODEL  = "llama3.2:3b-instruct-q4_K_M"

BANKROLL        = 5000.00

PAPER_TRADING = os.getenv("PAPER_TRADING", "true").lower() == "true"
PAPER_BALANCE = 5000.00

GAMMA_API       = "https://gamma-api.polymarket.com"
CLOB_HOST       = "https://clob.polymarket.com"
CHAIN_ID        = 137

POLY_PRIVATE_KEY = os.getenv("POLY_PRIVATE_KEY", "")
POLY_FUNDER      = os.getenv("POLY_FUNDER", "")
POLY_SIG_TYPE    = int(os.getenv("POLY_SIG_TYPE", "1"))

DRY_RUN          = os.getenv("DRY_RUN", "true").lower() != "false"

# -- Elite Risk & Thresholds --
MAX_TRADE_PCT               = 0.05
# OPTIMIZED: Increased to 0.50 (Half-Kelly) - Industry standard for aggressive compounding
# Previous 0.25 was stacking with other dampeners, creating $1-2 bets when $10-15 was appropriate
FRACTIONAL_KELLY_DAMPENER   = 0.50
MAX_TRADES_PER_HOUR         = 3

# -- Liquidity & Anti-Chop --
MAX_SPREAD_PCT              = 0.05
MIN_LIQUIDITY_MULTIPLIER    = 1.5
# Paper-mode liquidity simulation (to avoid optimistic paper fills).
PAPER_SIM_FALLBACK_SPREAD_PCT = 0.03
PAPER_SIM_ESTIMATED_DEPTH_USD = 75.0
PAPER_SIM_SCALE_HAIRCUT       = 0.95
MIN_ATR_THRESHOLD           = 15.0
EMA_SQUEEZE_PCT             = 0.00005
# -- AI / EV Guardrails --
# EV can only bypass AI when technical alignment is already strong.
EV_AI_BYPASS_THRESHOLD = 3.0
MIN_SCORE_TO_TRADE = 1
SCORE1_MIN_EV_PCT = 15.0
SCORE0_MIN_EV_PCT = 35.0
SCORE0_MAX_TOKEN_PRICE = 0.35
EV_BYPASS_MIN_SCORE = 3
EV_BYPASS_MIN_TOKEN_PRICE = 0.20

MIN_EV_PCT_TO_CALL_AI     = 1.0  # OPTIMIZED: Lowered from 1.5% to catch more borderline trades

MIN_SECONDS_REMAINING     = 30
MAX_SECONDS_FOR_NEW_BET   = 3540

MAX_CROWD_PROB_TO_CALL    = 96.0

EV_REENGAGE_DELTA         = 0.5
# Hybrid stop-loss controls (Anti-Wick Patch)
SL_EARLY_PHASE_SECS               = 1800
SL_MID_PHASE_SECS                 = 600
SL_NEAR_EXPIRY_SECS               = 300
SL_LOOSEN_EARLY_MULT              = 1.50
SL_LOOSEN_MID_MULT                = 1.20
# OPTIMIZED: Massively increased confirmations to ignore AMM liquidity vacuums
SL_CONFIRM_BREACH_EARLY           = 12   # Require 60 seconds of sustained drop
SL_CONFIRM_BREACH_MID             = 8    # Require 40 seconds of sustained drop
SL_CONFIRM_BREACH_LATE            = 6    # Require 30 seconds of sustained drop
SL_RECOVERY_RESET_BUFFER          = 0.01
# Keep SL reachable for low-priced entries (e.g., 10-20c tokens).
# This caps SL by a percentage of entry so it cannot require an impossible drop.
SL_ENTRY_REL_MAX_LOSS_PCT         = 0.55
SL_ENTRY_REL_MIN_CENTS            = 0.03
# Only allow take-profit early exits near expiry; otherwise hold winners.
TP_EARLY_EXIT_WINDOW_SECS         = 180
# Force immediate TP on extreme windfalls, even outside the TP time gate.
FORCE_TP_ROI_PCT                  = 0.70  # 70% ROI
FORCE_TP_DELTA_ABS                = 0.35  # +35c token-price delta

# Same-slug re-entry controls
MAX_REENTRIES_PER_SLUG            = 1
REENTRY_COOLDOWN_SECS             = 120
REENTRY_MIN_EV_IMPROVEMENT_AFTER_SL_PCT = 1.5
REENTRY_MIN_EV_IMPROVEMENT_AFTER_TP_PCT = 0.0
REENTRY_SAME_DIR_SL_EV_BYPASS_PCT = 1.5

# OPTIMIZED: Lowered from 40K to 12K - more realistic for 15-min Bitcoin volume
CVD_DIVERGENCE_THRESHOLD  = 12000.0  
# OPTIMIZED: Lowered from 25K to 15K to prevent fighting active smart-money flow
CVD_CONTRA_VETO_THRESHOLD = 15000.0
# OPTIMIZED: Lowered from 0.6% to 0.4% to prevent buying the absolute local top/bottom
VWAP_OVEREXTEND_PCT       = 0.004
BODY_STRENGTH_MULTIPLIER  = 0.5

EVAL_TICK_SECONDS = 5
MAX_HISTORY     = 120

AI_TIMEOUT_CONNECT  = 5
AI_TIMEOUT_TOTAL    = 30
AI_MAX_RETRIES      = 1
AI_RETRY_DELAY      = 2
AI_MAX_TOKENS       = 120
# AI re-query controls (anti-spam / anti-overfitting on one slug)
AI_VETO_COOLDOWN_SECS = 60
AI_VETO_MIN_EV_IMPROVEMENT_PCT = 4.0
AI_VETO_OVERRIDE_EV_JUMP_PCT = 20.0
AI_MAX_CALLS_PER_SLUG = 6

CB_FAILURE_THRESHOLD = 5  
CB_COOLDOWN_SECS     = 30  

RESOLVE_POLL_INTERVAL        = 15
RESOLVE_POLL_MAX_TRIES       = 60
RESOLVE_CONFIRMED_THRESHOLD  = 0.95

STRIKE_PRICE_CACHE_TTL = 300  

# Adaptive thresholds
VOLATILITY_LOOKBACK = 96  
ATR_PERCENTILE = 0.30  
CVD_ADAPTIVE_MULTIPLIER = 1.5  

# ============================================================
# ASYNC ML DATA LOGGER
# ============================================================
ML_FILE = "ai_training_data.csv"
ML_QUEUE_MAX = 5000
DB_QUEUE_MAX = 10000

async def log_ml_data(row: dict):
    try:
        await ml_queue.put(dict(row))
    except Exception as e:
        log.error(f"[ML LOG ERROR] Queue put failed: {e}")

# ============================================================
# ELITE RISK MANAGEMENT ENGINE
# ============================================================
class RiskManager:
    def __init__(self, max_daily_loss_pct=0.15, max_trade_pct=MAX_TRADE_PCT):
        self.max_daily_loss_pct = max_daily_loss_pct
        self.max_trade_pct = max_trade_pct
        self.current_daily_pnl = 0.0
        self.trades_this_hour = 0
        now_utc = datetime.now(timezone.utc)
        self.current_hour = now_utc.hour
        self.current_day = now_utc.date()

    def reset_stats(self):
        self.current_daily_pnl = 0.0
        self.trades_this_hour = 0
        self.current_hour = datetime.now(timezone.utc).hour
        log.info("[RISK] Daily stats reset. New session started.")

    def can_trade(self, current_balance, trade_size):
        now_utc = datetime.now(timezone.utc)
        if now_utc.date() != self.current_day:
            self.current_day = now_utc.date()
            self.reset_stats()

        dynamic_loss_limit = current_balance * self.max_daily_loss_pct

        if self.current_daily_pnl <= -dynamic_loss_limit:
            return False, f"Daily loss limit (-${dynamic_loss_limit:.2f}) reached."

        if trade_size > (current_balance * self.max_trade_pct):
            return False, f"Trade size ${trade_size:.2f} exceeds max 5% risk."

        now_hour = now_utc.hour
        if now_hour != self.current_hour:
            self.trades_this_hour = 0
            self.current_hour = now_hour

        if self.trades_this_hour >= MAX_TRADES_PER_HOUR:
            return False, f"Max trades per hour ({MAX_TRADES_PER_HOUR}) reached."

        return True, "Approved"

# ============================================================
# SIGNAL TRACKER
# ============================================================
class SignalTracker:
    def __init__(self):
        self.signals = {}

    def log_resolution(self, signals_list: list, result_str: str, pnl_impact: float):
        for sig in signals_list:
            clean_sig = sig.replace("AI confirmed: ", "").strip()
            if not clean_sig: continue

            if clean_sig not in self.signals:
                self.signals[clean_sig] = {"wins": 0, "losses": 0, "pnl": 0.0, "trades": 0}

            self.signals[clean_sig]["trades"] += 1
            self.signals[clean_sig]["pnl"] += pnl_impact

            if "WIN" in result_str:
                self.signals[clean_sig]["wins"] += 1
            elif "LOSS" in result_str:
                self.signals[clean_sig]["losses"] += 1

    def get_signal_performance(self) -> dict:
        perf = {}
        for sig, data in self.signals.items():
            if data["trades"] > 0:
                win_rate = (data["wins"] / data["trades"]) * 100
                perf[sig] = {"trades": data["trades"], "win_rate": win_rate, "avg_pnl": data["pnl"]}
        return perf

signal_tracker = SignalTracker()

# ============================================================
# LOGGING & LIVE STREAMING
# ============================================================
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_file_handler = logging.FileHandler("trading_log.txt", encoding="utf-8")
_file_handler.setFormatter(_fmt)
_stream_handler = logging.StreamHandler(io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True))
_stream_handler.setFormatter(_fmt)

recent_logs = deque(maxlen=50)

class DequeHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            recent_logs.append(msg)
        except Exception:
            self.handleError(record)

_deque_handler = DequeHandler()
_deque_handler.setFormatter(_fmt)

log = logging.getLogger("alpha_z_engine")
log.setLevel(logging.INFO)
log.addHandler(_stream_handler)
log.addHandler(_deque_handler)
log.propagate = False

def ui_log(msg: str, level: str = "info"):
    timestamp = datetime.now().strftime('%H:%M:%S')
    formatted_msg = f"[{timestamp}] [{level.upper()}] {msg}"
    recent_logs.append(formatted_msg)
    if level == "info": log.info(msg)
    elif level == "warning": log.warning(msg)
    elif level == "error": log.error(msg)

ui_log("Elite Quant Engine v2 Initialized. Monitoring 15m WebSocket...", "info")

# ============================================================
# SQLITE STATS TRACKING
# ============================================================
DB_FILE = "alpha_z_history.db"

async def init_db():
    def _sync_init():
        from contextlib import closing
        with closing(sqlite3.connect(DB_FILE)) as conn:
            with conn: 
                conn.execute("PRAGMA journal_mode=WAL;")
                
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS trades (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXT,
                        slug TEXT,
                        decision TEXT,
                        strike REAL,
                        final_price REAL,
                        actual_outcome TEXT,
                        result TEXT,
                        win_rate REAL,
                        pnl_impact REAL,
                        local_calc_outcome TEXT,
                        official_outcome TEXT,
                        match_status TEXT,
                        trigger_reason TEXT
                    )
                """)
                
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS execution_metrics (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXT,
                        slug TEXT,
                        direction TEXT,
                        expected_price REAL,
                        actual_price REAL,
                        slippage_bps REAL,
                        spread_cents REAL,
                        liquidity_check TEXT
                    )
                """)
    await asyncio.to_thread(_sync_init)
    
async def get_historical_pnl() -> float:
    def _sync_fetch():
        try:
            from contextlib import closing
            with closing(sqlite3.connect(DB_FILE, timeout=5.0)) as conn:
                cursor = conn.execute("SELECT SUM(pnl_impact) FROM trades")
                result = cursor.fetchone()[0]
                return float(result) if result else 0.0
        except Exception as e:
            log.debug(f"[DB] get_historical_pnl failed, defaulting to 0: {e}")
            return 0.0
    return await asyncio.to_thread(_sync_fetch)

async def log_trade_to_db(slug, decision, strike, final_price, actual_outcome, result, win_rate,
                     pnl_impact, local_calc_outcome="", official_outcome="", match_status="",
                     trigger_reason=""):
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    payload = (
        timestamp, slug, decision, strike, final_price, actual_outcome,
        result, win_rate, pnl_impact, local_calc_outcome, official_outcome, match_status, trigger_reason
    )
    try:
        await db_queue.put({"type": "trade", "payload": payload})
    except Exception as e:
        log.error(f"[DB ERROR] Queue put failed: {e}")

async def log_execution_metrics(slug: str, direction: str, expected_price: float, 
                                actual_price: float, spread_cents: float, liq_check: str):
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    slippage_bps = ((actual_price - expected_price) / expected_price * 10000) if expected_price > 0 else 0
    payload = (timestamp, slug, direction, expected_price, actual_price, slippage_bps, spread_cents, liq_check)
    try:
        await db_queue.put({"type": "exec", "payload": payload})
    except Exception as e:
        log.error(f"[EXEC METRICS ERROR] Queue put failed: {e}")

async def ml_writer_worker():
    file_exists = os.path.isfile(ML_FILE)
    is_empty = not file_exists or os.path.getsize(ML_FILE) == 0
    header_written = not is_empty
    with open(ML_FILE, mode="a", newline="", encoding="utf-8") as f:
        while True:
            row = await ml_queue.get()
            try:
                writer = csv.DictWriter(f, fieldnames=row.keys())
                if not header_written:
                    writer.writeheader()
                    header_written = True
                writer.writerow(row)
                f.flush()
            except Exception as e:
                log.error(f"[ML LOG ERROR] Worker write failed: {e}")
            finally:
                ml_queue.task_done()

async def db_writer_worker():
    conn = sqlite3.connect(DB_FILE, timeout=5.0)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        while True:
            item = await db_queue.get()
            try:
                if item["type"] == "trade":
                    conn.execute("""
                        INSERT INTO trades (
                            timestamp, slug, decision, strike, final_price, actual_outcome,
                            result, win_rate, pnl_impact, local_calc_outcome, official_outcome, match_status, trigger_reason
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, item["payload"])
                elif item["type"] == "exec":
                    conn.execute("""
                        INSERT INTO execution_metrics
                        (timestamp, slug, direction, expected_price, actual_price, slippage_bps, spread_cents, liquidity_check)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, item["payload"])
                conn.commit()
            except Exception as e:
                log.error(f"[DB ERROR] Worker write failed: {e}")
            finally:
                db_queue.task_done()
    finally:
        conn.close()

# ============================================================
# STATE
# ============================================================
ai_call_count           = 0
ai_consecutive_failures = 0
ai_circuit_open_until   = 0.0
background_tasks = set()
ml_queue = asyncio.Queue(maxsize=ML_QUEUE_MAX)
db_queue = asyncio.Queue(maxsize=DB_QUEUE_MAX)
state_lock = asyncio.Lock()
api_semaphore = asyncio.Semaphore(5)

candle_history: list[dict] = []
target_slug: str    = ""
market_family_prefix: str = ""
total_wins          = 0
total_losses        = 0
active_predictions: dict = {}
risk_manager = RiskManager()
simulated_balance = PAPER_BALANCE

committed_slugs: set = set()
soft_skipped_slugs: set = set()
best_ev_seen: dict = {}
slug_reentry_state: dict = {}
slug_ai_state: dict = {}

ai_call_in_flight: str = ""
ai_processing_lock = asyncio.Lock()  
strike_price_cache: dict = {}  

clob_client = None
live_price: float = 0.0
live_candle: dict = {}
last_closed_kline_ms: int = 0
last_agg_trade_ms: int = 0

cvd_total:        float = 0.0
cvd_1min_buffer:  deque  = deque()
cvd_snapshot_at_candle_open: float = 0.0
last_cvd_1min:    float = 0.0

vwap_cum_pv:  float = 0.0
vwap_cum_vol: float = 0.0
vwap_date:    str   = ""

_poly_cache: dict        = {}
_poly_cache_slug: str    = ""
_poly_cache_ts:   float  = 0.0
POLY_CACHE_TTL:   float  = 4.5

last_ai_interaction = {"prompt": "No AI calls yet.", "response": "N/A", "timestamp": ""}

adaptive_atr_min: float = MIN_ATR_THRESHOLD
adaptive_cvd_threshold: float = CVD_DIVERGENCE_THRESHOLD

# ============================================================
# UTILITIES
# ============================================================
def fire_and_forget(coro):
    task = asyncio.create_task(coro)
    background_tasks.add(task)
    task.add_done_callback(background_tasks.discard)

@asynccontextmanager
async def api_get(session: aiohttp.ClientSession, url: str, **kwargs):
    async with api_semaphore:
        async with session.get(url, **kwargs) as resp:
            yield resp

def _record_full_exit_for_reentry(slug: str, pred: dict, exit_reason: str):
    stats = slug_reentry_state.setdefault(slug, {
        "closed_trades": 0,
        "last_exit_ts": 0.0,
        "last_exit_reason": "",
        "last_exit_dir": "",
        "last_entry_ev_pct": 0.0,
    })

    try:
        last_ev = float(pred.get("ml_data", {}).get("ev_pct", 0.0))
    except (TypeError, ValueError):
        last_ev = 0.0

    stats["closed_trades"] = int(stats.get("closed_trades", 0)) + 1
    stats["last_exit_ts"] = time.time()
    stats["last_exit_reason"] = exit_reason
    stats["last_exit_dir"] = pred.get("decision", "")
    stats["last_entry_ev_pct"] = last_ev

    max_total_trades = 1 + MAX_REENTRIES_PER_SLUG
    if stats["closed_trades"] < max_total_trades:
        committed_slugs.discard(slug)
        soft_skipped_slugs.discard(slug)
        best_ev_seen.pop(slug, None)
    else:
        committed_slugs.add(slug)

def check_reentry_eligibility(slug: str, direction: str, ev_pct: float) -> tuple[bool, str]:
    stats = slug_reentry_state.get(slug)
    if not stats:
        return True, "fresh slug"

    max_total_trades = 1 + MAX_REENTRIES_PER_SLUG
    closed_trades = int(stats.get("closed_trades", 0))
    if closed_trades >= max_total_trades:
        return False, f"re-entry cap reached ({closed_trades}/{max_total_trades})"

    last_exit_ts = float(stats.get("last_exit_ts", 0.0))
    if last_exit_ts > 0:
        elapsed = time.time() - last_exit_ts
        if elapsed < REENTRY_COOLDOWN_SECS:
            return False, f"cooldown active ({int(REENTRY_COOLDOWN_SECS - elapsed)}s left)"

    last_entry_ev_pct = float(stats.get("last_entry_ev_pct", 0.0))
    last_exit_reason = str(stats.get("last_exit_reason", ""))
    last_exit_dir = str(stats.get("last_exit_dir", ""))
    if "STOP_LOSS" in last_exit_reason and direction == last_exit_dir:
        if ev_pct < (last_entry_ev_pct + REENTRY_SAME_DIR_SL_EV_BYPASS_PCT):
            return False, f"same-direction re-entry blocked after stop-loss ({direction})"

    ev_step = (
        REENTRY_MIN_EV_IMPROVEMENT_AFTER_SL_PCT
        if "STOP_LOSS" in last_exit_reason
        else REENTRY_MIN_EV_IMPROVEMENT_AFTER_TP_PCT
    )
    min_required_ev = last_entry_ev_pct + ev_step
    if ev_pct < min_required_ev:
        return False, f"EV improvement not met ({ev_pct:.2f}% < {min_required_ev:.2f}%)"

    return True, "eligible"

def _get_slug_ai_state(slug: str) -> dict:
    return slug_ai_state.setdefault(slug, {
        "ai_calls": 0,
        "last_veto_ts": 0.0,
        "last_veto_ev_pct": 0.0,
    })

def check_ai_requery_eligibility(slug: str, ev_pct: float) -> tuple[bool, str]:
    state = _get_slug_ai_state(slug)
    calls = int(state.get("ai_calls", 0))
    if calls >= AI_MAX_CALLS_PER_SLUG:
        return False, f"AI cap reached ({calls}/{AI_MAX_CALLS_PER_SLUG})"

    last_veto_ts = float(state.get("last_veto_ts", 0.0))
    if last_veto_ts > 0:
        elapsed = time.time() - last_veto_ts
        last_veto_ev_pct = float(state.get("last_veto_ev_pct", 0.0))
        if elapsed < AI_VETO_COOLDOWN_SECS:
            override_ev = last_veto_ev_pct + AI_VETO_OVERRIDE_EV_JUMP_PCT
            if ev_pct < override_ev:
                return False, f"AI veto cooldown active ({int(AI_VETO_COOLDOWN_SECS - elapsed)}s left)"
        else:
            min_ev = last_veto_ev_pct + AI_VETO_MIN_EV_IMPROVEMENT_PCT
            if ev_pct < min_ev:
                return False, f"AI recheck EV delta not met ({ev_pct:.2f}% < {min_ev:.2f}%)"

    return True, "eligible"

def record_ai_attempt(slug: str):
    state = _get_slug_ai_state(slug)
    state["ai_calls"] = int(state.get("ai_calls", 0)) + 1

def record_ai_veto(slug: str, ev_pct: float):
    state = _get_slug_ai_state(slug)
    state["last_veto_ts"] = time.time()
    state["last_veto_ev_pct"] = float(ev_pct)

def get_dynamic_threshold(secs_remaining: float) -> tuple[float, float]:
    if secs_remaining <= 120: 
        return 1.00, -1.00     
    elif secs_remaining <= 600: 
        return 0.15, -0.20     
    else: 
        return 0.25, -0.30     

def build_market_family_prefix(seed_slug: str) -> str:
    if not seed_slug: return ""
    known_prefix = re.match(r"^(btc-(?:updown|up-or-down)(?:-[0-9]+m?)?)", seed_slug)
    if known_prefix: return known_prefix.group(1)
    parts = seed_slug.split("-")
    return "-".join(parts[:4]) if len(parts) >= 4 else seed_slug

def _parse_seconds_remaining(end_date_str: str) -> float:
    if not end_date_str: return -1.0
    try:
        end_dt = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
        return (end_dt - datetime.now(timezone.utc)).total_seconds()
    except Exception as e:
        log.debug(f"[TIME] Failed to parse end date '{end_date_str}': {e}")
        return -1.0

def increment_slug_by_interval(slug: str) -> str:
    pattern = r"bitcoin-up-or-down-(\w+)-(\d+)-(\d+)(am|pm)-et"
    match = re.search(pattern, slug)
    
    if not match:
        return slug 

    month_str, day, hour, ampm = match.groups()
    
    try:
        current_year = datetime.now().year
        dt_str = f"{month_str} {day} {current_year} {hour}{ampm}"
        dt = datetime.strptime(dt_str, "%B %d %Y %I%p")
        
        next_dt = dt + timedelta(hours=1)
        
        new_month = next_dt.strftime('%B').lower()
        new_day = next_dt.day
        new_hour = next_dt.strftime('%I').lstrip('0') 
        new_ampm = next_dt.strftime('%p').lower()
        
        return f"bitcoin-up-or-down-{new_month}-{new_day}-{new_hour}{new_ampm}-et"
    except Exception as e:
        log.debug(f"[SLUG] increment_slug_by_interval failed for {slug}: {e}")
        return slug
    
def parse_candle(raw: dict, override_close: float = 0.0) -> dict:
    o, h, l, c, v = float(raw['o']), float(raw['h']), float(raw['l']), float(raw['c']), float(raw['v'])
    c = override_close if override_close > 0 else c
    return {
        "timestamp":  datetime.fromtimestamp(raw['t']/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
        "open": o, "high": h, "low": l, "close": c, "volume": v,
        "body_size": abs(c - o), "upper_wick": h - max(o, c), "lower_wick": min(o, c) - l,
        "structure": "BULLISH" if c > o else "BEARISH",
    }

def extract_slug_from_market_url(raw_input: str) -> str:
    cleaned = (raw_input or "").strip()
    if not cleaned: return ""
    if "/event/" in cleaned:
        path = urlparse(cleaned).path
        return path.split("/event/", 1)[-1].strip("/").lower()
    return cleaned.strip("/").lower()

async def fetch_live_balance(session: aiohttp.ClientSession) -> float:
    if clob_client is None: return BANKROLL
    try:
        from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
        params = BalanceAllowanceParams(signature_type=POLY_SIG_TYPE, asset_type=AssetType.COLLATERAL)
        resp = await asyncio.to_thread(clob_client.get_balance_allowance, params=params)
        fetched = int(resp.get("balance", 0)) / 1_000_000
        return fetched if fetched > 0 else BANKROLL
    except Exception as e:
        log.debug(f"[BALANCE] fetch_live_balance failed, using fallback bankroll: {e}")
        return BANKROLL

async def warmup_ai(session: aiohttp.ClientSession):
    log.info(f"[SYSTEM] Warming up local AI ({LOCAL_AI_MODEL}) into RAM...")
    payload = {
        "model": LOCAL_AI_MODEL,
        "messages": [{"role": "user", "content": "hello"}],
        "keep_alive": -1
    }
    try:
        async with session.post(LOCAL_AI_URL, json=payload, timeout=60) as r:
            r.raise_for_status()
            log.info("[SYSTEM] AI Warmup complete. Engine is hot and ready.")
    except Exception as e:
        log.warning(f"[SYSTEM] AI Warmup failed! Is Ollama running? Error: {e}")

def update_adaptive_thresholds(history: list[dict]):
    global adaptive_atr_min, adaptive_cvd_threshold
    
    if len(history) < VOLATILITY_LOOKBACK:
        return
    
    recent_atrs = []
    for c in history[-VOLATILITY_LOOKBACK:]:
        atr_proxy = c.get('body_size', 0) + c.get('upper_wick', 0) + c.get('lower_wick', 0)
        recent_atrs.append(atr_proxy)
    
    recent_atrs.sort()
    percentile_idx = int(len(recent_atrs) * ATR_PERCENTILE)
    adaptive_atr_min = max(MIN_ATR_THRESHOLD * 0.5, recent_atrs[percentile_idx])
    
    recent_volumes = [c.get('volume', 0) for c in history[-24:]]  
    avg_volume = sum(recent_volumes) / len(recent_volumes) if recent_volumes else 1000
    adaptive_cvd_threshold = max(CVD_DIVERGENCE_THRESHOLD, avg_volume * CVD_ADAPTIVE_MULTIPLIER)
    
    log.debug(f"[ADAPTIVE] ATR min: {adaptive_atr_min:.1f} | CVD threshold: {adaptive_cvd_threshold:,.0f}")

# ============================================================
# INDICATOR & CONTEXT ENGINE
# ============================================================
class StreamingEMA:
    def __init__(self, period: int):
        self.period = period
        self.k = 2.0 / (period + 1)
        self.ema = None
        self.history = []

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
            if not self.history: return price
            temp_avg = (sum(self.history) + price) / (len(self.history) + 1)
            return temp_avg
        return (price * self.k) + (self.ema * (1.0 - self.k))

class StreamingRSI:
    def __init__(self, period: int = 14):
        self.period = period
        self.seed_gains = deque(maxlen=period)
        self.seed_losses = deque(maxlen=period)
        self.avg_gain = None
        self.avg_loss = None
        self.last_price = None

    def _rsi_from_avgs(self, avg_gain: float, avg_loss: float) -> float:
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

        # Seed with simple averages for the first RSI value.
        if self.avg_gain is None or self.avg_loss is None:
            self.seed_gains.append(gain)
            self.seed_losses.append(loss)
            if len(self.seed_gains) < self.period:
                return 50.0
            self.avg_gain = sum(self.seed_gains) / self.period
            self.avg_loss = sum(self.seed_losses) / self.period
            return self._rsi_from_avgs(self.avg_gain, self.avg_loss)

        # Wilder smoothing (RMA): decay prior averages, add current gain/loss.
        self.avg_gain = ((self.avg_gain * (self.period - 1)) + gain) / self.period
        self.avg_loss = ((self.avg_loss * (self.period - 1)) + loss) / self.period
        return self._rsi_from_avgs(self.avg_gain, self.avg_loss)

    def peek(self, price: float) -> float:
        if self.last_price is None:
            return 50.0

        change = price - self.last_price
        gain = max(change, 0.0)
        loss = max(-change, 0.0)

        # During warm-up, simulate the first seeded RSI value.
        if self.avg_gain is None or self.avg_loss is None:
            temp_gains = list(self.seed_gains) + [gain]
            temp_losses = list(self.seed_losses) + [loss]
            if len(temp_gains) < self.period:
                return 50.0
            temp_avg_gain = sum(temp_gains[-self.period:]) / self.period
            temp_avg_loss = sum(temp_losses[-self.period:]) / self.period
            return self._rsi_from_avgs(temp_avg_gain, temp_avg_loss)

        # After warm-up, simulate one Wilder-smoothed step without mutating state.
        temp_avg_gain = ((self.avg_gain * (self.period - 1)) + gain) / self.period
        temp_avg_loss = ((self.avg_loss * (self.period - 1)) + loss) / self.period
        return self._rsi_from_avgs(temp_avg_gain, temp_avg_loss)

live_ema_9  = StreamingEMA(period=9)
live_ema_21 = StreamingEMA(period=21)
live_rsi    = StreamingRSI(period=14)

def get_vwap() -> float:
    return (vwap_cum_pv / vwap_cum_vol) if vwap_cum_vol > 0 else 0.0

def update_vwap(candle: dict):
    global vwap_cum_pv, vwap_cum_vol, vwap_date, cvd_snapshot_at_candle_open, cvd_total
    today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    
    if vwap_date != today_str:
        vwap_cum_pv, vwap_cum_vol = 0.0, 0.0
        cvd_total = 0.0  
        vwap_date = today_str
        log.info(f"[VWAP] Reset for new day: {today_str} (CVD also reset)")

    typical_price = (candle['high'] + candle['low'] + candle['close']) / 3.0
    vwap_cum_pv  += typical_price * candle['volume']
    vwap_cum_vol += candle['volume']
    cvd_snapshot_at_candle_open = cvd_total

async def process_agg_trade(msg: dict):
    global cvd_total, cvd_1min_buffer, last_cvd_1min, live_price, last_agg_trade_ms
    qty = float(msg['q'])
    is_buyer_maker = msg['m']
    trade_price = float(msg['p'])
    trade_ts = int(msg.get('T', 0) or 0)
    delta = -qty if is_buyer_maker else qty

    async with state_lock:
        cvd_total += delta
        cvd_1min_buffer.append((time.time(), delta))
        live_price = trade_price
        if trade_ts > 0:
            last_agg_trade_ms = max(last_agg_trade_ms, trade_ts)

        cutoff = time.time() - 60
        while cvd_1min_buffer and cvd_1min_buffer[0][0] < cutoff:
            cvd_1min_buffer.popleft()

        last_cvd_1min = sum(d for _, d in cvd_1min_buffer)

def detect_market_regime(history: list[dict]) -> str:
    if len(history) < 30:
        return "UNKNOWN"
    closes = [c['close'] for c in history[-30:]]
    ema_short = sum(closes[-10:]) / 10
    ema_long  = sum(closes) / 30
    atr_vals = [c.get('body_size', 0) + c.get('upper_wick', 0) + c.get('lower_wick', 0) for c in history[-14:]]
    atr = sum(atr_vals) / len(atr_vals) if atr_vals else 0

    current_price = closes[-1]
    dynamic_vol_limit = current_price * 0.0065
    
    regime = None
    if atr > dynamic_vol_limit: 
        regime = "VOLATILE"
    # UPDATED: Tightened from 0.001 to 0.0003 so it doesn't block normal price drift
    elif abs(ema_short - ema_long) / ema_long < 0.0003: 
        regime = "RANGING"
    else:
        regime = "TRENDING"
    
    return regime

def build_technical_context(current_candle: dict, history: list[dict]) -> dict:
    price = current_candle['close']
    ema_9_live  = live_ema_9.peek(price)
    ema_21_live = live_ema_21.peek(price)
    rsi_live    = live_rsi.peek(price)

    if len(history) >= 14:
        atr_components = [(c['high'] - c['low']) for c in history[-14:]]
        atr = sum(atr_components) / len(atr_components)
    else:
        atr = current_candle.get('body_size', 0) * 2

    vwap = get_vwap()
    vwap_distance = price - vwap if vwap > 0 else 0.0
    cvd_candle_delta = cvd_total - cvd_snapshot_at_candle_open

    vol_last_20 = [c['volume'] for c in history[-20:]] if len(history) >= 20 else [current_candle['volume']]
    vol_sma_20 = sum(vol_last_20) / len(vol_last_20)

    return {
        "price": price, "ema_9": ema_9_live, "ema_21": ema_21_live, "rsi": rsi_live,
        "atr": atr, "vwap": vwap, "vwap_distance": vwap_distance,
        "cvd_candle_delta": cvd_candle_delta, "cvd_1min": last_cvd_1min,
        "current_volume": current_candle['volume'], "vol_sma_20": vol_sma_20,
        "body_size": current_candle.get('body_size', 0)
    }

def compute_directional_prob(ctx: dict, strike: float, secs_remaining: float) -> tuple[float, float]:
    price = ctx['price']
    distance = price - strike

    time_fraction = max(secs_remaining / 3600.0, 0.01)
    expected_move = ctx['atr'] * math.sqrt(time_fraction)

    if expected_move == 0:
        return 50.0, 50.0

    z_score = distance / expected_move

    def norm_cdf(x: float) -> float:
        return 0.5 * (1.0 + math.erf(x / math.sqrt(2)))

    prob_up_base = norm_cdf(z_score) * 100.0

    rsi_deviation = ctx['rsi'] - 50.0
    rsi_bias = math.copysign(
        math.log(1.0 + abs(rsi_deviation) / 10.0) * 5.5,
        rsi_deviation
    )
    rsi_bias = max(-8.5, min(8.5, rsi_bias))

    cvd_delta = ctx['cvd_candle_delta']
    CVD_SCALE_FACTOR = 50_000.0
    cvd_bias = max(-5.0, min(5.0, (cvd_delta / CVD_SCALE_FACTOR) * 5.0))
    if abs(cvd_delta) < 35_000:
        cvd_bias = cvd_bias * (abs(cvd_delta) / 35_000) 

    candle_structure = ctx.get('candle_structure', 'NEUTRAL')
    structure_bias = 0.0
    ema_9, ema_21 = ctx.get('ema_9', 0), ctx.get('ema_21', 0)
    if ema_9 > 0 and ema_21 > 0:
        ema_bullish = ema_9 > ema_21
        structure_bias = 1.5 if ema_bullish else -1.5

    total_bias = rsi_bias + cvd_bias + structure_bias
    total_bias = max(-14.0, min(14.0, total_bias))

    prob_up = max(15.0, min(85.0, prob_up_base + total_bias))
    prob_down = 100.0 - prob_up

    return round(prob_up, 2), round(prob_down, 2)

# ============================================================
# CVD DIVERGENCE DETECTOR
# ============================================================
def detect_cvd_divergence(ctx: dict, current_candle: dict) -> tuple[str, float]:
    cvd_delta = ctx['cvd_candle_delta']
    price_structure = current_candle['structure']

    threshold = adaptive_cvd_threshold

    if price_structure == "BULLISH" and cvd_delta < -threshold:
        strength = min(abs(cvd_delta) / 100000.0, 1.0)
        return "BEARISH_DIV", round(strength, 3)

    if price_structure == "BEARISH" and cvd_delta > threshold:
        strength = min(abs(cvd_delta) / 100000.0, 1.0)
        return "BULLISH_DIV", round(strength, 3)

    return "NONE", 0.0

# ============================================================
# TIME-ADJUSTED BET SIZING 
# ============================================================
def get_time_adjusted_bet(kelly_bet: float, secs_remaining: float, confidence_level: str = "Medium") -> float:
    """
    OPTIMIZED: Removed conviction multiplier - if AI approved, trust the math.
    Only adjusts for time remaining to account for resolution uncertainty.
    """
    if kelly_bet <= 0:
        return 0.0

    if secs_remaining > 1800:
        time_multiplier = 0.50  # 30+ min away: reduce for uncertainty
    elif secs_remaining > 600:
        time_multiplier = 1.0   # 10-30 min: optimal window
    elif secs_remaining > 360:
        time_multiplier = 0.75  # 6-10 min: slight reduction
    elif secs_remaining > MIN_SECONDS_REMAINING:
        time_multiplier = 0.50  # <6 min: reduce for execution risk
    else:
        return 0.0  # Too close to expiry

    # REMOVED: conviction_multipliers - AI validation is the confidence check
    # Previous logic: High=1.0, Medium=0.75, Scout=0.5
    # New logic: If trade passed AI/EV gates, bet the full time-adjusted Kelly

    final_bet = round(kelly_bet * time_multiplier, 2)
    if final_bet < 1.00:
        return 0.0
    return final_bet

# ============================================================
# DETERMINISTIC AI FILTER
# ============================================================
def deterministic_ai_filter(rule_decision: dict, ctx: dict, current_candle: dict) -> dict:
    favored_dir = rule_decision["decision"]
    veto_reasons = []

    # 1. Extreme RSI Veto
    if favored_dir == "UP" and ctx['rsi'] > 75:
        veto_reasons.append(f"RSI Extreme Overbought ({ctx['rsi']:.1f})")
    elif favored_dir == "DOWN" and ctx['rsi'] < 25:
        veto_reasons.append(f"RSI Extreme Oversold ({ctx['rsi']:.1f})")

    # 2. VWAP Mean Reversion Veto (Tighter 0.4% threshold)
    if ctx['vwap'] > 0:
        vwap_dist_pct = abs(ctx['vwap_distance']) / ctx['price']
        if vwap_dist_pct > VWAP_OVEREXTEND_PCT:
            if (favored_dir == "UP" and ctx['price'] < ctx['vwap']) or \
               (favored_dir == "DOWN" and ctx['price'] > ctx['vwap']):
                veto_reasons.append(f"VWAP Overextended vs Direction ({vwap_dist_pct*100:.2f}%)")

    # 3. CVD Flow Veto (Tighter 15K threshold)
    CVD_HARD_VETO = CVD_CONTRA_VETO_THRESHOLD
    if favored_dir == "UP" and ctx['cvd_candle_delta'] < -CVD_HARD_VETO:
        veto_reasons.append(f"Strong CVD Selling (${ctx['cvd_candle_delta']:,.0f})")
    elif favored_dir == "DOWN" and ctx['cvd_candle_delta'] > CVD_HARD_VETO:
        veto_reasons.append(f"Strong CVD Buying (${ctx['cvd_candle_delta']:,.0f})")

    # 4. NEW: Wick Rejection Veto (Sudden Reversal Protection)
    # If the rejection wick is 1.5x larger than the candle body, the market is pivoting
    body = current_candle.get('body_size', 0.0001) or 0.0001
    upper_wick = current_candle.get('upper_wick', 0)
    lower_wick = current_candle.get('lower_wick', 0)

    if favored_dir == "UP" and upper_wick > (body * 1.5):
        veto_reasons.append(f"Bearish Rejection Wick detected")
    elif favored_dir == "DOWN" and lower_wick > (body * 1.5):
        veto_reasons.append(f"Bullish Rejection Wick detected")

    if veto_reasons:
        log.info(f"[DET_FILTER] Vetoed {favored_dir}: {' | '.join(veto_reasons)}")
        return {**rule_decision, "decision": "SKIP", "bet_size": 0.0,
                "reason": f"DET_FILTER: {' | '.join(veto_reasons)}"}

    return rule_decision

# ============================================================
# KELLY CRITERION & EV MATH WITH SLIPPAGE (FIX 3)
# ============================================================
def compute_ev_with_slippage(
    true_prob_pct: float,
    market_prob_pct: float,
    current_balance: float,
    bet_size: float,
    estimated_spread_pct: float = 0.02, 
) -> dict:
    
    token_price = market_prob_pct / 100.0
    true_prob = true_prob_pct / 100.0
    
    if not (0.01 < token_price < 0.99):
        return {
            "ev_pct": 0.0,
            "ev_pct_gross": 0.0,
            "kelly_bet": 0.0,
            "slippage_cost_pct": 0.0,
            "edge": 0.0,
            "approved": False
        }
    
    net_win = 1.0 - token_price  
    loss = token_price  
    
    gross_ev = true_prob * net_win - (1 - true_prob) * loss
    gross_ev_pct = (gross_ev / token_price) * 100
    
    if bet_size < 5.0:
        slippage_cost_pct = estimated_spread_pct * 0.5
    elif bet_size < 20.0:
        slippage_cost_pct = estimated_spread_pct * 0.75
    else:
        size_impact = min((bet_size / 1000.0) * 0.01, 0.01)  
        slippage_cost_pct = estimated_spread_pct + size_impact
    
    adjusted_token_price = token_price * (1 + slippage_cost_pct)
    adjusted_net_win = 1.0 - adjusted_token_price
    
    net_ev = true_prob * adjusted_net_win - (1 - true_prob) * adjusted_token_price
    net_ev_pct = (net_ev / adjusted_token_price) * 100
    
    b = adjusted_net_win / adjusted_token_price  
    
    if net_ev > 0:
        kelly_fraction = max(0.0, (b * true_prob - (1 - true_prob)) / b)
    else:
        kelly_fraction = 0.0
    
    raw_bet = kelly_fraction * current_balance * FRACTIONAL_KELLY_DAMPENER
    
    MIN_BET = 1.00
    # UPDATED: Increased hard cap from 5.00 to 50.00 so max risk size (5%) can actually be used
    absolute_ceiling = min(current_balance * MAX_TRADE_PCT, 50.00)
    
    kelly_bet = 0.0
    if raw_bet >= MIN_BET:
        kelly_bet = round(min(raw_bet, absolute_ceiling), 2)
    
    edge = true_prob_pct - market_prob_pct
    
    return {
        "ev_pct": round(net_ev_pct, 2),  
        "ev_pct_gross": round(gross_ev_pct, 2),  
        "kelly_bet": kelly_bet,
        "slippage_cost_pct": round(slippage_cost_pct * 100, 2),
        "edge": round(edge, 2),
        "approved": net_ev_pct > 0  
    }

# ============================================================
# POLYMARKET CLOB & FETCHERS
# ============================================================
async def fetch_market_meta_from_slug(session: aiohttp.ClientSession, slug: str) -> dict | None:
    try:
        async with api_get(session, f"{GAMMA_API}/events/slug/{slug}", timeout=5) as r:
            if r.status != 200: return None
            event = await r.json()
            markets = event.get("markets", [])
            active_market = next((m for m in markets if m.get("active") and not m.get("closed")), None)
            if not active_market: return None
            return {"title": active_market.get("question", event.get("title", "")), "market": active_market}
    except Exception as e:
        log.debug(f"[META] fetch_market_meta_from_slug failed for {slug}: {e}")
        return None

async def fetch_price_to_beat_for_market(session: aiohttp.ClientSession, slug: str) -> float:
    if slug in strike_price_cache:
        cached_price, cached_time = strike_price_cache[slug]
        if time.time() - cached_time < STRIKE_PRICE_CACHE_TTL:
            return cached_price
    
    meta = await fetch_market_meta_from_slug(session, slug)
    if not meta: return 0.0

    if "up-or-down" in slug or "updown" in slug:
        end_time_str = meta["market"].get("endDate", "")
        if end_time_str:
            try:
                end_dt = datetime.fromisoformat(end_time_str.replace("Z", "+00:00"))
                start_dt = end_dt - timedelta(hours=1)
                start_ts = int(start_dt.timestamp() * 1000)

                params = {"symbol": "BTCUSDT", "interval": "1h", "startTime": start_ts, "limit": 1}
                async with api_get(session, "https://api.binance.com/api/v3/klines", params=params, timeout=5) as r:
                    if r.status == 200:
                        data = await r.json()
                        if data:
                            strike = float(data[0][1]) 
                            strike_price_cache[slug] = (strike, time.time())
                            return strike
            except Exception as e:
                log.debug(f"Failed to fetch Binance 1H Open for strike: {e}")
    
    title = meta.get("title", "")
    match = re.search(r'\$([\d,]+\.?\d*)', title)
    if match:
        strike = float(match.group(1).replace(',', ''))
        strike_price_cache[slug] = (strike, time.time())
        return strike

    end_time_str = meta["market"].get("endDate", "")
    if not end_time_str: return 0.0
    try:
        end_dt = datetime.fromisoformat(end_time_str.replace("Z", "+00:00"))
        
        if "-15m" in slug:
            start_dt = end_dt - timedelta(minutes=15)
            variant = "fiveminute"
        else:
            start_dt = end_dt - timedelta(hours=1)
            variant = "hourly"

        params = {"symbol": "BTC", "eventStartTime": start_dt.strftime('%Y-%m-%dT%H:%M:%SZ'),
                  "variant": variant, "endDate": end_dt.strftime('%Y-%m-%dT%H:%M:%SZ')}
                  
        async with api_get(session, "https://polymarket.com/api/crypto/crypto-price", params=params, timeout=5) as r:
            if r.status == 200:
                data = await r.json()
                if data.get("openPrice"):
                    strike = float(data["openPrice"])
                    strike_price_cache[slug] = (strike, time.time())
                    return strike
    except Exception as e: 
        log.debug(f"Failed to fetch strike price from API fallback: {e}")
        
    return 0.0

async def get_polymarket_odds_cached(session: aiohttp.ClientSession, slug: str) -> dict:
    global _poly_cache, _poly_cache_slug, _poly_cache_ts
    now = time.time()
    if _poly_cache_slug == slug and (now - _poly_cache_ts) < POLY_CACHE_TTL and _poly_cache:
        return _poly_cache
    result = await _fetch_polymarket_odds(session, slug)
    if result.get("market_found"):
        _poly_cache      = result
        _poly_cache_slug = slug
        _poly_cache_ts   = now
    return result

async def _fetch_polymarket_odds(session: aiohttp.ClientSession, slug: str) -> dict:
    if not slug: return {"market_found": False}
    try:
        async with api_get(session, f"{GAMMA_API}/events", params={"slug": slug}, timeout=6) as r:
            data = await r.json()
            for event in data:
                if event.get("slug", "") != slug: continue
                active_market = next((m for m in event.get("markets", []) if m.get("active") and not m.get("closed")), None)
                if not active_market: return {"market_found": False}

                prices = json.loads(active_market.get("outcomePrices", "[]"))
                token_ids = json.loads(active_market.get("clobTokenIds", "[]"))
                strike_price = await fetch_price_to_beat_for_market(session, slug)

                return {
                    "market_found": True,
                    "up_prob": float(prices[0]) * 100, "down_prob": float(prices[1]) * 100,
                    "seconds_remaining": _parse_seconds_remaining(event.get("endDate", "")),
                    "token_id_up": token_ids[0], "token_id_down": token_ids[1],
                    "strike_price": strike_price
                }
    except Exception as e:
        log.debug(f"[ODDS] _fetch_polymarket_odds failed for {slug}: {e}")
    return {"market_found": False}

# ============================================================================
# DYNAMIC SIZING & LIQUIDITY CHECK (FIX 2)
# ============================================================================
async def check_liquidity_and_spread_v2(
    token_id: str,
    intended_bet: float,
    poly_data: Optional[dict] = None,
    clob_client = None,
    session: Optional[aiohttp.ClientSession] = None,
    PAPER_TRADING: bool = True,
    MAX_SPREAD_PCT: float = 0.05,
    MIN_LIQUIDITY_MULTIPLIER: float = 1.5
) -> Tuple[bool, str, float]:
    amm_spread = None
    if poly_data and poly_data.get("market_found"):
        up_prob = poly_data.get("up_prob", 0.0)
        down_prob = poly_data.get("down_prob", 0.0)

        if token_id == poly_data.get("token_id_up", ""):
            amm_price = up_prob / 100.0
            complement = down_prob / 100.0
        elif token_id == poly_data.get("token_id_down", ""):
            amm_price = down_prob / 100.0
            complement = up_prob / 100.0
        else:
            amm_price = 0.0
            complement = 0.0

        if 0.01 < amm_price < 0.99 and 0.01 < complement < 0.99:
            amm_spread = abs(1.0 - amm_price - complement)

    if PAPER_TRADING:
        sim_spread = amm_spread if amm_spread is not None else PAPER_SIM_FALLBACK_SPREAD_PCT
        if sim_spread > MAX_SPREAD_PCT:
            return False, f"Paper sim spread {sim_spread*100:.2f}c > max {MAX_SPREAD_PCT*100:.0f}c", 0.0

        max_fillable = PAPER_SIM_ESTIMATED_DEPTH_USD / max(MIN_LIQUIDITY_MULTIPLIER, 1.0)
        if intended_bet > max_fillable:
            scaled_bet = round(max_fillable * PAPER_SIM_SCALE_HAIRCUT, 2)
            if scaled_bet < 1.0:
                return False, f"Paper sim depth too thin (max fill ${max_fillable:.2f})", 0.0
            return True, (
                f"Paper sim scaled | spread={sim_spread*100:.2f}c | depth~${PAPER_SIM_ESTIMATED_DEPTH_USD:.0f}"
            ), scaled_bet

        return True, (
            f"Paper sim OK | spread={sim_spread*100:.2f}c | depth~${PAPER_SIM_ESTIMATED_DEPTH_USD:.0f}"
        ), intended_bet

    if clob_client is None:
        return False, "Live trading requires initialized CLOB client", 0.0

    clob_error = None
    try:
        try:
            spread_data = await asyncio.to_thread(clob_client.get_spread, token_id)
            if spread_data:
                fast_spread = float(spread_data.get("spread", 0))
                if fast_spread > MAX_SPREAD_PCT:
                    # Advisory only; orderbook remains the source of truth.
                    log.debug(f"[LIQ] Fast CLOB spread high for {token_id}: {fast_spread*100:.2f}c")
        except Exception as e:
            log.debug(f"[LIQ] Fast spread fetch failed for {token_id}: {e}")
        
        book = await asyncio.to_thread(clob_client.get_order_book, token_id)
        if not book or not book.asks:
            raise RuntimeError("CLOB orderbook unavailable")
        
        best_bid = float(book.bids[0].price) if book.bids else 0.01
        best_ask = float(book.asks[0].price)
        tob_spread = best_ask - best_bid
        
        if tob_spread >= 0.90:
            raise RuntimeError("CLOB shows stub quotes")
        
        if not (0.01 <= best_ask <= 0.99):
            raise RuntimeError(f"Invalid CLOB ask price: {best_ask}")
        
        if tob_spread > MAX_SPREAD_PCT:
            return False, f"CLOB spread {tob_spread*100:.2f}c > max", 0.0
        
        mid_price = (best_bid + best_ask) / 2.0
        
        remaining_dollars = intended_bet
        total_shares_bought = 0.0
        levels_consumed = 0
        
        for ask in book.asks:
            ask_price = float(ask.price)
            ask_size_shares = float(ask.size)
            ask_level_dollars = ask_price * ask_size_shares
            levels_consumed += 1
            
            if remaining_dollars <= ask_level_dollars:
                shares_at_level = remaining_dollars / ask_price
                total_shares_bought += shares_at_level
                remaining_dollars = 0.0
                break
            else:
                total_shares_bought += ask_size_shares
                remaining_dollars -= ask_level_dollars
            
            if levels_consumed >= 10:
                break
        
        if total_shares_bought <= 0:
            return False, "No depth available", 0.0
        
        fillable_dollars = intended_bet - remaining_dollars
        
        if remaining_dollars > 0.01:
            if fillable_dollars < intended_bet * 0.25:
                return False, f"Insufficient depth (only ${fillable_dollars:.2f} fillable)", 0.0
            
            scaled_bet = round(fillable_dollars * 0.95, 2)
            
            avg_exec_price = scaled_bet / (total_shares_bought * 0.95)
            slippage = (avg_exec_price - mid_price) / mid_price
            
            if slippage > 0.03:  
                return False, f"Slippage {slippage*100:.2f}% too high even scaled", 0.0
            
            return True, f"OK (CLOB scaled) | spread={tob_spread*100:.2f}c | impact={slippage*100:.2f}%", scaled_bet
        
        avg_exec_price = intended_bet / total_shares_bought
        slippage = (avg_exec_price - mid_price) / mid_price
        
        if slippage > 0.025:  
            scaled_bet = round(intended_bet * 0.8, 2)
            return True, "OK (CLOB) | Scaled 20% for slippage", scaled_bet
        
        top3_depth = sum(float(ask.price) * float(ask.size) for ask in book.asks[:3])
        if top3_depth < intended_bet * MIN_LIQUIDITY_MULTIPLIER:
            scaled_bet = round(top3_depth * 0.8, 2)
            if scaled_bet < 1.00:
                return False, f"Insufficient depth (${top3_depth:.2f})", 0.0
            
            return True, f"OK (CLOB depth-limited) | spread={tob_spread*100:.2f}c", scaled_bet
        
        return True, f"OK (CLOB) | spread={tob_spread*100:.2f}c | impact={slippage*100:.2f}% | levels={levels_consumed}", intended_bet
    
    except Exception as e:
        clob_error = str(e)
        log.debug(f"[LIQ] CLOB primary check failed for {token_id}: {e}. Falling back to AMM.")

    if amm_spread is not None:
        if amm_spread > MAX_SPREAD_PCT:
            return False, (
                f"AMM fallback spread {amm_spread*100:.2f}c > max {MAX_SPREAD_PCT*100:.0f}c "
                f"(CLOB unavailable: {clob_error})"
            ), 0.0

        ESTIMATED_AMM_DEPTH = 1000.0
        if intended_bet > ESTIMATED_AMM_DEPTH * 0.5:
            scaled_bet = round(ESTIMATED_AMM_DEPTH * 0.4, 2)
            return True, (
                f"OK (AMM fallback scaled) | spread={amm_spread*100:.2f}c "
                f"(CLOB unavailable: {clob_error})"
            ), scaled_bet

        return True, (
            f"OK (AMM fallback) | spread={amm_spread*100:.2f}c "
            f"(CLOB unavailable: {clob_error})"
        ), intended_bet

    return False, f"Liquidity unavailable (CLOB: {clob_error}; no AMM fallback)", 0.0
    
# ============================================================
# PARALLEL POLYMARKET RESOLUTION (FIX 1)
# ============================================================
async def fetch_polymarket_resolution_v2(
    session: aiohttp.ClientSession, 
    slug: str,
    strike_price: float,
    end_date_str: str
) -> dict:
    
    async def poll_polymarket():
        POLL_INTERVAL = 10  
        MAX_POLLS = 18  
        
        for attempt in range(1, MAX_POLLS + 1):
            await asyncio.sleep(POLL_INTERVAL)
            
            try:
                async with api_get(
                    session,
                    f"https://gamma-api.polymarket.com/events/slug/{slug}", 
                    timeout=8
                ) as r:
                    if r.status == 429:
                        await asyncio.sleep(30)
                        continue
                    
                    if r.status != 200:
                        continue
                    
                    event = await r.json()
                    markets = event.get("markets", [])
                    if not markets:
                        continue
                    
                    market = markets[0]
                    
                    raw_prices = market.get("outcomePrices", "[]")
                    if isinstance(raw_prices, str):
                        raw_prices = json.loads(raw_prices)
                    
                    if len(raw_prices) < 2:
                        continue
                    
                    p_up, p_down = float(raw_prices[0]), float(raw_prices[1])
                    
                    if p_up >= 0.95:
                        return {
                            "outcome": "UP",
                            "source": "POLYMARKET_API",
                            "p_up": p_up,
                            "p_down": p_down,
                            "poll_count": attempt
                        }
                    
                    if p_down >= 0.95:
                        return {
                            "outcome": "DOWN",
                            "source": "POLYMARKET_API",
                            "p_up": p_up,
                            "p_down": p_down,
                            "poll_count": attempt
                        }
                    
                    if market.get("closed", False):
                        outcome = "UP" if p_up > p_down else "DOWN"
                        return {
                            "outcome": outcome,
                            "source": "POLYMARKET_CLOSED",
                            "p_up": p_up,
                            "p_down": p_down,
                            "poll_count": attempt
                        }
            
            except asyncio.TimeoutError:
                log.debug(f"[RESOLVE] Polymarket poll timeout for {slug} (attempt {attempt}/{MAX_POLLS})")
            except Exception as e:
                log.debug(f"[RESOLVE] Polymarket poll failed for {slug} (attempt {attempt}/{MAX_POLLS}): {e}")
        
        return None
    
    async def fetch_binance_resolution():
        try:
            end_dt = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
        except Exception as e:
            log.debug(f"[RESOLVE] Invalid end_date for {slug}: {e}")
            return None
        
        now = datetime.now(timezone.utc)
        wait_seconds = (end_dt - now).total_seconds()
        
        if wait_seconds > 0:
            await asyncio.sleep(wait_seconds + 30)  
        
        for attempt in range(1, 7):  
            try:
                start_dt = end_dt - timedelta(hours=1)
                start_ts = int(start_dt.timestamp() * 1000)
                
                params = {
                    "symbol": "BTCUSDT",
                    "interval": "1h",
                    "startTime": start_ts,
                    "limit": 1
                }
                
                async with api_get(
                    session,
                    "https://api.binance.com/api/v3/klines",
                    params=params,
                    timeout=5
                ) as r:
                    if r.status == 200:
                        data = await r.json()
                        if data:
                            close_price = float(data[0][4]) 
                            
                            outcome = "UP" if close_price >= strike_price else "DOWN"
                            
                            return {
                                "outcome": outcome,
                                "source": "BINANCE_FALLBACK",
                                "final_price": close_price,
                                "strike_price": strike_price,
                                "p_up": 1.0 if outcome == "UP" else 0.0,
                                "p_down": 0.0 if outcome == "UP" else 1.0
                            }
            
            except Exception as e:
                log.debug(f"[RESOLVE] Binance fallback fetch failed for {slug} (attempt {attempt}/6): {e}")
            
            if attempt < 6:
                await asyncio.sleep(10)  
        
        return None
    
    poly_task = asyncio.create_task(poll_polymarket())
    binance_task = asyncio.create_task(fetch_binance_resolution())
    
    done, pending = await asyncio.wait(
        [poly_task, binance_task],
        return_when=asyncio.FIRST_COMPLETED
    )
    
    if poly_task in done:
        poly_result = await poly_task
        if poly_result is not None:
            binance_task.cancel()
            return poly_result
    
    binance_result = await binance_task
    
    if not poly_task.done():
        poly_task.cancel()
    
    if binance_result is not None:
        return binance_result
    
    return {
        "outcome": "ERROR",
        "source": "TOTAL_FAILURE",
        "p_up": 0.5,
        "p_down": 0.5
    }

async def resolve_market_outcome(session: aiohttp.ClientSession, slug: str, decision: str,
                                  strike: float, local_price_fallback: float, bet_size: float = 1.01,
                                  bought_price: float = 0.0):
    async with state_lock:
        live_pred = active_predictions.get(slug)
        if not live_pred or live_pred.get("status") not in ["OPEN", "CLOSING", "RESOLVING"]:
            return
        pred = dict(live_pred)
    global total_wins, total_losses, simulated_balance

    log.info(f"[RESOLVE] Market expired -> {slug}  |  Our call: {decision}  |  Strike: ${strike:,.2f}")
    local_calc_outcome = "TIE"

    meta = await fetch_market_meta_from_slug(session, slug)
    end_date_str = meta["market"].get("endDate", "") if meta else ""

    resolution = await fetch_polymarket_resolution_v2(
        session, slug, strike, end_date_str
    )

    actual_outcome = resolution["outcome"]
    resolution_source = resolution.get("source", "UNKNOWN")
    final_price = resolution.get("final_price", local_price_fallback)
    local_calc_outcome = "UP" if final_price >= strike else "DOWN"

    if actual_outcome == "ERROR":
        actual_outcome = local_calc_outcome
        match_status = "[WARN] FALLBACK (both APIs failed)"
    elif actual_outcome == local_calc_outcome:
        match_status = f"[OK] MATCH ({resolution_source})"
    else:
        match_status = f"[WARN] MISMATCH (local={local_calc_outcome} api={actual_outcome})"

    is_dust = bet_size < 1.00
    win_profit = round(bet_size * (1.0 / bought_price - 1.0), 4) if (0 < bought_price < 1) else bet_size

    if actual_outcome == "TIE": result_str = "TIE"; pnl_impact = 0.0
    elif decision == actual_outcome: result_str = "DUST_WIN" if is_dust else "WIN"; pnl_impact = win_profit
    else: result_str = "DUST_LOSS" if is_dust else "LOSS"; pnl_impact = -bet_size

    if PAPER_TRADING: simulated_balance += pnl_impact
    risk_manager.current_daily_pnl += pnl_impact

    if "signals" in pred: signal_tracker.log_resolution(pred["signals"], result_str, pnl_impact)
    if "WIN" in result_str: total_wins += 1
    elif "LOSS" in result_str: total_losses += 1

    win_rate = (total_wins / max(1, total_wins + total_losses)) * 100
    log.info(f"[STATS] W:{total_wins} L:{total_losses} | WinRate:{win_rate:.2f}% | Daily PnL: ${risk_manager.current_daily_pnl:.2f} | Match: {match_status}")

    # Extract the reason array and format it as a string
    reason_str = " | ".join(pred.get("signals", [])) if isinstance(pred.get("signals"), list) else str(pred.get("signals", ""))

    await log_trade_to_db(slug, decision, strike, final_price, actual_outcome, result_str, win_rate, pnl_impact,
                    local_calc_outcome=local_calc_outcome, official_outcome=actual_outcome, match_status=match_status,
                    trigger_reason=reason_str)

    if "ml_data" in pred:
        ml_row = dict(pred["ml_data"])
        ml_row["outcome_binary"] = 1 if "WIN" in result_str else 0
        ml_row["actual_pnl"] = pnl_impact
        await log_ml_data(ml_row)

    async with state_lock:
        active_predictions.pop(slug, None)

def run_gatekeeper(
    ctx: dict,
    poly_data: dict,
    current_balance: float,
    current_candle: dict,
    history_snapshot: list[dict] | None = None,
) -> tuple:
    if not poly_data["market_found"]: 
        return False, "No Polymarket data", {}, {}

    seconds_left = poly_data.get("seconds_remaining", 0)
    
    if seconds_left < MIN_SECONDS_REMAINING: 
        return False, f"Too close to expiry ({int(seconds_left)}s < {MIN_SECONDS_REMAINING}s)", {}, {}
    if seconds_left > MAX_SECONDS_FOR_NEW_BET: 
        return False, f"Too early ({int(seconds_left)}s > {MAX_SECONDS_FOR_NEW_BET}s)", {}, {}

    regime = detect_market_regime(history_snapshot if history_snapshot is not None else candle_history)
    # OPTIMIZED: Allow ranging markets for mean reversion strategies
    # Only block UNKNOWN regime (insufficient data)
    if regime == "UNKNOWN":
        return False, f"Market {regime} - insufficient data", {}, {}
    
    # OPTIMIZED: Different strategies for different regimes
    regime_context = {"regime": regime}  # Pass regime to rule engine

    if ctx['atr'] < adaptive_atr_min:
        return False, f"Dead market (ATR {ctx['atr']:.1f} < {adaptive_atr_min:.1f})", {}, {}

    ema_spread_pct = abs(ctx['ema_9'] - ctx['ema_21']) / ctx['ema_21']
    if ema_spread_pct < EMA_SQUEEZE_PCT:
        return False, f"EMA Squeeze (spread {ema_spread_pct*100:.3f}%)", {}, {}

    if poly_data["up_prob"] > MAX_CROWD_PROB_TO_CALL or poly_data["down_prob"] > MAX_CROWD_PROB_TO_CALL:
        return False, "Crowd skew too high", {}, {}

    strike = poly_data.get("strike_price", 0.0)
    if strike <= 0:
        return False, "Invalid strike price", {}, {}

    prob_up, prob_down = compute_directional_prob(ctx, strike, seconds_left)
    
    kelly_up_temp = compute_ev_with_slippage(
        prob_up, poly_data["up_prob"], current_balance, bet_size=2.50
    )
    kelly_down_temp = compute_ev_with_slippage(
        prob_down, poly_data["down_prob"], current_balance, bet_size=2.50
    )

    ev_up = compute_ev_with_slippage(
        prob_up, 
        poly_data["up_prob"],
        current_balance,
        bet_size=kelly_up_temp["kelly_bet"],
        estimated_spread_pct=0.02 
    )
    ev_down = compute_ev_with_slippage(
        prob_down,
        poly_data["down_prob"],
        current_balance,
        bet_size=kelly_down_temp["kelly_bet"],
        estimated_spread_pct=0.02
    )

    return True, f"Passed Gate [Regime: {regime}]", ev_up, ev_down

def rule_engine_decide(ctx: dict, ev_up: dict, ev_down: dict,
                        poly_data: dict, current_candle: dict) -> dict:
    target_dir = "UP" if ev_up["ev_pct"] > ev_down["ev_pct"] else "DOWN"
    target_ev  = ev_up if target_dir == "UP" else ev_down

    log.info(f"[EV] {target_dir} | "
             f"Gross: {target_ev['ev_pct_gross']:+.2f}% | "
             f"Slippage: -{target_ev['slippage_cost_pct']:.2f}% | "
             f"Net: {target_ev['ev_pct']:+.2f}%")

    if not target_ev.get("approved", False):
        return {
            "decision": "SKIP",
            "reason": f"Net EV {target_ev['ev_pct']:.2f}% <= 0 after slippage"
        }

    score = 0
    bonus_score = 0
    reasons = []

    if (target_dir == "UP" and ctx['price'] > ctx['vwap']) or \
       (target_dir == "DOWN" and ctx['price'] < ctx['vwap']):
        score += 1; reasons.append("VWAP Trend")

    if (target_dir == "UP" and ctx['rsi'] > 50) or \
       (target_dir == "DOWN" and ctx['rsi'] < 50):
        score += 1; reasons.append("RSI Momentum")

    if ctx['current_volume'] > ctx['vol_sma_20'] * 1.05:
        score += 1; reasons.append("Vol Spike")

    cvd_delta = ctx['cvd_candle_delta']
    if (target_dir == "UP" and cvd_delta > 10000) or \
       (target_dir == "DOWN" and cvd_delta < -10000):
        score += 1; reasons.append("CVD Aligned")

    secs_remaining = poly_data.get("seconds_remaining", 0)
    target_ev_pct = target_ev.get("ev_pct", 0.0)
    target_token_price = (
        poly_data.get("up_prob", 0.0) / 100.0 if target_dir == "UP"
        else poly_data.get("down_prob", 0.0) / 100.0
    )
    allow_score0_extreme_ev = (
        score == 0
        and target_ev_pct >= SCORE0_MIN_EV_PCT
        and target_token_price <= SCORE0_MAX_TOKEN_PRICE
    )

    # Hard floor: never place trades with weak technical alignment.
    if score < MIN_SCORE_TO_TRADE and not allow_score0_extreme_ev:
        return {
            "decision": "SKIP",
            "confidence": "Low",
            "score": score,
            "reason": f"Insufficient technical confirmation ({score}/4 < {MIN_SCORE_TO_TRADE}/4)"
        }

    # Score 1 trades are allowed only when EV is significantly strong, and still require AI.
    if score == 1 and target_ev_pct < SCORE1_MIN_EV_PCT:
        return {
            "decision": "SKIP",
            "confidence": "Low",
            "score": score,
            "reason": f"Score 1 requires EV >= {SCORE1_MIN_EV_PCT:.2f}% (got {target_ev_pct:.2f}%)"
        }

    if allow_score0_extreme_ev:
        confidence = "Scout"
        needs_ai = True
        reasons.append(
            f"EXTREME EV OVERRIDE (score=0/4, EV={target_ev_pct:.1f}%, px={target_token_price:.3f})"
        )
    elif score >= 4:  # All signals aligned
        confidence = "High"
        needs_ai = False
    elif (
        target_ev_pct >= EV_AI_BYPASS_THRESHOLD
        and score >= EV_BYPASS_MIN_SCORE
        and target_token_price >= EV_BYPASS_MIN_TOKEN_PRICE
    ):
        confidence = "High"
        needs_ai = False
        reasons.append(
            f"EV BYPASS ({target_ev_pct:.1f}% >= {EV_AI_BYPASS_THRESHOLD}%, "
            f"score={score}/4, px={target_token_price:.3f})"
        )
    elif target_ev_pct >= EV_AI_BYPASS_THRESHOLD:
        confidence = "Scout"
        needs_ai = True
        reasons.append(
            f"HIGH EV requires AI (score={score}/4, px={target_token_price:.3f}; "
            f"bypass needs score>={EV_BYPASS_MIN_SCORE}, px>={EV_BYPASS_MIN_TOKEN_PRICE:.2f})"
        )
    elif target_ev_pct >= MIN_EV_PCT_TO_CALL_AI:
        confidence = "Scout"
        needs_ai = True
        reasons.append(f"AI VALIDATION REQUIRED (score={score}/4)")
    else:
        return {
            "decision": "SKIP",
            "confidence": "Low",
            "score": score,
            "reason": f"Net EV {target_ev_pct:.2f}% below AI trigger ({MIN_EV_PCT_TO_CALL_AI:.2f}%)"
        }

    raw_bet = target_ev.get("kelly_bet", 0.0)
    bet = get_time_adjusted_bet(raw_bet, secs_remaining, confidence)
    
    return {
        "decision": target_dir, 
        "confidence": confidence, 
        "bet_size": bet,
        "score": score, 
        "bonus": bonus_score, 
        "reason": " | ".join(reasons), 
        "needs_ai": needs_ai
    }

# ============================================================
# EXECUTION & AI PIPELINE
# ============================================================
async def _commit_decision(
    slug: str,
    result: dict,
    poly_data: dict,
    current_ev_pct: float = 0.0,
    ctx: dict = None
):
    strike   = poly_data.get("strike_price", 0.0)
    decision = result["decision"]

    if decision in ["UP", "DOWN"]:
        if result.get("bet_size", 0) >= 1.00:
            committed_slugs.add(slug)
            soft_skipped_slugs.discard(slug)
            best_ev_seen.pop(slug, None)

            bought_price = poly_data["up_prob"] / 100.0 if decision == "UP" else poly_data["down_prob"] / 100.0
            token_id = poly_data.get("token_id_up", "") if decision == "UP" else poly_data.get("token_id_down", "")

            ml_data = {
                "timestamp": datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                "market_slug": slug, 
                "direction": decision,
                "price_vs_vwap_pct": round((ctx['price'] - ctx['vwap']) / ctx['vwap'] * 100, 4) if ctx and ctx.get('vwap', 0) > 0 else 0,
                "rsi_14": round(ctx['rsi'], 1) if ctx else 50, 
                "atr_14": round(ctx['atr'], 2) if ctx else 0,
                "cvd_candle_delta": round(ctx['cvd_candle_delta'], 0) if ctx else 0,
                "ev_pct": round(current_ev_pct, 2),
                "rule_score": result.get("score", 0),
                "bonus_score": result.get("bonus", 0),
                "trigger_reason": result.get("reason", "UNKNOWN"),
                "confidence_level": result.get("confidence", "Medium")  
            }

            async with state_lock:
                active_predictions[slug] = {
                    "decision": decision, "strike": strike, "score": result.get("score", 0),
                    "bet_size": result.get("bet_size", 0.0), "bought_price": bought_price,
                    "token_id": token_id, "status": "OPEN", "entry_time": time.time(),
                    "signals": result.get("reason", "").split(" | "), "ml_data": ml_data,
                    "sl_breach_count": 0,
                    "tp_gate_logged": False
                }
            log.info(f"DECISION LOCKED: {decision} | Confidence: {result.get('confidence', '?')} | "
                    f"Score: {result.get('score','?')}/4 | Bonus: {result.get('bonus',0)} | Bet: ${result.get('bet_size',0.0):.2f}")
            fire_and_forget(place_bet(slug, decision, result.get("bet_size", 0.0), poly_data))
        else:
            log.warning(f"[DUST REJECT] {decision} on {slug} discarded. Bet size ${result.get('bet_size', 0):.2f} < $1.00 Minimum")
            soft_skipped_slugs.add(slug)
    else:
        log.info(f"[SKIP LOG] Market {slug} safely bypassed. Reason: {result.get('reason', 'None')}")
        soft_skipped_slugs.add(slug)

async def place_bet(slug: str, decision: str, bet_size: float, poly_data: dict):
    global clob_client, simulated_balance
    token_id = poly_data.get("token_id_up", "") if decision == "UP" else poly_data.get("token_id_down", "")

    if not PAPER_TRADING and clob_client is None:
        log.error(f"[REJECTED] {slug}: Live mode but CLOB client is not initialized")
        async with state_lock:
            active_predictions.pop(slug, None)
        committed_slugs.discard(slug)
        return

    liq_ok, liq_msg, executable_bet = await check_liquidity_and_spread_v2(
        token_id=token_id,
        intended_bet=bet_size,
        poly_data=poly_data,
        clob_client=clob_client,
        session=None,
        PAPER_TRADING=PAPER_TRADING,
        MAX_SPREAD_PCT=MAX_SPREAD_PCT,
        MIN_LIQUIDITY_MULTIPLIER=MIN_LIQUIDITY_MULTIPLIER
    )

    if not liq_ok:
        log.warning(f"[REJECTED] {slug}: {liq_msg}")
        async with state_lock:
            active_predictions.pop(slug, None)
        committed_slugs.discard(slug)
        return

    if executable_bet < bet_size:
        log.info(f"[BET SCALED] {slug}: ${bet_size:.2f} -> ${executable_bet:.2f}")
        bet_size = executable_bet
        async with state_lock:
            if slug in active_predictions:
                active_predictions[slug]["bet_size"] = bet_size
                active_predictions[slug]["ml_data"]["final_bet_size"] = bet_size

    if bet_size < 1.00:
        log.warning(f"[REJECTED] {slug}: Scaled bet ${bet_size:.2f} < $1.00 minimum")
        async with state_lock:
            active_predictions.pop(slug, None)
        committed_slugs.discard(slug)
        return

    async with state_lock:
        if slug in active_predictions:
            active_predictions[slug]["ml_data"]["spread_eval"] = liq_msg

    risk_manager.trades_this_hour += 1
    market_prob = poly_data["up_prob"] if decision == "UP" else poly_data["down_prob"]
    expected_price = market_prob / 100.0

    log.info(f"[BET] BET PLACED [{'PAPER' if PAPER_TRADING else 'LIVE'}] {decision} on {slug} | "
            f"Bet: ${bet_size:.2f} | Expected: {expected_price:.4f} | Liq: {liq_msg}")

    if not PAPER_TRADING and not DRY_RUN and clob_client:
        # OPTIMIZED: Use Limit Orders with slippage protection instead of Market Orders
        # Market orders can slip 4-5c on thin liquidity, destroying 6c ATR targets
        
        # Calculate maximum acceptable entry price (expected + 2c max slippage)
        MAX_ENTRY_SLIPPAGE_CENTS = 0.02  # 2 cents maximum slippage
        max_entry_price = expected_price + MAX_ENTRY_SLIPPAGE_CENTS
        max_entry_price = min(max_entry_price, 0.99)  # Never pay more than 99c
        
        # Round to nearest cent for CLOB compatibility
        tick = Decimal("0.01")
        limit_price = float(Decimal(str(max_entry_price)).quantize(tick, rounding=ROUND_UP))
        shares_to_buy = float(
            (Decimal(str(bet_size)) / Decimal(str(limit_price))).quantize(tick, rounding=ROUND_DOWN)
        )
        if shares_to_buy < 0.01:
            log.warning(f"[REJECTED] {slug}: Bet ${bet_size:.2f} too small at limit price {limit_price:.4f}")
            async with state_lock:
                active_predictions.pop(slug, None)
            committed_slugs.discard(slug)
            return

        def _sign_and_submit_limit():
            from py_clob_client.clob_types import LimitOrderArgs, OrderType
            from py_clob_client.order_builder.constants import BUY

            order_args = LimitOrderArgs(
                token_id=token_id,
                price=str(limit_price),  # Maximum price we'll pay
                size=shares_to_buy,
                side=BUY
            )
            signed = clob_client.create_order(order_args)
            return clob_client.post_order(signed, OrderType.FOK)  # Fill-or-Kill: all or nothing
        
        log.info(f"[ENTRY ORDER] Limit @ {limit_price:.4f} (expected {expected_price:.4f} + max {MAX_ENTRY_SLIPPAGE_CENTS*100:.0f}c slip)")

        try:
            resp = await asyncio.wait_for(asyncio.to_thread(_sign_and_submit_limit), timeout=4.0)

            status = resp.get("status", "")
            
            actual_price = expected_price  
            if "transactions" in resp and resp["transactions"]:
                total_cost = sum(float(tx.get("price", 0)) * float(tx.get("size", 0)) for tx in resp["transactions"])
                total_shares = sum(float(tx.get("size", 0)) for tx in resp["transactions"])
                if total_shares > 0:
                    actual_price = total_cost / total_shares
            
            if status == "matched":
                spread_cents = float(liq_msg.split("spread=")[1].split("c")[0]) if "spread=" in liq_msg else 0
                await log_execution_metrics(slug, decision, expected_price, actual_price, spread_cents, liq_msg)
                
                slippage_bps = ((actual_price - expected_price) / expected_price * 10000) if expected_price > 0 else 0
                slippage_cents = (actual_price - expected_price) * 100
                
                log.info(f"[OK] ORDER FILLED: {decision} on {slug} | ${bet_size:.2f} | "
                        f"Fill: {actual_price:.4f} (expected {expected_price:.4f}) | "
                        f"Slippage: {slippage_bps:+.1f}bps ({slippage_cents:+.1f}c)")

                async with state_lock:
                    if slug in active_predictions:
                        active_predictions[slug]["bought_price"] = actual_price
                    
            else:
                # OPTIMIZED: Strict cutoff. No market order fallback. 
                log.warning(f"[WARN] LIMIT REJECTED [{status}]: {resp.get('errorMsg', 'Price moved beyond limit')} | {slug}")
                log.info(f"[STOP] Trade abandoned. Price ran past our +2c slippage guard.")
                async with state_lock:
                    active_predictions.pop(slug, None)
                committed_slugs.discard(slug)

        except asyncio.TimeoutError:
            log.error(f"[TIMEOUT] CLOB TIMEOUT (>4s) for {slug}. Limit order status uncertain.")
            async with state_lock:
                if slug in active_predictions:
                    active_predictions[slug]["status"] = "UNCERTAIN"

        except Exception as e:
            log.error(f"[ERROR] CLOB execution failed: {e}")
            async with state_lock:
                active_predictions.pop(slug, None)
            committed_slugs.discard(slug)

async def execute_early_exit(session: aiohttp.ClientSession, slug: str, exit_reason: str, current_token_price: float):
    global simulated_balance, total_wins, total_losses

    async with state_lock:
        live_pred = active_predictions.get(slug)
        if not live_pred or live_pred.get("status") not in ("OPEN", "CLOSING"):
            return
        live_pred["status"] = "CLOSING"
        pred = dict(live_pred)

    bet_size = pred["bet_size"]
    bought_price = pred["bought_price"]
    shares_owned = bet_size / bought_price if bought_price > 0 else 0.0

    if shares_owned < 0.01:
        async with state_lock:
            live_pred = active_predictions.get(slug)
            if live_pred:
                _record_full_exit_for_reentry(slug, live_pred, f"DUST_EXIT:{exit_reason}")
            active_predictions.pop(slug, None)
        return

    # OPTIMIZED: ROI-based exit guard instead of absolute capture ratio
    # This prevents exiting for tiny gains that don't justify spread costs
    if "TAKE_PROFIT" in exit_reason:
        roi_pct = (current_token_price - bought_price) / bought_price if bought_price > 0 else 0.0
        
        # Require minimum 8% ROI to justify early exit (covers spread + slippage)
        # For a 26.5c entry, this is ~2.1c minimum gain
        MIN_ROI_FOR_EARLY_EXIT = 0.08
        
        if roi_pct < MIN_ROI_FOR_EARLY_EXIT:
            log.info(f"[EXIT GUARD] {slug}: ROI only {roi_pct*100:.1f}% (need {MIN_ROI_FOR_EARLY_EXIT*100}%). Holding for larger move.")
            async with state_lock:
                live_pred = active_predictions.get(slug)
                if live_pred:
                    live_pred["status"] = "OPEN"
            return
        
        log.info(f"[EXIT APPROVED] {slug}: ROI {roi_pct*100:.1f}% exceeds {MIN_ROI_FOR_EARLY_EXIT*100}% threshold. Executing exit.")

    if PAPER_TRADING:
        roi_pct = (current_token_price / bought_price) - 1.0 if bought_price > 0 else 0.0
        pnl_impact = bet_size * roi_pct
        result_str = "WIN" if pnl_impact > 0 else "LOSS"
        simulated_balance += pnl_impact
        risk_manager.current_daily_pnl += pnl_impact
        if result_str == "WIN":
            total_wins += 1
        else:
            total_losses += 1

        log.info(f"[EARLY EXIT] [FAST] {slug} | Reason: {exit_reason} | PnL: ${pnl_impact:+.4f}")

        if "ml_data" in pred:
            ml_row = dict(pred["ml_data"])
            ml_row["outcome_binary"] = 1 if result_str == "WIN" else 0
            ml_row["actual_pnl"] = pnl_impact
            await log_ml_data(ml_row)

        await log_trade_to_db(
            slug, pred["decision"], pred["strike"], live_price, "EARLY_EXIT",
            result_str, (total_wins / max(1, total_wins + total_losses) * 100),
            pnl_impact, local_calc_outcome=exit_reason, official_outcome="SOLD"
        )
        async with state_lock:
            live_pred = active_predictions.get(slug)
            if live_pred:
                _record_full_exit_for_reentry(slug, live_pred, exit_reason)
            active_predictions.pop(slug, None)

    elif not DRY_RUN and clob_client:
        def _parse_fill_stats(resp: dict, expected_shares: float, fallback_price: float) -> tuple[float, float]:
            txs = resp.get("transactions") or []
            if txs:
                total_shares = sum(float(tx.get("size", 0) or 0.0) for tx in txs)
                total_cost = sum(
                    float(tx.get("price", fallback_price) or fallback_price) * float(tx.get("size", 0) or 0.0)
                    for tx in txs
                )
                if total_shares > 0:
                    avg_fill_price = (total_cost / total_shares) if total_cost > 0 else fallback_price
                    return total_shares, avg_fill_price

            if resp.get("status") == "matched":
                return expected_shares, fallback_price
            if "matchedAmount" in resp:
                val = float(resp["matchedAmount"])
                if val > 0:
                    return val, fallback_price
            if "takerAmount" in resp:
                val = float(resp.get("takerAmount", 0))
                if val > 0:
                    return val, fallback_price
            return 0.0, fallback_price

        async def _attempt_ioc_sell(floor_price: float) -> tuple[bool, float, float]:
            from py_clob_client.clob_types import LimitOrderArgs, OrderType
            from py_clob_client.order_builder.constants import SELL

            tick = Decimal("0.01")
            floor_rounded = float(Decimal(str(floor_price)).quantize(tick, rounding=ROUND_DOWN))
            floor_rounded = max(0.01, floor_rounded)

            order_args = LimitOrderArgs(
                token_id=pred["token_id"],
                price=str(floor_rounded),
                size=round(shares_owned, 2),
                side=SELL
            )

            def _sign_and_post():
                signed = clob_client.create_order(order_args)
                return clob_client.post_order(signed, OrderType.FAK)

            try:
                resp = await asyncio.wait_for(asyncio.to_thread(_sign_and_post), timeout=4.0)
                shares_sold, avg_fill_price = _parse_fill_stats(resp, shares_owned, floor_rounded)
                return shares_sold > 0, shares_sold, avg_fill_price
            except asyncio.TimeoutError:
                log.warning(f"[EXIT] IOC timed out for {slug}")
                return False, 0.0, 0.0

        floor_price_1 = current_token_price * 0.98
        success, shares_sold, avg_fill_price = await _attempt_ioc_sell(floor_price_1)

        if not success:
            log.warning(f"[EXIT] IOC attempt 1 failed for {slug}. Retrying at wider floor...")
            await asyncio.sleep(0.5) 
            floor_price_2 = current_token_price * 0.96
            success, shares_sold, avg_fill_price = await _attempt_ioc_sell(floor_price_2)

        if shares_sold > 0:
            fraction_sold = min(shares_sold / shares_owned, 1.0)
            realized_bet_size = bet_size * fraction_sold
            effective_exit_price = avg_fill_price if avg_fill_price > 0 else current_token_price
            pnl_impact = realized_bet_size * ((effective_exit_price / bought_price) - 1.0)
            risk_manager.current_daily_pnl += pnl_impact

            log.info(f"[OK] IOC EXIT: {slug} | Sold {fraction_sold*100:.1f}% | Realized PnL: ${pnl_impact:+.2f} | Reason: {exit_reason}")

            if "ml_data" in pred:
                ml_row = dict(pred["ml_data"])
                ml_row["outcome_binary"] = 1 if pnl_impact > 0 else 0
                ml_row["actual_pnl"] = pnl_impact
                await log_ml_data(ml_row)

            await log_trade_to_db(
                slug, pred["decision"], pred["strike"], live_price, "EARLY_EXIT",
                "WIN" if pnl_impact > 0 else "LOSS", 0.0, pnl_impact,
                local_calc_outcome=exit_reason,
                official_outcome="FULL_SELL" if fraction_sold >= 0.99 else "PARTIAL_SELL"
            )

            remaining_shares = max(0.0, shares_owned - shares_sold)
            if remaining_shares < 0.01:
                async with state_lock:
                    live_pred = active_predictions.get(slug)
                    if live_pred:
                        _record_full_exit_for_reentry(slug, live_pred, exit_reason)
                    active_predictions.pop(slug, None)
            else:
                async with state_lock:
                    live_pred = active_predictions.get(slug)
                    if live_pred:
                        live_pred["bet_size"] = remaining_shares * bought_price
                        live_pred["status"] = "OPEN"
                log.info(f"[EXIT] Partial fill. {remaining_shares:.3f} shares remain open.")
        else:
            log.error(f"[BLOCK] IOC FAILED after 2 attempts for {slug}. Will hold to resolution.")
            async with state_lock:
                live_pred = active_predictions.get(slug)
                if live_pred:
                    live_pred["status"] = "OPEN"

async def call_local_ai(session: aiohttp.ClientSession, current_candle: dict, history: list,
                         poly_data: dict, ev: dict, counter_ev: dict, math_prob: float,
                         slug: str, rule_decision: dict, ctx: dict):
    global ai_call_count, ai_consecutive_failures, ai_circuit_open_until, ai_call_in_flight

    async with ai_processing_lock:
        if ai_call_in_flight == slug:
            return  
        ai_call_in_flight = slug

    try:
        pre_filtered = deterministic_ai_filter(rule_decision, ctx, current_candle)
        if pre_filtered["decision"] == "SKIP":
            await _commit_decision(slug, pre_filtered, poly_data, ev.get("ev_pct", 0.0), ctx)
            return

        if time.time() < ai_circuit_open_until:
            rule_decision["needs_ai"] = False
            await _commit_decision(slug, rule_decision, poly_data, ev.get("ev_pct", 0.0), ctx)
            return

        record_ai_attempt(slug)
        ai_call_count += 1
        slug_ai_calls = _get_slug_ai_state(slug).get("ai_calls", 0)
        log.info(
            f"[AI CONFIRM] Borderline score - asking {LOCAL_AI_MODEL} "
            f"(global call #{ai_call_count}, slug {slug_ai_calls}/{AI_MAX_CALLS_PER_SLUG})..."
        )

        favored_dir = rule_decision["decision"]
        regime = detect_market_regime(history)

        if ctx['rsi'] > 65: rsi_desc = "Overbought (Strong Bullish)"
        elif ctx['rsi'] > 51: rsi_desc = "Bullish"
        elif ctx['rsi'] < 35: rsi_desc = "Oversold (Strong Bearish)"
        elif ctx['rsi'] < 49: rsi_desc = "Bearish"
        else: rsi_desc = "Neutral"

        trend_desc = "Bullish (Short-term trend is UP)" if ctx['ema_9'] > ctx['ema_21'] else "Bearish (Short-term trend is DOWN)"
        vwap_desc = "Bullish (Price is holding ABOVE VWAP)" if ctx['vwap_distance'] > 0 else "Bearish (Price is trapped BELOW VWAP)"

        if ctx['cvd_candle_delta'] > 15000: cvd_desc = "Strong Buying Pressure"
        elif ctx['cvd_candle_delta'] < -15000: cvd_desc = "Strong Selling Pressure"
        else: cvd_desc = "Neutral / Market Noise" 

        decision_score = int(rule_decision.get("score", 0))
        target_ev_pct = float(ev.get("ev_pct", 0.0))
        target_token_price = (
            poly_data.get("up_prob", 0.0) / 100.0 if favored_dir == "UP"
            else poly_data.get("down_prob", 0.0) / 100.0
        )

        # =====================================================================
        # UPDATED AI PROMPT: Smoothed persona & explicit "majority rules" logic
        # =====================================================================
        prompt = (
            f"You are a precise quantitative trading AI evaluating a BTC/USDT Polymarket trade for Alpha Z.\n"
            f"Your mandate is to protect capital while executing high-probability edges.\n\n"
            f"PROPOSED TRADE: {favored_dir}\n"
            f"TIME TO EXPIRY: {int(poly_data.get('seconds_remaining', 0))}s\n"
            f"MARKET REGIME: {regime}\n\n"
            f"QUANTITATIVE EDGE (CRITICAL):\n"
            f"  Expected Value (EV): {ev.get('ev_pct', 0.0):+.2f}%\n"
            f"  System Score: {rule_decision.get('score', 0)}/4\n\n"
            f"  Token Price: {target_token_price:.3f}\n\n"
            f"TECHNICAL CONTEXT:\n"
            f"  RSI: {rsi_desc}\n"
            f"  EMA Trend: {trend_desc}\n"
            f"  VWAP: {vwap_desc}\n"
            f"  CVD Flow: {cvd_desc}\n\n"
            f"STRICT RULES:\n"
            f"1. If Expected Value (EV) is negative (< 0.00%), output 'SKIP'.\n"
            f"2. If score=0 and EV < {SCORE0_MIN_EV_PCT:.2f}%, output 'SKIP'.\n"
            f"3. If score=1 and EV < {SCORE1_MIN_EV_PCT:.2f}%, output 'SKIP'.\n"
            f"4. If score=0 but EV >= {SCORE0_MIN_EV_PCT:.2f}% AND token price <= {SCORE0_MAX_TOKEN_PRICE:.2f}, "
            f"you MAY approve '{favored_dir}' when technicals are not strongly contradictory.\n"
            f"5. If EV is strongly positive and technicals are mixed (not strongly opposite), prefer '{favored_dir}'.\n\n"
            f"OUTPUT FORMAT (REQUIRED): FINAL:{favored_dir} or FINAL:SKIP\n"
            f"Return only one line."
        )
        last_ai_interaction["prompt"] = prompt
        last_ai_interaction["timestamp"] = datetime.now().strftime('%H:%M:%S')

        payload = {
            "model": LOCAL_AI_MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.0,
            "max_tokens": 10,
            "keep_alive": -1,
            "stream": False
        }
        ai_word = None
        raw_response = ""

        def _parse_ai_decision(text: str, favored: str) -> str | None:
            text_up = (text or "").upper()

            # Preferred strict format: FINAL:UP / FINAL:DOWN / FINAL:SKIP
            m = re.search(r"\bFINAL\s*[:=]\s*(UP|DOWN|SKIP)\b", text_up)
            if m:
                token = m.group(1)
                if token in (favored, "SKIP"):
                    return token
                return "SKIP"

            # Fallback: first explicit keyword token.
            tokens = re.findall(r"\b(UP|DOWN|SKIP)\b", text_up)
            if not tokens:
                return None
            first = tokens[0]
            if first in (favored, "SKIP"):
                return first
            return "SKIP"

        for attempt in range(1, AI_MAX_RETRIES + 1):
            try:
                async with session.post(LOCAL_AI_URL, json=payload,
                                        timeout=aiohttp.ClientTimeout(total=AI_TIMEOUT_TOTAL)) as r:
                    r.raise_for_status()
                    raw_response = (await r.json())['choices'][0]['message']['content'].strip()
                    ai_word = _parse_ai_decision(raw_response, favored_dir)
                    ai_consecutive_failures = 0
                    break
            except Exception as e:
                ai_consecutive_failures += 1
                log.warning(f"[AI] Attempt {attempt} failed: {e}")
                if attempt < AI_MAX_RETRIES:
                    await asyncio.sleep(AI_RETRY_DELAY)

        if ai_consecutive_failures >= CB_FAILURE_THRESHOLD:
            cooldown = min(300, CB_COOLDOWN_SECS * (2 ** (ai_consecutive_failures - CB_FAILURE_THRESHOLD)))
            ai_circuit_open_until = time.time() + cooldown
            log.error(f"[CIRCUIT] Tripped. AI paused {cooldown}s (exponential backoff).")

        if ai_word is None:
            # Fail-open on parse ambiguity for valid borderline setups; do not auto-veto malformed outputs.
            if decision_score >= 1 and target_ev_pct >= MIN_EV_PCT_TO_CALL_AI:
                log.warning(f"[AI] Unparseable response; defaulting to {favored_dir}. Raw: {raw_response!r}")
                ai_word = favored_dir
            else:
                ai_word = "SKIP"

        if ai_word == favored_dir:
            final = {**rule_decision, "reason": f"AI confirmed: {rule_decision['reason']}"}
        else:
            record_ai_veto(slug, ev.get("ev_pct", 0.0))
            final = {**rule_decision, "decision": "SKIP", "bet_size": 0.0,
                     "reason": "AI vetoed borderline signal"}

        last_ai_interaction["response"] = ai_word or "FAILED"
        await _commit_decision(slug, final, poly_data, ev.get("ev_pct", 0.0), ctx)
        
    finally:
        async with ai_processing_lock:
            if ai_call_in_flight == slug:
                ai_call_in_flight = ""

# ============================================================================
# EVALUATION LOOP
# ============================================================================
async def evaluation_loop(session: aiohttp.ClientSession):
    global target_slug, ai_call_in_flight

    while True:
        await asyncio.sleep(EVAL_TICK_SECONDS)

        async with state_lock:
            history_snapshot = list(candle_history)
            live_candle_snapshot = dict(live_candle) if live_candle else {}
            live_price_snapshot = live_price
            predictions_snapshot = [(slug, dict(pred)) for slug, pred in active_predictions.items()]

        if len(history_snapshot) >= VOLATILITY_LOOKBACK and (int(time.time()) % 50 == 0):
            update_adaptive_thresholds(history_snapshot)

        if not history_snapshot or not live_candle_snapshot:
            continue

        current_price = live_price_snapshot if live_price_snapshot > 0 else float(live_candle_snapshot.get('c', 0))
        k = live_candle_snapshot

        o_p = float(k.get('o', current_price))
        h_p = float(k.get('h', current_price))
        l_p = float(k.get('l', current_price))
        c_p = current_price

        current_candle = {
            "timestamp": datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            "open":   o_p,
            "high":   h_p,
            "low":    l_p,
            "close":  c_p,
            "volume": float(k.get('v', 0)),
            "body_size": abs(c_p - o_p),
            "upper_wick": h_p - max(o_p, c_p),
            "lower_wick": min(o_p, c_p) - l_p,
            "structure": "BULLISH" if c_p >= o_p else "BEARISH"
        }

        ctx = build_technical_context(current_candle, history_snapshot)

        # --- FIX: INDEPENDENT POSITION MONITOR ---
        for slug, pred in predictions_snapshot:
            if pred.get("status") in ("CLOSING", "RESOLVING", "UNCERTAIN"):
                continue

            poly_data_open = await get_polymarket_odds_cached(session, slug)
            secs_left = poly_data_open.get("seconds_remaining", 0)

            # If market is missing/closed OR expired, force resolution
            if not poly_data_open.get("market_found") or secs_left <= 0:
                resolve_decision = pred.get("decision", "")
                resolve_strike = pred.get("strike", 0.0)
                resolve_bet_size = pred.get("bet_size", 1.01)
                resolve_bought_price = pred.get("bought_price", 0.0)
                should_resolve = False
                async with state_lock:
                    live_pred = active_predictions.get(slug)
                    if live_pred and live_pred.get("status") not in ("RESOLVING", "UNCERTAIN"):
                        live_pred["status"] = "RESOLVING"
                        resolve_decision = live_pred.get("decision", resolve_decision)
                        resolve_strike = live_pred.get("strike", resolve_strike)
                        resolve_bet_size = live_pred.get("bet_size", resolve_bet_size)
                        resolve_bought_price = live_pred.get("bought_price", resolve_bought_price)
                        should_resolve = True
                if should_resolve:
                    fire_and_forget(resolve_market_outcome(
                        session, slug, resolve_decision, resolve_strike,
                        current_price, resolve_bet_size, resolve_bought_price
                    ))
                continue

            if pred.get("decision") not in ("UP", "DOWN"):
                continue
            bought_price = float(pred.get("bought_price", 0.0))
            if bought_price <= 0:
                continue

            # Get current token price for exit evaluation
            current_token_price = (
                poly_data_open["up_prob"] / 100.0 if pred["decision"] == "UP"
                else poly_data_open["down_prob"] / 100.0
            )

            # OPTIMIZED: ATR-BASED DYNAMIC STOP LOSS & TAKE PROFIT
            # Calculate ATR for volatility-adjusted stops
            if len(history_snapshot) >= 14:
                recent_candles = history_snapshot[-14:]
                atr_values = [(c['high'] - c['low']) for c in recent_candles]
                current_atr = sum(atr_values) / len(atr_values)
            else:
                current_atr = 50.0  # Default fallback

            # FIXED: ATR volatility modifier for token-probability stops.
            # Keep TP/SL in token cents space and scale by volatility.
            volatility_modifier = current_atr / 150.0
            volatility_modifier = max(0.6, min(volatility_modifier, 1.8))
            tp_delta = 0.08 * volatility_modifier   # Base +8c TP scaled by vol
            sl_delta = -0.10 * volatility_modifier  # Base -10c SL scaled by vol
            tp_delta = max(tp_delta, 0.06)          # Absolute 6c floor
            sl_delta = min(sl_delta, -0.08)         # Absolute 8c floor

            # Time-aware stop profile: "Iron Hands" implementation
            disable_sl = False
            if secs_left > SL_EARLY_PHASE_SECS:
                sl_delta *= SL_LOOSEN_EARLY_MULT
                sl_confirms_needed = SL_CONFIRM_BREACH_EARLY
            elif secs_left > SL_MID_PHASE_SECS:
                sl_delta *= SL_LOOSEN_MID_MULT
                sl_confirms_needed = SL_CONFIRM_BREACH_MID
            elif secs_left < 180: # Less than 3 minutes left
                # "DEATH ZONE": Gamma is massive. Token prices whip wildly.
                # A stop loss here guarantees a terrible fill. We hold to resolution.
                disable_sl = True
                sl_confirms_needed = 999
            elif secs_left < SL_NEAR_EXPIRY_SECS:
                tp_delta *= 0.7
                # Do NOT tighten SL near expiry. Volatility is peaking. Widen it.
                sl_delta *= 1.50
                sl_confirms_needed = SL_CONFIRM_BREACH_LATE
            else:
                sl_confirms_needed = SL_CONFIRM_BREACH_MID

            # Small settling window right after entry.
            entry_age = time.time() - pred.get("entry_time", time.time())
            if entry_age < 180:
                sl_delta *= 1.20 # Give the trade 3 minutes to breathe

            # Cap SL by entry price so low-priced tokens have a reachable stop.
            # Example: if bought at 0.12, do not require a 0.14+ drop (impossible).
            entry_based_sl = -max(SL_ENTRY_REL_MIN_CENTS, bought_price * SL_ENTRY_REL_MAX_LOSS_PCT)
            # Keep at least ~1c theoretical room to avoid below-zero targets.
            reachable_floor = -(max(bought_price - 0.01, 0.0))
            if reachable_floor < 0:
                entry_based_sl = max(entry_based_sl, reachable_floor)
            sl_delta = max(sl_delta, entry_based_sl)

            # Remove the emergency SL variable entirely
            price_delta = current_token_price - bought_price

            if price_delta >= tp_delta:
                should_exit = False
                tp_gate_hold = secs_left > TP_EARLY_EXIT_WINDOW_SECS
                roi_pct_now = ((current_token_price - bought_price) / bought_price) if bought_price > 0 else 0.0
                force_tp = (roi_pct_now >= FORCE_TP_ROI_PCT) or (price_delta >= FORCE_TP_DELTA_ABS)
                async with state_lock:
                    live_pred = active_predictions.get(slug)
                    if live_pred and live_pred.get("status") == "OPEN":
                        live_pred["sl_breach_count"] = 0
                        if tp_gate_hold and not force_tp:
                            if not live_pred.get("tp_gate_logged", False):
                                live_pred["tp_gate_logged"] = True
                                log.info(
                                    f"[TP HOLD] {slug} | TP reached (+{price_delta*100:.1f}c) "
                                    f"but holding until <= {TP_EARLY_EXIT_WINDOW_SECS}s to expiry "
                                    f"(now {int(secs_left)}s)"
                                )
                        else:
                            live_pred["status"] = "CLOSING"
                            should_exit = True
                            live_pred["tp_gate_logged"] = False
                            if tp_gate_hold and force_tp:
                                log.info(
                                    f"[TP FORCE] {slug} | Extreme gain override: "
                                    f"ROI {roi_pct_now*100:.1f}% / Delta +{price_delta*100:.1f}c"
                                )
                if should_exit:
                    log.info(f"[TP HIT] {slug} | Target: +{tp_delta*100:.1f}c | Actual: +{price_delta*100:.1f}c")
                    asyncio.create_task(execute_early_exit(session, slug, f"TAKE_PROFIT (ATR-based: +{price_delta*100:.1f}c)", current_token_price))

            # --- EMERGENCY SL BLOCK HAS BEEN DELETED ---

            elif not disable_sl and price_delta <= sl_delta:
                breach_count = 0
                sl_confirmed = False
                async with state_lock:
                    live_pred = active_predictions.get(slug)
                    if live_pred and live_pred.get("status") == "OPEN":
                        live_pred["sl_breach_count"] = live_pred.get("sl_breach_count", 0) + 1
                        breach_count = live_pred["sl_breach_count"]
                        if breach_count >= sl_confirms_needed:
                            live_pred["status"] = "CLOSING"
                            sl_confirmed = True
                if sl_confirmed:
                    log.info(
                        f"[SL HIT] {slug} | Threshold: {sl_delta*100:.1f}c | Actual: {price_delta*100:.1f}c | "
                        f"Confirmed: {breach_count}/{sl_confirms_needed}"
                    )
                    asyncio.create_task(execute_early_exit(session, slug, f"STOP_LOSS_CONFIRMED ({price_delta*100:.1f}c)", current_token_price))
                elif breach_count > 0:
                    log.info(
                        f"[SL WATCH] {slug} | Breach {breach_count}/{sl_confirms_needed} | "
                        f"Threshold: {sl_delta*100:.1f}c | Actual: {price_delta*100:.1f}c"
                    )
            elif price_delta > (sl_delta + SL_RECOVERY_RESET_BUFFER):
                did_reset = False
                async with state_lock:
                    live_pred = active_predictions.get(slug)
                    if (
                        live_pred
                        and live_pred.get("status") == "OPEN"
                        and live_pred.get("sl_breach_count", 0) > 0
                    ):
                        live_pred["sl_breach_count"] = 0
                        did_reset = True
                if did_reset:
                    log.info(f"[SL WATCH] {slug} | AMM liquidity recovered. Breach counter reset.")
        # -----------------------------------------

        # --- TARGET HUNTING LOGIC ---
        poly_data = await get_polymarket_odds_cached(session, target_slug)
        secs = poly_data.get("seconds_remaining", 0)

        # Increment target market if current one is unavailable or expired
        if not poly_data.get("market_found") or secs <= 0:
            old_slug = target_slug
            target_slug = increment_slug_by_interval(target_slug)
            committed_slugs.discard(old_slug)
            soft_skipped_slugs.discard(old_slug)
            slug_reentry_state.pop(old_slug, None)
            slug_ai_state.pop(old_slug, None)
            fire_and_forget(fetch_price_to_beat_for_market(session, target_slug))
            continue

        async with ai_processing_lock:
            is_processing_ai = (ai_call_in_flight == target_slug)
        
        if target_slug in committed_slugs or is_processing_ai:
            continue

        bal = simulated_balance if PAPER_TRADING else await fetch_live_balance(session)
        can_trade, rm_msg = risk_manager.can_trade(bal, 1.50)
        if not can_trade:
            log.info(f"[RISK] {rm_msg}")
            continue

        should_call, skip_msg, ev_up, ev_down = run_gatekeeper(
            ctx,
            poly_data,
            bal,
            current_candle,
            history_snapshot=history_snapshot,
        )

        if should_call:
            current_best_ev = max(ev_up.get("ev_pct", 0), ev_down.get("ev_pct", 0))
            prev_best_ev = best_ev_seen.get(target_slug, 0)

            if target_slug in soft_skipped_slugs:
                if current_best_ev < prev_best_ev + EV_REENGAGE_DELTA:
                    continue
                else:
                    soft_skipped_slugs.discard(target_slug)
                    log.info(f"[EV MEMORY] Re-engaging {target_slug} - EV improved to {current_best_ev:.2f}%")

            best_ev_seen[target_slug] = max(current_best_ev, prev_best_ev)
        else:
            log.info(f"[GATE] Skipped: {skip_msg}")
            continue

        result = rule_engine_decide(ctx, ev_up, ev_down, poly_data, current_candle)

        if result["decision"] in ["UP", "DOWN"]:
            selected_ev = ev_up if result["decision"] == "UP" else ev_down
            counter_ev = ev_down if result["decision"] == "UP" else ev_up

            reentry_ok, reentry_msg = check_reentry_eligibility(
                target_slug,
                result["decision"],
                selected_ev.get("ev_pct", 0.0)
            )
            if not reentry_ok:
                log.info(f"[REENTRY] Skipped {target_slug}: {reentry_msg}")
                soft_skipped_slugs.add(target_slug)
                continue

            if not result.get("needs_ai"):
                await _commit_decision(target_slug, result, poly_data, selected_ev.get("ev_pct", 0.0), ctx)
            else:
                ai_ok, ai_msg = check_ai_requery_eligibility(target_slug, selected_ev.get("ev_pct", 0.0))
                if not ai_ok:
                    log.info(f"[AI GATE] Skipped {target_slug}: {ai_msg}")
                    soft_skipped_slugs.add(target_slug)
                    continue
                asyncio.create_task(call_local_ai(
                    session, current_candle, history_snapshot,
                    poly_data, selected_ev, counter_ev, 50.0,
                    target_slug, result, ctx
                ))
        else:
            log.info(f"[RULE] Skip: {result.get('reason', '')}")
            soft_skipped_slugs.add(target_slug)

async def prefill_history(session: aiohttp.ClientSession):
    global vwap_cum_pv, vwap_cum_vol, vwap_date, cvd_snapshot_at_candle_open, last_closed_kline_ms
    now = datetime.now(timezone.utc)
    start_time_ms = int(now.replace(hour=0, minute=0, second=0).timestamp() * 1000)

    log.info(f"[SYSTEM] Fetching {MAX_HISTORY} context candles from Binance...")
    try:
        params = {"symbol": "BTCUSDT", "interval": "15m", "limit": MAX_HISTORY}
        async with api_get(session, "https://api.binance.com/api/v3/klines", params=params) as r:
            data = await r.json()
            for k in data:
                candle_time_ms = int(k[0])
                o, h, l, c, v = float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5])

                candle_time_str = datetime.fromtimestamp(candle_time_ms / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

                candle = {"timestamp": candle_time_str, "open": o, "high": h, "low": l, "close": c, "volume": v,
                          "body_size": abs(c - o), "structure": "BULLISH" if c > o else "BEARISH"}
                
                if candle_time_ms >= start_time_ms:
                    vwap_cum_pv += ((h+l+c)/3.0) * v
                    vwap_cum_vol += v

                async with state_lock:
                    candle_history.append(candle)
                    live_ema_9.update(c)
                    live_ema_21.update(c)
                    live_rsi.update(c)
                    last_closed_kline_ms = max(last_closed_kline_ms, candle_time_ms)
                
        log.info(f"[SYSTEM] Loaded {len(candle_history)} candles. VWAP={get_vwap():,.2f}")
        
        update_adaptive_thresholds(candle_history)
        
    except Exception as e:
        log.error(f"Prefill failed: {e}")

async def backfill_missing_klines(session: aiohttp.ClientSession):
    global last_closed_kline_ms
    if last_closed_kline_ms <= 0:
        return
    params = {
        "symbol": "BTCUSDT",
        "interval": "15m",
        "startTime": last_closed_kline_ms + 1,
        "limit": 20,
    }
    try:
        async with api_get(session, "https://api.binance.com/api/v3/klines", params=params, timeout=6) as r:
            if r.status != 200:
                log.debug(f"[KLINE REST] Backfill HTTP {r.status}")
                return
            rows = await r.json()
        if not rows:
            return

        added = 0
        for row in rows:
            candle_time_ms = int(row[0])
            if candle_time_ms <= last_closed_kline_ms:
                continue

            o, h, l, c, v = float(row[1]), float(row[2]), float(row[3]), float(row[4]), float(row[5])
            candle_time_str = datetime.fromtimestamp(candle_time_ms / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            candle = {
                "timestamp": candle_time_str,
                "open": o, "high": h, "low": l, "close": c, "volume": v,
                "body_size": abs(c - o),
                "structure": "BULLISH" if c > o else "BEARISH",
            }
            async with state_lock:
                candle_history.append(candle)
                if len(candle_history) > MAX_HISTORY:
                    candle_history.pop(0)
                update_vwap(candle)
                live_ema_9.update(c)
                live_ema_21.update(c)
                live_rsi.update(c)
                last_closed_kline_ms = max(last_closed_kline_ms, candle_time_ms)
            added += 1

        if added > 0:
            log.info(f"[KLINE REST] Backfilled {added} missing closed candles after reconnect.")
    except Exception as e:
        log.debug(f"[KLINE REST] Backfill failed: {e}")

async def backfill_missing_agg_trades(session: aiohttp.ClientSession):
    async with state_lock:
        last_seen_trade_ms = last_agg_trade_ms
    if last_seen_trade_ms <= 0:
        return
    next_start = last_seen_trade_ms + 1
    total_backfilled = 0

    try:
        for _ in range(3):  # cap catch-up workload per reconnect
            params = {"symbol": "BTCUSDT", "startTime": next_start, "limit": 1000}
            async with api_get(session, "https://api.binance.com/api/v3/aggTrades", params=params, timeout=6) as r:
                if r.status != 200:
                    log.debug(f"[TRADE REST] Backfill HTTP {r.status}")
                    break
                trades = await r.json()
            if not trades:
                break

            max_ts = next_start
            for trade in trades:
                trade_ts = int(trade.get("T", 0) or 0)
                max_ts = max(max_ts, trade_ts)
                if trade_ts < next_start:
                    continue
                await process_agg_trade(trade)
                total_backfilled += 1

            if len(trades) < 1000:
                break
            next_start = max_ts + 1

        if total_backfilled > 0:
            log.info(f"[TRADE REST] Backfilled {total_backfilled} agg trades after reconnect.")
    except Exception as e:
        log.debug(f"[TRADE REST] Backfill failed: {e}")

async def kline_stream_loop(session: aiohttp.ClientSession):
    global live_candle, live_price, last_closed_kline_ms
    while True:
        try:
            async with websockets.connect(SOCKET_KLINE) as ws:
                await backfill_missing_klines(session)
                async for msg in ws:
                    k = json.loads(msg)['k']
                    async with state_lock:
                        live_candle = k
                        if live_price == 0.0:
                            live_price = float(k['c'])
                    if k['x']:
                        closed_price = float(k['c'])
                        async with state_lock:
                            candle_history.append(parse_candle(k, live_price))
                            if len(candle_history) > MAX_HISTORY:
                                candle_history.pop(0)
                            update_vwap(candle_history[-1])
                            live_ema_9.update(closed_price)
                            live_ema_21.update(closed_price)
                            live_rsi.update(closed_price)
                            k_ms = int(k.get('t', 0) or 0)
                            if k_ms > 0:
                                last_closed_kline_ms = max(last_closed_kline_ms, k_ms)
        except Exception as e:
            log.warning(f"[KLINE WS] Disconnected: {e}. Reconnecting...")
            await asyncio.sleep(3)

async def agg_trade_listener(session: aiohttp.ClientSession):
    while True:
        try:
            async with websockets.connect(SOCKET_TRADE) as ws:
                await backfill_missing_agg_trades(session)
                async for msg in ws:
                    await process_agg_trade(json.loads(msg))
        except Exception as e:
            log.warning(f"[TRADE WS] Disconnected: {e}. Reconnecting...")
            await asyncio.sleep(3)

async def main():
    global target_slug, clob_client, simulated_balance
    
    await init_db()
    
    historical_pnl = await get_historical_pnl()
    simulated_balance = PAPER_BALANCE + historical_pnl
    log.info(f"[INIT] Simulated balance initialized: ${simulated_balance:.2f} (base: ${PAPER_BALANCE} + historical: ${historical_pnl:.2f})")

    if not PAPER_TRADING and not POLY_PRIVATE_KEY:
        raise RuntimeError("PAPER_TRADING=false requires POLY_PRIVATE_KEY to be set")
    
    if POLY_PRIVATE_KEY:
        try:
            from py_clob_client.client import ClobClient
            clob_client = ClobClient(host=CLOB_HOST, key=POLY_PRIVATE_KEY, chain_id=CHAIN_ID,
                                     signature_type=POLY_SIG_TYPE, funder=POLY_FUNDER)
            clob_client.set_api_creds(clob_client.create_or_derive_api_creds())
            log.info("[CLOB] Client initialized successfully.")
        except Exception as e:
            log.error(f"CLOB Init Failed: {e}")
            if not PAPER_TRADING:
                raise

    async with aiohttp.ClientSession() as session:
        await prefill_history(session)
        await warmup_ai(session)
        await asyncio.gather(
            kline_stream_loop(session),
            agg_trade_listener(session),
            evaluation_loop(session),
            ml_writer_worker(),
            db_writer_worker(),
        )

if __name__ == "__main__":
    print("\n" + "="*70)
    print("[WARN] CRITICAL: Market Type Selection")
    print("="*70)
    print("\n15-MINUTE MARKETS:")
    print("  - Spread: 98c (you lose 28% instantly)")
    print("  - Depth: $2-10")
    print("  - NOT RECOMMENDED - Market makers avoid these")
    print("\n1-HOUR MARKETS:")
    print("  - Spread: 1-3c")
    print("  - Depth: $500-2000")
    print("  - RECOMMENDED - Real liquidity")
    print("\nDAILY MARKETS:")
    print("  - Spread: 0.5-2c")
    print("  - Depth: $5K-20K")
    print("  - RECOMMENDED - Maximum liquidity")
    print("\n" + "="*70 + "\n")
    
    market_type = input("Choose market type (1h/24h/15m): ").strip().lower()
    
    if market_type == "15m":
        confirm = input("\n[WARN] WARNING: 15m markets have NO liquidity. Continue anyway? (yes/no): ").strip().lower()
        if confirm != "yes":
            print("Switching to 1h markets (recommended)")
            market_type = "1h"
    
    now = int(time.time())
    if market_type == "24h":
        interval = 86400  
        next_market = ((now // interval) + 1) * interval
        slug_template = "btc-updown-24h"
    elif market_type == "15m":
        interval = 900  
        next_market = ((now // interval) + 1) * interval
        slug_template = "btc-updown-15m"
    else:  
        interval = 3600  
        next_market = ((now // interval) + 1) * interval
        slug_template = "btc-updown-1h"
    
    slug_in = input(f"\n[INPUT] Polymarket slug (Enter for auto {market_type}): ").strip()
    target_slug = extract_slug_from_market_url(slug_in) if slug_in else f"{slug_template}-{next_market}"
    
    print(f"\n{'='*70}")
    print(f"[TARGET] TARGET MARKET: {target_slug}")
    print(f"{'='*70}\n")
    print("Verifying liquidity before starting...")
    print("(If you see '98c spread' errors, switch to 1h/24h markets)\n")
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[SYSTEM] Engine stopped.")

