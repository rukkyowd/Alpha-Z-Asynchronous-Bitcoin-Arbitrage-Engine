import asyncio
import aiohttp
import websockets
import json
import logging
import sys
import re
import os
import sqlite3
import time
import math
import io
import csv
from collections import deque
from urllib.parse import urlparse
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from typing import Optional
from decimal import Decimal, ROUND_DOWN

load_dotenv()

# ============================================================
# CONFIGURATION & CONSTANTS
# ============================================================
SOCKET_KLINE    = "wss://stream.binance.com:9443/ws/btcusdt@kline_15m"
SOCKET_TRADE    = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"

LOCAL_AI_URL    = "http://localhost:11434/v1/chat/completions"
LOCAL_AI_MODEL  = "llama3.2:3b-instruct-q4_K_M"

BANKROLL        = 50.00

PAPER_TRADING = os.getenv("PAPER_TRADING", "true").lower() == "true"
PAPER_BALANCE = 50.00

GAMMA_API       = "https://gamma-api.polymarket.com"
CLOB_HOST       = "https://clob.polymarket.com"
CHAIN_ID        = 137

POLY_PRIVATE_KEY = os.getenv("POLY_PRIVATE_KEY", "")
POLY_FUNDER      = os.getenv("POLY_FUNDER", "")
POLY_SIG_TYPE    = int(os.getenv("POLY_SIG_TYPE", "1"))

DRY_RUN          = os.getenv("DRY_RUN", "true").lower() != "false"

# ── Elite Risk & Thresholds ──
MAX_TRADE_PCT               = 0.05
FRACTIONAL_KELLY_DAMPENER   = 0.10
MAX_TRADES_PER_HOUR         = 3

# ── Liquidity & Anti-Chop ──
MAX_SPREAD_PCT              = 0.03
MIN_LIQUIDITY_MULTIPLIER    = 3.0
MIN_ATR_THRESHOLD           = 25.0
EMA_SQUEEZE_PCT             = 0.0007  # Patched: Tighter anti-chop
# ── AI Bypass Threshold ──
EV_AI_BYPASS_THRESHOLD = 40.0  

MIN_EV_PCT_TO_CALL_AI     = 3.0  # Patched: Increased from 1.0 to avoid noise

MIN_SECONDS_REMAINING     = 60   
MAX_SECONDS_FOR_NEW_BET   = 780   

MAX_CROWD_PROB_TO_CALL    = 86.0

EV_REENGAGE_DELTA         = 0.5
CVD_DIVERGENCE_THRESHOLD  = 40000.0  # Patched: Raised from 30k
CVD_CONTRA_VETO_THRESHOLD = 65000.0  # Patched: Raised from 50k
VWAP_OVEREXTEND_PCT       = 0.006
BODY_STRENGTH_MULTIPLIER  = 0.5

EVAL_TICK_SECONDS = 5
MAX_HISTORY     = 120

AI_TIMEOUT_CONNECT  = 5
AI_TIMEOUT_TOTAL    = 30
AI_MAX_RETRIES      = 1
AI_RETRY_DELAY      = 2
AI_MAX_TOKENS       = 120

CB_FAILURE_THRESHOLD = 3
CB_COOLDOWN_SECS     = 60

RESOLVE_POLL_INTERVAL        = 15
RESOLVE_POLL_MAX_TRIES       = 60
RESOLVE_CONFIRMED_THRESHOLD  = 0.95

# ============================================================
# ASYNC ML DATA LOGGER
# ============================================================
ML_FILE = "ai_training_data.csv"

async def log_ml_data(row: dict):
    def _sync_write():
        file_exists = os.path.isfile(ML_FILE)
        try:
            with open(ML_FILE, mode="a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=row.keys())
                if not file_exists:
                    writer.writeheader()
                writer.writerow(row)
        except Exception as e:
            print(f"[ML LOG ERROR] Failed to write data: {e}", file=sys.stderr)
    
    await asyncio.to_thread(_sync_write)

# ============================================================
# ELITE RISK MANAGEMENT ENGINE
# ============================================================
class RiskManager:
    def __init__(self, max_daily_loss_pct=0.15, max_trade_pct=MAX_TRADE_PCT):
        self.max_daily_loss_pct = max_daily_loss_pct
        self.max_trade_pct = max_trade_pct
        self.current_daily_pnl = 0.0
        self.trades_this_hour = 0
        self.current_hour = datetime.now(timezone.utc).hour

    def reset_stats(self):
        self.current_daily_pnl = 0.0
        self.trades_this_hour = 0
        log.info("[RISK] Daily stats reset. New session started.")

    def can_trade(self, current_balance, trade_size):
        dynamic_loss_limit = current_balance * self.max_daily_loss_pct

        if self.current_daily_pnl <= -dynamic_loss_limit:
            return False, f"Daily loss limit (-${dynamic_loss_limit:.2f}) reached."

        if trade_size > (current_balance * self.max_trade_pct):
            return False, f"Trade size ${trade_size:.2f} exceeds max 5% risk."

        now_hour = datetime.now(timezone.utc).hour
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
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT, slug TEXT, decision TEXT, strike REAL,
                    final_price REAL, actual_outcome TEXT, result TEXT,
                    win_rate REAL, pnl_impact REAL, local_calc_outcome TEXT,
                    official_outcome TEXT, match_status TEXT
                )
            """)
    await asyncio.to_thread(_sync_init)

async def get_historical_pnl() -> float:
    def _sync_fetch():
        try:
            with sqlite3.connect(DB_FILE, timeout=5.0) as conn:
                cursor = conn.execute("SELECT SUM(pnl_impact) FROM trades")
                result = cursor.fetchone()[0]
                return float(result) if result else 0.0
        except Exception:
            return 0.0
    return await asyncio.to_thread(_sync_fetch)

async def log_trade_to_db(slug, decision, strike, final_price, actual_outcome, result, win_rate,
                     pnl_impact, local_calc_outcome="", official_outcome="", match_status=""):
    def _sync_write():
        try:
            timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            with sqlite3.connect(DB_FILE, timeout=5.0) as conn:
                conn.execute("""
                    INSERT INTO trades (
                        timestamp, slug, decision, strike, final_price, actual_outcome,
                        result, win_rate, pnl_impact, local_calc_outcome, official_outcome, match_status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    timestamp, slug, decision, strike, final_price, actual_outcome,
                    result, win_rate, pnl_impact, local_calc_outcome, official_outcome, match_status
                ))
        except Exception as e:
            print(f"[DB ERROR] Failed to write trade: {e}", file=sys.stderr)
    await asyncio.to_thread(_sync_write)

# ============================================================
# STATE
# ============================================================
ai_call_count           = 0
ai_consecutive_failures = 0
ai_circuit_open_until   = 0.0

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

ai_call_in_flight: str = ""
strike_price_cache: dict = {}

clob_client = None
live_price: float = 0.0
live_candle: dict = {}

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

# ============================================================
# UTILITIES
# ============================================================
def get_dynamic_threshold(secs_remaining: float) -> tuple[float, float]:
    if secs_remaining <= 120: return float('inf'), float('-inf')
    elif secs_remaining <= 300: return 0.20, -0.25
    else: return 0.15, -0.20

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
    except Exception:
        return -1.0

def increment_slug_by_15m(slug: str) -> str:
    match = re.search(r'(\d+)$', slug)
    if match:
        new_id = int(match.group(1)) + 900
        return re.sub(r'\d+$', str(new_id), slug)
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
    except Exception:
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
        self.gains = deque(maxlen=period)
        self.losses = deque(maxlen=period)
        self.last_price = None

    def update(self, price: float) -> float:
        if self.last_price is None:
            self.last_price = price
            return 50.0

        change = price - self.last_price
        self.gains.append(max(change, 0))
        self.losses.append(max(-change, 0))
        self.last_price = price

        if len(self.gains) < self.period:
            return 50.0

        avg_gain = sum(self.gains) / self.period
        avg_loss = sum(self.losses) / self.period
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    def peek(self, price: float) -> float:
        if self.last_price is None:
            return 50.0

        change = price - self.last_price
        temp_gains = list(self.gains) + [max(change, 0)]
        temp_losses = list(self.losses) + [max(-change, 0)]

        if len(temp_gains) < self.period:
            return 50.0

        avg_gain = sum(temp_gains[-self.period:]) / self.period
        avg_loss = sum(temp_losses[-self.period:]) / self.period
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

live_ema_9  = StreamingEMA(period=9)
live_ema_21 = StreamingEMA(period=21)
live_rsi    = StreamingRSI(period=14)

def get_vwap() -> float:
    return (vwap_cum_pv / vwap_cum_vol) if vwap_cum_vol > 0 else 0.0

def update_vwap(candle: dict):
    global vwap_cum_pv, vwap_cum_vol, vwap_date, cvd_snapshot_at_candle_open
    today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    
    # Patched: Corrected VWAP reset logic to reset anytime the day rolls over
    if vwap_date != today_str:
        vwap_cum_pv, vwap_cum_vol = 0.0, 0.0
        vwap_date = today_str
        log.info(f"[VWAP] Reset for new day: {today_str}")

    typical_price = (candle['high'] + candle['low'] + candle['close']) / 3.0
    vwap_cum_pv  += typical_price * candle['volume']
    vwap_cum_vol += candle['volume']
    cvd_snapshot_at_candle_open = cvd_total

def process_agg_trade(msg: dict):
    global cvd_total, cvd_1min_buffer, last_cvd_1min, live_price
    qty = float(msg['q'])
    is_buyer_maker = msg['m']
    delta = -qty if is_buyer_maker else qty
    cvd_total += delta
    cvd_1min_buffer.append((time.time(), delta))
    live_price = float(msg['p'])

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
    elif abs(ema_short - ema_long) / ema_long < 0.002: 
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

# ============================================================================
# PATCH 2: compute_directional_prob — Sharper Alpha Signal
# ============================================================================
def compute_directional_prob(ctx: dict, strike: float, secs_remaining: float) -> tuple[float, float]:
    price = ctx['price']
    distance = price - strike

    time_fraction = max(secs_remaining / 900.0, 0.01)
    expected_move = ctx['atr'] * math.sqrt(time_fraction)

    if expected_move == 0:
        return 50.0, 50.0

    z_score = distance / expected_move

    def norm_cdf(x: float) -> float:
        return 0.5 * (1.0 + math.erf(x / math.sqrt(2)))

    prob_up_base = norm_cdf(z_score) * 100.0

    # ── RSI Bias: Log-scaled, asymmetric ──
    rsi_deviation = ctx['rsi'] - 50.0
    rsi_bias = math.copysign(
        math.log(1.0 + abs(rsi_deviation) / 10.0) * 5.5,
        rsi_deviation
    )
    rsi_bias = max(-8.5, min(8.5, rsi_bias))

    # ── CVD Bias: Continuous, higher threshold ──
    cvd_delta = ctx['cvd_candle_delta']
    CVD_SCALE_FACTOR = 50_000.0
    cvd_bias = max(-5.0, min(5.0, (cvd_delta / CVD_SCALE_FACTOR) * 5.0))
    if abs(cvd_delta) < 35_000:
        cvd_bias = cvd_bias * (abs(cvd_delta) / 35_000) 

    # ── Candle Structure Bias ──
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

    if price_structure == "BULLISH" and cvd_delta < -CVD_DIVERGENCE_THRESHOLD:
        strength = min(abs(cvd_delta) / 100000.0, 1.0)
        return "BEARISH_DIV", round(strength, 3)

    if price_structure == "BEARISH" and cvd_delta > CVD_DIVERGENCE_THRESHOLD:
        strength = min(abs(cvd_delta) / 100000.0, 1.0)
        return "BULLISH_DIV", round(strength, 3)

    return "NONE", 0.0

# ============================================================
# TIME-ADJUSTED BET SIZING
# ============================================================
def get_time_adjusted_bet(kelly_bet: float, secs_remaining: float) -> float:
    if secs_remaining > 600:
        multiplier = 1.0
    elif secs_remaining > 360:
        multiplier = 0.75
    elif secs_remaining > MIN_SECONDS_REMAINING:
        multiplier = 0.50
    else:
        return 0.0
    return round(kelly_bet * multiplier, 2)

# ============================================================
# DETERMINISTIC AI FILTER
# ============================================================
def deterministic_ai_filter(rule_decision: dict, ctx: dict, current_candle: dict) -> dict:
    favored_dir = rule_decision["decision"]
    veto_reasons = []

    if favored_dir == "UP" and ctx['rsi'] > 78:
        veto_reasons.append(f"RSI Extreme Overbought ({ctx['rsi']:.1f})")
    elif favored_dir == "DOWN" and ctx['rsi'] < 22:
        veto_reasons.append(f"RSI Extreme Oversold ({ctx['rsi']:.1f})")

    if ctx['vwap'] > 0:
        vwap_dist_pct = abs(ctx['vwap_distance']) / ctx['price']
        if vwap_dist_pct > VWAP_OVEREXTEND_PCT:
            if (favored_dir == "UP" and ctx['price'] < ctx['vwap']) or \
               (favored_dir == "DOWN" and ctx['price'] > ctx['vwap']):
                veto_reasons.append(f"VWAP Overextended vs Direction ({vwap_dist_pct*100:.2f}%)")

    CVD_HARD_VETO = 60000.0
    if favored_dir == "UP" and ctx['cvd_candle_delta'] < -CVD_HARD_VETO:
        veto_reasons.append(f"Strong CVD Selling (Δ${ctx['cvd_candle_delta']:,.0f})")
    elif favored_dir == "DOWN" and ctx['cvd_candle_delta'] > CVD_HARD_VETO:
        veto_reasons.append(f"Strong CVD Buying (Δ${ctx['cvd_candle_delta']:,.0f})")

    if veto_reasons:
        log.info(f"[DET_FILTER] Vetoed {favored_dir}: {' | '.join(veto_reasons)}")
        return {**rule_decision, "decision": "SKIP", "bet_size": 0.0,
                "reason": f"DET_FILTER: {' | '.join(veto_reasons)}"}

    return rule_decision

# ============================================================
# KELLY CRITERION & EV MATH
# ============================================================
def compute_ev(true_prob_pct: float, market_prob_pct: float, current_balance: float) -> dict:
    token = market_prob_pct / 100.0
    prob  = true_prob_pct   / 100.0
    
    if not (0.01 < token < 0.99):
        return {"ev_pct": 0.0, "kelly_bet": 0.0}

    net_win = 1.0 - token
    loss = token
    ev = prob * net_win - (1 - prob) * loss
    b = net_win / token
    kelly_fraction = max(0.0, (b * prob - (1 - prob)) / b)

    raw_bet = kelly_fraction * current_balance * FRACTIONAL_KELLY_DAMPENER
    MIN_BET = 1.50
    absolute_ceiling = min(current_balance * risk_manager.max_trade_pct, 5.00)
    
    kelly_bet = 0.0
    if raw_bet >= MIN_BET:
        kelly_bet = round(min(raw_bet, absolute_ceiling), 2)

    return {
        "ev_pct": round((ev / token) * 100, 2),
        "kelly_bet": kelly_bet
    }

# ============================================================
# POLYMARKET CLOB & FETCHERS
# ============================================================
async def fetch_market_meta_from_slug(session: aiohttp.ClientSession, slug: str) -> dict | None:
    try:
        async with session.get(f"{GAMMA_API}/events/slug/{slug}", timeout=5) as r:
            if r.status != 200: return None
            event = await r.json()
            markets = event.get("markets", [])
            active_market = next((m for m in markets if m.get("active") and not m.get("closed")), None)
            if not active_market: return None
            return {"title": active_market.get("question", event.get("title", "")), "market": active_market}
    except: return None

async def fetch_price_to_beat_for_market(session: aiohttp.ClientSession, slug: str) -> float:
    if slug in strike_price_cache: return strike_price_cache[slug]
    meta = await fetch_market_meta_from_slug(session, slug)
    if not meta: return 0.0
    end_time_str = meta["market"].get("endDate", "")
    if not end_time_str: return 0.0
    try:
        end_dt   = datetime.fromisoformat(end_time_str.replace("Z", "+00:00"))
        start_dt = end_dt - timedelta(minutes=15)
        params   = {"symbol": "BTC", "eventStartTime": start_dt.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    "variant": "fiveminute", "endDate": end_dt.strftime('%Y-%m-%dT%H:%M:%SZ')}
        async with session.get("https://polymarket.com/api/crypto/crypto-price", params=params, timeout=5) as r:
            if r.status == 200:
                data = await r.json()
                if data.get("openPrice"):
                    strike = float(data["openPrice"])
                    strike_price_cache[slug] = strike
                    return strike
    except: pass
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
        async with session.get(f"{GAMMA_API}/events", params={"slug": slug}, timeout=6) as r:
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
    except Exception: pass
    return {"market_found": False}

# ============================================================================
# PATCH 1: check_liquidity_and_spread — TWO-STAGE FAST FILTER
# ============================================================================
async def check_liquidity_and_spread(
    token_id: str,
    intended_bet: float,
    session: Optional[aiohttp.ClientSession] = None
) -> tuple[bool, str]:
    if clob_client is None:
        return True, "Paper bypass"

    try:
        spread_data = await asyncio.to_thread(clob_client.get_spread, token_id)
        if spread_data:
            raw_spread = spread_data.get("spread")
            if raw_spread is not None:
                fast_spread = float(raw_spread)
                if fast_spread > MAX_SPREAD_PCT:
                    return False, f"[Stage1] Spread {fast_spread*100:.2f}¢ > max {MAX_SPREAD_PCT*100:.0f}¢"
    except Exception as e:
        log.debug(f"[LIQ] /spread pre-flight unavailable: {e}, falling through to L2")

    mid_price = None
    try:
        price_data = await asyncio.to_thread(clob_client.get_price, token_id, "buy")
        if price_data:
            best_ask_fast = float(price_data.get("price", 0))
            price_data_bid = await asyncio.to_thread(clob_client.get_price, token_id, "sell")
            best_bid_fast = float(price_data_bid.get("price", 0)) if price_data_bid else 0.0
            if best_ask_fast > 0 and best_bid_fast > 0:
                mid_price = (best_ask_fast + best_bid_fast) / 2.0
                fast_spread_2 = best_ask_fast - best_bid_fast
                if fast_spread_2 > MAX_SPREAD_PCT:
                    return False, f"[Stage2] Spread {fast_spread_2*100:.2f}¢ confirmed wide"
    except Exception as e:
        log.debug(f"[LIQ] /price pre-flight unavailable: {e}, falling through to L2")

    try:
        book = await asyncio.to_thread(clob_client.get_order_book, token_id)
        if not book or not book.bids or not book.asks:
            return False, "Empty orderbook"

        best_bid = float(book.bids[0].price)
        best_ask = float(book.asks[0].price)
        if best_bid <= 0 or best_ask <= 0:
            return False, "Invalid TOB prices"

        if mid_price is None:
            mid_price = (best_bid + best_ask) / 2.0

        tob_spread = best_ask - best_bid
        if tob_spread > MAX_SPREAD_PCT:
            return False, f"[Stage3] TOB Spread {tob_spread*100:.2f}¢ too wide"

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

        if remaining_dollars > 0.01:
            return False, f"Insufficient depth: ${remaining_dollars:.2f} unfilled after {levels_consumed} levels"

        if total_shares_bought <= 0:
            return False, "Zero shares calculated"

        avg_exec_price = intended_bet / total_shares_bought
        slippage_from_mid = (avg_exec_price - mid_price) / mid_price
        half_spread_cost = (best_ask - mid_price) / mid_price
        incremental_impact = slippage_from_mid - half_spread_cost

        MAX_TOTAL_SLIPPAGE_FROM_MID = 0.025
        MAX_INCREMENTAL_IMPACT = 0.015

        if slippage_from_mid > MAX_TOTAL_SLIPPAGE_FROM_MID:
            return False, f"Total slippage from mid {slippage_from_mid*100:.2f}% exceeds {MAX_TOTAL_SLIPPAGE_FROM_MID*100:.0f}%"

        if incremental_impact > MAX_INCREMENTAL_IMPACT:
            return False, f"Market impact {incremental_impact*100:.2f}% above half-spread tolerance"

        total_top3_liquidity = sum(float(ask.price) * float(ask.size) for ask in book.asks[:3])
        if total_top3_liquidity < intended_bet * MIN_LIQUIDITY_MULTIPLIER:
            return False, f"Top-3 depth ${total_top3_liquidity:.2f} < {MIN_LIQUIDITY_MULTIPLIER}× bet"

        return True, (f"OK | spread={tob_spread*100:.2f}¢ | impact={slippage_from_mid*100:.2f}% | levels={levels_consumed}")

    except Exception as e:
        return False, f"L2 Check Error: {e}"
    
# ============================================================
# POLYMARKET RESOLUTION
# ============================================================
async def fetch_polymarket_resolution(session: aiohttp.ClientSession, slug: str) -> dict:
    t_start   = time.time()
    snapshots = []
    log.info(f"[POLY-RESOLVE] Watching for official resolution: {slug} (poll every {RESOLVE_POLL_INTERVAL}s)")

    for attempt in range(1, RESOLVE_POLL_MAX_TRIES + 1):
        await asyncio.sleep(RESOLVE_POLL_INTERVAL)
        elapsed = time.time() - t_start
        try:
            async with session.get(f"{GAMMA_API}/events", params={"slug": slug}, timeout=8) as r:
                if r.status != 200: continue
                data = await r.json()

                for event in data:
                    if event.get("slug", "") != slug: continue
                    markets = event.get("markets", [])
                    if not markets: break

                    market    = markets[0]
                    is_closed = market.get("closed", False)
                    is_active = market.get("active", True)
                    raw_prices = json.loads(market.get("outcomePrices", "[]")) if isinstance(market.get("outcomePrices"), str) else market.get("outcomePrices")

                    if not raw_prices or len(raw_prices) < 2: break
                    p_up, p_down = float(raw_prices[0]), float(raw_prices[1])
                    snapshots.append({"poll": attempt, "elapsed_secs": round(elapsed, 1), "p_up": p_up, "p_down": p_down})

                    if p_up >= RESOLVE_CONFIRMED_THRESHOLD:
                        return {"outcome": "UP", "p_up": p_up, "p_down": p_down, "is_closed": is_closed, "poll_count": attempt}
                    if p_down >= RESOLVE_CONFIRMED_THRESHOLD:
                        return {"outcome": "DOWN", "p_up": p_up, "p_down": p_down, "is_closed": is_closed, "poll_count": attempt}
                    if is_closed and not is_active:
                        outcome = "UP" if p_up > p_down else ("DOWN" if p_down > p_up else "TIE")
                        return {"outcome": outcome, "p_up": p_up, "p_down": p_down, "is_closed": True, "poll_count": attempt}
                    break
        except Exception: pass

    last_p_up   = snapshots[-1]["p_up"] if snapshots else 0.5
    last_p_down = snapshots[-1]["p_down"] if snapshots else 0.5
    return {"outcome": "TIMEOUT", "p_up": last_p_up, "p_down": last_p_down, "is_closed": False, "poll_count": len(snapshots)}

async def resolve_market_outcome(session: aiohttp.ClientSession, slug: str, decision: str,
                                  strike: float, local_price_fallback: float, bet_size: float = 1.01,
                                  bought_price: float = 0.0):
    pred = active_predictions.get(slug)
    if not pred or pred.get("status") == "CLOSING": return
    global total_wins, total_losses, simulated_balance

    log.info(f"[RESOLVE] Market expired → {slug}  |  Our call: {decision}  |  Strike: ${strike:,.2f}")
    final_price = local_price_fallback
    local_calc_outcome = "TIE"

    meta = await fetch_market_meta_from_slug(session, slug)
    if meta:
        end_time_str = meta["market"].get("endDate", "")
        if end_time_str:
            for _ in range(6):
                try:
                    end_dt   = datetime.fromisoformat(end_time_str.replace("Z", "+00:00"))
                    start_dt = end_dt - timedelta(minutes=15)
                    params   = {"symbol": "BTC", "eventStartTime": start_dt.strftime('%Y-%m-%dT%H:%M:%SZ'),
                                "variant": "fiveminute", "endDate": end_dt.strftime('%Y-%m-%dT%H:%M:%SZ')}
                    async with session.get("https://polymarket.com/api/crypto/crypto-price", params=params, timeout=5) as r:
                        if r.status == 200:
                            data = await r.json()
                            if "price" in data and data["price"]:
                                final_price = float(data["price"])
                                break
                except Exception: pass
                await asyncio.sleep(10)

    local_calc_outcome = "UP" if final_price > strike else ("DOWN" if final_price < strike else "TIE")
    poly_res         = await fetch_polymarket_resolution(session, slug)
    official_outcome = poly_res["outcome"]

    if official_outcome == "TIMEOUT":
        actual_outcome = local_calc_outcome
        match_status   = "⏱️ POLY TIMED OUT — used local calc"
    elif official_outcome == local_calc_outcome:
        actual_outcome = official_outcome
        match_status   = "✅ MATCH"
    else:
        actual_outcome = official_outcome
        match_status   = f"⚠️ MISMATCH (local={local_calc_outcome} poly={official_outcome})"

    is_dust = bet_size < 1.00
    win_profit = round(bet_size * (1.0 / bought_price - 1.0), 4) if (0 < bought_price < 1) else bet_size

    if actual_outcome == "TIE": result_str = "TIE"; pnl_impact = 0.0
    elif decision == actual_outcome: result_str = "DUST_WIN" if is_dust else "WIN"; pnl_impact = win_profit
    else: result_str = "DUST_LOSS" if is_dust else "LOSS"; pnl_impact = -bet_size

    if PAPER_TRADING: simulated_balance += pnl_impact
    risk_manager.current_daily_pnl += pnl_impact

    if "signals" in pred: signal_tracker.log_resolution(pred["signals"], result_str, pnl_impact)
    if result_str == "WIN": total_wins += 1
    elif result_str == "LOSS": total_losses += 1

    win_rate = (total_wins / max(1, total_wins + total_losses)) * 100
    log.info(f"[STATS] W:{total_wins} L:{total_losses} | WinRate:{win_rate:.2f}% | Daily PnL: ${risk_manager.current_daily_pnl:.2f} | Match: {match_status}")

    await log_trade_to_db(slug, decision, strike, final_price, actual_outcome, result_str, win_rate, pnl_impact,
                    local_calc_outcome=local_calc_outcome, official_outcome=official_outcome, match_status=match_status)

    if "ml_data" in pred:
        ml_row = pred["ml_data"]
        ml_row["outcome_binary"] = 1 if "WIN" in result_str else 0
        ml_row["actual_pnl"] = pnl_impact
        await log_ml_data(ml_row)

    active_predictions.pop(slug, None)

def run_gatekeeper(ctx: dict, poly_data: dict, current_balance: float, current_candle: dict) -> tuple:
    if not poly_data["market_found"]: 
        return False, "No Polymarket data", {}, {}

    seconds_left = poly_data.get("seconds_remaining", 0)
    
    if seconds_left < MIN_SECONDS_REMAINING: 
        return False, f"Too close to expiry ({int(seconds_left)}s < {MIN_SECONDS_REMAINING}s)", {}, {}
    if seconds_left > MAX_SECONDS_FOR_NEW_BET: 
        return False, f"Too early ({int(seconds_left)}s > {MAX_SECONDS_FOR_NEW_BET}s)", {}, {}

    regime = detect_market_regime(candle_history)
    if regime in ["RANGING", "VOLATILE", "UNKNOWN"]:
        return False, f"Market {regime} — skip", {}, {}

    if ctx['atr'] < MIN_ATR_THRESHOLD:
        return False, f"Dead market (ATR {ctx['atr']:.1f} < {MIN_ATR_THRESHOLD})", {}, {}

    ema_spread_pct = abs(ctx['ema_9'] - ctx['ema_21']) / ctx['ema_21']
    if ema_spread_pct < EMA_SQUEEZE_PCT:
        return False, f"EMA Squeeze (spread {ema_spread_pct*100:.3f}%)", {}, {}

    if poly_data["up_prob"] > MAX_CROWD_PROB_TO_CALL or poly_data["down_prob"] > MAX_CROWD_PROB_TO_CALL:
        return False, "Crowd skew too high", {}, {}

    strike = poly_data.get("strike_price", 0.0)
    if strike <= 0:
        return False, "Invalid strike price", {}, {}

    prob_up, prob_down = compute_directional_prob(ctx, strike, seconds_left)
    ev_up   = compute_ev(prob_up, poly_data["up_prob"], current_balance)
    ev_down = compute_ev(prob_down, poly_data["down_prob"], current_balance)

    return True, f"Passed Gate [Regime: {regime}]", ev_up, ev_down

# ============================================================================
# PATCH 3: rule_engine_decide — Directional Vol/Flow Signal
# ============================================================================
def rule_engine_decide(ctx: dict, ev_up: dict, ev_down: dict,
                        poly_data: dict, current_candle: dict) -> dict:
    target_dir = "UP" if ev_up["ev_pct"] > ev_down["ev_pct"] else "DOWN"
    target_ev  = ev_up if target_dir == "UP" else ev_down

    score = 0
    bonus_score = 0
    reasons = []

    if (target_dir == "UP" and ctx['price'] > ctx['vwap']) or \
       (target_dir == "DOWN" and ctx['price'] < ctx['vwap']):
        score += 1
        reasons.append("VWAP Trend")

    if (target_dir == "UP" and ctx['rsi'] > 51) or \
       (target_dir == "DOWN" and ctx['rsi'] < 49):
        score += 1
        reasons.append("RSI Momentum")

    if ctx['current_volume'] > ctx['vol_sma_20'] * 1.2:
        score += 1
        reasons.append("Vol Spike")

    cvd_delta = ctx['cvd_candle_delta']
    CVD_ALIGN_THRESHOLD = 35_000 
    if (target_dir == "UP" and cvd_delta > CVD_ALIGN_THRESHOLD) or \
       (target_dir == "DOWN" and cvd_delta < -CVD_ALIGN_THRESHOLD):
        score += 1
        reasons.append(f"CVD Aligned (Δ${cvd_delta:+,.0f})")

    CVD_CONTRA_THRESHOLD = 60_000
    if (target_dir == "UP" and cvd_delta < -CVD_CONTRA_THRESHOLD) or \
       (target_dir == "DOWN" and cvd_delta > CVD_CONTRA_THRESHOLD):
        score -= 2
        reasons.append(f"CVD_CONTRA (Δ${cvd_delta:+,.0f})")

    cvd_signal, cvd_strength = detect_cvd_divergence(ctx, current_candle)
    if (target_dir == "UP" and cvd_signal == "BEARISH_DIV") or \
       (target_dir == "DOWN" and cvd_signal == "BULLISH_DIV"):
        score -= 2
        reasons.append(f"CVD_DIV_PATTERN (str={cvd_strength:.2f})")

    if ctx['atr'] > 0 and ctx['body_size'] > ctx['atr'] * BODY_STRENGTH_MULTIPLIER:
        bonus_score += 1
        reasons.append("Strong Body")

    if ctx['vwap'] > 0:
        vwap_dist_pct = abs(ctx['vwap_distance']) / ctx['price']
        if 0.001 < vwap_dist_pct < VWAP_OVEREXTEND_PCT:
            bonus_score += 1
            reasons.append("VWAP Distance")

    if ctx.get('ema_9', 0) > 0 and ctx.get('ema_21', 0) > 0:
        ema_aligned = (target_dir == "UP" and ctx['ema_9'] > ctx['ema_21']) or \
                      (target_dir == "DOWN" and ctx['ema_9'] < ctx['ema_21'])
        if ema_aligned:
            bonus_score += 1
            reasons.append("EMA Aligned")

    secs_remaining = poly_data.get("seconds_remaining", 0)

    if score >= 4 or (score >= 3 and bonus_score >= 1):
        raw_bet = target_ev.get("kelly_bet", 0.0)
        bet = get_time_adjusted_bet(raw_bet, secs_remaining)
        if bet > 0:
            return {
                "decision": target_dir, "confidence": "High", "bet_size": bet,
                "score": score, "bonus": bonus_score,
                "reason": " | ".join(reasons), "needs_ai": False
            }

    elif score >= 3 and target_ev.get("ev_pct", 0.0) >= EV_AI_BYPASS_THRESHOLD:
        raw_bet = target_ev.get("kelly_bet", 0.0)
        bet = get_time_adjusted_bet(raw_bet, secs_remaining)
        reasons.append(f"EV_BYPASS (+{target_ev.get('ev_pct', 0.0):.1f}%)")
        if bet > 0:
            log.info(f"[BYPASS] Massive EV detected (+{target_ev.get('ev_pct', 0.0):.1f}%). Overriding AI.")
            return {
                "decision": target_dir, "confidence": "High", "bet_size": bet,
                "score": score, "bonus": bonus_score,
                "reason": " | ".join(reasons), "needs_ai": False
            }

    elif (score >= 3 or (score >= 2 and bonus_score >= 2)) and \
          target_ev.get("ev_pct", 0.0) >= MIN_EV_PCT_TO_CALL_AI:
        raw_bet = target_ev.get("kelly_bet", 0.0) * 0.5
        bet = get_time_adjusted_bet(raw_bet, secs_remaining)
        return {
            "decision": target_dir, "confidence": "Borderline", "bet_size": bet,
            "score": score, "bonus": bonus_score,
            "reason": " | ".join(reasons), "needs_ai": True
        }

    return {
        "decision": "SKIP", "confidence": "Low", "score": score,
        "reason": f"Only {score}/4 core + {bonus_score} bonus. Signals: {' | '.join(reasons) or 'None'}"
    }

# ============================================================
# EXECUTION & AI PIPELINE
# ============================================================
def _commit_decision(slug: str, result: dict, poly_data: dict, current_ev_pct: float = 0.0, ctx: dict = None):
    strike   = poly_data.get("strike_price", 0.0)
    decision = result["decision"]

    if decision in ["UP", "DOWN"] and result.get("bet_size", 0) >= 1.50:
        committed_slugs.add(slug)
        soft_skipped_slugs.discard(slug)
        best_ev_seen.pop(slug, None)

        bought_price = poly_data["up_prob"] / 100.0 if decision == "UP" else poly_data["down_prob"] / 100.0
        token_id = poly_data.get("token_id_up", "") if decision == "UP" else poly_data.get("token_id_down", "")

        ml_data = {
            "timestamp": datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            "market_slug": slug, 
            "direction": decision,
            "price_vs_vwap_pct": round((ctx['price'] - ctx['vwap']) / ctx['vwap'] * 100, 4) if ctx else 0,
            "rsi_14": round(ctx['rsi'], 1) if ctx else 50, 
            "atr_14": round(ctx['atr'], 2) if ctx else 0,
            "cvd_candle_delta": round(ctx['cvd_candle_delta'], 0) if ctx else 0,
            "ev_pct": round(current_ev_pct, 2),
            "rule_score": result.get("score", 0),
            "bonus_score": result.get("bonus", 0),
            "trigger_reason": result.get("reason", "UNKNOWN")
        }

        active_predictions[slug] = {
            "decision": decision, "strike": strike, "score": result.get("score", 0),
            "bet_size": result.get("bet_size", 0.0), "bought_price": bought_price,
            "token_id": token_id, "status": "OPEN", "entry_time": time.time(),
            "signals": result.get("reason", "").split(" | "), "ml_data": ml_data
        }
        log.info(f"DECISION LOCKED: {decision} | Score: {result.get('score','?')}/4 | Bonus: {result.get('bonus',0)} | Bet: ${result.get('bet_size',0.0):.2f}")
        asyncio.create_task(place_bet(slug, decision, result.get("bet_size", 0.0), poly_data))
    else:
        soft_skipped_slugs.add(slug)

# ============================================================================
# PATCH 4: place_bet — Pipelined Execution
# ============================================================================
async def place_bet(slug: str, decision: str, bet_size: float, poly_data: dict):
    global clob_client, simulated_balance
    token_id = poly_data.get("token_id_up", "") if decision == "UP" else poly_data.get("token_id_down", "")

    liq_ok, liq_msg = await check_liquidity_and_spread(token_id, bet_size)
    if not liq_ok:
        log.warning(f"[REJECTED] {slug}: {liq_msg}")
        active_predictions.pop(slug, None)
        committed_slugs.discard(slug)
        return

    if slug in active_predictions:
        active_predictions[slug]["ml_data"]["spread_eval"] = liq_msg

    risk_manager.trades_this_hour += 1
    market_prob = poly_data["up_prob"] if decision == "UP" else poly_data["down_prob"]

    log.info(f"🎯 BET PLACED [{'PAPER' if PAPER_TRADING else 'LIVE'}] {decision} on {slug} | Bet: ${bet_size:.2f} | Liq: {liq_msg}")

    if not PAPER_TRADING and not DRY_RUN and clob_client:
        def _sign_and_submit():
            from py_clob_client.clob_types import MarketOrderArgs, OrderType
            from py_clob_client.order_builder.constants import BUY
            shares_to_buy = round(bet_size / (market_prob / 100.0), 2)
            order_args = MarketOrderArgs(token_id=token_id, amount=shares_to_buy, side=BUY)
            signed = clob_client.create_market_order(order_args)
            return clob_client.post_order(signed, OrderType.FAK)

        try:
            resp = await asyncio.wait_for(asyncio.to_thread(_sign_and_submit), timeout=4.0)

            status = resp.get("status", "")
            if status == "matched":
                log.info(f"✅ ORDER MATCHED: {decision} on {slug} | ${bet_size:.2f}")
            elif "insufficient" in (resp.get("errorMsg", "") or "").lower():
                log.error(f"💸 INSUFFICIENT FUNDS for {slug}. Check USDC balance.")
                active_predictions.pop(slug, None)
                committed_slugs.discard(slug)
            else:
                log.warning(f"⚠️ CLOB Rejected [{status}]: {resp.get('errorMsg', 'unknown')} | {slug}")
                active_predictions.pop(slug, None)
                committed_slugs.discard(slug)

        except asyncio.TimeoutError:
            log.error(f"⏱️ CLOB TIMEOUT (>4s) for {slug}. Order may or may not have been placed.")
            if slug in active_predictions:
                active_predictions[slug]["status"] = "UNCERTAIN"

        except Exception as e:
            log.error(f"✗ CLOB execution failed: {e}")
            active_predictions.pop(slug, None)
            committed_slugs.discard(slug)

# ============================================================================
# PATCH 5: execute_early_exit — Robust IOC with Retry
# ============================================================================
async def execute_early_exit(session: aiohttp.ClientSession, slug: str, exit_reason: str, current_token_price: float):
    global simulated_balance, total_wins, total_losses

    pred = active_predictions.get(slug)
    if not pred or pred.get("status") != "OPEN":
        return
    
    pred["status"] = "CLOSING"

    bet_size = pred["bet_size"]
    bought_price = pred["bought_price"]
    shares_owned = bet_size / bought_price if bought_price > 0 else 0.0

    if shares_owned < 0.01:
        active_predictions.pop(slug, None)
        return

    if "TAKE_PROFIT" in exit_reason:
        hold_value_per_share = 1.0  
        exit_value_per_share = current_token_price
        capture_ratio = exit_value_per_share / hold_value_per_share
        if capture_ratio < 0.80:
            log.info(f"[EXIT GUARD] {slug}: Exit capture only {capture_ratio*100:.1f}%. Holding to resolution.")
            pred["status"] = "OPEN"
            return

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

        log.info(f"[EARLY EXIT] ⚡ {slug} | Reason: {exit_reason} | PnL: ${pnl_impact:+.4f}")

        if "ml_data" in pred:
            ml_row = pred["ml_data"]
            ml_row["outcome_binary"] = 1 if result_str == "WIN" else 0
            ml_row["actual_pnl"] = pnl_impact
            await log_ml_data(ml_row)

        await log_trade_to_db(
            slug, pred["decision"], pred["strike"], live_price, "EARLY_EXIT",
            result_str, (total_wins / max(1, total_wins + total_losses) * 100),
            pnl_impact, local_calc_outcome=exit_reason, official_outcome="SOLD"
        )
        active_predictions.pop(slug, None)

    elif not DRY_RUN and clob_client:
        def _parse_shares_sold(resp: dict, expected_shares: float) -> float:
            if resp.get("status") == "matched":
                return expected_shares
            if "matchedAmount" in resp:
                val = float(resp["matchedAmount"])
                if val > 0: return val
            if "transactions" in resp:
                total = sum(float(tx.get("size", 0)) for tx in resp["transactions"])
                if total > 0: return total
            if "takerAmount" in resp:
                val = float(resp.get("takerAmount", 0))
                if val > 0: return val
            return 0.0

        async def _attempt_ioc_sell(floor_price: float) -> tuple[bool, float]:
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
                shares_sold = _parse_shares_sold(resp, shares_owned)
                return shares_sold > 0, shares_sold
            except asyncio.TimeoutError:
                log.warning(f"[EXIT] IOC timed out for {slug}")
                return False, 0.0

        floor_price_1 = current_token_price * 0.98
        success, shares_sold = await _attempt_ioc_sell(floor_price_1)

        if not success:
            log.warning(f"[EXIT] IOC attempt 1 failed for {slug}. Retrying at wider floor...")
            await asyncio.sleep(0.5) 
            floor_price_2 = current_token_price * 0.96
            success, shares_sold = await _attempt_ioc_sell(floor_price_2)

        if shares_sold > 0:
            fraction_sold = min(shares_sold / shares_owned, 1.0)
            realized_bet_size = bet_size * fraction_sold
            pnl_impact = realized_bet_size * ((current_token_price / bought_price) - 1.0)
            risk_manager.current_daily_pnl += pnl_impact

            log.info(f"✅ IOC EXIT: {slug} | Sold {fraction_sold*100:.1f}% | Realized PnL: ${pnl_impact:+.2f} | Reason: {exit_reason}")

            if "ml_data" in pred:
                ml_row = pred["ml_data"]
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
                active_predictions.pop(slug, None)
            else:
                pred["bet_size"] = remaining_shares * bought_price
                pred["status"] = "OPEN"
                log.info(f"[EXIT] Partial fill. {remaining_shares:.3f} shares remain open.")
        else:
            log.error(f"⛔ IOC FAILED after 2 attempts for {slug}. Will hold to resolution.")
            pred["status"] = "OPEN"

async def call_local_ai(session: aiohttp.ClientSession, current_candle: dict, history: list,
                         poly_data: dict, ev: dict, counter_ev: dict, math_prob: float,
                         slug: str, rule_decision: dict, ctx: dict):
    global ai_call_count, ai_consecutive_failures, ai_circuit_open_until, ai_call_in_flight

    pre_filtered = deterministic_ai_filter(rule_decision, ctx, current_candle)
    if pre_filtered["decision"] == "SKIP":
        _commit_decision(slug, pre_filtered, poly_data, ev.get("ev_pct", 0.0), ctx)
        return

    if time.time() < ai_circuit_open_until:
        rule_decision["needs_ai"] = False
        _commit_decision(slug, rule_decision, poly_data, ev.get("ev_pct", 0.0), ctx)
        return

    ai_call_in_flight = slug
    ai_call_count += 1
    log.info(f"[AI CONFIRM] Borderline score — asking {LOCAL_AI_MODEL} (call #{ai_call_count})...")

    favored_dir = rule_decision["decision"]
    regime = detect_market_regime(candle_history)

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

    prompt = (
        f"You are a quantitative trading assistant. Evaluate this 15-minute BTC/USDT Polymarket position.\n\n"
        f"PROPOSED DIRECTION: {favored_dir}\n"
        f"TIME TO EXPIRY: {int(poly_data.get('seconds_remaining', 0))}s\n"
        f"MARKET REGIME: {regime}\n\n"
        f"TECHNICALS:\n"
        f"  Momentum (RSI): {rsi_desc}\n"
        f"  Trend (EMA): {trend_desc}\n"
        f"  VWAP Status: {vwap_desc}\n"
        f"  Order Flow (CVD): {cvd_desc}\n\n"
        f"EDGE:\n"
        f"  Expected Value: {ev.get('ev_pct', 0.0):+.2f}%\n"
        f"  System Score: {rule_decision.get('score', 0)}/4 + {rule_decision.get('bonus', 0)} bonus\n\n"
        f"Based on the technicals supporting the proposed direction, respond with ONLY one word: '{favored_dir}' to confirm or 'SKIP' to reject."
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

    for attempt in range(1, AI_MAX_RETRIES + 1):
        try:
            async with session.post(LOCAL_AI_URL, json=payload,
                                    timeout=aiohttp.ClientTimeout(total=AI_TIMEOUT_TOTAL)) as r:
                r.raise_for_status()
                raw_response = (await r.json())['choices'][0]['message']['content'].strip()
                ai_word = favored_dir if favored_dir in raw_response.upper() else \
                          ("SKIP" if "SKIP" in raw_response.upper() else None)
                ai_consecutive_failures = 0
                break
        except Exception as e:
            ai_consecutive_failures += 1
            log.warning(f"[AI] Attempt {attempt} failed: {e}")
            if attempt < AI_MAX_RETRIES:
                await asyncio.sleep(AI_RETRY_DELAY)

    if ai_consecutive_failures >= CB_FAILURE_THRESHOLD:
        ai_circuit_open_until = time.time() + CB_COOLDOWN_SECS
        log.error(f"[CIRCUIT] ⚡ Tripped. AI paused {CB_COOLDOWN_SECS}s.")

    if ai_word == favored_dir:
        final = {**rule_decision, "reason": f"AI confirmed: {rule_decision['reason']}"}
    else:
        final = {**rule_decision, "decision": "SKIP", "bet_size": 0.0,
                 "reason": "AI vetoed borderline signal"}

    last_ai_interaction["response"] = ai_word or "FAILED"
    _commit_decision(slug, final, poly_data, ev.get("ev_pct", 0.0), ctx)
    ai_call_in_flight = ""

# ============================================================================
# PATCH 6: evaluation_loop — Race Condition Fix
# ============================================================================
async def evaluation_loop(session: aiohttp.ClientSession):
    global target_slug, ai_call_in_flight

    while True:
        await asyncio.sleep(EVAL_TICK_SECONDS)
        if not candle_history or not live_candle:
            continue

        current_price = live_price if live_price > 0 else float(live_candle.get('c', 0))
        k = live_candle
        current_candle = {
            "timestamp": datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            "open":   float(k.get('o', current_price)),
            "high":   float(k.get('h', current_price)),
            "low":    float(k.get('l', current_price)),
            "close":  current_price,
            "volume": float(k.get('v', 0)),
            "body_size": abs(current_price - float(k.get('o', current_price))),
            "structure": "BULLISH" if current_price >= float(k.get('o', current_price)) else "BEARISH"
        }

        ctx = build_technical_context(current_candle, candle_history)
        poly_data = await get_polymarket_odds_cached(session, target_slug)
        secs = poly_data.get("seconds_remaining", 0)

        if not poly_data.get("market_found") or secs <= 0:
            if target_slug in active_predictions and active_predictions[target_slug].get("status") != "CLOSING":
                pred = active_predictions[target_slug]
                pred["status"] = "RESOLVING"
                asyncio.create_task(resolve_market_outcome(
                    session, target_slug, pred["decision"], pred["strike"],
                    current_price, pred.get("bet_size", 1.01), pred.get("bought_price", 0.0)
                ))

            old_slug = target_slug
            target_slug = increment_slug_by_15m(target_slug)
            committed_slugs.discard(old_slug)
            soft_skipped_slugs.discard(old_slug)
            asyncio.create_task(fetch_price_to_beat_for_market(session, target_slug))
            continue

        if target_slug in active_predictions:
            pred = active_predictions[target_slug]
            pred_status = pred.get("status")

            if pred_status in ("CLOSING", "RESOLVING", "UNCERTAIN"):
                continue

            if pred_status == "OPEN":
                current_token_price = (
                    poly_data["up_prob"] / 100.0 if pred["decision"] == "UP"
                    else poly_data["down_prob"] / 100.0
                )
                tp_threshold, sl_threshold = get_dynamic_threshold(secs)
                roi_pct = (current_token_price / pred["bought_price"]) - 1.0 \
                    if pred["bought_price"] > 0 else 0

                if roi_pct >= tp_threshold:
                    pred["status"] = "CLOSING" 
                    asyncio.create_task(
                        execute_early_exit(session, target_slug,
                                           f"TAKE_PROFIT ({roi_pct*100:.1f}%)",
                                           current_token_price)
                    )
                    continue 

                elif roi_pct <= sl_threshold:
                    pred["status"] = "CLOSING" 
                    asyncio.create_task(
                        execute_early_exit(session, target_slug,
                                           f"STOP_LOSS ({roi_pct*100:.1f}%)",
                                           current_token_price)
                    )
                    continue

        if target_slug in committed_slugs or ai_call_in_flight == target_slug:
            continue

        bal = simulated_balance if PAPER_TRADING else await fetch_live_balance(session)
        can_trade, rm_msg = risk_manager.can_trade(bal, 1.50)
        if not can_trade:
            log.info(f"[RISK] {rm_msg}")
            continue

        should_call, skip_msg, ev_up, ev_down = run_gatekeeper(ctx, poly_data, bal, current_candle)

        if should_call:
            current_best_ev = max(ev_up.get("ev_pct", 0), ev_down.get("ev_pct", 0))
            prev_best_ev = best_ev_seen.get(target_slug, 0)

            if target_slug in soft_skipped_slugs:
                if current_best_ev < prev_best_ev + EV_REENGAGE_DELTA:
                    continue
                else:
                    soft_skipped_slugs.discard(target_slug)
                    log.info(f"[EV MEMORY] Re-engaging {target_slug} — EV improved to {current_best_ev:.2f}%")

            best_ev_seen[target_slug] = max(current_best_ev, prev_best_ev)
        else:
            log.info(f"[GATE] Skipped: {skip_msg}")
            continue

        result = rule_engine_decide(ctx, ev_up, ev_down, poly_data, current_candle)

        if result["decision"] in ["UP", "DOWN"]:
            if not result.get("needs_ai"):
                _commit_decision(target_slug, result, poly_data, ev_up.get("ev_pct", 0.0), ctx)
            else:
                asyncio.create_task(call_local_ai(
                    session, current_candle, candle_history,
                    poly_data, ev_up, ev_down, 50.0,
                    target_slug, result, ctx
                ))
        else:
            log.info(f"[RULE] Skip: {result.get('reason', '')}")
            soft_skipped_slugs.add(target_slug)

async def prefill_history(session: aiohttp.ClientSession):
    global vwap_cum_pv, vwap_cum_vol, vwap_date, cvd_snapshot_at_candle_open
    now = datetime.now(timezone.utc)
    start_time_ms = int(now.replace(hour=0, minute=0, second=0).timestamp() * 1000)

    log.info(f"[SYSTEM] Fetching {MAX_HISTORY} context candles from Binance...")
    try:
        params = {"symbol": "BTCUSDT", "interval": "15m", "limit": MAX_HISTORY}
        async with session.get("https://api.binance.com/api/v3/klines", params=params) as r:
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

                candle_history.append(candle)
                live_ema_9.update(c)
                live_ema_21.update(c)
                live_rsi.update(c)
                
        log.info(f"[SYSTEM] Loaded {len(candle_history)} candles. VWAP={get_vwap():,.2f}")
    except Exception as e:
        log.error(f"Prefill failed: {e}")
        
async def kline_stream_loop():
    global live_candle, live_price
    while True:
        try:
            async with websockets.connect(SOCKET_KLINE) as ws:
                async for msg in ws:
                    k = json.loads(msg)['k']
                    live_candle = k
                    if live_price == 0.0: live_price = float(k['c'])
                    if k['x']:
                        candle_history.append(parse_candle(k, live_price))
                        if len(candle_history) > MAX_HISTORY: candle_history.pop(0)
                        update_vwap(candle_history[-1])
                        closed_price = float(k['c'])
                        live_ema_9.update(closed_price)
                        live_ema_21.update(closed_price)
                        live_rsi.update(closed_price)
        except Exception as e:
            log.warning(f"[KLINE WS] Disconnected: {e}. Reconnecting...")
            await asyncio.sleep(3)

async def agg_trade_listener():
    while True:
        try:
            async with websockets.connect(SOCKET_TRADE) as ws:
                async for msg in ws: process_agg_trade(json.loads(msg))
        except Exception as e:
            log.warning(f"[TRADE WS] Disconnected: {e}. Reconnecting...")
            await asyncio.sleep(3)

async def main():
    global target_slug, clob_client, simulated_balance
    
    await init_db()
    
    historical_pnl = await get_historical_pnl()
    simulated_balance = PAPER_BALANCE + historical_pnl
    log.info(f"[INIT] Simulated balance initialized: ${simulated_balance:.2f} (base: ${PAPER_BALANCE} + historical: ${historical_pnl:.2f})")
    
    if POLY_PRIVATE_KEY:
        try:
            from py_clob_client.client import ClobClient
            clob_client = ClobClient(host=CLOB_HOST, key=POLY_PRIVATE_KEY, chain_id=CHAIN_ID,
                                     signature_type=POLY_SIG_TYPE, funder=POLY_FUNDER)
            clob_client.set_api_creds(clob_client.create_or_derive_api_creds())
            log.info("[CLOB] Client initialized successfully.")
        except Exception as e:
            log.error(f"CLOB Init Failed: {e}")

    async with aiohttp.ClientSession() as session:
        await prefill_history(session)
        await warmup_ai(session)
        await asyncio.gather(
            kline_stream_loop(),
            agg_trade_listener(),
            evaluation_loop(session)
        )

if __name__ == "__main__":
    slug_in = input("[INPUT] Polymarket BTC 15m slug (Enter for auto): ").strip()
    target_slug = extract_slug_from_market_url(slug_in) if slug_in else \
                  f"btc-updown-15m-{((int(time.time()) // 900) + 1) * 900}"
    print(f"[AUTO] Target: {target_slug}")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[SYSTEM] Engine stopped.")