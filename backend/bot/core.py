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
from collections import deque
from urllib.parse import urlparse
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# CONFIG
# ============================================================
SOCKET_KLINE    = "wss://stream.binance.com:9443/ws/btcusdt@kline_1m"
SOCKET_TRADE    = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"

LOCAL_AI_URL    = "http://localhost:11434/v1/chat/completions"
LOCAL_AI_MODEL  = "mistral-nemo"   # 12B — best local option for structured quant inference

BANKROLL        = 10000.00

PAPER_TRADING = os.getenv("PAPER_TRADING", "true").lower() == "true"
PAPER_BALANCE = 100.00

GAMMA_API       = "https://gamma-api.polymarket.com"
CLOB_HOST       = "https://clob.polymarket.com"
CHAIN_ID        = 137

POLY_PRIVATE_KEY = os.getenv("POLY_PRIVATE_KEY", "")
POLY_FUNDER      = os.getenv("POLY_FUNDER", "")
POLY_SIG_TYPE    = int(os.getenv("POLY_SIG_TYPE", "1"))

DRY_RUN          = os.getenv("DRY_RUN", "true").lower() != "false"

# ── Thresholds ──
MIN_EV_PCT_TO_CALL_AI     = 0.1
MIN_SECONDS_REMAINING     = 45
MAX_SECONDS_FOR_NEW_BET   = 298
MIN_BODY_SIZE             = 0.001
MAX_CROWD_PROB_TO_CALL    = 97.0

EVAL_TICK_SECONDS = 5
MAX_HISTORY     = 120
VWAP_RESET_HOUR = 0

AI_TIMEOUT_CONNECT  = 5
AI_TIMEOUT_TOTAL    = 30
AI_MAX_RETRIES      = 2
AI_RETRY_DELAY      = 2
AI_MAX_TOKENS       = 120

CB_FAILURE_THRESHOLD = 3
CB_COOLDOWN_SECS     = 60

# ── Resolution polling config ──
RESOLVE_POLL_INTERVAL        = 15    # seconds between Polymarket resolution polls
RESOLVE_POLL_MAX_TRIES       = 60    # max attempts (~10 min total)
RESOLVE_CONFIRMED_THRESHOLD  = 0.95  # token price considered "resolved"

# ============================================================
# RISK MANAGEMENT ENGINE
# ============================================================
class RiskManager:
    def __init__(self, max_daily_loss_pct=0.10, max_trade_pct=0.20):
        # Default to a 10% daily drawdown limit instead of a flat $5.00
        self.max_daily_loss_pct = max_daily_loss_pct
        self.max_trade_pct = max_trade_pct
        self.current_daily_pnl = 0.0

    def reset_stats(self):
        self.current_daily_pnl = 0.0
        log.info("[RISK] Daily stats reset. New session started.")

    def can_trade(self, current_balance, trade_size):
        # Dynamically calculate the maximum allowed loss in dollars
        dynamic_loss_limit = current_balance * self.max_daily_loss_pct
        
        # Check if current losses exceed the dynamic limit
        if self.current_daily_pnl <= -dynamic_loss_limit:
            return False, f"Daily loss limit reached (PnL: ${self.current_daily_pnl:.2f} / Limit: -${dynamic_loss_limit:.2f})"
            
        if trade_size > (current_balance * self.max_trade_pct):
            return False, f"Trade size ${trade_size:.2f} exceeds max risk pct."
            
        return True, "Approved"

# ============================================================
# LOGGING & LIVE STREAMING (FIXED)
# ============================================================
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_file_handler = logging.FileHandler("trading_log.txt", encoding="utf-8")
_file_handler.setFormatter(_fmt)
_stream_handler = logging.StreamHandler(io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True))
_stream_handler.setFormatter(_fmt)

# 1. THE SHARED BUFFER
recent_logs = deque(maxlen=50)

class DequeHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            recent_logs.append(msg) #
        except Exception:
            self.handleError(record)

_deque_handler = DequeHandler()
_deque_handler.setFormatter(_fmt)

# 2. THE BOT LOGGER
log = logging.getLogger("alpha_z_engine")
log.setLevel(logging.INFO) 
log.addHandler(_stream_handler)
log.addHandler(_deque_handler)
log.propagate = False 

# 3. THE FAIL-SAFE LOGGING FUNCTION
def ui_log(msg: str, level: str = "info"):
    """
    Manually pushing to the deque is safer if the 
    logger is hijacked by Uvicorn.
    """
    timestamp = datetime.now().strftime('%H:%M:%S')
    formatted_msg = f"[{timestamp}] [{level.upper()}] {msg}"
    
    # CRITICAL: Always push to the same deque the WebSocket reads
    recent_logs.append(formatted_msg) #
    
    # Also log to file/console
    if level == "info": log.info(msg)
    elif level == "warning": log.warning(msg)
    elif level == "error": log.error(msg)

# Initialize with a message so the UI isn't blank
ui_log("Quant Engine Initialized. Monitoring WebSocket...", "info")

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
risk_manager = RiskManager(max_daily_loss_pct=0.10, max_trade_pct=0.30)
simulated_balance = PAPER_BALANCE
last_seconds_remaining: int = 0

committed_slugs: set = set()
soft_skipped_slugs: set = set()
best_ev_seen: dict = {}

ai_call_in_flight: str = ""
market_open_prices: dict = {}
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

# ============================================================
# SQLITE STATS TRACKING (Replaces CSV)
# ============================================================
DB_FILE = "alpha_z_history.db"

def init_db():
    """Initializes the database and enables WAL mode for concurrent reads/writes."""
    with sqlite3.connect(DB_FILE) as conn:
        # Enable Write-Ahead Logging for high-performance concurrency
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
                match_status TEXT
            )
        """)

# Call this once when the module loads
init_db()

def log_trade_to_db(slug, decision, strike, final_price, actual_outcome, result, win_rate, 
                     pnl_impact, local_calc_outcome="", official_outcome="", match_status=""):
    """Inserts a resolved trade into the SQLite database."""
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
        log.error(f"[DB ERROR] Failed to write trade: {e}")

# ============================================================
# UTILITIES
# ============================================================
def get_dynamic_threshold(secs_remaining: float) -> tuple[float, float]:
    """
    Returns the required (take_profit_pct, stop_loss_pct) for an early exit.
    Closer to expiration = tighter thresholds since time value is gone.
    FIX: Previous 20-30% TP thresholds were never reachable — tightened to realistic levels.
    """
    if secs_remaining <= 45:
        # Final 45s: Order book is toxic/thin. Let the binary outcome ride.
        return float('inf'), float('-inf')
    
    elif secs_remaining <= 90:
        # 45s - 90s: Late stage — take 12% gain, cut at -15%
        return 0.12, -0.15
        
    elif secs_remaining <= 180:
        # 1.5m - 3m: Mid stage — take 9% gain, cut at -12%
        return 0.09, -0.12
        
    else:
        # > 3m left: Early stage — take 8% gain, cut at -12%
        return 0.08, -0.12

def _parse_seconds_remaining(end_date_str: str) -> float:
    if not end_date_str: return -1.0
    try:
        end_dt = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
        return (end_dt - datetime.now(timezone.utc)).total_seconds()
    except Exception:
        return -1.0

def increment_slug_by_300(slug: str) -> str:
    match = re.search(r'(\d+)$', slug)
    if match:
        new_id = int(match.group(1)) + 300
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
        slug = path.split("/event/", 1)[-1].strip("/")
    else:
        slug = cleaned.strip("/")
    return slug.lower()

def build_market_family_prefix(seed_slug: str) -> str:
    if not seed_slug: return ""
    known_prefix = re.match(r"^(btc-(?:updown|up-or-down)(?:-[0-9]+m?)?)", seed_slug)
    if known_prefix: return known_prefix.group(1)
    parts = seed_slug.split("-")
    return "-".join(parts[:4]) if len(parts) >= 4 else seed_slug

# ============================================================
# BALANCE HELPER
# ============================================================
async def fetch_live_balance(session: aiohttp.ClientSession) -> float:
    if clob_client is None:
        return BANKROLL
    try:
        from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
        params = BalanceAllowanceParams(
            signature_type=POLY_SIG_TYPE,
            asset_type=AssetType.COLLATERAL
        )
        resp = await asyncio.to_thread(clob_client.get_balance_allowance, params=params)
        raw_balance = int(resp.get("balance", 0))
        fetched = raw_balance / 1_000_000
        if fetched > 0:
            log.info(f"[BALANCE] Live Polymarket balance: ${fetched:.2f}")
            return fetched
        log.info(f"[BALANCE] Live Polymarket balance: ${fetched:.2f} (empty)")
        return fetched
    except Exception as e:
        log.warning(f"[BALANCE] get_balance_allowance failed: {e}")
    log.warning(f"[BALANCE] Could not fetch live balance. Using fallback ${BANKROLL}.")
    return BANKROLL

# ============================================================
# VWAP ENGINE
# ============================================================
def update_vwap(candle: dict):
    global vwap_cum_pv, vwap_cum_vol, vwap_date
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    if today != vwap_date:
        vwap_cum_pv  = 0.0
        vwap_cum_vol = 0.0
        vwap_date    = today
        risk_manager.reset_stats()
        log.info(f"[VWAP] Session reset for {today}")
    typical_price  = (candle['high'] + candle['low'] + candle['close']) / 3.0
    vwap_cum_pv  += typical_price * candle['volume']
    vwap_cum_vol += candle['volume']

def get_vwap() -> float:
    if vwap_cum_vol == 0: return 0.0
    return vwap_cum_pv / vwap_cum_vol

# ============================================================
# EMA ENGINE
# ============================================================
def calculate_ema(closes: list[float], period: int) -> float:
    if len(closes) < period:
        return sum(closes) / len(closes) if closes else 0.0
    k = 2.0 / (period + 1)
    ema = sum(closes[:period]) / period
    for price in closes[period:]:
        ema = price * k + ema * (1 - k)
    return ema

# ============================================================
# VOLATILITY MATH
# ============================================================
def calculate_strike_probability(current_price: float, strike_price: float,
                                  history: list, seconds_remaining: float, direction: str) -> float:
    if seconds_remaining <= 0 or not history or strike_price <= 0: return 50.0
    minutes_remaining = seconds_remaining / 60.0
    closes = [c['close'] for c in history] + [current_price]
    if len(closes) < 3: return 50.0
    log_returns = [math.log(closes[i] / closes[i-1]) for i in range(1, len(closes)) if closes[i-1] > 0]
    if not log_returns: return 50.0
    mean_ret = sum(log_returns) / len(log_returns)
    variance = sum((r - mean_ret) ** 2 for r in log_returns) / len(log_returns)
    std_dev  = math.sqrt(variance) if variance > 0 else 1e-6
    time_scaled_vol = std_dev * math.sqrt(minutes_remaining)
    if strike_price <= 0: return 50.0
    log_dist = math.log(current_price / strike_price)
    z_score  = log_dist / time_scaled_vol if time_scaled_vol > 1e-9 else 0.0
    prob = 0.5 * (1.0 + math.erf(z_score / math.sqrt(2.0)))
    if direction == "DOWN": prob = 1.0 - prob
    return max(1.0, min(prob * 100.0, 99.0))

# ============================================================
# CVD ENGINE
# ============================================================
def process_agg_trade(trade: dict):
    global cvd_total, last_cvd_1min, live_price
    price   = float(trade['p'])
    qty     = float(trade['q'])
    ts_ms   = int(trade['T'])
    is_sell = trade['m']
    delta   = -qty if is_sell else qty
    cvd_total += delta * price
    cvd_1min_buffer.append((ts_ms, delta * price))
    cutoff_ms = ts_ms - 60_000
    while cvd_1min_buffer and cvd_1min_buffer[0][0] < cutoff_ms:
        cvd_1min_buffer.popleft()
    last_cvd_1min = sum(v for _, v in cvd_1min_buffer)
    live_price = price

def get_cvd_candle_delta() -> float:
    return cvd_total - cvd_snapshot_at_candle_open

# ============================================================
# KELLY / EV MATH
# ============================================================
def compute_ev(true_prob_pct: float, market_prob_pct: float, current_balance: float, is_high_conviction: bool = False) -> dict:
    token = market_prob_pct / 100.0
    prob  = true_prob_pct   / 100.0
    if not (0 < token < 1):
        return {"ev": 0.0, "ev_pct": 0.0, "edge": 0.0, "kelly_fraction": 0.0, "kelly_bet": 0.0}
    
    net_win = 1.0 - token
    ev      = prob * net_win - (1 - prob) * token
    ev_pct  = (ev / token) * 100
    b       = net_win / token
    
    # Kelly Formula: f* = (bp - q) / b
    kelly_fraction = max(0.0, (b * prob - (1 - prob)) / b)
    
    # DYNAMIC MULTIPLIER: 10% for normal, 20% for high conviction
    # This keeps you further from the "ruin" threshold of full Kelly
    multiplier = 0.20 if is_high_conviction else 0.10
    raw_kelly_bet = kelly_fraction * current_balance * multiplier
    
    # Use the RiskManager's defined ceiling (max_trade_pct = 0.30)
    # No artificial floor — if Kelly says < $0, the edge isn't real; skip it.
    absolute_ceiling = current_balance * risk_manager.max_trade_pct
    dynamic_bet      = min(raw_kelly_bet, absolute_ceiling)  # FIX: removed max(1.01, ...) floor
    
    return {
        "ev": round(ev, 4), 
        "ev_pct": round(ev_pct, 2), 
        "edge": round(prob - token, 4),
        "kelly_fraction": round(kelly_fraction, 4), 
        "kelly_bet": round(dynamic_bet, 2)
    }
# ============================================================
# TECHNICAL CONTEXT
# ============================================================
def build_technical_context(current_candle: dict, history: list) -> dict:
    direction = current_candle['structure']
    body      = current_candle['body_size']
    upper     = current_candle['upper_wick']
    lower     = current_candle['lower_wick']

    vol_delta_pct  = 0.0
    vol_vs_avg_pct = 0.0
    recent_5       = history[-5:] if len(history) >= 5 else history

    if history:
        pv = history[-1]['volume']
        if pv > 0: vol_delta_pct = ((current_candle['volume'] - pv) / pv) * 100
        av = sum(c['volume'] for c in recent_5) / max(len(recent_5), 1)
        if av > 0: vol_vs_avg_pct = ((current_candle['volume'] - av) / av) * 100

    streak = 0
    for c in reversed(recent_5):
        if c['structure'] == direction: streak += 1
        else: break
    bull_n = sum(1 for c in recent_5 if c['structure'] == "BULLISH")
    bear_n = len(recent_5) - bull_n

    if streak == len(recent_5) and streak > 0: streak_text = f"{streak} consecutive {direction} candles"
    elif streak >= 2:                          streak_text = f"{streak}-candle {direction} streak"
    else:                                      streak_text = f"Mixed ({bull_n} bull / {bear_n} bear in last {len(recent_5)})"

    if body > 0:
        if upper > body*1.5 and lower < body*0.5:   wick_bias = "Strong SELL pressure (upper wick rejection)"
        elif lower > body*1.5 and upper < body*0.5: wick_bias = "Strong BUY pressure (lower wick rejection)"
        elif upper > body and lower > body:          wick_bias = "Indecision (both wicks prominent)"
        else:                                        wick_bias = "Clean body -- no strong wick rejection"
    else: wick_bias = "Doji / near-doji"

    vwap          = get_vwap()
    current_close = current_candle['close']
    closes_all    = [c['close'] for c in history] + [current_close]

    ema_9  = calculate_ema(closes_all, 9)
    ema_21 = calculate_ema(closes_all, 21)
    ema_50 = calculate_ema(closes_all, 50)

    vwap_distance = current_close - vwap if vwap > 0 else 0.0
    vwap_signal   = "ABOVE VWAP (bullish bias)" if vwap_distance > 0 else "BELOW VWAP (bearish bias)"
    ema_cross     = "9-EMA ABOVE 21-EMA (bullish)" if ema_9 > ema_21 else "9-EMA BELOW 21-EMA (bearish)"
    rsi           = calculate_rsi(closes_all, 14)

    cvd_candle_delta = get_cvd_candle_delta()
    cvd_1m           = last_cvd_1min

    if cvd_candle_delta > 5000:    cvd_signal = f"STRONG BUY FLOW (CVD +${cvd_candle_delta:,.0f} this candle)"
    elif cvd_candle_delta > 1000:  cvd_signal = f"Moderate buy flow (CVD +${cvd_candle_delta:,.0f})"
    elif cvd_candle_delta < -5000: cvd_signal = f"STRONG SELL FLOW (CVD ${cvd_candle_delta:,.0f} this candle)"
    elif cvd_candle_delta < -1000: cvd_signal = f"Moderate sell flow (CVD ${cvd_candle_delta:,.0f})"
    else:                          cvd_signal = f"Neutral flow (CVD ${cvd_candle_delta:,.0f})"

    price_direction_up = current_close > history[-1]['close'] if history else True
    price_move         = abs(current_close - history[-1]['close']) if history else 0
    # FIX: use 0.05% of current price as threshold — $30 was noise-level at BTC's current price
    divergence_price_threshold = current_close * 0.0005
    cvd_divergence     = ""
    if price_direction_up and price_move > divergence_price_threshold and cvd_candle_delta < -3000:
        cvd_divergence = "⚠️  BEARISH DIVERGENCE: Price rising but heavy SELL flow"
    elif not price_direction_up and price_move > divergence_price_threshold and cvd_candle_delta > 3000:
        cvd_divergence = "⚠️  BULLISH DIVERGENCE: Price falling but heavy BUY flow"

    return {
        "direction": direction, "vol_delta_pct": vol_delta_pct, "vol_vs_avg_pct": vol_vs_avg_pct,
        "streak_text": streak_text, "wick_bias": wick_bias, "vwap": vwap,
        "vwap_distance": vwap_distance, "vwap_signal": vwap_signal, "ema_9": ema_9,
        "ema_21": ema_21, "ema_50": ema_50, "ema_cross": ema_cross, "rsi": rsi,
        "cvd_signal": cvd_signal, "cvd_divergence": cvd_divergence, "cvd_1min": cvd_1m,
        "cvd_candle_delta": cvd_candle_delta,
    }

def calculate_rsi(closes: list[float], period: int = 14) -> float:
    if len(closes) < period + 1: return 50.0
    gains, losses = [], []
    for i in range(1, len(closes)):
        delta = closes[i] - closes[i-1]
        gains.append(max(delta, 0))
        losses.append(max(-delta, 0))
    relevant_gains  = gains[-(period*3):]
    relevant_losses = losses[-(period*3):]
    avg_gain = sum(relevant_gains[:period]) / period
    avg_loss = sum(relevant_losses[:period]) / period
    for g, l in zip(relevant_gains[period:], relevant_losses[period:]):
        avg_gain = (avg_gain * (period - 1) + g) / period
        avg_loss = (avg_loss * (period - 1) + l) / period
    if avg_loss == 0: return 100.0
    rs  = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    if math.isnan(rsi): return 50.0
    return round(rsi, 1)

# ============================================================
# API FETCHERS
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
        start_dt = end_dt - timedelta(minutes=5)
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
    if not slug: return {"market_found": False, "error": "No slug"}
    try:
        async with session.get("https://gamma-api.polymarket.com/events", params={"slug": slug}, timeout=6) as r:
            r.raise_for_status()
            data = await r.json()
            for event in data:
                if event.get("slug", "") != slug: continue
                markets   = event.get("markets", [])
                if not markets: return {"market_found": False, "error": "No markets"}
                market    = markets[0]
                prices    = json.loads(market.get("outcomePrices", "[]")) if isinstance(market.get("outcomePrices"), str) else market.get("outcomePrices")
                token_ids = json.loads(market.get("clobTokenIds", "[]")) if isinstance(market.get("clobTokenIds"), str) else market.get("clobTokenIds")
                strike_price = await fetch_price_to_beat_for_market(session, slug)
                return {
                    "market_found": True, "title": event.get("title", slug),
                    "up_prob": float(prices[0]) * 100, "down_prob": float(prices[1]) * 100,
                    "strike_price": strike_price, "seconds_remaining": _parse_seconds_remaining(event.get("endDate", "")),
                    "token_id_up":   token_ids[0] if len(token_ids) > 0 else "",
                    "token_id_down": token_ids[1] if len(token_ids) > 1 else "",
                }
    except Exception as e: return {"market_found": False, "error": str(e)}
    return {"market_found": False, "error": "Not found"}

async def check_market_liquidity(session: aiohttp.ClientSession, token_id: str, bet_size: float) -> tuple[bool, str]:
    """Queries the Polymarket CLOB to ensure tight spread and sufficient depth."""
    if not token_id:
        return False, "No token ID available"
        
    try:
        async with session.get(f"{CLOB_HOST}/book", params={"token_id": token_id}, timeout=3) as r:
            if r.status != 200:
                return False, f"CLOB HTTP {r.status}"
            data = await r.json()

            bids = data.get("bids", [])
            asks = data.get("asks", [])

            if not bids or not asks:
                return False, "Order book is empty"

            best_bid = float(bids[0]["price"])
            best_ask = float(asks[0]["price"])
            spread = best_ask - best_bid

            # 1. Spread Check: Max 4 cents slippage
            if spread > 0.04:
                return False, f"Spread too wide ({spread*100:.1f}¢) - Bid: {best_bid}, Ask: {best_ask}"

            # 2. Depth Check: Top 3 ask levels must absorb 2x our bet size
            available_liquidity = sum(float(ask["size"]) * float(ask["price"]) for ask in asks[:3])
            
            if available_liquidity < (bet_size * 2):
                return False, f"Thin book (Avail: ${available_liquidity:.0f}, Need: ${bet_size*2:.0f})"

            return True, f"Liquid (Spread: {spread*100:.1f}¢, Depth: ${available_liquidity:.0f})"
            
    except Exception as e:
        return False, f"Liquidity API error: {e}"

# ============================================================
# POLYMARKET OFFICIAL RESOLUTION FETCHER
#
# Dedicated poller that watches the Gamma API until one token
# reaches >= RESOLVE_CONFIRMED_THRESHOLD (0.99) or the market
# shows closed=True with a decisive price split.
#
# Returns a structured dict so the caller can do a full
# side-by-side comparison against the local price calculation.
# ============================================================
async def fetch_polymarket_resolution(session: aiohttp.ClientSession, slug: str) -> dict:
    """
    Poll Polymarket until the market officially resolves.

    Returns:
        outcome       : "UP" | "DOWN" | "TIE" | "TIMEOUT"
        p_up          : float  – final UP token price  (0-1 scale)
        p_down        : float  – final DOWN token price (0-1 scale)
        is_closed     : bool
        poll_count    : int    – how many polls were needed
        elapsed_secs  : float
        snapshots     : list[dict] – every poll's raw prices for audit trail
    """
    t_start   = time.time()
    snapshots = []

    log.info(
        f"[POLY-RESOLVE] Watching for official resolution: {slug}  "
        f"(poll every {RESOLVE_POLL_INTERVAL}s, up to {RESOLVE_POLL_MAX_TRIES} tries)"
    )

    for attempt in range(1, RESOLVE_POLL_MAX_TRIES + 1):
        await asyncio.sleep(RESOLVE_POLL_INTERVAL)
        elapsed = time.time() - t_start

        try:
            async with session.get(
                f"{GAMMA_API}/events", params={"slug": slug}, timeout=8
            ) as r:
                if r.status != 200:
                    log.warning(f"[POLY-RESOLVE] Poll {attempt:02d}: HTTP {r.status}")
                    continue

                data = await r.json()

                for event in data:
                    if event.get("slug", "") != slug:
                        continue

                    markets = event.get("markets", [])
                    if not markets:
                        break

                    market    = markets[0]
                    is_closed = market.get("closed", False)
                    is_active = market.get("active", True)

                    raw_prices = market.get("outcomePrices", "[]")
                    if isinstance(raw_prices, str):
                        raw_prices = json.loads(raw_prices)

                    if not raw_prices or len(raw_prices) < 2:
                        break

                    p_up   = float(raw_prices[0])
                    p_down = float(raw_prices[1])

                    # Record snapshot for audit trail
                    snapshots.append({
                        "poll": attempt, "elapsed_secs": round(elapsed, 1),
                        "p_up": p_up, "p_down": p_down,
                        "is_closed": is_closed, "is_active": is_active,
                    })

                    log.info(
                        f"[POLY-RESOLVE] Poll {attempt:02d} (+{elapsed:.0f}s)  "
                        f"UP={p_up:.4f}  DOWN={p_down:.4f}  "
                        f"closed={is_closed}  active={is_active}"
                    )

                    # ── Condition 1: token resolves to 1.0 (canonical resolution) ──
                    if p_up >= RESOLVE_CONFIRMED_THRESHOLD:
                        log.info(f"[POLY-RESOLVE] ✅ RESOLVED → UP  (p_up={p_up:.4f})")
                        return {
                            "outcome": "UP", "p_up": p_up, "p_down": p_down,
                            "is_closed": is_closed, "poll_count": attempt,
                            "elapsed_secs": round(elapsed, 1), "snapshots": snapshots,
                        }
                    if p_down >= RESOLVE_CONFIRMED_THRESHOLD:
                        log.info(f"[POLY-RESOLVE] ✅ RESOLVED → DOWN  (p_down={p_down:.4f})")
                        return {
                            "outcome": "DOWN", "p_up": p_up, "p_down": p_down,
                            "is_closed": is_closed, "poll_count": attempt,
                            "elapsed_secs": round(elapsed, 1), "snapshots": snapshots,
                        }

                    # ── Condition 2: market is fully closed, use dominant price ──
                    if is_closed and not is_active:
                        if p_up > p_down:
                            outcome = "UP"
                        elif p_down > p_up:
                            outcome = "DOWN"
                        else:
                            outcome = "TIE"
                        log.info(
                            f"[POLY-RESOLVE] ✅ MARKET CLOSED → {outcome}  "
                            f"(p_up={p_up:.4f}  p_down={p_down:.4f})"
                        )
                        return {
                            "outcome": outcome, "p_up": p_up, "p_down": p_down,
                            "is_closed": True, "poll_count": attempt,
                            "elapsed_secs": round(elapsed, 1), "snapshots": snapshots,
                        }

                    # Still live — keep polling
                    break

        except asyncio.TimeoutError:
            log.warning(f"[POLY-RESOLVE] Poll {attempt:02d}: request timed out")
        except Exception as e:
            log.warning(f"[POLY-RESOLVE] Poll {attempt:02d}: {type(e).__name__}: {e}")

    # Ran out of attempts
    elapsed      = time.time() - t_start
    last_p_up    = snapshots[-1]["p_up"]   if snapshots else 0.5
    last_p_down  = snapshots[-1]["p_down"] if snapshots else 0.5
    log.warning(
        f"[POLY-RESOLVE] ⏱️  TIMEOUT after {len(snapshots)} polls ({elapsed:.0f}s). "
        f"Last seen: UP={last_p_up:.4f}  DOWN={last_p_down:.4f}"
    )
    return {
        "outcome": "TIMEOUT", "p_up": last_p_up, "p_down": last_p_down,
        "is_closed": False, "poll_count": len(snapshots),
        "elapsed_secs": round(elapsed, 1), "snapshots": snapshots,
    }

# ============================================================
# RESOLUTION LOGIC
# ============================================================
async def resolve_market_outcome(session: aiohttp.ClientSession, slug: str, decision: str,
                                  strike: float, local_price_fallback: float, bet_size: float = 1.01,
                                  bought_price: float = 0.0):
    pred = active_predictions.get(slug)
    if not pred:
        log.warning(f"[RESOLVE] {slug} not in active_predictions — already cleaned up")
        return
    if pred.get("status") == "CLOSING":
        log.warning(f"[RESOLVE] {slug} already being handled (status={pred.get('status')}) — skipping")
        return
                                  
    global total_wins, total_losses, simulated_balance

    log.info(
        f"[RESOLVE] Market expired → {slug}  |  Our call: {decision}  |  Strike: ${strike:,.2f}"
    )

    # ==================================================================
    # STEP 1 — LOCAL PRICE (Polymarket's own crypto-price API / Binance)
    # ==================================================================
    final_price        = local_price_fallback
    local_calc_outcome = "TIE"

    meta = await fetch_market_meta_from_slug(session, slug)
    if meta:
        end_time_str = meta["market"].get("endDate", "")
        if end_time_str:
            for _ in range(6):
                try:
                    end_dt   = datetime.fromisoformat(end_time_str.replace("Z", "+00:00"))
                    start_dt = end_dt - timedelta(minutes=5)
                    params   = {
                        "symbol":         "BTC",
                        "eventStartTime": start_dt.strftime('%Y-%m-%dT%H:%M:%SZ'),
                        "variant":        "fiveminute",
                        "endDate":        end_dt.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    }
                    async with session.get(
                        "https://polymarket.com/api/crypto/crypto-price",
                        params=params, timeout=5
                    ) as r:
                        if r.status == 200:
                            data = await r.json()
                            if "price" in data and data["price"]:
                                final_price = float(data["price"])
                                log.info(
                                    f"[RESOLVE] crypto-price API → BTC final: ${final_price:,.2f}"
                                )
                                break
                except Exception:
                    pass
                await asyncio.sleep(10)

    if final_price > strike:
        local_calc_outcome = "UP"
    elif final_price < strike:
        local_calc_outcome = "DOWN"

    log.info(
        f"[RESOLVE] Local calc: final=${final_price:,.2f}  strike=${strike:,.2f}  "
        f"→ {local_calc_outcome}"
    )

    # ==================================================================
    # STEP 2 — OFFICIAL POLYMARKET RESOLUTION (dedicated token-price poller)
    # ==================================================================
    poly_res         = await fetch_polymarket_resolution(session, slug)
    official_outcome = poly_res["outcome"]
    poly_p_up        = poly_res["p_up"]
    poly_p_down      = poly_res["p_down"]
    poly_polls       = poly_res["poll_count"]
    poly_elapsed     = poly_res["elapsed_secs"]
    poly_closed      = poly_res["is_closed"]

    # ==================================================================
    # STEP 3 — COMPARE: what we calculated vs what Polymarket actually paid
    # ==================================================================
    if official_outcome == "TIMEOUT":
        actual_outcome = local_calc_outcome
        match_status   = "⏱️  POLY TIMED OUT — used local calc as fallback"
        log.warning(
            f"[RESOLVE] Polymarket did not resolve in time. "
            f"Falling back to local calc: {local_calc_outcome}"
        )
    elif official_outcome == local_calc_outcome:
        actual_outcome = official_outcome
        match_status   = "✅ MATCH"
    else:
        # Mismatch — Polymarket is the actual payout authority, trust it
        actual_outcome = official_outcome
        match_status   = (
            f"⚠️  MISMATCH  "
            f"(local={local_calc_outcome}  poly={official_outcome})"
        )
        log.warning(
            f"[RESOLVE] *** RESOLUTION MISMATCH ***  "
            f"Local says {local_calc_outcome}, Polymarket says {official_outcome}.  "
            f"Trusting Polymarket.  "
            f"final_price=${final_price:,.2f}  strike=${strike:,.2f}  "
            f"p_up={poly_p_up:.4f}  p_down={poly_p_down:.4f}"
        )

    # ==================================================================
    # STEP 4 — WIN / LOSS ACCOUNTING (UPDATED FOR DUST)
    # ==================================================================
    # Detect if this is a fractional remainder (dust) from an IOC partial fill.
    # Since the minimum initial bet is 1.01, anything < 1.00 is a partial bag.
    is_dust = bet_size < 1.00

    if bought_price > 0 and bought_price < 1:
        win_profit = round(bet_size * (1.0 / bought_price - 1.0), 4)
    else:
        win_profit = bet_size  # fallback: 1:1

    if actual_outcome == "TIE":
        result_str = "TIE"
        pnl_impact = 0.0
    elif decision == actual_outcome:
        result_str = "DUST_WIN" if is_dust else "WIN"
        pnl_impact = win_profit
    else:
        result_str = "DUST_LOSS" if is_dust else "LOSS"
        pnl_impact = -bet_size

    if PAPER_TRADING:
        simulated_balance += pnl_impact

    risk_manager.current_daily_pnl += pnl_impact

    # CRITICAL: Protect the Win Rate! 
    # Only increment the win/loss counters for primary trades, not dust remnants.
    if result_str == "WIN":    total_wins   += 1
    elif result_str == "LOSS": total_losses += 1

    total_trades = total_wins + total_losses
    win_rate     = (total_wins / total_trades) * 100 if total_trades > 0 else 0.0
    bar_filled   = int(win_rate / 5)
    win_bar      = "█" * bar_filled + "░" * (20 - bar_filled)

    # ── Result line ──
    if is_dust:
        outcome_line = f"  🧹  DUST SETTLED  {'+' if pnl_impact > 0 else '-'}${abs(pnl_impact):.4f} (fractional bag)"
    elif result_str == "WIN":
        outcome_line = f"  ✅  WIN   +${pnl_impact:.4f}  (bought @ {bought_price:.4f} → payout ${bet_size / bought_price:.4f})"
    elif result_str == "LOSS":
        outcome_line = f"  ❌  LOSS  -${bet_size:.2f}  (bought @ {bought_price:.4f}, stake lost)"
    else:
        outcome_line = f"  ➖  TIE   $0.00"

    balance_line = f"  Balance     : ${simulated_balance:.2f}" if PAPER_TRADING else ""

    # ── Token price arrows (show which token "won") ──
    up_marker   = " ◀ WINNER" if poly_p_up   >= RESOLVE_CONFIRMED_THRESHOLD else ""
    down_marker = " ◀ WINNER" if poly_p_down >= RESOLVE_CONFIRMED_THRESHOLD else ""

    print("\n" + "═" * 64)
    print(f"  🏁  MARKET RESOLVED  [{slug}]")
    print("═" * 64)
    print(f"{outcome_line}")
    print(f"  Our call    : {decision}  →  Actual outcome: {actual_outcome}")
    print(f"  Strike      : ${strike:,.2f}  |  Final (Binance API): ${final_price:,.2f}")
    print(f"  ─────────────────────────────────────────────────────────")
    print(f"  LOCAL CALC  : {local_calc_outcome:4s}  "
          f"(${final_price:,.2f} {'>' if final_price > strike else '<' if final_price < strike else '='} ${strike:,.2f})")
    print(f"  POLY OFFICIAL: {official_outcome:7s}  "
          f"(UP token={poly_p_up:.4f}{up_marker}  DOWN token={poly_p_down:.4f}{down_marker})")
    print(f"  Poly status  : closed={poly_closed}  |  "
          f"polls taken={poly_polls}  |  wait={poly_elapsed:.0f}s")
    print(f"  Comparison   : {match_status}")
    if balance_line:
        print(balance_line)
    print(f"  Daily PnL    : ${risk_manager.current_daily_pnl:+.2f}")
    print(f"  ─────────────────────────────────────────────────────────")
    print(f"  Record       : {total_wins}W / {total_losses}L  ({win_rate:.1f}% win rate)")
    print(f"  Win Rate     : [{win_bar}] {win_rate:.1f}%")
    print("═" * 64 + "\n")

    log.info(
        f"[STATS] W:{total_wins} L:{total_losses} | WinRate:{win_rate:.2f}% | "
        f"Daily PnL: ${risk_manager.current_daily_pnl:.2f} | Match: {match_status}"
    )

    log_trade_to_db(
        slug, decision, strike, final_price, actual_outcome,
        result_str, win_rate,
        pnl_impact,
        local_calc_outcome=local_calc_outcome,
        official_outcome=official_outcome,
        match_status=match_status,
    )
    active_predictions.pop(slug, None)
    log.info(f"[RESOLVE] {slug} closed and removed from active predictions.")

# ============================================================
# ENGINE LOGIC
# ============================================================
def run_gatekeeper(current_candle: dict, history: list, poly_data: dict, current_balance: float, ctx: dict, is_high_conviction: bool = False) -> tuple:
    if not poly_data["market_found"]: return False, "No Polymarket data", {}, {}, 0.0
    
    seconds_left = poly_data.get("seconds_remaining", 0)
    if seconds_left < MIN_SECONDS_REMAINING: return False, f"Only {int(seconds_left)}s left", {}, {}, 0.0
    if seconds_left > MAX_SECONDS_FOR_NEW_BET: return False, f"{int(seconds_left)}s remaining -- too early", {}, {}, 0.0
    if current_candle['body_size'] < MIN_BODY_SIZE and ctx['cvd_candle_delta'] == 0:
        return False, "Dead market (no body & no volume)", {}, {}, 0.0

    # FIX: Add momentum body filter — require candle body > 30% of avg candle range
    # to avoid entering on indecisive micro-moves that have no real directional momentum.
    recent_5 = history[-5:] if len(history) >= 5 else history
    avg_candle_range = sum((c['high'] - c['low']) for c in recent_5) / max(len(recent_5), 1) if recent_5 else 10.0
    if current_candle['body_size'] < avg_candle_range * 0.30 and not is_high_conviction:
        return False, f"Weak candle body (${current_candle['body_size']:.2f} < 30% of avg range ${avg_candle_range*0.30:.2f})", {}, {}, 0.0

    # FIX: Unify favored_dir — always use strike-relative price, not candle structure.
    # Previously gatekeeper used candle structure but rule_engine used price-vs-strike,
    # which caused EV to be computed for the wrong direction on contradicting signals.
    strike = poly_data.get("strike_price", 0.0)
    current_price = current_candle['close']
    if strike > 0:
        favored_dir = "UP" if current_price > strike else "DOWN"
    else:
        # No strike available — fall back to candle structure
        favored_dir = "UP" if current_candle['structure'] == "BULLISH" else "DOWN"

    if favored_dir == "UP":
        market_prob, counter_prob = poly_data["up_prob"], poly_data["down_prob"]
    else:
        market_prob, counter_prob = poly_data["down_prob"], poly_data["up_prob"]

    if market_prob > MAX_CROWD_PROB_TO_CALL: return False, f"Crowd already at {market_prob:.1f}%", {}, {}, 0.0

    math_prob = calculate_strike_probability(current_price, strike, history, seconds_left, favored_dir)
    
    # One-shot EV calculation with the correct conviction multiplier
    ev         = compute_ev(math_prob, market_prob, current_balance, is_high_conviction=is_high_conviction)
    counter_ev = compute_ev(100-math_prob, counter_prob, current_balance, is_high_conviction=is_high_conviction)
    
    best_ev_pct = max(ev["ev_pct"], counter_ev["ev_pct"])

    if best_ev_pct < MIN_EV_PCT_TO_CALL_AI: 
        return False, f"Math EV {best_ev_pct:+.2f}% < threshold", ev, counter_ev, math_prob
        
    return True, "", ev, counter_ev, math_prob

def rule_engine_decide(current_candle: dict, history: list, poly_data: dict, ev: dict, math_prob: float, current_balance: float = 100.0) -> dict:
    ctx = build_technical_context(current_candle, history)
    current_price = current_candle['close']
    strike = poly_data.get("strike_price", 0.0)

    # --- DYNAMIC STRIKE BUFFER ---
    # Calculate average High-Low spread of the last 5 minutes to measure current volatility
    recent_5 = history[-5:] if len(history) >= 5 else history
    avg_candle_size = sum((c['high'] - c['low']) for c in recent_5) / max(len(recent_5), 1) if recent_5 else 10.0
    
    # Set the buffer to 60% of the average candle size (capped between $5 and $25)
    dynamic_buffer = max(5.0, min(avg_candle_size * 0.60, 25.0))

    if strike > 0 and abs(current_price - strike) < dynamic_buffer:
        return {"decision": "SKIP", "confidence": "Low", "bet_size": 0.0,
                "score": 0, "reason": f"Price inside noise zone (${abs(current_price-strike):.2f} < buffer ${dynamic_buffer:.2f})", "needs_ai": False}
    # -----------------------------

    score = 0
    signal_log = []

    # 1. Determine the bias
    if strike > 0: 
        favored_dir = "UP" if current_price > strike else "DOWN"
    else: 
        favored_dir = "UP" if current_candle['structure'] == "BULLISH" else "DOWN"

    # 2. STANDARD INDICATORS (+1 / -1)
    # VWAP Check
    if (favored_dir == "UP" and current_price > ctx["vwap"]) or (favored_dir == "DOWN" and current_price < ctx["vwap"]):
        score += 1; signal_log.append(f"VWAP Aligned (+1)")
    else:
        score -= 1; signal_log.append(f"VWAP Opposed (-1)")

    # EMA Trend
    if (favored_dir == "UP" and ctx["ema_9"] > ctx["ema_21"]) or (favored_dir == "DOWN" and ctx["ema_9"] < ctx["ema_21"]):
        score += 1; signal_log.append(f"EMA Trend Aligned (+1)")

    # 3. HIGH-WEIGHT SIGNAL: CVD DIVERGENCE (+2)
    div_text = ctx.get("cvd_divergence", "")
    if "BULLISH DIVERGENCE" in div_text and favored_dir == "UP":
        score += 2
        signal_log.append("🎯 STRONG BULLISH DIVERGENCE (+2)")
    elif "BEARISH DIVERGENCE" in div_text and favored_dir == "DOWN":
        score += 2
        signal_log.append("🎯 STRONG BEARISH DIVERGENCE (+2)")
    elif div_text:
        # If there is a divergence opposing our trade, we penalize heavily
        score -= 2
        signal_log.append("⚠️ OPPOSING DIVERGENCE DETECTED (-2)")

    # 4. Math Edge
    market_prob = poly_data["up_prob"] if favored_dir == "UP" else poly_data["down_prob"]
    edge = math_prob - market_prob
    if edge > 5: 
        score += 1; signal_log.append(f"Strong Math Edge (+1)")

    # 5. FINAL DECISION
    if score >= 3: # Boosted threshold because of higher weights
        bet = ev.get("kelly_bet", 0.0)
        if bet <= 0:
            return {"decision": "SKIP", "confidence": "Low", "bet_size": 0.0,
                    "score": score, "reason": "Kelly returned 0 — no real edge", "needs_ai": False}
        return {"decision": favored_dir, "confidence": "High", "bet_size": bet,
                "score": score, "reason": " | ".join(signal_log), "needs_ai": False}
    
    elif score >= 1 and ev.get("ev_pct", 0.0) >= 2.0:
        half_kelly = ev.get("kelly_bet", 0.0) * 0.5
        if half_kelly <= 0:
            return {"decision": "SKIP", "confidence": "Low", "bet_size": 0.0,
                    "score": score, "reason": "Kelly returned 0 on borderline — skipping", "needs_ai": False}
        return {"decision": favored_dir, "confidence": "Low", "bet_size": half_kelly,
                "score": score, "reason": " | ".join(signal_log), "needs_ai": True}

    return {"decision": "SKIP", "confidence": "Low", "bet_size": 0.0,
            "score": score, "reason": f"Insufficient conviction (score={score})", "needs_ai": False}

def _commit_decision(slug: str, result: dict, poly_data: dict, current_ev_pct: float = 0.0):
    strike   = poly_data.get("strike_price", 0.0)
    decision = result["decision"]
    
    if decision in ["UP", "DOWN"]:
        committed_slugs.add(slug)
        soft_skipped_slugs.discard(slug)
        best_ev_seen.pop(slug, None)
        
        bought_price = poly_data["up_prob"] / 100.0 if decision == "UP" else poly_data["down_prob"] / 100.0
        # Determine the exact token we are buying so we can sell it later
        token_id = poly_data.get("token_id_up", "") if decision == "UP" else poly_data.get("token_id_down", "")
        
        active_predictions[slug] = {
            "decision":   decision,
            "strike":     strike,
            "score":      result.get("score", 0),
            "confidence": result.get("confidence", "Low"),
            "bet_size":   result.get("bet_size", 0.0),
            "bought_price": bought_price,
            "token_id":   token_id,   # <--- ADDED
            "status":     "OPEN",     # <--- ADDED
            "entry_time": time.time() # <--- ADDED: track entry time for min hold period
        }
        log.info(f"DECISION LOCKED: {decision} | Score: {result.get('score','?')}/4 | Bet: ${result.get('bet_size',0.0):.2f}")
        asyncio.create_task(place_bet(slug, decision, result.get("bet_size", 0.0), poly_data))
    else:
        soft_skipped_slugs.add(slug)
        best_ev_seen[slug] = max(best_ev_seen.get(slug, -999.0), current_ev_pct)

async def place_bet(slug: str, decision: str, bet_size: float, poly_data: dict):
    global clob_client, simulated_balance

    # FIX: Guard against zero/near-zero bets that slipped through (Kelly returned no edge)
    if bet_size < 0.50:
        log.warning(f"[BET] ✗ Bet size ${bet_size:.2f} is below minimum — skipping order")
        return

    if PAPER_TRADING:
        current_effective_bankroll = simulated_balance
    else:
        async with aiohttp.ClientSession() as s:
            current_effective_bankroll = await fetch_live_balance(s)

    allowed, message = risk_manager.can_trade(current_effective_bankroll, bet_size)
    if not allowed:
        log.warning(f"[RISK REJECTED] {slug}: {message}")
        return

    token_id        = poly_data.get("token_id_up", "") if decision == "UP" else poly_data.get("token_id_down", "")
    market_prob     = poly_data["up_prob"] if decision == "UP" else poly_data["down_prob"]
    strike          = poly_data.get("strike_price", 0.0)
    price           = round(market_prob / 100.0, 4)
    mode_label      = "📋 PAPER" if PAPER_TRADING else ("🔍 DRY RUN" if DRY_RUN else "🔴 LIVE")
    direction_arrow = "📈 UP  " if decision == "UP" else "📉 DOWN"

    if not token_id:
        log.error(f"[BET] ✗ No token_id for {decision} on {slug}")
        return

    total_trades = total_wins + total_losses
    win_rate     = (total_wins / total_trades) * 100 if total_trades > 0 else 0.0
    bar_filled   = int(win_rate / 5)
    win_bar      = "█" * bar_filled + "░" * (20 - bar_filled)

    print("\n" + "═" * 58)
    print(f"  🎯  BET PLACED  [{mode_label}]")
    print("═" * 58)
    print(f"  Direction : {direction_arrow}  ({market_prob:.1f}% market odds)")
    print(f"  Strike    : ${strike:,.2f}")
    print(f"  Bet Size  : ${bet_size:.2f} USDC")
    print(f"  Kelly @   : {price:.4f}  (paying {market_prob:.1f}¢ per $1)")
    print(f"  Balance   : ${current_effective_bankroll:.2f}")
    print(f"  Daily PnL : ${risk_manager.current_daily_pnl:+.2f}")
    print(f"  Market    : {slug}")
    print(f"  ─────────────────────────────────────────────")
    print(f"  Record    : {total_wins}W / {total_losses}L  ({win_rate:.1f}% win rate)")
    print(f"  Win Rate  : [{win_bar}] {win_rate:.1f}%")
    print("═" * 58 + "\n")

    if PAPER_TRADING:
        log.info(f"[PAPER] Simulated {decision} | ${bet_size:.2f} | Balance: ${simulated_balance:.2f}")
        return

    if DRY_RUN:
        log.info(f"[DRY RUN] Would place: {decision} | ${bet_size:.2f} | Balance: ${current_effective_bankroll:.2f}")
        return

    if clob_client is None:
        log.error("[BET] ✗ CLOB client not initialised")
        return

    try:
        from py_clob_client.clob_types import MarketOrderArgs, OrderType
        from py_clob_client.order_builder.constants import BUY
        order_args = MarketOrderArgs(token_id=token_id, amount=bet_size, side=BUY)
        signed     = await asyncio.to_thread(clob_client.create_market_order, order_args)
        resp       = await asyncio.to_thread(clob_client.post_order, signed, OrderType.FOK)
        if resp.get("status") == "matched":
            log.info(f"✅ ORDER MATCHED: {decision} on {slug} | ${bet_size:.2f}")
        else:
            log.warning(f"[BET] ⚠️  Order status: {resp.get('status')} | {resp.get('errorMsg','')}")
    except Exception as e:
        log.error(f"[BET] ✗ Live execution failed: {e}")

async def execute_early_exit(session: aiohttp.ClientSession, slug: str, exit_reason: str, current_token_price: float):
    global simulated_balance, total_wins, total_losses
    
    pred = active_predictions.get(slug)
    if not pred or pred.get("status") != "OPEN": 
        return

    # 1. Lock the trade so the evaluation loop doesn't spam duplicate sell orders
    pred["status"] = "CLOSING"
    
    bet_size = pred["bet_size"]
    bought_price = pred["bought_price"]
    
    # Calculate the exact number of shares you currently own
    shares_owned = bet_size / bought_price if bought_price > 0 else 0.0
    
    if shares_owned == 0:
        active_predictions.pop(slug, None)
        return

    # =========================================================
    # PAPER TRADING LOGIC (Assumes 100% infinite liquidity)
    # =========================================================
    if PAPER_TRADING:
        roi_pct = (current_token_price / bought_price) - 1.0
        pnl_impact = bet_size * roi_pct
        result_str = "WIN" if pnl_impact > 0 else "LOSS"
        
        simulated_balance += pnl_impact
        risk_manager.current_daily_pnl += pnl_impact
        if result_str == "WIN": total_wins += 1
        else: total_losses += 1

        log.info(
            f"[EARLY EXIT] ⚡ {slug} | Reason: {exit_reason} | "
            f"ROI: {roi_pct*100:+.1f}% | PnL: ${pnl_impact:+.4f} | "
            f"Balance: ${simulated_balance:.2f}"
        )
        log_trade_to_db(
            slug, pred["decision"], pred["strike"], live_price, "EARLY_EXIT", 
            result_str, (total_wins / max(1, total_wins + total_losses) * 100), 
            pnl_impact, local_calc_outcome=exit_reason, official_outcome="SOLD", match_status="⚡ EARLY EXIT (PAPER)"
        )
        # Fully close the trade
        active_predictions.pop(slug, None)
        log.info(f"[EARLY EXIT] {slug} closed and removed from active predictions.")
        
    # =========================================================
    # LIVE EXECUTION LOGIC (Handles Partial IOC Fills)
    # =========================================================
    elif not DRY_RUN and clob_client:
        try:
            from py_clob_client.clob_types import MarketOrderArgs, OrderType
            from py_clob_client.order_builder.constants import SELL
            
            # Fire the SELL order using OrderType.IOC
            order_args = MarketOrderArgs(token_id=pred["token_id"], amount=shares_owned, side=SELL)
            signed = await asyncio.to_thread(clob_client.create_market_order, order_args)
            resp = await asyncio.to_thread(clob_client.post_order, signed, OrderType.IOC)
            
            status = resp.get("status", "")
            error_msg = resp.get("errorMsg", "")
            
            # Determine how many shares were actually bought by the order book
            shares_sold = 0.0
            
            if status == "matched":
                # Order was fully absorbed
                shares_sold = shares_owned
            else:
                # Order was partially filled or completely cancelled. 
                # Parse the transaction array to sum up the fractions sold.
                transactions = resp.get("transactions", [])
                for tx in transactions:
                    shares_sold += float(tx.get("size", 0.0))
                    
                # Fallback for different py_clob_client response structures
                if shares_sold == 0 and "matchedAmount" in resp:
                    shares_sold = float(resp.get("matchedAmount", 0.0))

            # --- PROCESS THE EXECUTION ---
            if shares_sold > 0:
                # Calculate the percentage of the bag that successfully sold
                fraction_sold = shares_sold / shares_owned
                realized_bet_size = bet_size * fraction_sold
                
                # Calculate PnL ONLY on the fraction that sold
                roi_pct = (current_token_price / bought_price) - 1.0
                pnl_impact = realized_bet_size * roi_pct
                result_str = "WIN" if pnl_impact > 0 else "LOSS"

                log.info(f"✅ IOC EXECUTION: {slug} | Sold {shares_sold:.2f}/{shares_owned:.2f} shares ({fraction_sold*100:.1f}%) | Realized PnL: ${pnl_impact:+.2f}")

                # Update global accounting
                risk_manager.current_daily_pnl += pnl_impact
                if result_str == "WIN": total_wins += 1
                else: total_losses += 1

                log_trade_to_db(
                    slug, pred["decision"], pred["strike"], live_price, "EARLY_EXIT", 
                    result_str, (total_wins / max(1, total_wins + total_losses) * 100), 
                    pnl_impact, local_calc_outcome=exit_reason, official_outcome="PARTIAL_SELL", match_status=f"⚡ IOC EXIT ({fraction_sold*100:.0f}%)"
                )

                # Update the active prediction state
                remaining_shares = shares_owned - shares_sold
                
                # If we have less than $0.50 worth of shares left, consider it "dust" and close it out
                if (remaining_shares * current_token_price) < 0.50:
                    active_predictions.pop(slug, None)
                else:
                    # Update the remaining stake and unlock the trade to try selling the rest next tick
                    pred["bet_size"] = remaining_shares * bought_price
                    pred["status"] = "OPEN"
                    
            else:
                # 0 shares sold (order book was empty at our limits)
                log.warning(f"⚠️ IOC FAILED (0 shares sold): {error_msg} | Retrying next tick.")
                # Unlock the trade so it tries again
                pred["status"] = "OPEN"

        except Exception as e:
            log.error(f"[EXIT ERROR] Live IOC execution failed: {e}")
            # Unlock the trade so it doesn't get permanently stuck in "CLOSING" state
            pred["status"] = "OPEN"

# ============================================================
# LOOPS
# ============================================================
async def prefill_history(session: aiohttp.ClientSession):
    global cvd_snapshot_at_candle_open, vwap_cum_pv, vwap_cum_vol, vwap_date

    now           = datetime.now(timezone.utc)
    midnight      = now.replace(hour=0, minute=0, second=0, microsecond=0)
    start_time_ms = int(midnight.timestamp() * 1000)

    log.info(f"[SYSTEM] Fetching all candles since 00:00 UTC to sync True Daily VWAP...")

    all_klines    = []
    current_start = start_time_ms

    while True:
        params = {"symbol": "BTCUSDT", "interval": "1m", "startTime": current_start, "limit": 1000}
        try:
            async with session.get("https://api.binance.com/api/v3/klines", params=params, timeout=10) as r:
                r.raise_for_status()
                data = await r.json()
                if not data: break
                all_klines.extend(data)
                current_start = data[-1][0] + 60000
                if len(data) < 1000: break
        except Exception as e:
            log.error(f"[ERROR] Prefill failed: {e}")
            break

    vwap_cum_pv  = 0.0
    vwap_cum_vol = 0.0
    vwap_date    = now.strftime('%Y-%m-%d')
    candle_history.clear()

    for k in all_klines:
        o, h, l, c, v = float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5])
        candle = {
            "timestamp":  datetime.fromtimestamp(k[0]/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            "open": o, "high": h, "low": l, "close": c, "volume": v,
            "body_size": abs(c-o), "upper_wick": h-max(o,c), "lower_wick": min(o,c)-l,
            "structure": "BULLISH" if c > o else "BEARISH"
        }
        typical_price = (h + l + c) / 3.0
        vwap_cum_pv  += typical_price * v
        vwap_cum_vol += v
        candle_history.append(candle)

    while len(candle_history) > MAX_HISTORY:
        candle_history.pop(0)

    cvd_snapshot_at_candle_open = cvd_total
    log.info(f"[OK] True Daily VWAP synced perfectly at ${get_vwap():,.2f}")

async def kline_stream_loop():
    global live_candle, live_price, cvd_snapshot_at_candle_open
    while True:
        try:
            async with websockets.connect(SOCKET_KLINE) as ws:
                async for message in ws:
                    raw = json.loads(message)
                    k   = raw['k']
                    live_candle = k
                    if live_price == 0.0: live_price = float(k['c'])
                    if k['x']:
                        candle = parse_candle(k, override_close=live_price)
                        candle_history.append(candle)
                        if len(candle_history) > MAX_HISTORY: candle_history.pop(0)
                        update_vwap(candle)
                        cvd_snapshot_at_candle_open = cvd_total
        except Exception: await asyncio.sleep(3)

async def agg_trade_listener():
    while True:
        try:
            async with websockets.connect(SOCKET_TRADE) as ws:
                async for message in ws:
                    process_agg_trade(json.loads(message))
        except Exception: await asyncio.sleep(3)

async def _prefetch_strike(session: aiohttp.ClientSession, slug: str):
    await asyncio.sleep(3)
    await fetch_price_to_beat_for_market(session, slug)

# ============================================================
# AI CONFIRMATION
# ============================================================
last_ai_interaction = {
    "prompt": "No AI calls yet.",
    "response": "N/A",
    "timestamp": ""
}

async def call_local_ai(session: aiohttp.ClientSession, current_candle: dict, history: list,
                         poly_data: dict, ev: dict, counter_ev: dict, math_prob: float,
                         slug: str, rule_decision: dict):
    global ai_call_count, ai_consecutive_failures, ai_circuit_open_until, ai_call_in_flight
    ai_call_count += 1

    now = time.time()
    if now < ai_circuit_open_until:
        log.warning(f"[CIRCUIT] AI circuit OPEN ({int(ai_circuit_open_until - now)}s). Falling back to rule engine.")
        rule_decision["needs_ai"] = False
        _commit_decision(slug, rule_decision, poly_data, current_ev_pct=ev.get("ev_pct", 0.0))
        return

    if slug in committed_slugs or ai_call_in_flight == slug:
        return

    ai_call_in_flight = slug
    log.info(f"[AI CONFIRM] Borderline score -- asking Ollama (call #{ai_call_count})...")

    ctx           = build_technical_context(current_candle, history)
    current_price = current_candle['close']
    seconds_left  = int(poly_data.get("seconds_remaining", 0))
    strike        = poly_data.get("strike_price", 0.0)
    distance      = current_price - strike if strike > 0 else 0.0
    favored_dir   = rule_decision["decision"]
    market_prob   = poly_data["up_prob"] if favored_dir == "UP" else poly_data["down_prob"]

    # 1. Define the Market Regime
    regime = "TRENDING" if "streak" in ctx['streak_text'].lower() else "RANGING"

    # 2. Refined High-Density Prompt
    prompt = (
        f"### QUANT INFERENCE MANIFEST ###\n"
        f"TARGET: {favored_dir} | STRIKE: {strike:,.2f} | EXPIRY: {seconds_left}s\n"
        f"MARKET REGIME: {regime} | {ctx['streak_text'].upper()}\n\n"
    
        f"[PRICE ACTION]\n"
        f"- Spot: {current_price:,.2f} ({'NEAR' if abs(distance) < 20 else 'FAR'} to strike)\n"
        f"- Wick Bias: {ctx['wick_bias']}\n"
        f"- RSI(14): {ctx['rsi']} ({'OVERSOLD' if ctx['rsi'] < 30 else 'OVERBOUGHT' if ctx['rsi'] > 70 else 'NEUTRAL'})\n\n"
    
        f"[MOMENTUM & FLOW]\n"
        f"- Money Flow (CVD): {ctx['cvd_signal']}\n"
        f"- CVD 1m Velocity: ${ctx['cvd_1min']:+,.0f}\n"
        f"- Divergence Alert: {ctx['cvd_divergence'] or 'NONE'}\n\n"
    
        f"[MATHEMATICAL EDGE]\n"
        f"- Theoretical Prob: {math_prob:.1f}%\n"
        f"- Polymarket Odds: {market_prob:.1f}%\n"
        f"- Expected Value (EV): {ev.get('ev_pct', 0.0):+.2f}%\n\n"
    
        f"INSTRUCTION: Evaluate if technicals confirm the {favored_dir} edge. "
        f"Output ONLY '{favored_dir}' or 'SKIP'."
    )

    last_ai_interaction["prompt"] = prompt
    last_ai_interaction["timestamp"] = datetime.now().strftime('%H:%M:%S')

    payload = {
        "model":       LOCAL_AI_MODEL,
        "messages":    [{"role": "user", "content": prompt}],
        "temperature": 0.1,
        "max_tokens":  5,
        "stream":      False,
        "options":     {
            "num_predict": 5,
            "num_ctx": 1024   # FIX: bumped to match warmup context window for mistral-nemo
            },  
    }
    timeout = aiohttp.ClientTimeout(connect=AI_TIMEOUT_CONNECT, total=AI_TIMEOUT_TOTAL)
    ai_word = None

    for attempt in range(1, AI_MAX_RETRIES + 1):
        try:
            t0 = time.time()
            async with session.post(LOCAL_AI_URL, json=payload, timeout=timeout) as r:
                r.raise_for_status()
                data    = await r.json()
                ai_word = data['choices'][0]['message']['content'].strip().upper()
                elapsed = time.time() - t0
                log.info(f"[AI CONFIRM] Response: '{ai_word}' ({elapsed:.1f}s)")
                ai_consecutive_failures = 0
                break
        except asyncio.TimeoutError:
            ai_consecutive_failures += 1
            log.warning(f"[AI CONFIRM] Timeout attempt {attempt}/{AI_MAX_RETRIES}")
            if attempt == AI_MAX_RETRIES:
                log.warning("[AI CONFIRM] All retries timed out -- SKIP for safety.")
                _commit_decision(slug, {**rule_decision, "decision": "SKIP", "bet_size": 0.0,
                                         "reason": "AI confirmation timed out"},
                                 poly_data, current_ev_pct=ev.get("ev_pct", 0.0))
                ai_call_in_flight = ""
                return
            await asyncio.sleep(AI_RETRY_DELAY)
        except Exception as e:
            ai_consecutive_failures += 1
            log.error(f"[AI CONFIRM] {type(e).__name__}: {e} -- using rule engine fallback")
            break

    if ai_consecutive_failures >= CB_FAILURE_THRESHOLD:
        ai_circuit_open_until = time.time() + CB_COOLDOWN_SECS
        log.error(f"[CIRCUIT] ⚡ Tripped. AI paused {CB_COOLDOWN_SECS}s.")

    if ai_word == favored_dir:
        final = {**rule_decision, "reason": f"AI confirmed: {rule_decision['reason']}"}
    elif ai_word and "SKIP" in ai_word:
        final = {**rule_decision, "decision": "SKIP", "bet_size": 0.0,
                 "reason": "AI vetoed borderline signal"}
    else:
        log.warning(f"[AI CONFIRM] Unparseable '{ai_word}' -- using rule engine as-is")
        final = rule_decision

    last_ai_interaction["response"] = ai_word

    _commit_decision(slug, final, poly_data, current_ev_pct=ev.get("ev_pct", 0.0))
    ai_call_in_flight = ""

async def evaluation_loop(session: aiohttp.ClientSession):
    global target_slug, ai_call_in_flight, _poly_cache_slug
    await asyncio.sleep(EVAL_TICK_SECONDS)
    
    while True:
        await asyncio.sleep(EVAL_TICK_SECONDS)
        if not candle_history or not live_candle: continue
        
        slug = target_slug
        current_price = live_price if live_price > 0 else float(live_candle.get('c', 0))
        if current_price == 0: continue

        # --- 1. PREPARE CURRENT CANDLE & CONTEXT ---
        k = live_candle
        current_candle = {
            "timestamp":  datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            "open":       float(k.get('o', current_price)),
            "high":       float(k.get('h', current_price)),
            "low":        float(k.get('l', current_price)),
            "close":      current_price,
            "volume":     float(k.get('v', 0)),
            "body_size":  abs(current_price - float(k.get('o', current_price))),
            "upper_wick": float(k.get('h', current_price)) - max(current_price, float(k.get('o', current_price))),
            "lower_wick": min(current_price, float(k.get('o', current_price))) - float(k.get('l', current_price)),
            "structure":  "BULLISH" if current_price >= float(k.get('o', current_price)) else "BEARISH",
        }
        
        ctx = build_technical_context(current_candle, candle_history)
        is_high_conviction = "DIVERGENCE" in (ctx.get("cvd_divergence") or "")

        print(f"\r  [{datetime.now(timezone.utc).strftime('%H:%M:%S')}] Price: ${current_price:,.2f} | VWAP: ${get_vwap():,.2f} | Slug: {slug}   ", end="", flush=True)

        # --- 2. POLL POLYMARKET ---
        poly_data = await get_polymarket_odds_cached(session, slug)
        if not poly_data.get("market_found"): continue
        
        secs = poly_data.get("seconds_remaining", 0)
        
        # --- 3. HANDLE EXPIRATION / SLUG ROTATION ---
        if secs <= 0:
            if slug in active_predictions:
                pred = active_predictions[slug]
                if pred.get("status") != "CLOSING":
                    pred["status"] = "RESOLVING"
                    asyncio.create_task(resolve_market_outcome(
                        session, slug, pred["decision"], pred["strike"],
                        current_price, pred.get("bet_size", 1.01), pred.get("bought_price", 0.0)
                    ))

            # Cleanup and increment
            old_slug = slug
            target_slug = increment_slug_by_300(slug)
            committed_slugs.discard(old_slug)
            soft_skipped_slugs.discard(old_slug)
            best_ev_seen.pop(old_slug, None)
            market_open_prices.pop(old_slug, None)
            strike_price_cache.pop(old_slug, None)
            _poly_cache_slug = ""
            asyncio.create_task(_prefetch_strike(session, target_slug))
            continue

        # --- 4. EARLY EXIT MONITORING ---
        if slug in active_predictions and active_predictions[slug].get("status") == "OPEN":
            pred = active_predictions[slug]
            current_token_price = poly_data["up_prob"] / 100.0 if pred["decision"] == "UP" else poly_data["down_prob"] / 100.0
            
            # Check Take Profit / Stop Loss
            tp_threshold, sl_threshold = get_dynamic_threshold(secs)
            roi_pct = (current_token_price / pred["bought_price"]) - 1.0 if pred["bought_price"] > 0 else 0
            
            if roi_pct >= tp_threshold:
                asyncio.create_task(execute_early_exit(session, slug, f"TAKE_PROFIT ({roi_pct*100:.1f}%)", current_token_price))
                continue
            elif roi_pct <= sl_threshold:
                asyncio.create_task(execute_early_exit(session, slug, f"STOP_LOSS ({roi_pct*100:.1f}%)", current_token_price))
                continue

            # Signal Reversal Exit (after 90s)
            time_held = time.time() - pred.get("entry_time", time.time())
            if time_held > 90:
                if (pred["decision"] == "UP" and "BEARISH DIVERGENCE" in ctx.get("cvd_divergence", "")) or \
                   (pred["decision"] == "DOWN" and "BULLISH DIVERGENCE" in ctx.get("cvd_divergence", "")):
                    asyncio.create_task(execute_early_exit(session, slug, "SIGNAL_REVERSAL", current_token_price))
                    continue

        # --- 5. NEW ENTRY EVALUATION ---
        if slug in committed_slugs or ai_call_in_flight == slug: continue

        bal = simulated_balance if PAPER_TRADING else await fetch_live_balance(session)
        
        log.info(f"[EVAL] tick | balance=${bal:.2f} | secs={int(secs)} | conviction={'HIGH' if is_high_conviction else 'Normal'}")

        should_call, skip_reason, ev, counter_ev, math_prob = run_gatekeeper(
            current_candle, candle_history, poly_data, bal, ctx, is_high_conviction
        )
        
        if not should_call:
            log.info(f"[GATE] BLOCKED: {skip_reason}")
            continue

        # Pass the already computed 'ev' into the rule engine
        result = rule_engine_decide(current_candle, candle_history, poly_data, ev, math_prob, bal)

        log.info(f"[RULE] Score={result['score']}/3 → {result['decision']} | EV={ev['ev_pct']:+.2f}% | bet=${ev['kelly_bet']}")

        if result["decision"] in ("UP", "DOWN"):
            if not result.get("needs_ai"):
                _commit_decision(slug, result, poly_data, ev['ev_pct'])
            else:
                asyncio.create_task(call_local_ai(session, current_candle, candle_history, poly_data, ev, counter_ev, math_prob, slug, result))

async def warmup_ollama(session: aiohttp.ClientSession):
    """
    Pre-loads the model into Ollama's memory at startup so the first real
    AI call doesn't time out waiting for the model to load (~10-20s for 7B).
    Uses Ollama's native /api/generate endpoint with keep_alive=30m so the
    model stays resident between calls.
    """
    url = "http://localhost:11434/api/generate"
    payload = {
        "model":      LOCAL_AI_MODEL,
        "prompt":     "hi",
        "stream":     False,
        "keep_alive": "60m",   # keep model in VRAM/RAM for 60 minutes (mistral-nemo is larger)
        "num_ctx": 1024,       # FIX: bumped from 512 to give mistral-nemo enough context
    }
    try:
        log.info(f"[OLLAMA] Warming up {LOCAL_AI_MODEL} (this may take 10-20s)...")
        timeout = aiohttp.ClientTimeout(total=60)
        async with session.post(url, json=payload, timeout=timeout) as r:
            r.raise_for_status()
            log.info(f"[OLLAMA] ✓ Model loaded and ready. Will stay resident for 30m between calls.")
    except Exception as e:
        log.warning(f"[OLLAMA] Warmup failed ({type(e).__name__}: {e}) -- AI calls may be slow on first use.")

async def main():
    global target_slug, market_family_prefix, clob_client
    if not POLY_PRIVATE_KEY:
        log.warning("[CLOB] POLY_PRIVATE_KEY not set")
    else:
        try:
            from py_clob_client.client import ClobClient
            _kwargs = dict(host=CLOB_HOST, key=POLY_PRIVATE_KEY, chain_id=CHAIN_ID)
            if POLY_FUNDER:
                _kwargs["signature_type"] = POLY_SIG_TYPE
                _kwargs["funder"]         = POLY_FUNDER
            clob_client = ClobClient(**_kwargs)
            clob_client.set_api_creds(clob_client.create_or_derive_api_creds())
        except Exception as e:
            log.error(f"[CLOB] Failed init: {e}")

    async with aiohttp.ClientSession() as session:
        await warmup_ollama(session)
        await prefill_history(session)
        await asyncio.gather(kline_stream_loop(), agg_trade_listener(), evaluation_loop(session))

if __name__ == "__main__":
    user_market = input("[INPUT] Enter Polymarket BTC market URL/slug (or press Enter for auto): ").strip()
    if user_market:
        target_slug = extract_slug_from_market_url(user_market)
    else:
        now         = int(time.time())
        target_slug = f"btc-updown-5m-{((now // 300) + 1) * 300}"
        print(f"[AUTO] Using current market slug: {target_slug}")
    if not target_slug:
        raise SystemExit(1)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[SYSTEM] Engine stopped.")