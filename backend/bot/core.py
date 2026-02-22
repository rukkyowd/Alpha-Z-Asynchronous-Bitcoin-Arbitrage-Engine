import asyncio
import aiohttp
import websockets
import json
import logging
import sys
import re
import os
import csv
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
LOCAL_AI_MODEL  = "llama3.2:3b"

BANKROLL        = 10000.00

PAPER_TRADING = os.getenv("PAPER_TRADING", "true").lower() == "true"
PAPER_BALANCE = 100.00 # Starting fake bankroll for paper trading mode

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
MIN_BODY_SIZE             = 0.01
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

# ============================================================
# LOGGING (UTF-8 Enforced)
# ============================================================
_fmt            = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_file_handler   = logging.FileHandler("trading_log.txt", encoding="utf-8")
_file_handler.setFormatter(_fmt)
_utf8_stdout    = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
_stream_handler = logging.StreamHandler(_utf8_stdout)
_stream_handler.setFormatter(_fmt)
logging.root.setLevel(logging.INFO)
logging.root.addHandler(_file_handler)
logging.root.addHandler(_stream_handler)
log = logging.getLogger(__name__)

# ============================================================
# RISK MANAGEMENT ENGINE
# ============================================================
class RiskManager:
    def __init__(self, daily_loss_limit=5.00, max_trade_pct=0.20):
        self.daily_loss_limit = daily_loss_limit
        self.max_trade_pct = max_trade_pct
        self.current_daily_pnl = 0.0

    def reset_stats(self):
        self.current_daily_pnl = 0.0
        log.info("[RISK] Daily stats reset. New session started.")

    def can_trade(self, current_balance, trade_size):
        if self.current_daily_pnl <= -self.daily_loss_limit:
            return False, f"Daily loss limit reached (${self.current_daily_pnl:.2f})"
        if trade_size > (current_balance * self.max_trade_pct):
            return False, f"Trade size ${trade_size} exceeds max risk pct."
        return True, "Approved"

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
risk_manager = RiskManager(daily_loss_limit=5.00, max_trade_pct=0.30)
simulated_balance = PAPER_BALANCE

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
# CSV STATS TRACKING
# ============================================================
STATS_CSV_FILE = "ai_trade_history.csv"

def log_trade_to_csv(slug, decision, strike, final_price, actual_outcome, result, win_rate):
    file_exists = os.path.isfile(STATS_CSV_FILE)
    try:
        with open(STATS_CSV_FILE, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["Timestamp (UTC)", "Market Slug", "AI Decision",
                                  "Strike Price", "Final Price", "Actual Outcome",
                                  "Result", "Running Win Rate (%)"])
            timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            writer.writerow([timestamp, slug, decision, f"{strike:.2f}",
                              f"{final_price:.2f}", actual_outcome, result, f"{win_rate:.2f}"])
    except Exception as e:
        log.error(f"[CSV ERROR] {e}")

# ============================================================
# UTILITIES
# ============================================================
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
# Tries multiple SDK method names, then falls back to REST.
# On total failure, logs all balance-related method names to
# help identify the correct one for your SDK version.
# ============================================================
async def fetch_live_balance(session: aiohttp.ClientSession) -> float:
    """Fetch live Polymarket USDC balance. Falls back to BANKROLL on any failure."""
    if clob_client is None:
        return BANKROLL

    # ── Correct call: BalanceAllowanceParams with COLLATERAL asset type ──
    # Balance is returned in micro-USDC (6 decimals), so divide by 1e6.
    try:
        from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
        params = BalanceAllowanceParams(
            signature_type=POLY_SIG_TYPE,
            asset_type=AssetType.COLLATERAL
        )
        resp = await asyncio.to_thread(clob_client.get_balance_allowance, params=params)
        raw_balance = int(resp.get("balance", 0))
        fetched = raw_balance / 1_000_000  # micro-USDC → USDC
        if fetched > 0:
            log.info(f"[BALANCE] Live Polymarket balance: ${fetched:.2f}")
            return fetched
        # Balance of 0 is valid (broke!) — still return it so Kelly sizes to 0
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
def compute_ev(true_prob_pct: float, market_prob_pct: float, current_balance: float) -> dict:
    token = market_prob_pct / 100.0
    prob  = true_prob_pct   / 100.0
    if not (0 < token < 1):
        return {"ev": 0.0, "ev_pct": 0.0, "edge": 0.0, "kelly_fraction": 0.0, "kelly_bet": 0.0}
    net_win        = 1.0 - token
    ev             = prob * net_win - (1 - prob) * token
    ev_pct         = (ev / token) * 100
    edge           = prob - token
    b              = net_win / token
    kelly_fraction = max(0.0, (b * prob - (1 - prob)) / b)

    # Quarter-Kelly sized to current live portfolio
    raw_kelly_bet    = kelly_fraction * current_balance * 0.25
    absolute_ceiling = current_balance * 0.25
    dynamic_bet      = max(1.01, min(raw_kelly_bet, absolute_ceiling))

    return {"ev": round(ev,4), "ev_pct": round(ev_pct,2), "edge": round(edge,4),
            "kelly_fraction": round(kelly_fraction,4), "kelly_bet": round(dynamic_bet, 2)}

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
    cvd_divergence     = ""
    if price_direction_up and price_move > 30 and cvd_candle_delta < -3000:
        cvd_divergence = "⚠️  BEARISH DIVERGENCE: Price rising but heavy SELL flow"
    elif not price_direction_up and price_move > 30 and cvd_candle_delta > 3000:
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
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    
    # Final safety check
    if math.isnan(rsi):
        return 50.0
    return round(rsi,1)

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
        end_dt     = datetime.fromisoformat(end_time_str.replace("Z", "+00:00"))
        start_dt   = end_dt - timedelta(minutes=5)
        params = {"symbol": "BTC", "eventStartTime": start_dt.strftime('%Y-%m-%dT%H:%M:%SZ'),
                  "variant": "fiveminute", "endDate": end_dt.strftime('%Y-%m-%dT%H:%M:%SZ')}
        async with session.get("https://polymarket.com/api/crypto/crypto-price", params=params, timeout=5) as r:
            if r.status == 200:
                data  = await r.json()
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
                markets = event.get("markets", [])
                if not markets: return {"market_found": False, "error": "No markets"}
                market = markets[0]
                prices = json.loads(market.get("outcomePrices", "[]")) if isinstance(market.get("outcomePrices"), str) else market.get("outcomePrices")
                token_ids = json.loads(market.get("clobTokenIds", "[]")) if isinstance(market.get("clobTokenIds"), str) else market.get("clobTokenIds")
                strike_price = await fetch_price_to_beat_for_market(session, slug)
                return {
                    "market_found": True, "title": event.get("title", slug),
                    "up_prob": float(prices[0]) * 100, "down_prob": float(prices[1]) * 100,
                    "strike_price": strike_price, "seconds_remaining": _parse_seconds_remaining(event.get("endDate", "")),
                    "token_id_up": token_ids[0] if len(token_ids) > 0 else "",
                    "token_id_down": token_ids[1] if len(token_ids) > 1 else "",
                }
    except Exception as e: return {"market_found": False, "error": str(e)}
    return {"market_found": False, "error": "Not found"}

# ============================================================
# RESOLUTION LOGIC
# ============================================================
async def resolve_market_outcome(session: aiohttp.ClientSession, slug: str, decision: str, strike: float, local_price_fallback: float, bet_size: float = 1.01):
    global total_wins, total_losses, simulated_balance
    await asyncio.sleep(10)
    final_price = local_price_fallback
    actual_outcome = "TIE"

    meta = await fetch_market_meta_from_slug(session, slug)
    if meta:
        end_time_str = meta["market"].get("endDate", "")
        if end_time_str:
            for _ in range(6):
                try:
                    end_dt = datetime.fromisoformat(end_time_str.replace("Z", "+00:00"))
                    start_dt = end_dt - timedelta(minutes=5)
                    params = {"symbol": "BTC", "eventStartTime": start_dt.strftime('%Y-%m-%dT%H:%M:%SZ'),
                              "variant": "fiveminute", "endDate": end_dt.strftime('%Y-%m-%dT%H:%M:%SZ')}
                    async with session.get("https://polymarket.com/api/crypto/crypto-price", params=params, timeout=5) as r:
                        if r.status == 200:
                            data = await r.json()
                            if "price" in data and data["price"]:
                                final_price = float(data["price"])
                                break
                except: pass
                await asyncio.sleep(10)

    if final_price > strike: actual_outcome = "UP"
    elif final_price < strike: actual_outcome = "DOWN"

    result_str = "WIN" if decision == actual_outcome else "LOSS"

    if actual_outcome == "TIE": result_str = "TIE"

    pnl_impact = bet_size if result_str == "WIN" else -bet_size

    if PAPER_TRADING:
        # pnl_impact is +bet_size on a win, and -bet_size on a loss
        simulated_balance += pnl_impact
        log.info(f"[PAPER RESOLVE] {result_str} | New Virtual Balance: ${simulated_balance:.2f}")

    risk_manager.current_daily_pnl += pnl_impact

    if result_str == "WIN": total_wins += 1
    elif result_str == "LOSS": total_losses += 1

    total_trades = total_wins + total_losses
    win_rate = (total_wins / total_trades) * 100 if total_trades > 0 else 0.0

    total_trades = total_wins + total_losses
    bar_filled = int(win_rate / 5)
    win_bar = "█" * bar_filled + "░" * (20 - bar_filled)

    if result_str == "WIN":
        outcome_line = f"  ✅  WIN   +${bet_size:.2f}"
        balance_line = f"  Balance   : ${simulated_balance:.2f}" if PAPER_TRADING else ""
    elif result_str == "LOSS":
        outcome_line = f"  ❌  LOSS  -${bet_size:.2f}"
        balance_line = f"  Balance   : ${simulated_balance:.2f}" if PAPER_TRADING else ""
    else:
        outcome_line = f"  ➖  TIE   $0.00"
        balance_line = ""

    print("\n" + "═" * 58)
    print(f"  🏁  MARKET RESOLVED")
    print("═" * 58)
    print(f"  {outcome_line}")
    print(f"  Predicted : {decision}  |  Actual: {actual_outcome}")
    print(f"  Strike    : ${strike:,.2f}  →  Final: ${final_price:,.2f}")
    if balance_line:
        print(balance_line)
    print(f"  Daily PnL : ${risk_manager.current_daily_pnl:+.2f}")
    print(f"  ─────────────────────────────────────────────")
    print(f"  Record    : {total_wins}W / {total_losses}L  ({win_rate:.1f}% win rate)")
    print(f"  Win Rate  : [{win_bar}] {win_rate:.1f}%")
    print("═" * 58 + "\n")

    log.info(f"[STATS] W:{total_wins} L:{total_losses} | WinRate:{win_rate:.2f}% | Daily PnL: ${risk_manager.current_daily_pnl:.2f}")
    log_trade_to_csv(slug, decision, strike, final_price, actual_outcome, result_str, win_rate)

# ============================================================
# ENGINE LOGIC
# ============================================================
def run_gatekeeper(current_candle: dict, history: list, poly_data: dict, current_balance: float) -> tuple:
    if not poly_data["market_found"]: return False, "No Polymarket data", {}, {}, 0.0
    seconds_left = poly_data.get("seconds_remaining", 0)
    if seconds_left < MIN_SECONDS_REMAINING: return False, f"Only {int(seconds_left)}s left -- avoiding illiquid spread", {}, {}, 0.0
    if seconds_left > MAX_SECONDS_FOR_NEW_BET: return False, f"{int(seconds_left)}s remaining -- too early", {}, {}, 0.0
    if current_candle['body_size'] < MIN_BODY_SIZE: return False, f"Body too small", {}, {}, 0.0

    direction = current_candle['structure']
    if direction == "BULLISH": favored_dir, market_prob, counter_prob = "UP", poly_data["up_prob"], poly_data["down_prob"]
    else: favored_dir, market_prob, counter_prob = "DOWN", poly_data["down_prob"], poly_data["up_prob"]

    if market_prob > MAX_CROWD_PROB_TO_CALL: return False, f"Crowd already at {market_prob:.1f}%", {}, {}, 0.0

    strike    = poly_data.get("strike_price", 0.0)
    math_prob = calculate_strike_probability(current_candle['close'], strike, history, seconds_left, favored_dir)
    ev        = compute_ev(math_prob, market_prob, current_balance)
    counter_ev = compute_ev(100-math_prob, counter_prob, current_balance)
    best_ev   = max(ev["ev_pct"], counter_ev["ev_pct"])

    if best_ev < MIN_EV_PCT_TO_CALL_AI: return False, f"Math EV {best_ev:+.2f}% < threshold", ev, counter_ev, math_prob
    return True, "", ev, counter_ev, math_prob

def rule_engine_decide(current_candle: dict, history: list, poly_data: dict, ev: dict, math_prob: float, current_balance: float = 100.0) -> dict:
    ctx = build_technical_context(current_candle, history)
    current_price = current_candle['close']
    strike = poly_data.get("strike_price", 0.0)

    score = 0
    signal_log = []

    if strike > 0: favored_dir = "UP" if current_price > strike else "DOWN"
    else: favored_dir = "UP" if current_candle['structure'] == "BULLISH" else "DOWN"

    # 1. VWAP
    if favored_dir == "UP":
        if current_price > ctx["vwap"]: score += 1; signal_log.append("Above VWAP (+1)")
        else: score -= 1; signal_log.append("Below VWAP (-1)")
    else:
        if current_price < ctx["vwap"]: score += 1; signal_log.append("Below VWAP (+1)")
        else: score -= 1; signal_log.append("Above VWAP (-1)")

    # 2. EMA Cross
    if favored_dir == "UP":
        if ctx["ema_9"] > ctx["ema_21"]: score += 1; signal_log.append("9EMA>21EMA (+1)")
        else: score -= 1; signal_log.append("9EMA<21EMA (-1)")
    else:
        if ctx["ema_9"] < ctx["ema_21"]: score += 1; signal_log.append("9EMA<21EMA (+1)")
        else: score -= 1; signal_log.append("9EMA>21EMA (-1)")

    # 3. CVD Delta
    if favored_dir == "UP":
        if ctx["cvd_candle_delta"] > 200: score += 1; signal_log.append("CVD buy (+1)")
        elif ctx["cvd_candle_delta"] < -200: score -= 1; signal_log.append("CVD sell (-1)")
        else: signal_log.append("CVD neutral (0)")
    else:
        if ctx["cvd_candle_delta"] < -200: score += 1; signal_log.append("CVD sell (+1)")
        elif ctx["cvd_candle_delta"] > 200: score -= 1; signal_log.append("CVD buy (-1)")
        else: signal_log.append("CVD neutral (0)")

    # 4. Math Edge (Restored from poly_btc.py)
    market_prob = poly_data["up_prob"] if favored_dir == "UP" else poly_data["down_prob"]
    if strike > 0:
        edge = math_prob - market_prob
        if edge > 3:    score += 1; signal_log.append(f"Math edge +{edge:.1f}% (+1)")
        elif edge < -3: score -= 1; signal_log.append(f"Math edge {edge:.1f}% (-1)")
        else:           signal_log.append(f"Math edge {edge:+.1f}% (0)")

    # 5. Restored Aggressive Thresholds
    if score >= 2:
        return {"decision": favored_dir, "confidence": "High", "bet_size": ev.get("kelly_bet", 1.01),
                "score": score, "reason": " | ".join(signal_log), "needs_ai": False}
    elif score == 1 and ev.get("ev_pct", 0.0) >= 3.0:
        # Trigger AI call for tie-breaking borderline trades
        return {"decision": favored_dir, "confidence": "Low", "bet_size": max(1.01, ev.get("kelly_bet", 1.01) * 0.5),
                "score": score, "reason": " | ".join(signal_log), "needs_ai": True}

    return {"decision": "SKIP", "confidence": "Low", "bet_size": 0.0,
            "score": score, "reason": f"Neutral (score={score})", "needs_ai": False}

def _commit_decision(slug: str, result: dict, poly_data: dict, current_ev_pct: float = 0.0):
    strike = poly_data.get("strike_price", 0.0)
    decision = result["decision"]
    if decision in ["UP", "DOWN"]:
        committed_slugs.add(slug)
        soft_skipped_slugs.discard(slug)
        best_ev_seen.pop(slug, None)
        active_predictions[slug] = {
            "decision": decision,
            "strike": strike,
            "score": result.get("score", 0),
            "confidence": result.get("confidence", "Low"),
            "bet_size": result.get("bet_size", 0.0)
        }
        log.info(f"DECISION LOCKED: {decision} | Score: {result.get('score', '?')}/4 | Bet: ${result.get('bet_size', 0.0):.2f}")
        asyncio.create_task(place_bet(slug, decision, result.get("bet_size", 0.0), poly_data))
    else:
        soft_skipped_slugs.add(slug)
        best_ev_seen[slug] = max(best_ev_seen.get(slug, -999.0), current_ev_pct)

async def place_bet(slug: str, decision: str, bet_size: float, poly_data: dict):
    global clob_client, simulated_balance

    # ── Use live balance for accurate risk check ──
    if PAPER_TRADING:
        current_effective_bankroll = simulated_balance
    else:
        async with aiohttp.ClientSession() as s:
            current_effective_bankroll = await fetch_live_balance(s)
    # ─────────────────────────────────────────────

    allowed, message = risk_manager.can_trade(current_effective_bankroll, bet_size)
    if not allowed:
        log.warning(f"[RISK REJECTED] {slug}: {message}")
        return

    token_id = poly_data.get("token_id_up", "") if decision == "UP" else poly_data.get("token_id_down", "")
    market_prob = poly_data["up_prob"] if decision == "UP" else poly_data["down_prob"]
    strike = poly_data.get("strike_price", 0.0)
    price = round(market_prob / 100.0, 4)
    mode_label = "📋 PAPER" if PAPER_TRADING else ("🔍 DRY RUN" if DRY_RUN else "🔴 LIVE")
    direction_arrow = "📈 UP  " if decision == "UP" else "📉 DOWN"

    if not token_id:
        log.error(f"[BET] ✗ No token_id for {decision} on {slug}")
        return

    total_trades = total_wins + total_losses
    win_rate = (total_wins / total_trades) * 100 if total_trades > 0 else 0.0
    bar_filled = int(win_rate / 5)
    win_bar = "█" * bar_filled + "░" * (20 - bar_filled)

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
        signed = await asyncio.to_thread(clob_client.create_market_order, order_args)
        resp = await asyncio.to_thread(clob_client.post_order, signed, OrderType.FOK)
        if resp.get("status") == "matched":
            log.info(f"✅ ORDER MATCHED: {decision} on {slug} | ${bet_size:.2f}")
        else:
            log.warning(f"[BET] ⚠️  Order status: {resp.get('status')} | {resp.get('errorMsg','')}")
    except Exception as e:
        log.error(f"[BET] ✗ Live execution failed: {e}")

# ============================================================
# LOOPS
# ============================================================
async def prefill_history(session: aiohttp.ClientSession):
    global cvd_snapshot_at_candle_open, vwap_cum_pv, vwap_cum_vol, vwap_date
    
    # 1. Find the exact timestamp for 00:00 UTC today
    now = datetime.now(timezone.utc)
    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    start_time_ms = int(midnight.timestamp() * 1000)
    
    log.info(f"[SYSTEM] Fetching all candles since 00:00 UTC to sync True Daily VWAP...")
    
    all_klines = []
    current_start = start_time_ms
    
    # 2. Fetch all candles since midnight (Binance caps at 1000 per request, so we loop)
    while True:
        params = {
            "symbol": "BTCUSDT",
            "interval": "1m",
            "startTime": current_start,
            "limit": 1000
        }
        try:
            async with session.get("https://api.binance.com/api/v3/klines", params=params, timeout=10) as r:
                r.raise_for_status()
                data = await r.json()
                if not data:
                    break
                all_klines.extend(data)
                
                # Set the start time for the next loop to 1 minute after the last fetched candle
                current_start = data[-1][0] + 60000 
                
                # If we got less than 1000, we've caught up to the current live minute
                if len(data) < 1000:
                    break
        except Exception as e:
            log.error(f"[ERROR] Prefill failed: {e}")
            break

    # 3. Hard reset the VWAP engine to ensure it starts clean
    vwap_cum_pv = 0.0
    vwap_cum_vol = 0.0
    vwap_date = now.strftime('%Y-%m-%d')
    candle_history.clear()

    # 4. Rebuild the True Daily VWAP using every candle since midnight
    for k in all_klines:
        o, h, l, c, v = float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5])
        candle = {
            "timestamp": datetime.fromtimestamp(k[0]/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            "open": o, "high": h, "low": l, "close": c, "volume": v,
            "body_size": abs(c-o), "upper_wick": h-max(o,c), "lower_wick": min(o,c)-l,
            "structure": "BULLISH" if c > o else "BEARISH"
        }
        
        # Calculate typical price and add to cumulative volume trackers manually
        typical_price = (h + l + c) / 3.0
        vwap_cum_pv += typical_price * v
        vwap_cum_vol += v
        
        candle_history.append(candle)
        
    # 5. Trim the history array to MAX_HISTORY (120) so we don't bloat memory or ruin the 9-EMA
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
                    k = raw['k']
                    live_candle = k
                    if live_price == 0.0: live_price = float(k['c'])
                    if k['x']:
                        candle = parse_candle(k, override_close=live_price)
                        candle_history.append(candle)
                        if len(candle_history) > MAX_HISTORY: candle_history.pop(0)
                        update_vwap(candle)
                        cvd_snapshot_at_candle_open = cvd_total
        except Exception as e: await asyncio.sleep(3)

async def agg_trade_listener():
    while True:
        try:
            async with websockets.connect(SOCKET_TRADE) as ws:
                async for message in ws:
                    process_agg_trade(json.loads(message))
        except Exception as e: await asyncio.sleep(3)

async def _prefetch_strike(session: aiohttp.ClientSession, slug: str):
    await asyncio.sleep(3)
    await fetch_price_to_beat_for_market(session, slug)

# ============================================================
# AI CONFIRMATION
# ============================================================
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

    prompt = (
        f"BTC binary market. Score=1/4 signals agree with {favored_dir}.\n"
        f"Price: ${current_price:,.2f} | Strike: ${strike:,.2f} | Distance: {distance:+.2f}\n"
        f"VWAP: ${ctx['vwap']:,.2f} ({ctx['vwap_signal']}) | RSI: {ctx['rsi']} | "
        f"EMA: {ctx['ema_cross']}\n"
        f"CVD this candle: ${ctx['cvd_candle_delta']:+,.0f} | Last 1min: ${ctx['cvd_1min']:+,.0f}\n"
        f"Math prob {favored_dir}: {math_prob:.1f}% | Crowd: {market_prob:.1f}% | "
        f"EV: {ev.get('ev_pct', 0.0):+.2f}% | Time: {seconds_left}s\n"
        f"Signals: {rule_decision['reason']}\n\n"
        f"Reply with exactly one word: {favored_dir} or SKIP"
    )

    payload = {
        "model": LOCAL_AI_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.0,
        "max_tokens": 5,
        "stream": False,
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

    if ai_word and favored_dir in ai_word:
        final = {**rule_decision, "reason": f"AI confirmed: {rule_decision['reason']}"}
    elif ai_word and "SKIP" in ai_word:
        final = {**rule_decision, "decision": "SKIP", "bet_size": 0.0,
                 "reason": "AI vetoed borderline signal"}
    else:
        log.warning(f"[AI CONFIRM] Unparseable '{ai_word}' -- using rule engine as-is")
        final = rule_decision

    current_ev_pct = ev.get("ev_pct", 0.0)
    _commit_decision(slug, final, poly_data, current_ev_pct=current_ev_pct)
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

        k = live_candle
        current_candle = {
            "timestamp":  datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            "open": float(k.get('o', current_price)), "high": float(k.get('h', current_price)),
            "low": float(k.get('l', current_price)), "close": current_price,
            "volume": float(k.get('v', 0)), "body_size": abs(current_price - float(k.get('o', current_price))),
            "upper_wick": float(k.get('h', current_price)) - max(current_price, float(k.get('o', current_price))),
            "lower_wick": min(current_price, float(k.get('o', current_price))) - float(k.get('l', current_price)),
            "structure": "BULLISH" if current_price >= float(k.get('o', current_price)) else "BEARISH",
        }

        print(f"\r  [{datetime.now(timezone.utc).strftime('%H:%M:%S')}] Price: ${current_price:,.2f} | VWAP: ${get_vwap():,.2f} | Slug: {slug}   ", end="", flush=True)

        poly_data = await get_polymarket_odds_cached(session, slug)
        if not poly_data.get("market_found"): continue
        if poly_data["strike_price"] == 0.0:
            if slug not in market_open_prices: market_open_prices[slug] = current_price
            poly_data = {**poly_data, "strike_price": market_open_prices[slug]}

        secs = poly_data.get("seconds_remaining", 0)
        if secs <= 0:
            if slug in active_predictions:
                pred = active_predictions[slug]
                asyncio.create_task(resolve_market_outcome(
                    session, slug, pred["decision"], pred["strike"], current_price, pred.get("bet_size", 1.01)
                ))
                del active_predictions[slug]

            old_slug = slug
            target_slug = increment_slug_by_300(slug)
            committed_slugs.discard(old_slug)
            soft_skipped_slugs.discard(old_slug)
            best_ev_seen.pop(old_slug, None)
            market_open_prices.pop(old_slug, None)
            strike_price_cache.pop(old_slug, None)
            if ai_call_in_flight == old_slug: ai_call_in_flight = ""
            _poly_cache_slug = ""
            asyncio.create_task(_prefetch_strike(session, target_slug))
            continue

        if slug in committed_slugs or ai_call_in_flight == slug: continue

        # ── Fetch live portfolio balance ──
        if PAPER_TRADING:
            current_effective_balance = simulated_balance
        else:
            current_effective_balance = await fetch_live_balance(session)
        log.info(f"[EVAL] tick | paper={PAPER_TRADING} dry_run={DRY_RUN} | balance=${current_effective_balance:.2f} | secs={int(poly_data.get('seconds_remaining',0))} | strike=${poly_data.get('strike_price',0):.2f}")
        # ─────────────────────────────────

        should_call, skip_reason, ev, counter_ev, math_prob = run_gatekeeper(current_candle, candle_history, poly_data, current_effective_balance)
        if not should_call:
            log.info(f"[GATE] BLOCKED: {skip_reason} | secs={int(poly_data.get('seconds_remaining',0))} body={current_candle['body_size']:.2f} price={current_price:,.2f}")
            continue

        current_ev_pct = max(ev.get("ev_pct", 0.0), counter_ev.get("ev_pct", 0.0))
        result = rule_engine_decide(current_candle, candle_history, poly_data, ev, math_prob, current_effective_balance)

        log.info(f"[RULE] Score={result['score']}/3 → {result['decision']} | {result['reason']} | EV={current_ev_pct:+.2f}% | balance=${current_effective_balance:.2f}")

        if result["decision"] in ("UP", "DOWN"):
            if not result.get("needs_ai"):
                _commit_decision(slug, result, poly_data, current_ev_pct)
            else:
                # Dispatch the AI in the background so it doesn't block the websocket ticks
                asyncio.create_task(
                    call_local_ai(session, current_candle, candle_history,
                                  poly_data, ev, counter_ev, math_prob, slug, result)
                )
        else:
            soft_skipped_slugs.add(slug)
            best_ev_seen[slug] = max(best_ev_seen.get(slug, -999.0), current_ev_pct)
async def main():
    global target_slug, market_family_prefix, clob_client
    if not POLY_PRIVATE_KEY: log.warning("[CLOB] POLY_PRIVATE_KEY not set")
    else:
        try:
            from py_clob_client.client import ClobClient
            _kwargs = dict(host=CLOB_HOST, key=POLY_PRIVATE_KEY, chain_id=CHAIN_ID)
            if POLY_FUNDER:
                _kwargs["signature_type"] = POLY_SIG_TYPE
                _kwargs["funder"] = POLY_FUNDER
            clob_client = ClobClient(**_kwargs)
            clob_client.set_api_creds(clob_client.create_or_derive_api_creds())
        except Exception as e: log.error(f"[CLOB] Failed init: {e}")

    async with aiohttp.ClientSession() as session:
        await prefill_history(session)
        await asyncio.gather(kline_stream_loop(), agg_trade_listener(), evaluation_loop(session))

if __name__ == "__main__":
    user_market = input("[INPUT] Enter Polymarket BTC market URL/slug (or press Enter for auto): ").strip()
    if user_market:
        target_slug = extract_slug_from_market_url(user_market)
    else:
        now = int(time.time())
        target_slug = f"btc-updown-5m-{((now // 300) + 1) * 300}"
        print(f"[AUTO] Using current market slug: {target_slug}")
    if not target_slug: raise SystemExit(1)
    try: asyncio.run(main())
    except KeyboardInterrupt: print("\n[SYSTEM] Engine stopped.")