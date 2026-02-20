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

load_dotenv()   # loads .env from cwd; safe no-op if absent

# ============================================================
# CONFIG
# ============================================================
SOCKET_KLINE    = "wss://stream.binance.com:9443/ws/btcusdt@kline_1m"
SOCKET_TRADE    = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"

LOCAL_AI_URL    = "http://localhost:11434/v1/chat/completions"
LOCAL_AI_MODEL  = "llama3.2:3b"

BANKROLL        = 6.41
MAX_BET         = 1.01

GAMMA_API       = "https://gamma-api.polymarket.com"

# ── Polymarket CLOB betting ──────────────────────────────────
CLOB_HOST        = "https://clob.polymarket.com"
CHAIN_ID         = 137                               # Polygon mainnet

# Wallet credentials — loaded from .env (never hardcode these)
POLY_PRIVATE_KEY = os.getenv("POLY_PRIVATE_KEY", "")  # wallet private key
POLY_FUNDER      = os.getenv("POLY_FUNDER", "")        # proxy/funder address
# signature_type: 0=EOA, 1=email/Magic wallet, 2=browser wallet (MetaMask proxy)
POLY_SIG_TYPE    = int(os.getenv("POLY_SIG_TYPE", "1"))

# DRY_RUN=true  → log the bet but don't submit a real order (safe default)
# DRY_RUN=false → live trading; real USDC at risk
DRY_RUN          = os.getenv("DRY_RUN", "true").lower() != "false"

# ── Thresholds ───────────────────────────────────────────────
MIN_EV_PCT_TO_CALL_AI     = 0.1
MIN_SECONDS_REMAINING     = 60
MAX_SECONDS_FOR_NEW_BET   = 295
MIN_BODY_SIZE             = 0.1
MAX_CROWD_PROB_TO_CALL    = 97.0

# ── Concurrent evaluation tick rate ─────────────────────────
# How often the evaluation loop fires (seconds).
# 5s = responsive without hammering the Polymarket API.
EVAL_TICK_SECONDS = 5

# ── History Window ───────────────────────────────────────────
MAX_HISTORY     = 120
VWAP_RESET_HOUR = 0

# ── AI Timeout / Retry / Circuit Breaker ─────────────────────
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
poly_live_binance: float = 0.0
poly_live_chainlink: float = 0.0

# Slugs with a committed UP/DOWN decision — never re-evaluated.
committed_slugs: set = set()

# Slugs where the rule engine returned SKIP — still re-evaluated every tick.
# Cleared on market roll. Tracked so _commit_decision and logs have context.
soft_skipped_slugs: set = set()

# Tracks best EV% and last rule score per slug to suppress redundant log spam.
best_ev_seen: dict = {}          # slug → float (ev_pct)
                                 # slug+"__score" → int (last rule score)

ai_call_in_flight: str = ""
market_open_prices: dict = {}
strike_price_cache: dict = {}

# ── CLOB client (initialised in main(), injected where needed) ──
clob_client = None  

# ── Live price: written by the kline stream, read by evaluation loop ──
live_price: float = 0.0

# ── Live candle accumulator ──────────────────────────────────
# Holds the in-progress (not yet closed) kline so the evaluator
# always has fresh OHLCV data without waiting for candle close.
live_candle: dict = {}

# ── CVD State ────────────────────────────────────────────────
cvd_total:        float = 0.0
cvd_1min_buffer:  deque  = deque()
cvd_snapshot_at_candle_open: float = 0.0
last_cvd_1min:    float = 0.0

# ── VWAP State ───────────────────────────────────────────────
vwap_cum_pv:  float = 0.0
vwap_cum_vol: float = 0.0
vwap_date:    str   = ""

# ── Poly odds cache: refreshed every eval tick ───────────────
# Avoids firing a Polymarket HTTP request on every single aggTrade.
_poly_cache: dict        = {}
_poly_cache_slug: str    = ""
_poly_cache_ts:   float  = 0.0
POLY_CACHE_TTL:   float  = 4.5   # seconds — slightly under EVAL_TICK_SECONDS

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
# VWAP ENGINE
# ============================================================
def update_vwap(candle: dict):
    global vwap_cum_pv, vwap_cum_vol, vwap_date
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    if today != vwap_date:
        vwap_cum_pv  = 0.0
        vwap_cum_vol = 0.0
        vwap_date    = today
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
# VOLATILITY MATH (log returns)
# ============================================================
def calculate_strike_probability(current_price: float, strike_price: float,
                                  history: list, seconds_remaining: float, direction: str) -> float:
    if seconds_remaining <= 0 or not history or strike_price <= 0:
        return 50.0
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
    if direction == "DOWN":
        prob = 1.0 - prob
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
    # Keep live_price updated from the most recent trade
    live_price = price

def get_cvd_candle_delta() -> float:
    return cvd_total - cvd_snapshot_at_candle_open

# ============================================================
# KELLY / EV MATH
# ============================================================
def compute_ev(true_prob_pct: float, market_prob_pct: float, max_bet: float) -> dict:
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
    
    # OLD LOGIC:
    # kelly_bet      = round(min(kelly_fraction * BANKROLL * 0.25, max_bet), 2)
    
    # --- NEW LOGIC BELOW ---
    raw_kelly_bet = kelly_fraction * BANKROLL * 0.25
    
    # If the mathematical bet is greater than 0 but less than $1.00, round it up to the minimum.
    if 0 < raw_kelly_bet < 1.0:
        kelly_bet = 1.01
    else:
        # Otherwise, take the normal Kelly bet, but cap it at max_bet
        kelly_bet = min(raw_kelly_bet, max_bet)
        
    kelly_bet = round(kelly_bet, 2)
    # -----------------------

    return {"ev": round(ev,4), "ev_pct": round(ev_pct,2), "edge": round(edge,4),
            "kelly_fraction": round(kelly_fraction,4), "kelly_bet": kelly_bet}

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
    else:
        wick_bias = "Doji / near-doji"

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

    history_table = "\n".join([
        f"  [{c['timestamp']}] {c['structure']:<8} | O:{c['open']:.2f} C:{c['close']:.2f} | Body:{c['body_size']:.2f} | Vol:{c['volume']:.2f}"
        for c in recent_5
    ]) or "  (no history yet)"

    return {
        "direction": direction,
        "vol_delta_pct": vol_delta_pct, "vol_vs_avg_pct": vol_vs_avg_pct,
        "streak_text": streak_text, "wick_bias": wick_bias,
        "history_table": history_table,
        "vwap": vwap, "vwap_distance": vwap_distance, "vwap_signal": vwap_signal,
        "ema_9": ema_9, "ema_21": ema_21, "ema_50": ema_50, "ema_cross": ema_cross,
        "rsi": rsi,
        "cvd_signal": cvd_signal, "cvd_divergence": cvd_divergence,
        "cvd_1min": cvd_1m, "cvd_candle_delta": cvd_candle_delta,
    }

# ============================================================
# RSI
# ============================================================
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
    return round(100 - (100 / (1 + rs)), 1)

# ============================================================
# FETCH PRICE TO BEAT
# ============================================================
async def fetch_market_meta_from_slug(session: aiohttp.ClientSession, slug: str) -> dict | None:
    try:
        async with session.get(
            f"{GAMMA_API}/events/slug/{slug}",
            timeout=aiohttp.ClientTimeout(total=5)
        ) as r:
            if r.status != 200:
                log.warning(f"[STRIKE] Gamma API returned {r.status} for slug '{slug}'")
                return None
            event = await r.json()
            markets = event.get("markets", [])
            active_market = next(
                (m for m in markets if m.get("active") and not m.get("closed")), None
            )
            if not active_market:
                log.warning(f"[STRIKE] No active market found for slug '{slug}'")
                return None
            return {"title": active_market.get("question", event.get("title", "")),
                    "market": active_market}
    except Exception as e:
        log.warning(f"[STRIKE] fetch_market_meta failed for '{slug}': {e}")
        return None


async def fetch_price_to_beat_for_market(session: aiohttp.ClientSession, slug: str) -> float:
    if slug in strike_price_cache:
        return strike_price_cache[slug]

    log.info(f"[STRIKE] Fetching Price to Beat for '{slug}' via Chainlink API...")
    meta = await fetch_market_meta_from_slug(session, slug)
    if not meta:
        return 0.0

    end_time_str = meta["market"].get("endDate", "")
    if not end_time_str:
        log.warning(f"[STRIKE] No endDate on market for '{slug}'")
        return 0.0

    try:
        end_dt     = datetime.fromisoformat(end_time_str.replace("Z", "+00:00"))
        start_dt   = end_dt - timedelta(minutes=5)
        start_time = start_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        end_time   = end_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    except Exception as e:
        log.warning(f"[STRIKE] Date parse failed for '{slug}': {e}")
        return 0.0

    params = {
        "symbol":         "BTC",
        "eventStartTime": start_time,
        "variant":        "fiveminute",
        "endDate":        end_time,
    }
    try:
        async with session.get(
            "https://polymarket.com/api/crypto/crypto-price",
            params=params,
            timeout=aiohttp.ClientTimeout(total=5)
        ) as r:
            if r.status == 200:
                data  = await r.json()
                price = data.get("openPrice")
                if price:
                    strike = float(price)
                    log.info(f"[STRIKE] ✓ Price to Beat for '{slug}': ${strike:,.2f}")
                    strike_price_cache[slug] = strike
                    return strike
                log.warning(f"[STRIKE] openPrice missing: {data}")
            else:
                log.warning(f"[STRIKE] Chainlink API {r.status}: {(await r.text())[:120]}")
    except Exception as e:
        log.warning(f"[STRIKE] Chainlink API failed for '{slug}': {e}")

    return 0.0

# ============================================================
# ASYNC I/O
# ============================================================
async def get_chainlink_price(session: aiohttp.ClientSession) -> float:
    # BTC/USD Feed on Polygon: 0xc907E116054Ad103354f2D350FD2455D0EB91572
    url = "https://rpc.ankr.com/polygon"
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [
            {
                "to": "0xc907E116054Ad103354f2D350FD2455D0EB91572",
                "data": "0xfeaf968c" # latestRoundData()
            }, 
            "latest"
        ],
        "id": 1
    }
    try:
        async with session.post(url, json=payload, timeout=5) as r:
            data = await r.json()
            result = data.get("result")
            if result and result != "0x":
                # Chainlink returns (roundId, answer, startedAt, updatedAt, answeredInRound)
                # 'answer' is the second 32-byte word (index 66 to 130)
                raw_price = int(result[66:130], 16)
                return raw_price / 1e8  # BTC feed has 8 decimals
        return 0.0
    except Exception as e:
        log.error(f"[CHAINLINK] RPC Error: {e}")
        return 0.0


async def prefill_history(session: aiohttp.ClientSession):
    global cvd_snapshot_at_candle_open
    log.info(f"[SYSTEM] Prefilling {MAX_HISTORY} candles from Binance REST API...")
    try:
        async with session.get(
            "https://api.binance.com/api/v3/klines",
            params={"symbol": "BTCUSDT", "interval": "1m", "limit": MAX_HISTORY},
            timeout=10
        ) as r:
            r.raise_for_status()
            data = await r.json()
            for k in data:
                o, h, l, c, v = float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5])
                candle = {
                    "timestamp":  datetime.fromtimestamp(k[0]/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                    "open": o, "high": h, "low": l, "close": c, "volume": v,
                    "body_size": abs(c-o), "upper_wick": h-max(o,c), "lower_wick": min(o,c)-l,
                    "structure": "BULLISH" if c > o else "BEARISH",
                }
                candle_history.append(candle)
                update_vwap(candle)
            cvd_snapshot_at_candle_open = cvd_total
            log.info(f"[OK] {len(candle_history)} candles loaded. VWAP seeded at ${get_vwap():,.2f}")
    except Exception as e:
        log.error(f"[ERROR] Prefill failed: {e}")


async def get_polymarket_odds_cached(session: aiohttp.ClientSession, slug: str) -> dict:
    """
    Returns cached Polymarket odds if fresh enough, otherwise re-fetches.
    The cache TTL is slightly under EVAL_TICK_SECONDS so we get one fresh
    fetch per evaluation tick without redundant calls.
    """
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
        async with session.get(
            "https://gamma-api.polymarket.com/events",
            params={"slug": slug}, timeout=6
        ) as r:
            r.raise_for_status()
            data = await r.json()
            for event in data:
                if event.get("slug", "") != slug: continue
                markets = event.get("markets", [])
                if not markets: return {"market_found": False, "error": "No markets"}
                market = markets[0]

                seconds_remaining = _parse_seconds_remaining(event.get("endDate", ""))
                raw_prices        = market.get("outcomePrices", "[]")
                prices            = json.loads(raw_prices) if isinstance(raw_prices, str) else raw_prices
                strike_price      = await fetch_price_to_beat_for_market(session, slug)

                # clobTokenIds[0] = YES/UP token, [1] = NO/DOWN token
                raw_token_ids = market.get("clobTokenIds", "[]")
                token_ids     = json.loads(raw_token_ids) if isinstance(raw_token_ids, str) else raw_token_ids

                result = {
                    "market_found":      True,
                    "title":             event.get("title", slug),
                    "up_prob":           float(prices[0]) * 100,
                    "down_prob":         float(prices[1]) * 100,
                    "strike_price":      strike_price,
                    "seconds_remaining": seconds_remaining,
                    "start_date":        event.get("startDate", ""),
                    # Token IDs needed to place orders via CLOB
                    "token_id_up":       token_ids[0] if len(token_ids) > 0 else "",
                    "token_id_down":     token_ids[1] if len(token_ids) > 1 else "",
                }
                log.info(f"[POLY] Strike=${strike_price:,.2f} | UP={result['up_prob']:.1f}% | "
                         f"DOWN={result['down_prob']:.1f}% | {int(seconds_remaining)}s left")
                return result
    except Exception as e:
        return {"market_found": False, "error": str(e)}
    return {"market_found": False, "error": "Slug not matched"}

# ============================================================
# GATEKEEPER
# ============================================================
def run_gatekeeper(current_candle: dict, history: list, poly_data: dict) -> tuple:
    if not poly_data["market_found"]: return False, "No Polymarket data", {}, {}, 0.0
    seconds_left = poly_data.get("seconds_remaining", 0)
    if seconds_left < MIN_SECONDS_REMAINING:
        return False, f"Only {int(seconds_left)}s left -- avoiding illiquid spread", {}, {}, 0.0
    if seconds_left > MAX_SECONDS_FOR_NEW_BET:
        return False, f"{int(seconds_left)}s remaining -- too early", {}, {}, 0.0
    if current_candle['body_size'] < MIN_BODY_SIZE:
        return False, f"Body too small ({current_candle['body_size']:.2f})", {}, {}, 0.0

    direction = current_candle['structure']
    if direction == "BULLISH":
        favored_dir, market_prob, counter_prob = "UP",   poly_data["up_prob"],   poly_data["down_prob"]
    else:
        favored_dir, market_prob, counter_prob = "DOWN", poly_data["down_prob"], poly_data["up_prob"]

    if market_prob > MAX_CROWD_PROB_TO_CALL:
        return False, f"Crowd already at {market_prob:.1f}% -- payout negligible", {}, {}, 0.0

    strike    = poly_data.get("strike_price", 0.0)
    math_prob = calculate_strike_probability(
        current_candle['close'], strike, history, seconds_left, favored_dir
    )
    ev         = compute_ev(math_prob,     market_prob,  MAX_BET)
    counter_ev = compute_ev(100-math_prob, counter_prob, MAX_BET)
    best_ev    = max(ev["ev_pct"], counter_ev["ev_pct"])

    if best_ev < MIN_EV_PCT_TO_CALL_AI:
        return False, f"Math EV {best_ev:+.2f}% < threshold {MIN_EV_PCT_TO_CALL_AI}%", ev, counter_ev, math_prob

    return True, "", ev, counter_ev, math_prob

# ============================================================
# DETERMINISTIC RULE ENGINE
# ============================================================
def rule_engine_decide(current_candle: dict, history: list,
                        poly_data: dict, ev: dict, math_prob: float) -> dict:
    ctx           = build_technical_context(current_candle, history)
    current_price = current_candle['close']
    strike        = poly_data.get("strike_price", 0.0)
    ev_pct        = ev.get("ev_pct", 0.0)

    vwap      = ctx["vwap"]
    ema_9     = ctx["ema_9"]
    ema_21    = ctx["ema_21"]
    rsi       = ctx["rsi"]
    cvd_delta = ctx["cvd_candle_delta"]
    cvd_div   = ctx["cvd_divergence"]

    if cvd_div:
        return {"decision": "SKIP", "confidence": "Low", "bet_size": 0.0,
                "score": 0, "reason": "CVD divergence detected", "needs_ai": False}
    if rsi > 75:
        return {"decision": "SKIP", "confidence": "Low", "bet_size": 0.0,
                "score": 0, "reason": f"RSI overbought ({rsi})", "needs_ai": False}
    if rsi < 25:
        return {"decision": "SKIP", "confidence": "Low", "bet_size": 0.0,
                "score": 0, "reason": f"RSI oversold ({rsi})", "needs_ai": False}

    if strike > 0:
        favored_dir = "UP" if current_price > strike else "DOWN"
    else:
        favored_dir = "UP" if current_candle['structure'] == "BULLISH" else "DOWN"
        log.warning("[RULE] Strike=0 -- direction from candle structure")

    market_prob = poly_data["up_prob"] if favored_dir == "UP" else poly_data["down_prob"]
    score       = 0
    signal_log  = []

    # Signal 1: VWAP
    if favored_dir == "UP":
        if current_price > vwap:  score += 1; signal_log.append(f"Above VWAP +${current_price - vwap:.0f} (+1)")
        else:                     score -= 1; signal_log.append(f"Below VWAP -${vwap - current_price:.0f} (-1)")
    else:
        if current_price < vwap:  score += 1; signal_log.append(f"Below VWAP -${vwap - current_price:.0f} (+1)")
        else:                     score -= 1; signal_log.append(f"Above VWAP +${current_price - vwap:.0f} (-1)")

    # Signal 2: EMA cross
    if favored_dir == "UP":
        if ema_9 > ema_21:  score += 1; signal_log.append("9-EMA > 21-EMA (+1)")
        else:               score -= 1; signal_log.append("9-EMA < 21-EMA (-1)")
    else:
        if ema_9 < ema_21:  score += 1; signal_log.append("9-EMA < 21-EMA (+1)")
        else:               score -= 1; signal_log.append("9-EMA > 21-EMA (-1)")

    # Signal 3: CVD
    if favored_dir == "UP":
        if cvd_delta > 200:    score += 1; signal_log.append(f"CVD buy +${cvd_delta:,.0f} (+1)")
        elif cvd_delta < -200: score -= 1; signal_log.append(f"CVD sell ${cvd_delta:,.0f} (-1)")
        else:                  signal_log.append("CVD neutral (0)")
    else:
        if cvd_delta < -200:   score += 1; signal_log.append(f"CVD sell ${cvd_delta:,.0f} (+1)")
        elif cvd_delta > 200:  score -= 1; signal_log.append(f"CVD buy +${cvd_delta:,.0f} (-1)")
        else:                  signal_log.append("CVD neutral (0)")

    # Signal 4: Math edge
    if strike > 0:
        edge = math_prob - market_prob
        if edge > 3:    score += 1; signal_log.append(f"Math edge +{edge:.1f}% vs crowd (+1)")
        elif edge < -3: score -= 1; signal_log.append(f"Math edge {edge:.1f}% vs crowd (-1)")
        else:           signal_log.append(f"Math edge {edge:+.1f}% (0)")

    reason_str = " | ".join(signal_log)

    if score >= 3:
        bet_size   = ev["kelly_bet"] if ev_pct >= 3.0 else 1.0
        confidence = "High" if score == 4 else "Medium"
        return {"decision": favored_dir, "confidence": confidence,
                "bet_size": bet_size, "score": score, "reason": reason_str, "needs_ai": False}
    elif score == 2:
        return {"decision": favored_dir, "confidence": "Medium",
                "bet_size": 1.0, "score": score, "reason": reason_str, "needs_ai": False}
    elif score == 1 and ev_pct >= 3.0:
        return {"decision": favored_dir, "confidence": "Low", "bet_size": 1.0,
                "score": score, "reason": reason_str, "needs_ai": True}
    elif score <= -1:
        return {"decision": "SKIP", "confidence": "Low", "bet_size": 0.0, "score": score,
                "reason": f"Signals oppose {favored_dir} (score={score}). {reason_str}",
                "needs_ai": False}
    else:
        return {"decision": "SKIP", "confidence": "Low", "bet_size": 0.0, "score": score,
                "reason": f"Neutral (score=0). {reason_str}", "needs_ai": False}

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
        log.warning(f"[CIRCUIT] AI circuit OPEN ({int(ai_circuit_open_until - now)}s). Falling back.")
        _commit_decision(slug, rule_decision, poly_data, current_ev_pct=ev.get("ev_pct", 0.0))
        return

    if slug in committed_slugs:
        return
    if ai_call_in_flight == slug:
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
        f"BTC binary market. Score=2/4 signals agree with {favored_dir}.\n"
        f"Price: ${current_price:,.2f} | Strike: ${strike:,.2f} | Distance: {distance:+.2f}\n"
        f"VWAP: ${ctx['vwap']:,.2f} ({ctx['vwap_signal']}) | RSI: {ctx['rsi']} | "
        f"EMA: {ctx['ema_cross']}\n"
        f"CVD this candle: ${ctx['cvd_candle_delta']:+,.0f} | Last 1min: ${ctx['cvd_1min']:+,.0f}\n"
        f"Math prob {favored_dir}: {math_prob:.1f}% | Crowd: {market_prob:.1f}% | "
        f"EV: {ev['ev_pct']:+.2f}% | Time: {seconds_left}s\n"
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
                _commit_decision(slug, {**rule_decision, "decision": "SKIP",
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
        # AI vetoed — soft skip, not a hard lock. Conditions may still improve.
        final = {**rule_decision, "decision": "SKIP", "bet_size": 0.0,
                 "reason": "AI vetoed borderline signal"}
    else:
        log.warning(f"[AI CONFIRM] Unparseable '{ai_word}' -- using rule engine as-is")
        final = rule_decision

    current_ev_pct = ev.get("ev_pct", 0.0)
    _commit_decision(slug, final, poly_data, current_ev_pct=current_ev_pct)
    ai_call_in_flight = ""


def _commit_decision(slug: str, result: dict, poly_data: dict, current_ev_pct: float = 0.0):
    strike   = poly_data.get("strike_price", 0.0)
    decision = result["decision"]

    if decision in ["UP", "DOWN"]:
        # Hard lock — this market is done, no re-evaluation ever.
        committed_slugs.add(slug)
        soft_skipped_slugs.discard(slug)
        best_ev_seen.pop(slug, None)
        active_predictions[slug] = {"decision": decision, "strike": strike}
        log.info(
            f"\n{'━'*55}\n"
            f"  DECISION LOCKED: {decision}  |  Confidence: {result.get('confidence','?')}  |  Bet: ${result.get('bet_size',0):.2f}\n"
            f"  Score: {result.get('score','?')}/4  |  Strike: ${strike:,.2f}\n"
            f"  Reason: {result.get('reason','')}\n"
            f"{'━'*55}"
        )
        # Place the bet — fire-and-forget so the engine never blocks on I/O
        asyncio.create_task(
            place_bet(slug, decision, result.get("bet_size", 0.0), poly_data)
        )
    else:
        # Soft skip — remember it but stay re-evaluable.
        soft_skipped_slugs.add(slug)
        # Record the EV at the time of this skip so we only re-fire when it genuinely improves.
        prev_best = best_ev_seen.get(slug, -999.0)
        best_ev_seen[slug] = max(prev_best, current_ev_pct)
        log.info(f"[SKIP] {slug} -- {result.get('reason','')} (EV={current_ev_pct:+.2f}%, best={best_ev_seen[slug]:+.2f}%)")

# ============================================================
# BET PLACEMENT  (fires once per committed UP/DOWN decision)
# ============================================================
async def place_bet(slug: str, decision: str, bet_size: float, poly_data: dict):
    """
    Places a FOK market order on Polymarket via the CLOB API.
    - decision: "UP" buys the YES/UP token; "DOWN" buys the NO/DOWN token
    - bet_size: USDC amount to spend
    - Uses DRY_RUN flag: if True, logs the order but never submits it
    - All errors are caught and logged — a failed bet never crashes the engine
    """
    global clob_client

    token_id = (
        poly_data.get("token_id_up",   "")
        if decision == "UP"
        else poly_data.get("token_id_down", "")
    )
    market_prob = poly_data["up_prob"] if decision == "UP" else poly_data["down_prob"]
    # Price for a FOK market order: use current market price (probability / 100)
    # Polymarket prices are in cents (0.00–1.00)
    price = round(market_prob / 100.0, 4)

    if not token_id:
        log.error(f"[BET] ✗ No token_id for {decision} on {slug} — cannot place bet")
        return

    if DRY_RUN:
        log.info(
            f"\n{'─'*55}\n"
            f"  [DRY RUN] Would place bet:\n"
            f"    Market:   {slug}\n"
            f"    Decision: {decision}  (token: ...{token_id[-12:]})\n"
            f"    Size:     ${bet_size:.2f} USDC\n"
            f"    Price:    {price:.4f} ({market_prob:.1f}%)\n"
            f"  Set DRY_RUN=false in .env to trade live.\n"
            f"{'─'*55}"
        )
        return

    if clob_client is None:
        log.error("[BET] ✗ CLOB client not initialised — check POLY_PRIVATE_KEY in .env")
        return

    try:
        from py_clob_client.clob_types import MarketOrderArgs, OrderType
        from py_clob_client.order_builder.constants import BUY

        order_args = MarketOrderArgs(
            token_id   = token_id,
            amount     = bet_size,   # USDC amount (not shares)
            side       = BUY,
        )

        # create_market_order and post_order are synchronous — run in thread
        signed = await asyncio.to_thread(clob_client.create_market_order, order_args)
        resp   = await asyncio.to_thread(clob_client.post_order, signed, OrderType.FOK)

        status     = resp.get("status",    "unknown")
        order_id   = resp.get("orderID",   resp.get("id", "?"))
        error_msg  = resp.get("errorMsg",  "")
        filled     = resp.get("sizeFilled", "?")

        if status == "matched":
            log.info(
                f"\n{'━'*55}\n"
                f"  ✅ BET PLACED: {decision} on {slug}\n"
                f"     Order ID:  {order_id}\n"
                f"     Size:      ${bet_size:.2f} USDC  |  Filled: {filled}\n"
                f"     Price:     {price:.4f} ({market_prob:.1f}%)\n"
                f"{'━'*55}"
            )
        else:
            log.warning(
                f"[BET] ⚠️  Order not matched: status={status} "
                f"id={order_id} err='{error_msg}'"
            )

    except Exception as e:
        log.error(f"[BET] ✗ Exception placing bet for {slug}: {type(e).__name__}: {e}")

# ============================================================
# CONCURRENT TASK 1: kline stream
#   - Updates live_candle and live_price on every tick
#   - On candle close: appends to history, updates VWAP, snapshots CVD
# ============================================================
async def kline_stream_loop():
    global live_candle, live_price, cvd_snapshot_at_candle_open
    global target_slug, total_wins, total_losses, ai_call_in_flight

    while True:
        try:
            log.info("[KLINE] Connecting to Binance kline stream...")
            async with websockets.connect(SOCKET_KLINE) as ws:
                async for message in ws:
                    raw        = json.loads(message)
                    k          = raw['k']

                    # Always keep live_candle fresh (used by eval loop between closes)
                    live_candle = k

                    # live_price updated here as a fallback if aggTrade is lagging
                    if live_price == 0.0:
                        live_price = float(k['c'])

                    # ── Candle CLOSE ──────────────────────────────────
                    if k['x']:
                        candle = parse_candle(k, override_close=live_price)
                        candle_history.append(candle)
                        if len(candle_history) > MAX_HISTORY:
                            candle_history.pop(0)
                        update_vwap(candle)
                        # Snapshot CVD at each candle boundary so delta resets
                        cvd_snapshot_at_candle_open = cvd_total
                        log.info(
                            f"[CANDLE] {candle['structure']} | "
                            f"O:{candle['open']:.2f} H:{candle['high']:.2f} "
                            f"L:{candle['low']:.2f} C:{candle['close']:.2f} | "
                            f"VWAP: ${get_vwap():,.2f}"
                        )

        except Exception as e:
            log.warning(f"[KLINE] Reconnecting in 3s... ({e})")
            await asyncio.sleep(3)


# ============================================================
# CONCURRENT TASK 2: aggTrade stream
#   - Updates live_price and CVD on every trade tick
# ============================================================
async def agg_trade_listener():
    log.info("[CVD] Connecting to aggTrade stream...")
    while True:
        try:
            async with websockets.connect(SOCKET_TRADE) as ws:
                log.info("[CVD] aggTrade stream connected.")
                async for message in ws:
                    process_agg_trade(json.loads(message))
        except Exception as e:
            log.warning(f"[CVD] aggTrade error: {e} -- reconnecting in 3s")
            await asyncio.sleep(3)


# ============================================================
# CONCURRENT TASK 3: evaluation loop
#   - Fires every EVAL_TICK_SECONDS
#   - Uses live_price + live_candle (not gated on candle close)
#   - UP/DOWN decisions are committed and locked forever per market
#   - SKIP decisions are soft — rule engine re-runs every tick freely;
#     commits only when signals flip to UP/DOWN
# ============================================================
async def evaluation_loop(session: aiohttp.ClientSession):
    global target_slug, total_wins, total_losses, ai_call_in_flight

    log.info(f"[EVAL] Evaluation loop starting — tick every {EVAL_TICK_SECONDS}s")
    await asyncio.sleep(EVAL_TICK_SECONDS)   # let streams connect first

    while True:
        await asyncio.sleep(EVAL_TICK_SECONDS)

        # ── Need at least some history before evaluating ──────
        if not candle_history or not live_candle:
            continue

        slug          = target_slug
        current_price = live_price if live_price > 0 else float(live_candle.get('c', 0))
        if current_price == 0:
            continue

        # ── Build a synthetic "current candle" from live kline data ──
        # This gives the rule engine OHLCV without waiting for close.
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

        # ── Status line ───────────────────────────────────────
        print(
            f"\r  [{datetime.now(timezone.utc).strftime('%H:%M:%S')}] "
            f"Price: ${current_price:,.2f} | VWAP: ${get_vwap():,.2f} | "
            f"CVD(1m): ${last_cvd_1min:+,.0f} | Slug: {slug}   ",
            end="", flush=True
        )

        # ── Fetch (cached) Polymarket odds ────────────────────
        poly_data = await get_polymarket_odds_cached(session, slug)

        if not poly_data.get("market_found"):
            log.warning(f"[EVAL] {slug} not found on Polymarket -- waiting...")
            continue

        # ── Fallback strike if Chainlink returned 0 ───────────
        if poly_data["strike_price"] == 0.0:
            if slug not in market_open_prices:
                market_open_prices[slug] = current_price
                log.info(f"[STRIKE] ⚠️  Chainlink unavailable. Open-price snapshot: ${current_price:,.2f}")
            poly_data = {**poly_data, "strike_price": market_open_prices[slug]}

        secs = poly_data.get("seconds_remaining", 0)

        # ── Market expiry: evaluate result and roll ───────────
        if secs <= 0:
            if slug in active_predictions:
                pred     = active_predictions[slug]
                decision = pred["decision"]
                strike   = pred["strike"]

                if   current_price > strike: actual_outcome = "UP"
                elif current_price < strike: actual_outcome = "DOWN"
                else:                        actual_outcome = "TIE"

                if decision == actual_outcome:
                    total_wins  += 1; result_str = "WIN"
                    log.info(f"[WIN]  {slug}: {decision} ✓ | ${strike:,.2f} → ${current_price:,.2f}")
                elif actual_outcome == "TIE":
                    result_str = "TIE"
                    log.info(f"[TIE]  {slug}: flatline on strike")
                else:
                    total_losses += 1; result_str = "LOSS"
                    log.info(f"[LOSS] {slug}: predicted {decision}, actual {actual_outcome} | ${strike:,.2f} → ${current_price:,.2f}")

                total_trades = total_wins + total_losses
                win_rate     = (total_wins / total_trades) * 100 if total_trades > 0 else 0.0
                log.info(f"[STATS] W:{total_wins} L:{total_losses} | WinRate:{win_rate:.2f}%")
                log_trade_to_csv(slug, decision, strike, current_price,
                                 actual_outcome, result_str, win_rate)
                del active_predictions[slug]

            old_slug    = slug
            target_slug = increment_slug_by_300(slug)

            # Clean up all per-market state
            committed_slugs.discard(old_slug)
            soft_skipped_slugs.discard(old_slug)
            best_ev_seen.pop(old_slug, None)
            best_ev_seen.pop(f"{old_slug}__score", None)
            market_open_prices.pop(old_slug, None)
            strike_price_cache.pop(old_slug, None)
            if ai_call_in_flight == old_slug:
                ai_call_in_flight = ""

            # Invalidate poly cache so next tick fetches fresh data for new slug
            global _poly_cache_slug
            _poly_cache_slug = ""

            log.info(f"[ROLL] {old_slug} → {target_slug}")
            asyncio.create_task(_prefetch_strike(session, target_slug))
            continue

        # ── Hard gate: committed UP/DOWN — never re-evaluate ─────────────
        if slug in committed_slugs:
            log.info(f"[LOCK] {slug} committed -- holding ({int(secs)}s left).")
            continue
        if ai_call_in_flight == slug:
            log.info(f"[LOCK] AI confirming {slug} -- waiting.")
            continue

        # ── Gatekeeper ────────────────────────────────────────────────────
        # Gatekeeper failures (too early, EV too low, body too small, etc.)
        # are transient conditions — always re-check freely, never soft-skip.
        should_call, skip_reason, ev, counter_ev, math_prob = \
            run_gatekeeper(current_candle, candle_history, poly_data)

        if not should_call:
            log.info(f"[GATE] {skip_reason}")
            continue

        # ── Rule engine — runs on every tick that passes the gatekeeper ──
        # The rule engine itself is cheap (pure math, no I/O).
        # We always run it so we can detect when signals improve.
        current_ev_pct = max(ev.get("ev_pct", 0.0), counter_ev.get("ev_pct", 0.0))
        result = rule_engine_decide(current_candle, candle_history, poly_data, ev, math_prob)

        if result["decision"] in ("UP", "DOWN"):
            # ── Positive signal: commit regardless of prior soft-skip ────
            if slug in soft_skipped_slugs:
                log.info(
                    f"[REEVAL] ✓ {slug} previously skipped — signals now favour "
                    f"{result['decision']} (score={result['score']}/4, "
                    f"EV={current_ev_pct:+.2f}%) — committing!"
                )
                soft_skipped_slugs.discard(slug)
                best_ev_seen.pop(slug, None)

            log.info(f"[RULE] Score={result['score']}/4 → {result['decision']} | {result['reason']}")

            if not result["needs_ai"]:
                _commit_decision(slug, result, poly_data, current_ev_pct=current_ev_pct)
            else:
                asyncio.create_task(
                    call_local_ai(session, current_candle, candle_history,
                                  poly_data, ev, counter_ev, math_prob, slug, result)
                )

        else:
            # ── Rule engine SKIP ─────────────────────────────────────────
            # Only log + update best_ev if score/EV actually changed, to avoid
            # flooding the log on every 5s tick.
            prev_score = best_ev_seen.get(f"{slug}__score", None)
            cur_score  = result.get("score", 0)

            if prev_score != cur_score:
                log.info(
                    f"[RULE] Score={cur_score}/4 → SKIP | {result['reason']} "
                    f"(EV={current_ev_pct:+.2f}%)"
                )
                best_ev_seen[f"{slug}__score"] = cur_score

            # Mark as soft-skipped so _commit_decision knows the context,
            # but leave it open for re-evaluation on the next tick.
            soft_skipped_slugs.add(slug)
            best_ev_seen[slug] = max(best_ev_seen.get(slug, -999.0), current_ev_pct)

# ============================================================
# CONCURRENT TASK 4: polymarket loop
# =========
async def polymarket_rtds_loop():
    """
    Listens to Polymarket's own price feed (Binance and Chainlink sources).
    Useful for detecting 'Oracle Lag' before settlement.
    """
    global poly_live_binance, poly_live_chainlink
    url = "wss://ws-live-data.polymarket.com"
    
    while True:
        try:
            async with websockets.connect(url) as ws:
                # Helper to send heartbeat PINGs every 5s
                async def heartbeat():
                    while True:
                        await asyncio.sleep(5)
                        await ws.send("PING")
                
                heartbeat_task = asyncio.create_task(heartbeat())
                
                sub_msg = {
                    "action": "subscribe",
                    "subscriptions": [
                        {"topic": "crypto_prices", "type": "update", "filters": "btcusdt"},
                        {"topic": "crypto_prices_chainlink", "type": "*", "filters": "{\"symbol\":\"btc/usd\"}"}
                    ]
                }
                await ws.send(json.dumps(sub_msg))
                log.info("[POLY-RTDS] Subscribed to Binance & Chainlink.")

                async for message in ws:
                    # Handle PING/PONG (Send PING every 5s)
                    # Note: Most libs handle PONG automatically if you send PING.
                    data = json.loads(message)
                    topic = data.get("topic")
                    payload = data.get("payload", {})

                    if topic == "crypto_prices":
                        poly_live_binance = float(payload.get("value", 0))
                    elif topic == "crypto_prices_chainlink":
                        poly_live_chainlink = float(payload.get("value", 0))

        except Exception as e:
            log.warning(f"[POLY-RTDS] Error: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)
# ============================================================
# HELPER
# ============================================================
async def _prefetch_strike(session: aiohttp.ClientSession, slug: str):
    await asyncio.sleep(3)
    price = await fetch_price_to_beat_for_market(session, slug)
    if price > 0:
        log.info(f"[STRIKE] Pre-fetched next market strike: ${price:,.2f} for '{slug}'")
    else:
        log.warning(f"[STRIKE] Pre-fetch returned 0 for '{slug}' -- will retry on next eval tick.")


# ============================================================
# ENTRY POINT
# ============================================================
async def main():
    global target_slug, market_family_prefix, clob_client

    # ── Initialise CLOB client ────────────────────────────────
    if not POLY_PRIVATE_KEY:
        log.warning("[CLOB] POLY_PRIVATE_KEY not set — running in DRY_RUN mode regardless of .env")
    else:
        try:
            from py_clob_client.client import ClobClient
            _kwargs = dict(host=CLOB_HOST, key=POLY_PRIVATE_KEY, chain_id=CHAIN_ID)
            if POLY_FUNDER:
                _kwargs["signature_type"] = POLY_SIG_TYPE
                _kwargs["funder"]         = POLY_FUNDER
            clob_client = ClobClient(**_kwargs)
            # Derive/create API credentials (L2 auth header) — required for order placement
            clob_client.set_api_creds(clob_client.create_or_derive_api_creds())
            log.info(f"[CLOB] Client initialised. DRY_RUN={DRY_RUN}")
        except ImportError:
            log.error("[CLOB] py-clob-client not installed. Run: pip install py-clob-client")
            log.error("[CLOB] Continuing in DRY_RUN mode.")
        except Exception as e:
            log.error(f"[CLOB] Failed to initialise client: {e}")
            log.error("[CLOB] Continuing in DRY_RUN mode.")

    async with aiohttp.ClientSession() as session:
        # Seed history and indicators before anything else
        await prefill_history(session)

        # Pre-fetch strike for seed market
        initial_strike = await fetch_price_to_beat_for_market(session, target_slug)
        if initial_strike > 0:
            log.info(f"[STRIKE] Seed market strike pre-loaded: ${initial_strike:,.2f}")
        else:
            log.warning("[STRIKE] Could not pre-load seed strike -- will fetch on first eval tick.")

        # Launch all three concurrent loops
        await asyncio.gather(
            kline_stream_loop(),       # candle history + VWAP
            agg_trade_listener(),      # live_price + CVD
            polymarket_rtds_loop(),
            evaluation_loop(session),  # decision engine, fires every EVAL_TICK_SECONDS
        )


if __name__ == "__main__":
    user_market = input("[INPUT] Enter current Polymarket BTC market URL or slug: ").strip()
    target_slug = extract_slug_from_market_url(user_market)
    market_family_prefix = build_market_family_prefix(target_slug)

    if not target_slug:
        print("[ERROR] Invalid market URL/slug. Please restart.")
        raise SystemExit(1)

    print("\n" + "="*60)
    print("  BTC/USDT → Polymarket Arbitrage Engine")
    print("="*60)
    print(f"\n  Seed slug:          {target_slug}")
    print(f"  Family prefix:      {market_family_prefix}")
    print(f"  Local AI endpoint:  {LOCAL_AI_URL}")
    print(f"  Local AI model:     {LOCAL_AI_MODEL}")
    print(f"  History window:     {MAX_HISTORY} candles")
    print(f"\n  Betting:")
    print(f"    Mode:             {'⚠️  DRY RUN (no real orders)' if DRY_RUN else '🔴 LIVE — real USDC at risk'}")
    print(f"    Wallet key set:   {'Yes' if POLY_PRIVATE_KEY else 'No (set POLY_PRIVATE_KEY in .env)'}")
    print(f"    Funder set:       {'Yes' if POLY_FUNDER else 'No (set POLY_FUNDER in .env)'}")
    print(f"    Sig type:         {POLY_SIG_TYPE}  (0=EOA, 1=email/Magic, 2=MetaMask proxy)")
    print(f"    Max bet:          ${MAX_BET:.2f} USDC")
    print(f"\n  Concurrent tasks:")
    print(f"    ✓ kline_stream_loop   — candle history, VWAP, CVD snapshot")
    print(f"    ✓ agg_trade_listener  — live price, CVD (trade-level)")
    print(f"    ✓ evaluation_loop     — fires every {EVAL_TICK_SECONDS}s, one decision per market")
    print(f"\n  Thresholds:")
    print(f"    Min EV:           {MIN_EV_PCT_TO_CALL_AI}%")
    print(f"    Time window:      {MIN_SECONDS_REMAINING}s – {MAX_SECONDS_FOR_NEW_BET}s")
    print(f"    Min body:         {MIN_BODY_SIZE} USDT")
    print(f"    Max crowd:        {MAX_CROWD_PROB_TO_CALL}%")
    print(f"\n  Strike price:")
    print(f"    ✓ Chainlink API (fetch_price_to_beat)")
    print(f"    ✓ Cached per market slug")
    print(f"    ✓ Pre-fetched at startup and on market roll")
    print(f"    ✓ Open-price fallback if API unavailable")
    print(f"\n  AI protection:")
    print(f"    Connect: {AI_TIMEOUT_CONNECT}s | Total: {AI_TIMEOUT_TOTAL}s | "
          f"Retries: {AI_MAX_RETRIES}x | CB: {CB_FAILURE_THRESHOLD} fails → {CB_COOLDOWN_SECS}s cooldown")

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[SYSTEM] Engine stopped.")