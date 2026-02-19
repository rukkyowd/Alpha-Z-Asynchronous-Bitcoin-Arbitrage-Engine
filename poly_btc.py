import asyncio
import aiohttp
import websockets
import json
import logging
import sys
import re
import time
import math
import io
from urllib.parse import urlparse
from datetime import datetime, timezone

# ============================================================
# CONFIG
# ============================================================
SOCKET         = "wss://stream.binance.com:9443/ws/btcusdt@kline_1m"

# ── Multi-key rotation ──────────────────────────────────────
# Key A is used on even-numbered minutes, Key B on odd-numbered minutes.
# This effectively doubles your free-tier rate limit.
GEMINI_KEY_A   = "PASTE_YOUR_FIRST_GEMINI_API_KEY_HERE"   # <-- Account 1
GEMINI_KEY_B   = "PASTE_YOUR_SECOND_GEMINI_API_KEY_HERE"       # <-- Account 2

GEMINI_MODEL   = "gemini-2.0-flash"

def get_gemini_url() -> str:
    """Returns the correct Gemini endpoint based on the current minute (even=A, odd=B)."""
    minute = datetime.now().minute
    key    = GEMINI_KEY_A if minute % 2 == 0 else GEMINI_KEY_B
    label  = "A" if minute % 2 == 0 else "B"
    log.debug(f"[KEY ROUTER] Minute {minute} → Key {label}")
    return f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={key}"

BANKROLL       = 15.0
MAX_BET        = 2.0

# ── Thresholds (lowered to maximise call frequency) ─────────
MIN_EV_PCT_TO_CALL_GEMINI = 0.1   # was 2.0  -- fire on almost any edge
MIN_SECONDS_REMAINING     = 10    # was 30   -- trade closer to expiry
MAX_SECONDS_FOR_NEW_BET   = 295   # was 285  -- enter a little earlier
MIN_BODY_SIZE             = 0.1   # was 1.5  -- accept near-doji candles
MAX_CROWD_PROB_TO_CALL    = 97.0  # was 92.0 -- skip only extreme lock-ins

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
gemini_call_count = 0
gemini_skip_count = 0
candle_history: list[dict] = []
MAX_HISTORY = 10
target_slug: str = ""
market_family_prefix: str = ""

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

def parse_candle(raw: dict) -> dict:
    o, h, l, c, v = float(raw['o']), float(raw['h']), float(raw['l']), float(raw['c']), float(raw['v'])
    return {
        "timestamp":  datetime.fromtimestamp(raw['t']/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
        "open": o, "high": h, "low": l, "close": c, "volume": v,
        "body_size": abs(c-o), "upper_wick": h-max(o,c), "lower_wick": min(o,c)-l,
        "structure": "BULLISH" if c > o else "BEARISH",
    }

# ============================================================
# QUANTITATIVE MATH ENGINE
# ============================================================
def calculate_strike_probability(current_price: float, strike_price: float, history: list, seconds_remaining: float, direction: str) -> float:
    if seconds_remaining <= 0 or not history or strike_price <= 0:
        return 50.0

    minutes_remaining = seconds_remaining / 60.0
    closes = [c['close'] for c in history] + [current_price]
    if len(closes) < 2: return 50.0

    mean_close = sum(closes) / len(closes)
    variance = sum((x - mean_close)**2 for x in closes) / len(closes)
    std_dev = math.sqrt(variance)

    if std_dev < 1.0: std_dev = 1.0

    time_scaled_vol = std_dev * math.sqrt(minutes_remaining)
    distance = current_price - strike_price
    z_score = distance / time_scaled_vol

    prob = 0.5 * (1.0 + math.erf(z_score / math.sqrt(2.0)))

    if direction == "DOWN":
        prob = 1.0 - prob

    return max(1.0, min(prob * 100.0, 99.0))

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
    kelly_bet      = round(min((kelly_fraction * 0.5) * max_bet, max_bet), 2)
    return {"ev": round(ev,4), "ev_pct": round(ev_pct,2), "edge": round(edge,4),
            "kelly_fraction": round(kelly_fraction,4), "kelly_bet": kelly_bet}

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
        elif upper > body and lower > body:         wick_bias = "Indecision (both wicks prominent)"
        else:                                       wick_bias = "Clean body -- no strong wick rejection"
    else:
        wick_bias = "Doji / near-doji"
    
    history_table = "\n".join([
        f"  [{c['timestamp']}] {c['structure']:<8} | O:{c['open']:.2f} C:{c['close']:.2f} | Body:{c['body_size']:.2f} | Vol:{c['volume']:.2f}"
        for c in recent_5
    ]) or "  (no history yet)"
    
    return {"direction": direction, "vol_delta_pct": vol_delta_pct, "vol_vs_avg_pct": vol_vs_avg_pct,
            "streak_text": streak_text, "wick_bias": wick_bias, "history_table": history_table}

# ============================================================
# ASYNC I/O OPERATIONS
# ============================================================
async def find_active_5m_btc_market(session: aiohttp.ClientSession) -> str:
    url = "https://gamma-api.polymarket.com/events"
    params = {"active": "true", "closed": "false", "limit": 100}
    best_slug = ""
    best_secs = float('inf')

    try:
        async with session.get(url, params=params, timeout=5) as r:
            r.raise_for_status()
            events = await r.json()

            for event in events:
                slug = event.get("slug", "").lower()
                if "btc-updown-5m" in slug or "btc-up-or-down-5" in slug:
                    secs_left = _parse_seconds_remaining(event.get("endDate", ""))
                    if 0 < secs_left < best_secs:
                        best_secs = secs_left
                        best_slug = slug

        if best_slug:
            log.info(f"[HUNTER] Locked LIVE market: {best_slug} (~{int(best_secs)}s remaining)")
        else:
            log.warning("[HUNTER] No active live market found under 5m.")
        return best_slug
    except Exception as e:
        log.error(f"[HUNTER] Error: {e}")
        return ""

def extract_slug_from_market_url(raw_input: str) -> str:
    cleaned = (raw_input or "").strip()
    if not cleaned:
        return ""
    if "/event/" in cleaned:
        path = urlparse(cleaned).path
        slug = path.split("/event/", 1)[-1].strip("/")
    else:
        slug = cleaned.strip("/")
    return slug.lower()

def build_market_family_prefix(seed_slug: str) -> str:
    if not seed_slug:
        return ""
    known_prefix = re.match(r"^(btc-(?:updown|up-or-down)(?:-[0-9]+m?)?)", seed_slug)
    if known_prefix:
        return known_prefix.group(1)
    parts = seed_slug.split("-")
    return "-".join(parts[:4]) if len(parts) >= 4 else seed_slug

async def find_next_market_in_family(session: aiohttp.ClientSession, family_prefix: str, previous_slug: str = "") -> str:
    if not family_prefix:
        return ""

    url = "https://gamma-api.polymarket.com/events"
    params = {"active": "true", "closed": "false", "limit": 200}
    best_slug = ""
    best_secs = float('inf')

    try:
        async with session.get(url, params=params, timeout=5) as r:
            r.raise_for_status()
            events = await r.json()

        for event in events:
            slug = event.get("slug", "").lower()
            if not slug.startswith(family_prefix):
                continue

            secs_left = _parse_seconds_remaining(event.get("endDate", ""))
            if secs_left <= 0:
                continue

            if slug == previous_slug and secs_left > 0:
                return slug

            if secs_left < best_secs:
                best_secs = secs_left
                best_slug = slug

        if best_slug:
            log.info(f"[HUNTER] Rolled to next market in family: {best_slug} (~{int(best_secs)}s remaining)")
        else:
            log.warning(f"[HUNTER] No active market found for family prefix '{family_prefix}'.")
        return best_slug
    except Exception as e:
        log.error(f"[HUNTER] Error while finding next family market: {e}")
        return ""

async def prefill_history(session: aiohttp.ClientSession):
    log.info("[SYSTEM] Prefilling history from Binance REST API...")
    try:
        async with session.get("https://api.binance.com/api/v3/klines", params={"symbol": "BTCUSDT", "interval": "1m", "limit": 10}, timeout=5) as r:
            r.raise_for_status()
            data = await r.json()
            for k in data:
                o, h, l, c, v = float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5])
                candle_history.append({
                    "timestamp":  datetime.fromtimestamp(k[0]/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                    "open": o, "high": h, "low": l, "close": c, "volume": v,
                    "body_size": abs(c-o), "upper_wick": h-max(o,c), "lower_wick": min(o,c)-l,
                    "structure": "BULLISH" if c > o else "BEARISH",
                })
            log.info(f"[OK] {len(candle_history)} candles loaded.")
    except Exception as e:
        log.error(f"[ERROR] Prefill failed: {e}")

async def get_polymarket_odds(session: aiohttp.ClientSession, slug: str) -> dict:
    if not slug: return {"market_found": False, "error": "No slug"}
    try:
        async with session.get("https://gamma-api.polymarket.com/events", params={"slug": slug}, timeout=6) as r:
            r.raise_for_status()
            data = await r.json()
            for event in data:
                if event.get("slug", "") != slug: continue
                markets = event.get("markets", [])
                if not markets: return {"market_found": False, "error": "No markets"}
                
                market       = markets[0]
                description  = event.get("description", "")
                sm           = re.search(r'\$([0-9,]+\.[0-9]{2})', description)
                strike_price = float(sm.group(1).replace(',', '')) if sm else 0.0
                
                seconds_remaining = _parse_seconds_remaining(event.get("endDate", ""))
                
                raw_prices = market.get("outcomePrices", "[]")
                prices     = json.loads(raw_prices) if isinstance(raw_prices, str) else raw_prices
                raw_out    = market.get("outcomes", "[]")
                outcomes   = json.loads(raw_out) if isinstance(raw_out, str) else raw_out
                
                if len(prices) < 2 or len(outcomes) < 2: return {"market_found": False, "error": "Bad shape"}
                return {
                    "market_found": True, "title": event.get("title", slug),
                    "up_prob": float(prices[0])*100, "down_prob": float(prices[1])*100,
                    "strike_price": strike_price, "seconds_remaining": seconds_remaining,
                }
            return {"market_found": False, "error": f"Slug '{slug}' not in response"}
    except Exception as e:
        return {"market_found": False, "error": str(e)}

# ============================================================
# GATEKEEPER & BACKGROUND TASK
# ============================================================
def run_gatekeeper(current_candle: dict, history: list, poly_data: dict) -> tuple:
    if not poly_data["market_found"]: return False, "No Polymarket data", {}, {}, 0.0
    
    seconds_left = poly_data.get("seconds_remaining", 0)
    if seconds_left < MIN_SECONDS_REMAINING:
        return False, f"Only {int(seconds_left)}s left -- time decay too severe", {}, {}, 0.0
    if seconds_left > MAX_SECONDS_FOR_NEW_BET:
        return False, f"{int(seconds_left)}s remaining -- too early", {}, {}, 0.0
    if current_candle['body_size'] < MIN_BODY_SIZE:
        return False, f"Body too small (body={current_candle['body_size']:.2f})", {}, {}, 0.0
    
    direction = current_candle['structure']
    if direction == "BULLISH":
        favored_dir, market_prob, counter_prob = "UP",   poly_data["up_prob"],   poly_data["down_prob"]
    else:
        favored_dir, market_prob, counter_prob = "DOWN", poly_data["down_prob"], poly_data["up_prob"]
        
    if market_prob > MAX_CROWD_PROB_TO_CALL:
        return False, f"Crowd already at {market_prob:.1f}% -- payout negligible", {}, {}, 0.0
        
    strike    = poly_data.get("strike_price", 0.0)
    math_prob = calculate_strike_probability(current_candle['close'], strike, history, seconds_left, favored_dir)
    
    ev         = compute_ev(math_prob,       market_prob,  MAX_BET)
    counter_ev = compute_ev(100-math_prob, counter_prob, MAX_BET)
    best_ev    = max(ev["ev_pct"], counter_ev["ev_pct"])
    
    if best_ev < MIN_EV_PCT_TO_CALL_GEMINI:
        return False, f"Math EV {best_ev:+.2f}% < {MIN_EV_PCT_TO_CALL_GEMINI}%", ev, counter_ev, math_prob
        
    return True, "", ev, counter_ev, math_prob

async def call_gemini(session: aiohttp.ClientSession, current_candle: dict, history: list, poly_data: dict, ev: dict, counter_ev: dict, math_prob: float):
    global gemini_call_count
    gemini_call_count += 1

    # ── Key rotation: pick the correct URL at call time ──────
    gemini_url  = get_gemini_url()
    active_key  = "A" if datetime.now().minute % 2 == 0 else "B"
    log.info(f"[GEMINI] Firing background API call #{gemini_call_count} (Key {active_key})...")
    
    ctx             = build_technical_context(current_candle, history)
    direction       = ctx["direction"]
    current_price   = current_candle['close']
    seconds_left    = int(poly_data.get("seconds_remaining", 0))
    strike          = poly_data.get("strike_price", 0.0)
    strike_text     = f"${strike:,.2f}" if strike > 0 else "Unknown"
    distance_text   = f"{current_price - strike:+.2f} USDT" if strike > 0 else "N/A"
    favored_dir     = "UP" if direction == "BULLISH" else "DOWN"
    market_prob     = poly_data["up_prob"] if direction == "BULLISH" else poly_data["down_prob"]
    
    ev_block = (
        f"=== QUANTITATIVE ARBITRAGE ANALYSIS ===\n"
        f"  Favored Direction:       {favored_dir}\n"
        f"  Math Prob (Vol/Time):    {math_prob:.1f}%\n"
        f"  Polymarket Crowd Prob:   {market_prob:.1f}%\n"
        f"  EV% Return on Capital:   {ev['ev_pct']:+.2f}%\n"
        f"  Kelly Fraction / Bet:    {ev['kelly_fraction']:.4f} / ${ev['kelly_bet']:.2f}\n"
    )

    prompt = (
        "You are an elite statistical arbitrage engine for Polymarket binary crypto markets.\n"
        "The gatekeeper confirmed a mathematically high-EV setup based on volatility modeling. Verify it.\n\n"
        "=== THE PRICE TO BEAT ===\n"
        f"  Polymarket Strike: {strike_text}\n"
        f"  Current BTC Price: ${current_price:,.2f}\n"
        f"  Distance to Win:   {distance_text}\n"
        f"  Time Remaining:    {seconds_left} seconds\n\n"
        "=== LATEST CLOSED 1m CANDLE ===\n"
        f"  OHLC:             {current_candle['open']:.2f} / {current_candle['high']:.2f} / {current_candle['low']:.2f} / {current_candle['close']:.2f}\n"
        f"  Structure:        {current_candle['structure']}\n"
        f"  Body Size:        {current_candle['body_size']:.2f}\n"
        f"  Wick Bias:        {ctx['wick_bias']}\n"
        f"  Vol delta 5m avg: {ctx['vol_vs_avg_pct']:+.2f}%\n\n"
        f"{ev_block}\n\n"
        "=== OUTPUT FORMAT (strict -- no other text) ===\n"
        "DECISION: UP | DOWN | SKIP\n"
        "CONFIDENCE: Low | Medium | High\n"
        f"EV: {ev['ev_pct']:+.2f}%\n"
        "BET SIZE: $0 | $1 | $2\n"
        "REASON: <One sentence citing distance to strike, volatility math, and time remaining.>"
    )
    
    payload = {"contents": [{"parts": [{"text": prompt}]}]}
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            async with session.post(gemini_url, json=payload, timeout=12) as r:
                if r.status == 429:
                    # On rate limit, immediately try the other key
                    fallback_key  = GEMINI_KEY_B if active_key == "A" else GEMINI_KEY_A
                    fallback_url  = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={fallback_key}"
                    log.warning(f"[GEMINI] Key {active_key} rate limited -- falling back to other key")
                    async with session.post(fallback_url, json=payload, timeout=12) as r2:
                        if r2.status != 200:
                            wait = 2 ** attempt
                            log.warning(f"[GEMINI] Fallback also failed -- waiting {wait}s")
                            await asyncio.sleep(wait)
                            continue
                        r = r2
                r.raise_for_status()
                data = await r.json()
                signal = data['candidates'][0]['content']['parts'][0]['text'].strip()
                log.info(f"\n{'='*52}\n{signal}\n{'='*52}")
                return
        except Exception as e:
            log.error(f"[GEMINI] Error: {e}")
            break

# ============================================================
# MAIN EVENT LOOP
# ============================================================
async def engine_loop():
    global target_slug, gemini_skip_count, market_family_prefix
    
    async with aiohttp.ClientSession() as session:
        await prefill_history(session)
        
        while True:
            try:
                log.info("[SYSTEM] Connecting to Binance WebSocket...")
                async with websockets.connect(SOCKET) as ws:
                    async for message in ws:
                        raw = json.loads(message)
                        candle_raw = raw['k']
                        print(f"\r  BTC Live: ${float(candle_raw['c']):,.2f}", end="", flush=True)
                        
                        if not candle_raw['x']:
                            continue
                            
                        print()
                        candle = parse_candle(candle_raw)
                        candle_history.append(candle)
                        if len(candle_history) > MAX_HISTORY:
                            candle_history.pop(0)
                            
                        log.info(f"[CANDLE] [{candle['timestamp']}] {candle['structure']} | O:{candle['open']:.2f} C:{candle['close']:.2f} | Vol:{candle['volume']:.2f} | Body:{candle['body_size']:.2f}")
                        
                        if not target_slug:
                            target_slug = await find_next_market_in_family(session, market_family_prefix)
                            if not target_slug:
                                continue
                                
                        poly_data = await get_polymarket_odds(session, target_slug)
                        if poly_data["market_found"]:
                            seconds_left = poly_data.get("seconds_remaining", 0)
                            if seconds_left <= 0:
                                log.info("[SYSTEM] Market expired -- initiating new hunt...")
                                target_slug = ""
                                continue
                            log.info(f"[POLYMARKET] Strike: ${poly_data.get('strike_price', 0.0):,.2f} | UP: {poly_data['up_prob']:.1f}% | DOWN: {poly_data['down_prob']:.1f}% | Remaining: {int(seconds_left)}s")
                        else:
                            target_slug = ""
                            continue
                            
                        should_call, skip_reason, ev, counter_ev, math_prob = run_gatekeeper(candle, candle_history[:-1], poly_data)
                        if not should_call:
                            gemini_skip_count += 1
                            log.info(f"[GATE] SKIP -- {skip_reason}")
                            continue
                            
                        asyncio.create_task(call_gemini(session, candle, candle_history[:-1], poly_data, ev, counter_ev, math_prob))

            except Exception as e:
                log.warning(f"[SYSTEM] Reconnecting in 5s... ({e})")
                await asyncio.sleep(5)

if __name__ == "__main__":
    user_market = input("[INPUT] Enter current Polymarket BTC market URL or slug: ").strip()
    target_slug = extract_slug_from_market_url(user_market)
    market_family_prefix = build_market_family_prefix(target_slug)

    if not target_slug:
        print("[ERROR] Invalid market URL/slug. Please restart and provide a valid value.")
        raise SystemExit(1)

    print("\n" + "="*52)
    print("  BTC/USDT -> Polymarket Arbitrage Engine (QUANT MATH)")
    print("="*52)
    print(f"\n[OK] Seed market slug:      {target_slug}")
    print(f"[OK] Market family prefix:  {market_family_prefix}")
    print(f"[OK] Key A (even minutes):  {GEMINI_KEY_A[:16]}...")
    print(f"[OK] Key B (odd  minutes):  {GEMINI_KEY_B[:16]}...")
    print(f"\n[OK] Thresholds: EV>{MIN_EV_PCT_TO_CALL_GEMINI}% | Body>{MIN_BODY_SIZE} | {MIN_SECONDS_REMAINING}s-{MAX_SECONDS_FOR_NEW_BET}s window | Crowd<{MAX_CROWD_PROB_TO_CALL}%")
    print(f"[OK] Volatility/Time Z-Scoring active.")
    
    try:
        asyncio.run(engine_loop())
    except KeyboardInterrupt:
        print("\n[SYSTEM] Engine manually stopped.")
