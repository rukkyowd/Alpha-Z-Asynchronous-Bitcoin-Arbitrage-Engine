import pandas as pd
import numpy as np
import os
import sqlite3
import asyncio
import math
import aiohttp
import time
from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from bot import core # type: ignore

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

cache_df = None
last_csv_mtime = 0

_live_balance_cache: float = 0.0
_live_balance_ts: float = 0.0
BALANCE_CACHE_TTL = 15.0 

def sanitize_data(data):
    if isinstance(data, dict):
        return {k: sanitize_data(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [sanitize_data(v) for v in data]
    elif isinstance(data, float):
        if math.isnan(data) or math.isinf(data):
            return 0.0
    return data

def generate_ai_insights(win_rate, expectancy, current_streak, is_winning_streak, max_drawdown):
    insights = []
    
    # 1. Expectancy (The most important metric for an arbitrage engine)
    if expectancy > 0.05:
        insights.append({"type": "positive", "text": f"Strong positive expectancy (${expectancy:.2f} per trade). The math is in your favor."})
    elif expectancy < -0.05:
        insights.append({"type": "warning", "text": f"Negative expectancy (${expectancy:.2f}). The current edge is bleeding."})
        
    # 2. Win Rate Context
    if win_rate >= 55.0:
        insights.append({"type": "positive", "text": f"Solid win rate ({win_rate:.1f}%). Market volatility aligns well with your signals."})
    elif 0 < win_rate <= 45.0:
        insights.append({"type": "warning", "text": f"Win rate dropped to {win_rate:.1f}%. Consider tightening your AI entry criteria."})
        
    # 3. Streak Monitor
    if current_streak >= 3:
        if is_winning_streak:
            insights.append({"type": "positive", "text": f"Riding a {current_streak}-trade winning streak. Momentum is on your side."})
        else:
            insights.append({"type": "warning", "text": f"Active {current_streak}-trade losing streak. Keep an eye on the daily kill switch."})
            
    # 4. Drawdown Warning
    if max_drawdown <= -3.50:
        insights.append({"type": "warning", "text": f"Approaching daily loss limit. Deepest drawdown hit ${max_drawdown:.2f}."})
        
    # 5. Default Fallback
    if not insights:
        insights.append({"type": "default", "text": "Market conditions are neutral. Accumulating more execution data to find an edge."})
        
    # Limit to top 3 insights so the UI doesn't get cluttered
    return insights[:3]

def get_optimized_df():
    global cache_df
    
    if not os.path.exists("alpha_z_history.db"):
        return None
        
    try:
        # Timeout allows the DB to wait briefly if the engine is in the middle of a write
        with sqlite3.connect("alpha_z_history.db", timeout=5.0) as conn:
            # We alias the columns in SQL to perfectly match your existing frontend keys
            query = """
                SELECT 
                    timestamp as "Timestamp (UTC)", 
                    slug as "Market Slug", 
                    decision as "AI Decision",
                    strike as "Strike Price", 
                    final_price as "Final Price", 
                    actual_outcome as "Actual Outcome",
                    result as "Result", 
                    win_rate as "Running Win Rate (%)", 
                    pnl_impact as "PnL",
                    local_calc_outcome as "Local Calc", 
                    official_outcome as "Poly Official", 
                    match_status as "Match Status"
                FROM trades
            """
            cache_df = pd.read_sql_query(query, conn)
        return cache_df
    except Exception as e:
        print(f"[DB READ ERROR] {e}")
        return cache_df # Return the last known good cache if the read fails
    
async def get_current_balance() -> float:
    global _live_balance_cache, _live_balance_ts
    if core.PAPER_TRADING:
        return core.simulated_balance
    if time.time() - _live_balance_ts < BALANCE_CACHE_TTL and _live_balance_cache > 0:
        return _live_balance_cache
    import aiohttp
    try:
        async with aiohttp.ClientSession() as session:
            balance = await core.fetch_live_balance(session)
            if balance > 0:
                _live_balance_cache = balance
                _live_balance_ts = time.time()
                return balance
    except Exception as e:
        print(f"[BALANCE] Failed to fetch: {e}")
    return _live_balance_cache if _live_balance_cache > 0 else core.BANKROLL

def get_current_market_slug() -> str:
    now = int(time.time())
    current_boundary = (now // 900) * 900  # <--- Grabs the CURRENT 15m window
    return f"btc-updown-15m-{current_boundary}"

@app.on_event("startup")
async def start_trading_engine():
    core.target_slug = get_current_market_slug()
    core.market_family_prefix = core.build_market_family_prefix(core.target_slug)
    print(f"Starting Quant Engine: {core.target_slug}")
    asyncio.create_task(core.main())

@app.get("/api/metrics")
async def get_trading_metrics():
    df = get_optimized_df()
    current_effective_balance = await get_current_balance()
    current_time_sec = int(time.time())

    if df is None or df.empty:
        # Safe fetch for signals to prevent another potential AttributeError
        signal_perf = {}
        if hasattr(core, "signal_tracker"):
            signal_perf = core.signal_tracker.get_signal_performance()

        # RETURN DEFAULT ZEROED STATE IF NO TRADES EXIST YET
        response_data = {
            "metrics": {
                "win_rate": 0.0,
                "total_trades": 0,
                "max_drawdown": 0.0,
                "current_pnl": round(current_effective_balance, 2),  
                "sharpe": 0.0,
                "var_95": 0.0,
                "expectancy": 0.0,
                "kelly_optimal": "0.0%",  # <--- FIXED HERE
                "current_streak": 0,
                "is_winning_streak": False
            },
            "projections": {
                "median_180d": 0.0,
                "paths": []
            },
            "equity_curve": [],
            "heatmap": [],
            "insights": generate_ai_insights( 
                win_rate=0.0, 
                expectancy=0.0, 
                current_streak=0, 
                is_winning_streak=False, 
                max_drawdown=0.0
            ),
            "journal": [],
            "signals": signal_perf # <--- SAFEGUARDED HERE
        }
        return sanitize_data(response_data)
        
    try:
        # 1. DATA PREP
        df['dt'] = pd.to_datetime(df['Timestamp (UTC)'])
        df['hour'] = df['dt'].dt.hour
        df['is_win'] = (df['Result'] == 'WIN').astype(int)
        
        # SMART PNL: Use actual logged Kelly bets, fallback to hardcoded if old CSV
        if 'PnL' in df.columns:
            df["PnL"] = df["PnL"].astype(float)
        else:
            df["PnL"] = df["Result"].apply(lambda x: 1.00 if x == "WIN" else (-1.01 if x == "LOSS" else 0.0))
            
        df["Cum_PnL"] = df["PnL"].cumsum()

        # 2. EQUITY CALCULATIONS
        total_historical_pnl = df["PnL"].sum()
        starting_balance = current_effective_balance - total_historical_pnl
        df["Equity"] = starting_balance + df["Cum_PnL"]
        df['time'] = df['dt'].astype('int64') // 10**9

        # 3. STREAKS & DRAWDOWN
        df['streak'] = df['is_win'].groupby((df['is_win'] != df['is_win'].shift()).cumsum()).cumcount() + 1
        current_streak = int(df['streak'].iloc[-1])
        is_winning_streak = bool(df['is_win'].iloc[-1] == 1)
        df["Running_Max"] = df["Equity"].cummax()
        drawdown = df["Equity"] - df["Running_Max"]

        # 4. STATS
        total_trades = len(df)
        win_rate = df['is_win'].mean()
        avg_win = df[df['PnL'] > 0]['PnL'].mean() if not df[df['PnL'] > 0].empty else 1.0
        avg_loss = abs(df[df['PnL'] < 0]['PnL'].mean()) if not df[df['PnL'] < 0].empty else 1.01
        expectancy = (win_rate * avg_win) - ((1 - win_rate) * avg_loss)

        # 5. MONTE CARLO & KELLY
        mc_paths = []
        for _ in range(50):
            path = [float(df["Equity"].iloc[-1])]
            for _ in range(180):
                outcome = avg_win if np.random.random() < win_rate else -avg_loss
                path.append(max(0, path[-1] + outcome))
            mc_paths.append(path)

        win_loss_ratio = avg_win / avg_loss if avg_loss > 0 else 1
        kelly_fraction = win_rate - ((1 - win_rate) / win_loss_ratio)

        # 6. HEATMAP
        hourly_stats = df.groupby('hour')['Result'].value_counts().unstack(fill_value=0)
        for h in range(24):
            if h not in hourly_stats.index: hourly_stats.loc[h] = 0
        hourly_stats['total'] = hourly_stats.get('WIN', 0) + hourly_stats.get('LOSS', 0)
        hourly_stats['win_rate_pct'] = (hourly_stats.get('WIN', 0) / hourly_stats['total'] * 100).fillna(0)
        heatmap_data = [{"hour": int(h), "win_rate": round(wr, 1), "trades": int(t)}
                        for h, wr, t in zip(hourly_stats.index, hourly_stats['win_rate_pct'], hourly_stats['total'])]
        
        rolling_pnl = df["PnL"].rolling(window=5).sum().dropna()
        var_95_calc = round(np.percentile(rolling_pnl, 5), 2) if len(rolling_pnl) > 5 else 0

        # Safe fetch for signals here too
        signal_perf = []
        if hasattr(core, "signal_tracker"):
            signal_perf = core.signal_tracker.get_signal_performance()

        # 7. FINAL RESPONSE
        response_data = {
            "metrics": {
                "win_rate": round(win_rate * 100, 2),
                "total_trades": total_trades,
                "max_drawdown": round(drawdown.min(), 2),
                "current_pnl": round(current_effective_balance, 2),  
                "sharpe": round((df["PnL"].mean() / df["PnL"].std()) * np.sqrt(252), 2) if df["PnL"].std() > 0 else 0,
                "var_95": round(np.percentile(df["PnL"], 5), 2) if total_trades > 5 else 0,
                "expectancy": round(expectancy, 4),
                "kelly_optimal": f"{round(kelly_fraction * 100, 1)}%",
                "current_streak": current_streak,
                "is_winning_streak": is_winning_streak
            },
            "projections": {
                "median_180d": round(np.median([p[-1] for p in mc_paths]), 2),
                "paths": [p[::10] for p in mc_paths[:5]]
            },
            "equity_curve": df[["time", "Equity"]].rename(columns={"Equity": "value"}).to_dict(orient="records"),
            "heatmap": sorted(heatmap_data, key=lambda x: x['hour']),
            "insights": generate_ai_insights(
                win_rate=win_rate * 100, 
                expectancy=expectancy, 
                current_streak=current_streak, 
                is_winning_streak=is_winning_streak, 
                max_drawdown=drawdown.min()
            ),
            "journal": df.tail(50).iloc[::-1].to_dict(orient="records"),
            "signals": signal_perf
        }
        return sanitize_data(response_data)
        
    except Exception as e:
        import traceback
        print(traceback.format_exc())
        return {"error": str(e)}
    
@app.get("/api/history")
async def get_candle_history():
    try:
        if not core.candle_history:
            return {"history": []}
        df_hist = pd.DataFrame(core.candle_history)
        df_hist['time'] = pd.to_datetime(df_hist['timestamp']).astype('int64') // 10**9
        df_hist = df_hist.drop_duplicates(subset=['time'], keep='last').sort_values('time')
        formatted_history = df_hist[['time', 'open', 'high', 'low', 'close']].to_dict(orient="records")
        return {"history": formatted_history}
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/analysis/post-mortem")
async def get_post_mortem():
    df = get_optimized_df() # Reuses your existing caching logic
    if df is None or df.empty:
        return {"error": "No trade history available"}
    
    # Calculate Price Gaps and Mismatches
    df['price_gap'] = (df['Final Price'] - df['Strike Price']).abs()
    mismatches = df[df['Match Status'].str.contains('MISMATCH', na=False)]
    
    return {
        "avg_gap": round(df['price_gap'].mean(), 2),
        "total_mismatches": len(mismatches),
        "high_slippage_count": len(df[df['price_gap'] > 15.0]),
        "mismatch_details": mismatches.tail(5).to_dict(orient="records")
    }

@app.get("/api/history/replay")
async def get_replay_history(timestamp: int):
    """Fetches 30 mins of history around a specific trade time."""
    try:
        # Range: 20 mins before trade, 10 mins after
        start_time = (timestamp - (20 * 60)) * 1000
        end_time = (timestamp + (10 * 60)) * 1000
        
        async with aiohttp.ClientSession() as session:
            params = {
                "symbol": "BTCUSDT",
                "interval": "1m",
                "startTime": start_time,
                "endTime": end_time,
                "limit": 30
            }
            async with session.get("https://api.binance.com/api/v3/klines", params=params) as r:
                data = await r.json()
                # Format for lightweight-charts
                history = [
                    {
                        "time": int(k[0]/1000),
                        "open": float(k[1]), "high": float(k[2]),
                        "low": float(k[3]), "close": float(k[4])
                    } for k in data
                ]
                return {"history": history}
    except Exception as e:
        return {"error": str(e)}

@app.websocket("/ws/live")
async def live_data_feed(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            current_logs = list(core.recent_logs)
            k = core.live_candle if isinstance(core.live_candle, dict) else {}
            balance = await get_current_balance()

            now = int(time.time())
            next_boundary = ((now // 900) + 1) * 900
            time_left = next_boundary - now

            payload = {
                "price": core.live_price,
                "seconds_remaining": int(time_left),
                "logs": current_logs,
                "ai_log": core.last_ai_interaction,
                "vwap": round(core.get_vwap(), 2), 
                "balance": round(balance, 2),
                "is_paper": core.PAPER_TRADING,
                "active_trades": core.active_predictions, 
                "candle": {
                    "time": int(k.get('t', time.time()*1000)/1000),
                    "open": float(k.get('o', core.live_price)),
                    "high": float(k.get('h', core.live_price)),
                    "low": float(k.get('l', core.live_price)),
                    "close": core.live_price
                }
            }
            await websocket.send_json(payload)
            await asyncio.sleep(1)
    except Exception: pass

