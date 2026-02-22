import pandas as pd
import numpy as np
import os
import asyncio
import math
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

def get_optimized_df():
    global cache_df, last_csv_mtime
    csv_path = "ai_trade_history.csv"
    if not os.path.exists(csv_path): 
        return None
    current_mtime = os.path.getmtime(csv_path)
    if cache_df is None or current_mtime > last_csv_mtime:
        cache_df = pd.read_csv(csv_path)
        last_csv_mtime = current_mtime
    return cache_df

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
    next_boundary = ((now // 300) + 1) * 300
    return f"btc-updown-5m-{next_boundary}"

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
        response_data = {
            "metrics": {
                "current_pnl": round(current_effective_balance, 2), 
                "win_rate": 0, "max_drawdown": 0, "total_trades": 0,
                "sharpe": 0, "var_95": 0, "expectancy": 0,
                "kelly_optimal": "0%", "current_streak": 0, "is_winning_streak": False
            },
            "insights": [], "heatmap": [], 
            "equity_curve": [{"time": current_time_sec, "value": round(current_effective_balance, 2)}], 
            "projections": {"paths": []}, "journal": []
        }
        return sanitize_data(response_data)
    
    try:
        # 1. DATA PREP
        df['dt'] = pd.to_datetime(df['Timestamp (UTC)'])
        df['hour'] = df['dt'].dt.hour
        df['is_win'] = (df['Result'] == 'WIN').astype(int)
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
            "insights": [],
            "journal": df.tail(50).iloc[::-1].to_dict(orient="records")
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
    
@app.websocket("/ws/live")
async def live_data_feed(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            k = core.live_candle if isinstance(core.live_candle, dict) else {}
            balance = await get_current_balance()
            payload = {
                "price": core.live_price,
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