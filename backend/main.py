import pandas as pd
import numpy as np
import os
import sqlite3
import asyncio
import math
import aiohttp
import time
import tracemalloc
import traceback
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from typing import Optional
from fastapi import FastAPI, WebSocket, HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from starlette.websockets import WebSocketDisconnect, WebSocketState
from bot import core # type: ignore

app = FastAPI()
tracemalloc.start()

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
WS_PUSH_INTERVAL_SECS = 0.25
WS_PORTFOLIO_PUSH_INTERVAL_SECS = 2.0
WS_HISTORY_PUSH_INTERVAL_SECS = 1.5

_portfolio_ws_cache: dict | None = None
_portfolio_ws_cache_ts: float = 0.0
_portfolio_ws_lock = asyncio.Lock()


class EngineControlUpdate(BaseModel):
    kill_switch: Optional[bool] = None
    paper_trading: Optional[bool] = None
    max_trade_pct: Optional[float] = None
    max_daily_loss_pct: Optional[float] = None

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
                    match_status as "Match Status",
                    trigger_reason as "Trigger Reason"
                FROM trades
            """
            cache_df = pd.read_sql_query(query, conn)
        return cache_df
    except Exception as e:
        print(f"[DB READ ERROR] {e}")
        return cache_df # Return the last known good cache if the read fails

def get_execution_metrics_recent(limit: int = 300) -> list[dict]:
    if not os.path.exists("alpha_z_history.db"):
        return []
    try:
        with sqlite3.connect("alpha_z_history.db", timeout=5.0) as conn:
            query = """
                SELECT
                    timestamp,
                    slug,
                    direction,
                    expected_price,
                    actual_price,
                    slippage_bps,
                    spread_cents,
                    liquidity_check
                FROM execution_metrics
                ORDER BY id DESC
                LIMIT ?
            """
            df_exec = pd.read_sql_query(query, conn, params=(int(limit),))
        if df_exec.empty:
            return []
        df_exec = df_exec.iloc[::-1].reset_index(drop=True)
        ts = pd.to_datetime(df_exec["timestamp"], errors="coerce", utc=True)
        df_exec["time"] = (ts.view("int64") // 10**9).astype("int64")
        df_exec.loc[ts.isna(), "time"] = 0
        return df_exec.to_dict(orient="records")
    except Exception as e:
        print(f"[EXEC DB READ ERROR] {e}")
        return []
    
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


def _safe_epoch_seconds(timestamp_value) -> int:
    if timestamp_value is None:
        return 0
    if isinstance(timestamp_value, (int, float)):
        ts = float(timestamp_value)
        if ts > 10_000_000_000:  # milliseconds
            return int(ts / 1000)
        return int(ts)
    if isinstance(timestamp_value, datetime):
        dt = timestamp_value if timestamp_value.tzinfo else timestamp_value.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    raw = str(timestamp_value).strip()
    if not raw:
        return 0
    try:
        dt = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        pass
    try:
        dt = pd.to_datetime(raw, utc=True, errors="coerce")
        if pd.isna(dt):
            return 0
        return int(dt.timestamp())
    except Exception:
        return 0


def format_candle_history_snapshot(history_snapshot: list, limit: int = 240) -> list[dict]:
    if not history_snapshot:
        return []
    dedup: dict[int, dict] = {}
    for candle in history_snapshot[-(limit * 2):]:
        if not isinstance(candle, dict):
            continue
        ts = _safe_epoch_seconds(candle.get("timestamp"))
        if ts <= 0:
            continue
        dedup[ts] = {
            "time": ts,
            "open": float(candle.get("open", 0.0) or 0.0),
            "high": float(candle.get("high", 0.0) or 0.0),
            "low": float(candle.get("low", 0.0) or 0.0),
            "close": float(candle.get("close", 0.0) or 0.0),
        }
    if not dedup:
        return []
    return [dedup[t] for t in sorted(dedup.keys())][-limit:]


def compact_portfolio_snapshot(snapshot: dict) -> dict:
    if not isinstance(snapshot, dict):
        return {}
    metrics = snapshot.get("metrics", {}) or {}
    projections = snapshot.get("projections", {}) or {}
    return {
        "metrics": metrics,
        "signals": snapshot.get("signals", []) or [],
        "attribution": snapshot.get("attribution", {}) or {},
        "insights": (snapshot.get("insights", []) or [])[:3],
        "heatmap": snapshot.get("heatmap", []) or [],
        "daily_pnl": (snapshot.get("daily_pnl", []) or [])[-21:],
        "equity_curve": (snapshot.get("equity_curve", []) or [])[-500:],
        "journal": (snapshot.get("journal", []) or [])[:30],
        "execution_metrics": (snapshot.get("execution_metrics", []) or [])[-120:],
        "projections": {
            "median_180d": projections.get("median_180d", 0.0),
            "paths": projections.get("paths", []) or [],
        },
    }


async def get_portfolio_snapshot_for_ws(ttl_secs: float = WS_PORTFOLIO_PUSH_INTERVAL_SECS) -> dict:
    global _portfolio_ws_cache, _portfolio_ws_cache_ts
    now = time.time()
    if _portfolio_ws_cache is not None and (now - _portfolio_ws_cache_ts) < ttl_secs:
        return _portfolio_ws_cache

    async with _portfolio_ws_lock:
        now = time.time()
        if _portfolio_ws_cache is not None and (now - _portfolio_ws_cache_ts) < ttl_secs:
            return _portfolio_ws_cache
        snapshot = await get_trading_metrics()
        if not isinstance(snapshot, dict):
            snapshot = {}
        _portfolio_ws_cache = snapshot
        _portfolio_ws_cache_ts = now
        return snapshot

# In main.py (starting near line 114)
def get_current_market_slug() -> str:
    """Generates the descriptive hourly slug for the current ET window"""
    now_utc = datetime.now(timezone.utc)
    now_et = now_utc.astimezone(ZoneInfo("America/New_York"))

    month = now_et.strftime('%B').lower()
    day = now_et.day
    hour_24 = now_et.hour

    # Convert 24h to 12h format for the slug (e.g., 14 -> 2pm)
    hour_12 = hour_24 % 12
    if hour_12 == 0:
        hour_12 = 12
    ampm = 'am' if hour_24 < 12 else 'pm'

    return f"bitcoin-up-or-down-{month}-{day}-{hour_12}{ampm}-et"


def get_engine_control_snapshot() -> dict:
    return {
        "kill_switch": bool(getattr(core, "KILL_SWITCH", False)),
        "paper_trading": bool(core.PAPER_TRADING),
        "max_trade_pct": float(core.risk_manager.max_trade_pct),
        "max_daily_loss_pct": float(core.risk_manager.max_daily_loss_pct),
    }


def get_engine_health_snapshot() -> dict:
    now_ms = int(time.time() * 1000)
    last_feed_ms = max(int(getattr(core, "last_agg_trade_ms", 0) or 0), int(getattr(core, "last_closed_kline_ms", 0) or 0))
    feed_stale_ms = max(0, now_ms - last_feed_ms) if last_feed_ms > 0 else 0
    ws_latency_ms = min(5000, feed_stale_ms) if feed_stale_ms > 0 else 0
    mem_current, mem_peak = tracemalloc.get_traced_memory()
    mem_current_mb = mem_current / (1024 * 1024)
    mem_peak_mb = mem_peak / (1024 * 1024)
    ai_ms = float(getattr(core, "last_ai_response_ms", 0.0) or 0.0)
    ai_ema_ms = float(getattr(core, "ai_response_ema_ms", 0.0) or 0.0)

    if ai_ema_ms > 5000:
        ai_status = "DEGRADED"
    elif ai_ema_ms > 2500:
        ai_status = "SLOW"
    else:
        ai_status = "ACTIVE"

    api_capacity = 5
    sem_val = getattr(core.api_semaphore, "_value", api_capacity)
    api_inflight = max(0, api_capacity - int(sem_val))
    api_saturation = round((api_inflight / api_capacity) * 100, 1)

    return {
        "ws_latency_ms": int(ws_latency_ms),
        "feed_stale_ms": int(feed_stale_ms),
        "memory_mb": round(mem_current_mb, 2),
        "memory_peak_mb": round(mem_peak_mb, 2),
        "ai_response_ms": round(ai_ms, 1),
        "ai_response_ema_ms": round(ai_ema_ms, 1),
        "ai_status": ai_status,
        "ai_in_flight": bool(getattr(core, "ai_call_in_flight", "")),
        "ai_failures": int(getattr(core, "ai_consecutive_failures", 0) or 0),
        "api_inflight": api_inflight,
        "api_capacity": api_capacity,
        "api_saturation_pct": api_saturation,
        "kill_switch": bool(getattr(core, "KILL_SWITCH", False)),
    }

@app.on_event("startup")
async def start_trading_engine():
    core.target_slug = get_current_market_slug()
    prefix_parts = core.target_slug.split("-")[:-1]
    core.market_family_prefix = "-".join(prefix_parts)
    
    print(f"Starting Quant Engine: {core.target_slug} | Prefix: {core.market_family_prefix}")
    asyncio.create_task(core.main())


@app.get("/api/engine/health")
async def get_engine_health():
    return sanitize_data(get_engine_health_snapshot())


@app.get("/api/engine/control")
async def get_engine_control():
    return sanitize_data(get_engine_control_snapshot())


@app.post("/api/engine/control")
async def update_engine_control(update: EngineControlUpdate):
    if update.paper_trading is not None and not update.paper_trading:
        if not core.POLY_PRIVATE_KEY:
            raise HTTPException(status_code=400, detail="Cannot switch to LIVE: POLY_PRIVATE_KEY is missing")
        if core.clob_client is None:
            raise HTTPException(status_code=400, detail="Cannot switch to LIVE: CLOB client is not initialized")

    async with core.state_lock:
        if update.kill_switch is not None:
            core.KILL_SWITCH = bool(update.kill_switch)
        if update.paper_trading is not None:
            core.PAPER_TRADING = bool(update.paper_trading)
        if update.max_trade_pct is not None:
            core.risk_manager.max_trade_pct = max(0.005, min(float(update.max_trade_pct), 0.5))
        if update.max_daily_loss_pct is not None:
            core.risk_manager.max_daily_loss_pct = max(0.01, min(float(update.max_daily_loss_pct), 0.8))

    return sanitize_data({
        "ok": True,
        "control": get_engine_control_snapshot(),
    })

@app.get("/api/metrics")
async def get_trading_metrics():
    df = get_optimized_df()
    execution_metrics = get_execution_metrics_recent(300)
    current_effective_balance = await get_current_balance()
    current_time_sec = int(time.time())

    # --- EMPTY STATE (No Trades Yet) ---
    if df is None or df.empty:
        signal_perf = {}
        if hasattr(core, "signal_tracker"):
            signal_perf = core.signal_tracker.get_signal_performance()

        empty_attribution = {
            "ai_confirmed": {"trades": 0, "win_rate": 0.0, "pnl": 0.0},
            "system_only": {"trades": 0, "win_rate": 0.0, "pnl": 0.0}
        }

        response_data = {
            "metrics": {
                "win_rate": 0.0,
                "total_trades": 0,
                "total_wins": 0,
                "total_losses": 0,
                "max_drawdown": 0.0,
                "current_pnl": round(current_effective_balance, 2),  
                "sharpe": 0.0,
                "var_95": 0.0,
                "expectancy": 0.0,
                "kelly_optimal": "0.0%",  
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
            "daily_pnl": [],
            "execution_metrics": execution_metrics,
            "signals": signal_perf,
            "attribution": empty_attribution
        }
        return sanitize_data(response_data)
        
    # --- POPULATED STATE (We have trades) ---
    try:
        # 1. DATA PREP
        df['dt'] = pd.to_datetime(df['Timestamp (UTC)'])
        df['hour'] = df['dt'].dt.hour
        df['result_upper'] = df['Result'].fillna("").astype(str).str.upper()
        df['is_resolved'] = df['result_upper'].str.contains("WIN|LOSS", na=False)
        df['is_win'] = df['result_upper'].str.contains("WIN", na=False).astype(int)

        if 'PnL' in df.columns:
            df["PnL"] = df["PnL"].astype(float)
        else:
            df["PnL"] = df["result_upper"].apply(lambda x: 1.00 if "WIN" in x else (-1.01 if "LOSS" in x else 0.0))

        df["Cum_PnL"] = df["PnL"].cumsum()
        df["trade_date"] = df["dt"].dt.date

        # 2. EQUITY CALCULATIONS
        total_historical_pnl = df["PnL"].sum()
        starting_balance = current_effective_balance - total_historical_pnl
        df["Equity"] = starting_balance + df["Cum_PnL"]
        df['time'] = df['dt'].astype('int64') // 10**9

        # 3. STREAKS & DRAWDOWN
        resolved_trades = df[df['is_resolved']].copy()
        if resolved_trades.empty:
            current_streak = 0
            is_winning_streak = False
        else:
            resolved_trades['streak'] = resolved_trades['is_win'].groupby(
                (resolved_trades['is_win'] != resolved_trades['is_win'].shift()).cumsum()
            ).cumcount() + 1
            current_streak = int(resolved_trades['streak'].iloc[-1])
            is_winning_streak = bool(resolved_trades['is_win'].iloc[-1] == 1)

        df["Running_Max"] = df["Equity"].cummax()
        drawdown = df["Equity"] - df["Running_Max"]

        # 4. STATS
        total_trades = int(df['is_resolved'].sum())
        total_wins = int(((df['is_win'] == 1) & df['is_resolved']).sum())
        total_losses = max(total_trades - total_wins, 0)
        win_rate = (total_wins / total_trades) if total_trades > 0 else 0.0

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
        df['result_bucket'] = np.where(
            df['result_upper'].str.contains("WIN", na=False),
            "WIN",
            np.where(df['result_upper'].str.contains("LOSS", na=False), "LOSS", "OTHER")
        )
        hourly_stats = df.groupby('hour')['result_bucket'].value_counts().unstack(fill_value=0)
        for h in range(24):
            if h not in hourly_stats.index:
                hourly_stats.loc[h] = 0
        hourly_stats['total'] = hourly_stats.get('WIN', 0) + hourly_stats.get('LOSS', 0)
        hourly_stats['win_rate_pct'] = (hourly_stats.get('WIN', 0) / hourly_stats['total'] * 100).fillna(0)
        heatmap_data = [{"hour": int(h), "win_rate": round(wr, 1), "trades": int(t)}
                        for h, wr, t in zip(hourly_stats.index, hourly_stats['win_rate_pct'], hourly_stats['total'])]

        daily_rollup = (
            df.groupby("trade_date", as_index=False)
            .agg(pnl=("PnL", "sum"), trades=("PnL", "count"))
            .sort_values("trade_date")
            .tail(21)
        )
        daily_pnl = [
            {
                "date": str(row.trade_date),
                "day": pd.to_datetime(row.trade_date).strftime("%a"),
                "pnl": round(float(row.pnl), 2),
                "trades": int(row.trades),
            }
            for row in daily_rollup.itertuples(index=False)
        ]

        pnl_for_risk = df.loc[df['is_resolved'], 'PnL']
        if pnl_for_risk.empty:
            pnl_for_risk = df['PnL']

        signal_perf = []
        if hasattr(core, "signal_tracker"):
            signal_perf = core.signal_tracker.get_signal_performance()

        # --- AI EDGE ATTRIBUTION CALCULATION ---
        attribution = {
            "ai_confirmed": {"trades": 0, "win_rate": 0.0, "pnl": 0.0},
            "system_only": {"trades": 0, "win_rate": 0.0, "pnl": 0.0}
        }
        
        if "Trigger Reason" in df.columns:
            valid_trades = df[df["is_resolved"]]
            
            ai_trades = valid_trades[valid_trades["Trigger Reason"].str.contains("AI confirmed", na=False)]
            if len(ai_trades) > 0:
                ai_wins = int(ai_trades["result_upper"].str.contains("WIN", na=False).sum())
                attribution["ai_confirmed"] = {
                    "trades": len(ai_trades),
                    "win_rate": round((ai_wins / len(ai_trades)) * 100, 1),
                    "pnl": round(ai_trades["PnL"].sum(), 2)
                }
                
            sys_trades = valid_trades[~valid_trades["Trigger Reason"].str.contains("AI confirmed|AI vetoed", na=False)]
            if len(sys_trades) > 0:
                sys_wins = int(sys_trades["result_upper"].str.contains("WIN", na=False).sum())
                attribution["system_only"] = {
                    "trades": len(sys_trades),
                    "win_rate": round((sys_wins / len(sys_trades)) * 100, 1),
                    "pnl": round(sys_trades["PnL"].sum(), 2)
                }
        # ---------------------------------------------

        # 7. FINAL RESPONSE
        response_data = {
            "metrics": {
                "win_rate": round(win_rate * 100, 2),
                "total_trades": total_trades,
                "total_wins": total_wins,
                "total_losses": total_losses,
                "max_drawdown": round(drawdown.min(), 2),
                "current_pnl": round(current_effective_balance, 2),  
                "sharpe": round((pnl_for_risk.mean() / pnl_for_risk.std()) * np.sqrt(252), 2) if len(pnl_for_risk) > 1 and pnl_for_risk.std() > 0 else 0,
                "var_95": round(np.percentile(pnl_for_risk, 5), 2) if len(pnl_for_risk) > 5 else 0,
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
            "daily_pnl": daily_pnl,
            "execution_metrics": execution_metrics,
            "signals": signal_perf,
            "attribution": attribution
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
        last_portfolio_push_ts = 0.0
        last_history_push_ts = 0.0
        last_portfolio_sig = None
        while True:
            if websocket.client_state != WebSocketState.CONNECTED:
                break
            async with core.state_lock:
                current_logs = list(core.recent_logs)
                k = core.live_candle if isinstance(core.live_candle, dict) else {}
                price = core.live_price
                active_trades = dict(core.active_predictions)
                history_snapshot = list(core.candle_history)

            regime = core.detect_market_regime(history_snapshot) if history_snapshot else "UNKNOWN"
            atr = 0.0
            if history_snapshot:
                atr_window = history_snapshot[-14:]
                atr_components = [
                    max(float(c.get("high", 0.0)) - float(c.get("low", 0.0)), 0.0)
                    for c in atr_window
                    if isinstance(c, dict)
                ]
                if atr_components:
                    atr = sum(atr_components) / len(atr_components)

            balance = await get_current_balance()
            strategy_snapshot = core.get_live_strategy_snapshot(balance)
            engine_health_snapshot = get_engine_health_snapshot()
            engine_control_snapshot = get_engine_control_snapshot()

            now = int(time.time())
            next_boundary = ((now // 3600) + 1) * 3600  # Hourly markets
            time_left = next_boundary - now

            now_ts = time.time()
            history_payload = None
            if (now_ts - last_history_push_ts) >= WS_HISTORY_PUSH_INTERVAL_SECS:
                history_payload = format_candle_history_snapshot(history_snapshot, limit=180)
                last_history_push_ts = now_ts

            portfolio_payload = None
            if (now_ts - last_portfolio_push_ts) >= WS_PORTFOLIO_PUSH_INTERVAL_SECS:
                full_snapshot = await get_portfolio_snapshot_for_ws()
                compact_snapshot = compact_portfolio_snapshot(full_snapshot)
                metrics = compact_snapshot.get("metrics", {}) if isinstance(compact_snapshot, dict) else {}
                portfolio_sig = (
                    metrics.get("current_pnl", 0.0),
                    metrics.get("total_trades", 0),
                    metrics.get("total_wins", 0),
                    metrics.get("total_losses", 0),
                    metrics.get("current_streak", 0),
                )
                if portfolio_sig != last_portfolio_sig:
                    portfolio_payload = compact_snapshot
                    last_portfolio_sig = portfolio_sig
                last_portfolio_push_ts = now_ts

            payload = {
                "price": price,
                "seconds_remaining": int(time_left),
                "logs": current_logs,
                "ai_log": core.last_ai_interaction,
                "vwap": round(core.get_vwap(), 2), 
                "balance": round(balance, 2),
                "is_paper": core.PAPER_TRADING,
                "active_trades": active_trades,
                "regime": regime,
                "atr": round(atr, 2),
                "strategy": strategy_snapshot,
                "engine_health": engine_health_snapshot,
                "engine_control": engine_control_snapshot,
                "candle": {
                    "time": int(k.get('t', time.time()*1000)/1000),
                    "open": float(k.get('o', price)),
                    "high": float(k.get('h', price)),
                    "low": float(k.get('l', price)),
                    "close": price
                }
            }
            if history_payload is not None:
                payload["history"] = history_payload
            if portfolio_payload is not None:
                payload["portfolio"] = portfolio_payload
            # WebSocket path does not get FastAPI response encoding, so normalize manually.
            await websocket.send_json(jsonable_encoder(sanitize_data(payload)))
            await asyncio.sleep(WS_PUSH_INTERVAL_SECS)
    except WebSocketDisconnect:
        return
    except Exception as e:
        print(f"[WS /ws/live] {e}")
        traceback.print_exc()

