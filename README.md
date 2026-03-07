# Alpha-Z Asynchronous Bitcoin Arbitrage Engine

Alpha-Z is a full-stack Polymarket BTC binary trading engine with a modular Python backend and a typed Next.js dashboard.

- Backend: FastAPI + asyncio orchestration
- Frontend: Next.js live dashboard
- Market data: Binance WebSocket + REST backfill
- Venue data: Polymarket Gamma + CLOB
- Validation path: local Ollama / OpenAI-compatible chat endpoint
- Persistence: SQLite + CSV feature logging

## Current Status

- The active backend path is the new modular engine wired through `backend/main.py`.
- Paper trading startup and dashboard synchronization are in working shape.
- Live trading support exists, but it is not yet proven safe enough to trust with real money.

Use paper mode first. Validate fills, exits, reconnect/backfill behavior, and UI state before enabling live execution.

## Active Architecture

The current runtime is built from the modular `backend/bot/` stack:

- `backend/bot/models.py`: strict dataclasses and typed payload models
- `backend/bot/state.py`: centralized async engine state
- `backend/bot/indicators.py`: technical context, Bayesian probability, volatility models
- `backend/bot/strategy.py`: EV, Kelly sizing, gatekeeping, signal generation
- `backend/bot/risk.py`: drawdown guards, TP/SL logic, position monitoring
- `backend/bot/execution.py`: Polymarket CLOB routing and fill handling
- `backend/bot/ai_agent.py`: local LLM validation with circuit breaker
- `backend/bot/data_streams.py`: Binance ingestion, reconnects, REST backfill, Polymarket odds
- `backend/main.py`: orchestration, API, workers, websocket broadcast

`backend/bot/core.py` is still present in the repo as legacy reference code, but it is not the active orchestrator path.

## Quantitative Stack

The refactored engine is centered on:

- Bayesian log-odds probability updates
- Student-t move estimation
- Square-root market impact and slippage
- Continuous exponential time decay for sizing
- Parkinson and Garman-Klass intraday volatility
- Typed `TechnicalContext`, `TradeSignal`, and `ActivePosition` state

The frontend websocket payload now reflects this richer model and includes fields such as:

- `bayesian_probability`
- `garman_klass_volatility`
- `cvd_candle_delta`
- `expected_move_sigma`
- `tp_delta`
- `sl_delta`

## Requirements

- Python 3.10+
- Node.js 18+
- npm
- Ollama if you want local AI validation enabled

Python dependencies are defined in `backend/requirements.txt`:

- `fastapi`
- `uvicorn[standard]`
- `aiohttp`
- `websockets`
- `numpy`
- `pandas`
- `scipy`
- `py-clob-client`

## Backend Setup

From the repository root:

```powershell
cd backend
py -m venv .venv
.\.venv\Scripts\activate
py -m pip install --upgrade pip
py -m pip install -r requirements.txt
```

Create `backend/.env`:

```ini
# Operating mode
PAPER_TRADING=true
DRY_RUN=false
BANKROLL=5000

# Local AI
LOCAL_AI_MODEL=llama3.2:3b-instruct-q4_K_M

# Polymarket credentials (required for live trading)
POLY_PRIVATE_KEY=
POLY_FUNDER=
POLY_SIG_TYPE=2
POLY_HOST=https://clob.polymarket.com
POLY_CHAIN_ID=137
```

Start the backend:

```powershell
cd backend
py -m uvicorn main:app --host 0.0.0.0 --port 8000
```

## Frontend Setup

```powershell
cd frontend
npm install
npm run dev -- -H 0.0.0.0
```

Open [http://localhost:3000](http://localhost:3000).

Frontend connection config is environment-driven through `frontend/config.ts`.

For local development, the frontend falls back to:

- `http://localhost:8000`
- `ws://localhost:8000/ws/live`

For Vercel or any remote frontend deployment, set these public environment variables:

```ini
NEXT_PUBLIC_API_URL=https://your-public-backend-host
NEXT_PUBLIC_WS_URL=wss://your-public-backend-host/ws/live
```

### Vercel Deployment

In the Vercel project settings for the frontend, add:

```ini
NEXT_PUBLIC_API_URL=https://your-public-backend-host
NEXT_PUBLIC_WS_URL=wss://your-public-backend-host/ws/live
```

If you are tunneling the backend from your local machine, use the public ngrok URL for both values:

```ini
NEXT_PUBLIC_API_URL=https://<your-ngrok-subdomain>.ngrok-free.app
NEXT_PUBLIC_WS_URL=wss://<your-ngrok-subdomain>.ngrok-free.app/ws/live
```

Deployment rule:

- `NEXT_PUBLIC_API_URL` must be the public HTTPS base URL of the backend
- `NEXT_PUBLIC_WS_URL` must be the matching websocket endpoint ending in `/ws/live`
- if you rotate the ngrok URL, update both Vercel environment variables and redeploy
- if only `NEXT_PUBLIC_API_URL` is set, `frontend/config.ts` will derive the websocket URL automatically, but setting both explicitly is safer

`frontend/config.ts` also still accepts the older compatibility names:

- `NEXT_PUBLIC_BACKEND_HTTP_URL`
- `NEXT_PUBLIC_BACKEND_WS_URL`

but the preferred names are:

- `NEXT_PUBLIC_API_URL`
- `NEXT_PUBLIC_WS_URL`

The dashboard listens to the backend websocket and now expects a typed payload with these top-level keys:

- `market`
- `positions`
- `telemetry`
- `runtime`
- `drawdown_guard`

## Clean Paper Trading Reset

Before validating a new backend build, wipe the old persistence files so you do not mix old schema/data into the new run:

```powershell
cd backend
Remove-Item ".\alpha_z_history.db" -Force -ErrorAction SilentlyContinue
Remove-Item ".\ai_training_data.csv" -Force -ErrorAction SilentlyContinue
Remove-Item ".\trading_log.txt" -Force -ErrorAction SilentlyContinue
```

Then restart with:

```powershell
$env:PAPER_TRADING="True"
$env:DRY_RUN="False"
$env:BANKROLL="5000"
py -m uvicorn main:app --host 0.0.0.0 --port 8000
```

Expected clean startup log pattern:

```text
[INIT] Simulated balance initialized: $5000.00 (base: $5000.0 + historical: $0.00)
[SYSTEM] Loaded 120 candles. VWAP=...
[SYSTEM] Warming up local AI (llama3.2:3b-instruct-q4_K_M) into RAM...
[SYSTEM] AI Warmup complete. Engine is hot and ready.
```

## Paper Validation Checklist

Use this as the minimum validation bar before even considering live execution:

1. Start from a wiped `alpha_z_history.db` and `ai_training_data.csv`.
2. Confirm the backend seeds `120` Binance candles on boot.
3. Confirm the dashboard renders without websocket parse errors.
4. Confirm the dashboard shows the new quant metrics:
   - `Bayesian Win Prob`
   - `Intraday Volatility`
5. Let the engine run long enough to generate at least one signal evaluation.
6. Verify open positions, if any, display dynamic `tp_delta` and `sl_delta`.
7. Verify the backend can recover from a Binance websocket disconnect.

On reconnect, you want to see logs like:

```text
[KLINE WS] Disconnected: ...
[TRADE WS] Disconnected: ...
[KLINE REST] Backfilled ... missing closed candles after reconnect.
[TRADE REST] Backfilled ... agg trades after reconnect.
```

## API and WebSocket

Backend base URL: `http://localhost:8000`

- `GET /api/engine/health`: runtime health and mode summary
- `GET /api/engine/control`: current control flags and risk caps
- `POST /api/engine/control`: update kill switch / paper mode / risk caps
- `GET /api/metrics`: live metrics snapshot for the dashboard
- `GET /api/history`: typed candle history used by charts
- `GET /api/history/replay?timestamp=<unix_seconds>`: replay window around a trade/event
- `GET /api/analysis/post-mortem`: recent execution and mismatch diagnostics
- `WS /ws/live`: live typed engine payload for the dashboard

## One-Click Windows Start

```powershell
start_quant.bat
```

This launches the backend and frontend in separate command windows.

## Data Files

When the backend runs from `backend/`, it writes:

- `alpha_z_history.db`: trade/execution history and analytics
- `ai_training_data.csv`: ML feature rows
- `trading_log.txt`: runtime log output

## Live Trading Warning

Do not treat the current engine as production-ready for live capital yet.

Open items that still need paper or shadow validation:

- real CLOB fill quality across multiple market regimes
- entry/exit reliability under fast-moving books
- AI-gated decision latency under load
- reconnect/backfill correctness during active positions
- end-to-end consistency between backend state, DB writes, and dashboard display

A reasonable go/no-go bar before live trading is:

1. One full paper session with no crashes or state corruption.
2. Verified entry attempts using executable CLOB-adjusted prices.
3. Verified TP/SL exits on at least a few completed paper trades.
4. At least one clean websocket reconnect with successful REST backfill.
5. No frontend websocket/type parsing failures.

## Troubleshooting

- Frontend cannot reach backend:
  - Confirm backend is listening on port `8000`.
  - Confirm local firewall rules allow `8000`.
- Paper balance is not resetting:
  - Delete `backend/alpha_z_history.db` before restart.
- AI is slow on startup:
  - The model warmup is real; the first load can take tens of seconds.
- No trades are appearing:
  - Check gate logs first. The engine can legitimately skip on EV, crowd skew, time-to-expiry, or confirmation filters.
- Live mode is not placing orders:
  - Confirm Polymarket credentials are set.
  - Confirm the market has executable CLOB depth, not just public odds.

## Disclaimer

This repository is for research and engineering purposes.

Trading prediction markets and digital assets carries real financial risk. Run paper mode first, validate the execution path, and assume that unvalidated live trading can lose money.
