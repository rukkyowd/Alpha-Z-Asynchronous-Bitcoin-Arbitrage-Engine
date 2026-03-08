# Alpha-Z Hourly Polymarket BTC Engine

Alpha-Z is a full-stack trading engine for Binance-referenced hourly Polymarket BTC binary markets.

Today, the active runtime is an hourly-only engine:
- underlying source: Binance `BTCUSDT` spot
- market class: Polymarket `bitcoin-up-or-down-...-et` hourly markets
- resolution rule: `UP` if the finalized Binance `1h` candle closes at or above its open (`Close >= Open`), otherwise `DOWN`
- backend: FastAPI + asyncio
- frontend: Next.js dashboard
- persistence: SQLite + CSV feature logging
- AI gate: local Ollama / OpenAI-compatible chat endpoint

## What The Bot Trades

This codebase is intentionally focused on one market type only:
- hourly BTC Polymarket candle-open markets

It does not target:
- 5 minute markets
- 15 minute markets
- 24 hour markets
- generic strike-style BTC markets

The current strategy and settlement logic are aligned to the hourly candle-open contract structure.

## Market Resolution Rule

For a market such as `bitcoin-up-or-down-march-8-3am-et`:
- the reference is the Binance `BTCUSDT` `1h` candle that begins at that market hour
- the market resolves `UP` if the finalized candle satisfies `close >= open`
- otherwise it resolves `DOWN`
- the strict fallback source is Binance `BTCUSDT` `1h` data only

The backend treats this as a `CANDLE_OPEN` market internally.

## Current Status

The active engine path is the modular runtime under `backend/`.

Current state:
- hourly-only market targeting is wired
- Binance `1h` candles are the primary feature timeframe
- market audit logging is present
- paper trading with live CLOB shadow pricing is working
- strict settlement fails closed to `RESOLVING` if neither official outcome nor finalized Binance `1h` data is available

Live trading support exists, but paper and shadow validation should still come first.

## Active Backend Architecture

The current runtime is built from the modular `backend/bot/` stack:
- `backend/bot/models.py`: typed dataclasses and payload models
- `backend/bot/state.py`: centralized async engine state
- `backend/bot/indicators.py`: technical context, Bayesian probability, volatility models
- `backend/bot/strategy.py`: edge, EV, Kelly sizing, gatekeeping, signal generation
- `backend/bot/risk.py`: drawdown guards, TP/SL logic, position monitoring
- `backend/bot/execution.py`: Polymarket CLOB routing, paper shadow pricing, fill handling
- `backend/bot/ai_agent.py`: local LLM validation with circuit breaker
- `backend/bot/data_streams.py`: Binance ingestion, reconnects, REST backfill, Polymarket odds
- `backend/bot/clob_ws.py`: live CLOB websocket book manager
- `backend/main.py`: orchestration, API, workers, settlement, websocket broadcast

Legacy note:
- `backend/bot/core.py` is still present in the repo as reference code, but it is not the active orchestrator path.

## Quantitative Model

The current strategy stack is centered on:
- Binance `1h` candle-aligned features
- Bayesian log-odds probability updates
- Student-t move estimation around the hourly candle open
- Parkinson and Garman-Klass volatility
- square-root market impact and slippage
- fee-aware EV and Kelly sizing
- typed `TechnicalContext`, `TradeSignal`, and `ActivePosition` models
- calibration support plus Brier benchmarking in analytics

Important practical note:
- the EV math is binary-outcome math, not continuous directional trading PnL math
- the edge is computed against Polymarket entry price / fair probability for a contract that resolves on `Close >= Open`

## Operating Modes

### Paper mode
- simulated bankroll
- no real order posting
- strategy, risk, and monitoring all run normally

### Paper shadow CLOB mode
- paper mode plus real Polymarket CLOB reads for pricing, spread, and liquidity shadowing
- no live heartbeat kill-switch is armed in this mode
- useful for validating execution assumptions without risking capital

### Live mode
- real balance
- real CLOB interaction
- heartbeat kill-switch active
- should only be used after paper and shadow validation

## Requirements

- Python 3.10+
- Node.js 18+
- npm
- Ollama if you want local AI validation enabled

Backend Python dependencies are defined in `backend/requirements.txt`.

## Backend Setup

From the repository root:

```powershell
cd backend
py -m venv .venv
.\.venv\Scripts\activate
py -m pip install --upgrade pip
py -m pip install -r requirements.txt
```

Create `backend/.env`.

Minimum example:

```ini
# Operating mode
PAPER_TRADING=true
DRY_RUN=false
PAPER_USE_LIVE_CLOB=true
BANKROLL=5000

# AI
LOCAL_AI_MODEL=llama3.2:3b-instruct-q4_K_M

# Core risk / edge controls
MAX_TRADES_PER_HOUR=2
EXPECTED_EXIT_FEE_MULTIPLIER=0.50
LATENCY_EV_HAIRCUT_PCT=0.25
CLOSE_EQUALS_OPEN_UP_BIAS_PROB=0.0005

# Optional debugging
TRACE_MALLOC=false

# Optional control endpoint auth
ENGINE_CONTROL_API_KEY=

# Polymarket credentials
# Required for live trading
# Also required if you want paper shadow mode to read the private-auth CLOB path used by this codebase
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

Local fallback defaults:
- `http://localhost:8000`
- `ws://localhost:8000/ws/live`

For remote frontend deployments, set:

```ini
NEXT_PUBLIC_API_URL=https://your-public-backend-host
NEXT_PUBLIC_WS_URL=wss://your-public-backend-host/ws/live
```

Optional browser-side control auth header support:

```ini
NEXT_PUBLIC_ENGINE_CONTROL_API_KEY=your-control-key
```

Use the same value as `ENGINE_CONTROL_API_KEY` only if you intentionally want browser control writes enabled.

## Clean Reset For A New Evaluation Regime

Because the engine logic has changed significantly over time, do not mix old trade history into a fresh validation pass unless that is intentional.

Recommended approach:
- rename old persistence files instead of deleting them
- start a clean paper run for the current hourly-only strategy

From `backend/`:

```powershell
Rename-Item alpha_z_history.db alpha_z_history.pre_reset.db -ErrorAction SilentlyContinue
Rename-Item ai_training_data.csv ai_training_data.pre_reset.csv -ErrorAction SilentlyContinue
Rename-Item trading_log.txt trading_log.pre_reset.txt -ErrorAction SilentlyContinue
```

If you really want a full wipe:

```powershell
Remove-Item .\alpha_z_history.db -Force -ErrorAction SilentlyContinue
Remove-Item .\ai_training_data.csv -Force -ErrorAction SilentlyContinue
Remove-Item .\trading_log.txt -Force -ErrorAction SilentlyContinue
```

## Startup Validation Checklist

Use this as the minimum validation bar before trusting a run.

1. Start the backend in paper mode.
2. Confirm the backend logs `Monitoring 1h WebSocket...`
3. Confirm `Loaded 120 candles` appears.
4. Confirm a `[MARKET AUDIT]` line appears for the active hourly market.
5. If using shadow pricing, confirm:
   - `[INIT] Paper mode using live Polymarket CLOB for shadow pricing/liquidity.`
   - `[SYSTEM] CLOB WebSocket book manager started in paper shadow mode (heartbeat disabled).`
6. Confirm the frontend opens without websocket parse errors.
7. Confirm `/api/metrics`, `/api/engine/health`, `/api/engine/control`, and `/api/history` return successfully.

Expected startup pattern:

```text
[INIT] Simulated balance initialized: $5000.00 ...
[SYSTEM] Loaded 120 candles. VWAP=...
[MARKET AUDIT] requested_slug=... | matched_slug=... | outcomes=Up, Down | resolution=CANDLE_OPEN | ...
[SYSTEM] Warming up local AI (...)
[SYSTEM] AI Warmup complete. Engine is hot and ready.
```

## Runtime Behaviors To Expect

Normal skip reasons include:
- `Crowd skew too high`
- `Too early`
- `Too close to expiry`
- `Entry premium ... > ... max`
- `Net EV ... <= 0 after slippage`
- `Score 1 requires EV >= ...`
- `Countertrend blocked ...`

Normal position logs include:
- `[POSITION] OPEN ... | Mark: ... | U-PnL: ... | TP: ... | SL: ...`
- `TP Armed: Y/N`
- `SL Disabled: Y/N`

Normal settlement behavior:
- official outcome if available
- strict Binance `BTCUSDT` `1h` candle fallback otherwise
- `RESOLVING` if neither strict source is available yet

## APIs And WebSocket

Backend base URL:
- `http://localhost:8000`

Main endpoints:
- `GET /api/engine/health`
- `GET /api/engine/control`
- `POST /api/engine/control`
- `GET /api/metrics`
- `GET /api/history`
- `GET /api/history/replay?timestamp=<unix_seconds>`
- `GET /api/analysis/post-mortem`
- `WS /ws/live`

The dashboard websocket payload uses these top-level keys:
- `market`
- `positions`
- `telemetry`
- `runtime`
- `drawdown_guard`

## Data Files

When the backend runs from `backend/`, it writes:
- `alpha_z_history.db`: trade and execution history
- `ai_training_data.csv`: model / feature rows
- `trading_log.txt`: runtime log output

## Troubleshooting

### Dashboard websocket disconnects
If you see:
- `[WS] /ws/live send timed out; closing slow client.`

that means the server intentionally dropped a slow dashboard client to avoid backlog. This is not a trade-execution failure. The frontend should reconnect automatically.

### No trades are appearing
Check gate logs first. The engine can legitimately skip on:
- crowd skew
- time-to-expiry window
- negative EV after slippage and fees
- confirmation filters
- entry premium caps

### Paper shadow CLOB is not active
Check:
- `PAPER_USE_LIVE_CLOB=true`
- Polymarket credentials are present in the environment used by the backend
- the startup log explicitly says paper shadow CLOB mode is active

### Live mode is not placing orders
Check:
- Polymarket credentials
- market audit log for the active slug
- CLOB subscription log and websocket stability
- executable depth on the actual outcome token, not just public odds

## Safety Warning

Do not treat the current engine as production-safe by default.

Before using real money, validate all of the following in paper or shadow mode:
- multiple clean startups
- market rotation correctness
- CLOB websocket stability
- position monitoring behavior
- TP / SL / settlement correctness
- reconnect and backfill behavior
- frontend consistency with backend state

## Disclaimer

This repository is for research and engineering purposes.

Trading prediction markets and digital assets carries real financial risk. Run paper mode first, validate the execution path, and assume that unvalidated live trading can lose money.
