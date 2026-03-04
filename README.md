# Alpha-Z Asynchronous Bitcoin Arbitrage Engine

Alpha-Z is a full-stack trading system for Polymarket BTC binary markets.

- Backend: FastAPI + asyncio trading engine
- Frontend: Next.js dashboard with live charts and logs
- Data: Binance market streams + Polymarket odds/liquidity + SQLite trade history

## What It Does

The engine continuously:

1. Streams Binance price and trade-flow data.
2. Pulls Polymarket market probabilities and liquidity.
3. Computes EV/Kelly-adjusted bet sizing with risk and timing filters.
4. Optionally routes borderline decisions through a local LLM validator.
5. Tracks outcomes, attribution, and PnL in SQLite.
6. Exposes live metrics and history to the dashboard.

## Repository Structure

- `backend/main.py`: FastAPI app and API/WebSocket endpoints.
- `backend/bot/core.py`: Trading engine logic (signals, EV, sizing, execution, resolution).
- `backend/requirements.txt`: Python dependencies.
- `frontend/`: Next.js application.
- `start_quant.bat`: Convenience launcher for backend + frontend on Windows.

## Requirements

- Python 3.10+
- Node.js 18+
- npm
- Ollama (for local AI validation path)

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
# Trading mode
PAPER_TRADING=true
DRY_RUN=true

# Polymarket credentials (required for live trading)
POLY_PRIVATE_KEY=
POLY_FUNDER=
POLY_SIG_TYPE=1
```

Run backend API + engine:

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

Open `http://localhost:3000`.

The frontend auto-targets the host in the browser URL and uses backend port `8000` (see `frontend/config.ts`).

## One-Click Windows Start

```powershell
start_quant.bat
```

This launches backend and frontend in separate command windows.

## API and WebSocket

Backend base URL: `http://localhost:8000`

- `GET /api/metrics`: portfolio metrics, projections, equity curve, insights, journal, attribution.
- `GET /api/history`: candle history used by charts.
- `GET /api/analysis/post-mortem`: mismatch/slippage diagnostics.
- `GET /api/history/replay?timestamp=<unix_seconds>`: replay window around a trade.
- `WS /ws/live`: live price, VWAP, balance, logs, active trades, and candle snapshot.

## Notes on AI Path

The engine calls a local model endpoint at `http://localhost:11434/v1/chat/completions`.
Default model in code: `llama3.2:3b-instruct-q4_K_M`.

If Ollama is unavailable, deterministic logic still runs; AI-confirmation behavior will degrade gracefully.

## Data Files

When backend runs from `backend/`, it writes:

- `alpha_z_history.db`: trades and execution metrics.
- `trading_log.txt`: runtime logs.
- `ai_training_data.csv`: ML feature/outcome rows.

## Troubleshooting

- Backend not reachable from frontend:
  - Confirm backend is running on port `8000`.
  - Confirm firewall allows local port `8000`.
- `py` command not found:
  - Use `python` instead of `py` if your install exposes `python`.
- No live trades appearing:
  - Check `PAPER_TRADING`, `DRY_RUN`, and market availability.
  - Confirm Polymarket/Binance connectivity in backend logs.
- AI timeouts:
  - Ensure Ollama is running and model is pulled.

## Disclaimer

This software is for research and educational purposes.
Trading digital assets and prediction markets carries significant financial risk.
Use paper mode first and validate behavior before risking capital.