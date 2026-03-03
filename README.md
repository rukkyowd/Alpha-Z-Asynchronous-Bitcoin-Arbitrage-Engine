Here is a comprehensive, production-ready `README.md` for your repository. It captures the advanced quantitative architecture, the recent critical fixes we implemented, and the full stack setup (Python backend + Next.js frontend).

---

```markdown
# ⚡ Alpha Z: Elite Asynchronous Bitcoin Arbitrage Engine

Alpha Z is a high-performance, asynchronous quantitative trading engine designed explicitly for Polymarket's BTC/USD binary options (hourly and daily markets). It combines deterministic technical analysis, slippage-aware Expected Value (EV) modeling, and local Large Language Model (LLM) validation to execute +EV trades with strict risk management.

## 🚀 Key Features

* **Slippage-Aware Kelly Sizing:** Dynamically calculates fractional Kelly Criterion bet sizes by factoring in real-time AMM spreads, CLOB depth, and estimated market impact.
* **Parallel Resolution Engine:** Eliminates UMA oracle delays by simultaneously polling the Polymarket Gamma API and the Binance 1H candlestick close for instant, deterministic market settlement.
* **Dual-Layer Signal Filtering:** * *Layer 1 (Deterministic):* Evaluates Streaming EMA, RSI Momentum, VWAP distance, and Cumulative Volume Delta (CVD) divergence.
    * *Layer 2 (Generative AI):* Uses a local LLM (`llama3.2:3b`) acting as a ruthless quantitative risk manager to veto borderline setups based on strict mathematical rules.
* **Elite Risk Management:** Enforces daily loss limits, maximum trades per hour, and dynamic early-exit thresholds (Take Profit / Stop Loss) based on time-to-expiry.
* **Full-Stack Analytics Dashboard:** A Next.js frontend featuring a live price chart, dynamic equity curve, win-rate heatmaps, and real-time execution logs synced via WebSockets.

---

## 🏗️ System Architecture

### Backend (Python / `asyncio`)
* **Data Ingestion:** Streams live 15m klines and aggregate trades from Binance via WebSockets.
* **Execution:** Integrates with the Polymarket CLOB client for live/paper order routing.
* **Local AI Engine:** Communicates with Ollama running locally to process fast, private inference.
* **Database:** Uses a WAL-mode SQLite database (`alpha_z_history.db`) to track all trades, execution metrics, and AI attribution.

### Frontend (Next.js / React)
* **Real-time UI:** Built with TailwindCSS and Framer Motion for a sleek, terminal-like quant experience.
* **Charting:** Utilizes TradingView's `lightweight-charts` for rendering BTC price action, VWAP, and equity curves.
* **WebSocket Stream:** Listens to the Python backend to update live metrics, PnL, and AI reasoning logs instantly.

---

## 🛠️ Prerequisites

Before running the engine, ensure you have the following installed:
1.  [Python 3.10+](https://www.python.org/downloads/)
2.  [Node.js 18+](https://nodejs.org/)
3.  [Ollama](https://ollama.com/) (Must be running locally with the `llama3.2:3b-instruct-q4_K_M` model pulled)

```bash
# Pull the required local LLM model
ollama run llama3.2:3b-instruct-q4_K_M

```

---

## ⚙️ Installation & Setup

### 1. Clone the Repository

```bash
git clone [https://github.com/rukkyowd/alpha-z-asynchronous-bitcoin-arbitrage-engine.git](https://github.com/rukkyowd/alpha-z-asynchronous-bitcoin-arbitrage-engine.git)
cd alpha-z-asynchronous-bitcoin-arbitrage-engine

```

### 2. Environment Configuration

Create a `.env` file in the root of the `backend/` directory with the following configuration:

```ini
# --- TRADING MODE ---
PAPER_TRADING=true
DRY_RUN=true

# --- POLYMARKET CREDENTIALS (If live trading) ---
POLY_PRIVATE_KEY=your_wallet_private_key
POLY_FUNDER=your_wallet_public_address
POLY_SIG_TYPE=1

```

### 3. Backend Setup

```bash
cd backend
pip install -r requirements.txt

# Run the database migration/analysis tool (optional but recommended)
python analyze_performance.py

# Start the trading engine and API server
python main.py

```

### 4. Frontend Setup

Open a new terminal window and navigate to the frontend directory:

```bash
cd frontend
npm install

# Start the development server
npm run dev

```

Navigate to `http://localhost:3000` to view the Elite Dashboard.

---

## 📊 Analytics & Reporting

Alpha Z includes a built-in Python script for deep performance attribution. Run the analysis tool to evaluate your edge:

```bash
cd backend
python analyze_performance.py

```

This generates an **Edge Attribution Report** (comparing AI-confirmed trades vs. System-only trades), a **Market Regime Breakdown**, and visualizes your **Equity Curve**.

---

## ⚠️ Disclaimer

**This software is for educational and research purposes only.** Cryptocurrency and binary options trading carry a high level of risk and may not be suitable for all investors. The developers are not responsible for any financial losses incurred while using this software. Always test thoroughly in `PAPER_TRADING=true` mode before deploying live capital.

```

Would you like me to also check your `requirements.txt` or `package.json` to ensure all the dependencies (like `pandas`, `matplotlib`, `lightweight-charts`, etc.) are properly listed for your new setup?

```
