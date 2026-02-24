```markdown
# ⚡ Alpha-Z: Asynchronous Bitcoin Arbitrage Engine

A high-performance, full-stack quantitative trading system that executes statistical arbitrage between Binance BTC/USDT spot price movements and Polymarket "Up/Down" binary markets.

Built with an emphasis on low-latency execution and institutional-grade order flow analysis, Alpha-Z captures short-term price inefficiencies in 5-minute windows. 

---

## 🚀 Overview

Alpha-Z operates on a highly concurrent, asynchronous architecture. It monitors 1-minute BTC candle structures, True Daily Volume Weighted Average Price (VWAP), and Cumulative Volume Delta (CVD) to predict settlement outcomes. The backend engine streams live decisions to a React-based frontend dashboard for real-time performance tracking and Monte Carlo equity projections.

### ✨ Key Features

* **Sub-Second Data Ingestion:** Dual-track WebSockets for Binance K-Lines and aggregate trades ensure zero-lag price and volume updates.
* **Order Flow Analysis:** Calculates real-time CVD to detect institutional buying or selling pressure, pinpointing high-probability divergences.
* **Three-Stage Validation Pipeline:** 1. **Gatekeeper:** Evaluates liquidity, time decay, and mathematical Expected Value (EV).
  2. **Rule Engine:** Scores setups based on momentum and technical indicators.
  3. **High-Speed AI Validation:** Leverages optimized local LLMs (via Ollama) for final contextual verification. Tuned specifically for lightweight models (3B - 8B parameters) to ensure execution occurs within critical sub-5-second windows before alpha decays.
* **Dynamic Risk Management:** Implements fractional Kelly Criterion sizing, daily PnL circuit breakers, and dynamic Take-Profit/Stop-Loss thresholds based on time-to-expiration.
* **Robust Execution:** Uses Polymarket's CLOB with Immediate-Or-Cancel (IOC) partial fill handling and "dust" management.

---

## 🛠 Project Structure

```text
Alpha-Z-Asynchronous-Bitcoin-Arbitrage-Engine/
├── backend/                 # Trading engine and FastAPI server
│   ├── core.py              # Core logic, technical indicators, & Polymarket execution
│   ├── main.py              # Backend API & WebSocket management
│   ├── alpha_z_history.db   # SQLite (WAL mode) trade persistence
│   └── requirements.txt     # Python dependencies
└── frontend/                # React/Next.js performance dashboard
    ├── package.json         # Node.js dependencies
    └── (UI Components)      # Performance charts and live feeds

```

---

## 🚦 Setup & Installation

### Prerequisites

* Python 3.10+
* Node.js (v16+)
* [Ollama](https://ollama.com/) installed and running locally.
* *Note: For the 5-minute trading windows, low-latency inference is required. We highly recommend using `llama3.1` (8B), `qwen2.5:7b`, or ultra-lightweight models like `llama3.2` (3B) rather than heavier 12B+ models to avoid timeout failures.*


* Polymarket API Credentials (funded account on Polygon/Chain 137).

### 1. Backend Setup

Navigate to the backend directory, install dependencies, pull your preferred AI model, and configure your environment variables:

```bash
cd backend
pip install -r requirements.txt

# Pull the recommended high-speed reasoning model
ollama pull llama3.1

# Copy example environment variables to active .env
cp env.example .env

# Start the server
python main.py

```

### 2. Frontend Setup

Navigate to the frontend directory to launch the performance dashboard:

```bash
cd frontend
npm install
npm run dev

```

---

## 📊 Technical Strategy

Alpha-Z relies on a confluence of high-timeframe fairness and low-timeframe momentum:

| Indicator / Logic | Purpose |
| --- | --- |
| **VWAP** | Identifies "fair value" to determine the broader session bias. |
| **CVD Divergence** | Measures net market buy vs. sell volume to detect hidden exhaustion and institutional flow against price. |
| **EMA Cross (9/21)** | Confirms short-term trend momentum leading into market expiration. |
| **Log-Vol Math** | Calculates true mathematical probability based on historical log-returns to ensure positive Expected Value (EV). |
| **Kelly Criterion** | Dynamically scales bet sizes based on signal conviction and account bankroll. |

---

## 🏆 Performance (Simulated Run)

**Test Period:** Feb 20, 2026 – Feb 21, 2026

**Market:** Polymarket 5-Minute BTC/USD

During a continuous 18-hour stress test, the Alpha-Z engine successfully demonstrated a significant predictive edge in volatile conditions:

* **Overall Win Rate:** **74.6%** (147 Wins / 50 Losses)
* **Total Trades Executed:** 197
* **Alpha Generation:** Maintained a profitable 57.1% win rate even on counter-trend (DOWN) predictions, proving the efficacy of the CVD and AI-validation pipeline.

---

## ⚠️ Disclaimer

*This software is strictly for educational and research purposes. Trading cryptocurrency and binary options involves significant financial risk. The author is not responsible for any financial losses incurred through the use of this software. Always test with paper trading enabled.*

```

```
