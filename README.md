Here is the complete, professional `README.md` for your **Alpha-Z** project, formatted specifically for you to copy and paste directly into your root directory.

```markdown
# Alpha-Z: Asynchronous Bitcoin Arbitrage Engine

A high-performance, full-stack trading system that executes statistical arbitrage between Binance BTC/USDT spot price movements and Polymarket "Up/Down" binary markets.



## 🚀 Overview

Alpha-Z is a quantitative trading engine designed to exploit short-term price inefficiencies. It monitors 1-minute BTC candle structures, Volume Weighted Average Price (VWAP), and Cumulative Volume Delta (CVD) to predict settlement outcomes for Polymarket's 5-minute price targets. The system includes a React-based dashboard for real-time performance tracking and Monte Carlo projections.

### Key Features
* **Real-time Data:** Leverages WebSockets for Binance K-Lines and aggregate trades to achieve sub-second price updates.
* **Order Flow Analysis:** Calculates real-time CVD to detect institutional buying or selling pressure.
* **Validation Pipeline:** Features a three-stage filter: **Gatekeeper** (liquidity/EV) → **Rule Engine** (technical scoring) → **AI Validation** (Ollama LLM).
* **Live Dashboard:** Provides a FastAPI-powered UI for visualizing the equity curve and trade history.

---

## 🛠 Project Structure

```text
Alpha-Z-Asynchronous-Bitcoin-Arbitrage-Engine/
├── backend/                 # Trading engine and FastAPI server
│   ├── core.py              # Core logic & technical indicators
│   ├── main.py              # Backend API & WebSocket management
│   └── requirements.txt     # Python dependencies
└── frontend/                # React/Next.js performance dashboard
    ├── package.json         # Node.js dependencies
    └── (UI Components)      # Performance charts and live feeds

```

---

## 🚦 Setup & Installation

### 1. Backend Setup

Navigate to the backend directory, install dependencies, and configure your environment variables:

```bash
cd backend
pip install -r requirements.txt
# Copy example env to active .env
copy env.example .env
# Start the server
python main.py

```

### 2. Frontend Setup

Navigate to the frontend directory to launch the dashboard:

```bash
cd frontend
npm install
npm run dev

```

---

## 📊 Technical Strategy

| Indicator | Purpose |
| --- | --- |
| **VWAP** | Identifies "fair value" to determine session bias. |
| **CVD** | Measures net market buy vs. sell volume to detect breakouts. |
| **EMA (9/21/50)** | Confirms short and medium-term trend momentum. |
| **RSI** | Filters out entries in overextended market conditions. |
| **Log-Vol Math** | Calculates mathematical probability based on historical log-returns. |

---

## 🏆 Performance (Simulated Run)

**Test Period:** Feb 20, 2026 – Feb 21, 2026
**Market:** Polymarket 5-Minute BTC/USD

During an 18-hour continuous test, the Alpha-Z engine successfully demonstrated a significant predictive edge:

* **Overall Win Rate:** **74.6%** (147 Wins / 50 Losses)
* **Total Trades:** 197
* **Alpha Generation:** Maintained a profitable 57.1% win rate even on counter-trend (DOWN) calls.

---

## ⚠️ Disclaimer

*This software is for educational purposes. Trading cryptocurrency involves significant risk. The authors are not responsible for any financial losses incurred through the use of this bot.*
