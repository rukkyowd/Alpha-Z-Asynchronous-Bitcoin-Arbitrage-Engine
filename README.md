# BTC Polymarket Arbitrage Engine (v5)

A high-performance, asynchronous Python trading bot that executes statistical arbitrage between Binance BTC/USDT spot price movement and Polymarket "Up/Down" binary markets.

## 🚀 Overview

This engine monitors the 1-minute BTC candle structure, Volume Weighted Average Price (VWAP), and Cumulative Volume Delta (CVD) to predict settlement outcomes for Polymarket's 5-minute price targets. It uses a **Gatekeeper → Rule Engine → AI Validation** pipeline to ensure only high-probability trades are executed.

### Key Features

* **Real-time Data:** Uses WebSockets for Binance K-Lines and Aggregate Trades (aggTrade) for sub-second price updates.
* **Order Flow Analysis:** Calculates real-time CVD (Cumulative Volume Delta) to detect institutional buying or selling pressure.
* **Multi-Step Validation:** * **Gatekeeper:** Filters markets based on Time-to-Expiry, Expected Value (EV), and liquidity.
* **Rule Engine:** A deterministic scoring system (0-4) based on VWAP, EMA Crosses, RSI, and CVD.
* **Local AI (Ollama):** Uses `llama3.2:3b` to provide a "second opinion" on borderline trade setups.


* **Automated Betting:** Integrated with the Polymarket CLOB (Central Limit Order Book) for automated execution.
* **Safety First:** Includes a Circuit Breaker for AI timeouts, a "Dry Run" mode, and Kelly Criterion-based position sizing.

---

## 🛠 Setup & Installation

### 1. Prerequisites

* **Python 3.10+**
* **Ollama:** Must be running locally with the `llama3.2:3b` model loaded.
* **Polymarket Account:** A proxy/funder wallet address and your private key.

### 2. Install Dependencies

```bash
pip install asyncio aiohttp websockets py-clob-client python-dotenv

```

### 3. Environment Variables

Create a `.env` file in the root directory:

```env
POLY_PRIVATE_KEY=your_private_key_here
POLY_FUNDER=your_proxy_wallet_address
POLY_SIG_TYPE=1
DRY_RUN=true

```

> **Note:** Set `DRY_RUN=false` only when you are ready to risk real USDC.

---

## 📊 Technical Indicators Used

| Indicator | Description | Purpose |
| --- | --- | --- |
| **VWAP** | Volume Weighted Average Price | Identifies the true "fair value" of the session. |
| **CVD** | Cumulative Volume Delta | Measures the net difference between market buy and sell volume. |
| **EMA (9/21/50)** | Exponential Moving Averages | Detects short-term and medium-term trend momentum. |
| **RSI** | Relative Strength Index | Prevents entering trades in overextended (Overbought/Oversold) conditions. |
| **Log-Vol Prob** | Log-return Volatility Math | Calculates the mathematical probability of a strike hitting based on historical volatility. |

---

## 🚦 How it Works

1. **Prefill:** On startup, the bot fetches the last 120 minutes of BTC history to seed the indicators.
2. **Streams:** It opens three concurrent WebSocket connections:
* Binance Klines (1m candles).
* Binance aggTrade (Tick-by-tick price/CVD).
* Polymarket Live Data (Oracle price monitoring).


3. **Evaluation:** Every 5 seconds, the `evaluation_loop` checks the current market:
* If **Score ≥ 3**: Places a bet immediately.
* If **Score = 1 or 2**: Calls the local AI to decide whether to "SKIP" or "COMMIT."


4. **Market Roll:** Once a market expires, the bot automatically increments the slug (e.g., `...-1030` → `...-1035`) and begins evaluating the next window.

---

## 📈 Logging & Statistics

The bot maintains a detailed `trading_log.txt` for real-time debugging and an `ai_trade_history.csv` for performance tracking.

The CSV tracks:

* Strike Price vs. Final Price.
* AI Decisions & Rule Scores.
* Win/Loss outcome and running Win Rate.

---

## ⚠️ Disclaimer

*This software is for educational purposes. Trading cryptocurrency involves significant risk. The authors are not responsible for any financial losses incurred through the use of this bot.*

---
