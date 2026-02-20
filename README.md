# BTC-Polymarket-Arb: Statistical Arbitrage Engine

An automated trading research tool that identifies **Expected Value (EV)** opportunities in Polymarket’s Bitcoin "Up/Down" binary markets. It utilizes a dual-engine approach: quantitative Z-score modeling for probability estimation and a local LLM (via Ollama) for final technical analysis validation.

## 🚀 Overview

The engine monitors 1-minute BTC candles and cross-references them with Polymarket's order book. It calculates the mathematical probability of a "Price to Beat" settlement based on current volatility and time remaining, triggering an AI-assisted review when a significant "edge" is detected.

### Key Components

* **Source of Truth:** Uses **Chainlink’s BTC/USD Aggregator** on Polygon for ultra-precise settlement pricing.
* **Quantitative Gatekeeper:** Calculates probability using a time-scaled Z-score and the Error Function ().
* **AI Validator:** Fires a background task to a local **Llama 3.2:3b** model to verify price action structure before signaling.
* **Market Rolling:** Automatically increments the market slug (e.g., `...-14400` → `...-14700`) to follow the 5-minute cycle without manual restart.

---

## 🛠 Features

* **Real-time WebSocket:** Streams live candles from Binance.
* **Kelly Criterion Integration:** Suggests optimal bet sizing based on calculated edge and bankroll.
* **Wick & Body Analysis:** Filters out "noisy" price action like Dojis or extreme crowd sentiment (>97%).
* **Auto-Correction:** If the local price deviates from the settlement source, it overrides Binance data with Chainlink data for accuracy.

---

## 📋 Prerequisites

* **Python 3.10+**
* **Ollama:** Must be running locally with the `llama3.2:3b` model (or update `LOCAL_AI_MODEL` in config).
* **Active Internet Connection:** To fetch Gamma API (Polymarket) and Chainlink RPC data.

### Installation

1. **Clone the repo:**
```bash
git clone https://github.com/rukkyowd/btc-polymarket-arb.git
cd btc-polymarket-arb

```


2. **Install dependencies:**
```bash
pip install aiohttp websockets

```



---

## ⚙️ Configuration

You can tune the "Gatekeeper" in the script header to match your risk tolerance:

| Variable | Default | Description |
| --- | --- | --- |
| `BANKROLL` | `15.0` | Your current available trading capital. |
| `MIN_EV_PCT` | `0.1` | Minimum % Expected Value to trigger the AI. |
| `MIN_SECONDS` | `10` | Won't trade if the market expires in <10s. |
| `LOCAL_AI_URL` | `.../v1/...` | Endpoint for your local Ollama/Llama instance. |

---

## 🖥 Usage

1. Start your local AI server (e.g., `ollama serve`).
2. Run the engine:
```bash
python poly_btc.py

```


3. **Input:** When prompted, paste a Polymarket BTC Up/Down market URL or its slug.
* *Example:* `https://polymarket.com/event/bitcoin-price-at-830-pm-et`



---

## 📈 Probability Logic

The engine determines the "True Probability" () of a strike being hit using:

Where:

*  is the rolling standard deviation (volatility).
*  is the minutes remaining until expiry.
* The result is passed through the **Gaussian Error Function** to estimate the probability of the price finishing above or below the strike.

---

## ⚠️ Disclaimer

**This is a research tool.** Trading binary options and crypto futures involves significant risk. The "decisions" provided by the local AI are based on technical patterns and statistical models, not financial advice. Use this code to inform your own strategies, not to trade blindly.

---
