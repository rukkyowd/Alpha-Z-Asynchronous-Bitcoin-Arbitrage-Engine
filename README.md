## BTC-Arb-Link: Asynchronous Quantitative Arbitrage Engine

### Overview

BTC-Arb-Link is a high-frequency statistical arbitrage engine designed to exploit pricing discrepancies between live Bitcoin order flow and Polymarket binary prediction markets. The system leverages a non-blocking asynchronous architecture to process real-time market data and calculate execution signals using quantitative volatility modeling.

---

### Core Technical Architecture

#### 1. Asynchronous Execution Core

The engine is built on Python’s `asyncio` and `websockets` libraries. Unlike standard synchronous scripts that block execution during API calls, this system maintains a persistent connection to the Binance L1 WebSocket. While the engine performs heavy quantitative calculations or awaits an LLM response, the market listener continues to stream and buffer tick data without latency or connection drops.

#### 2. Quantitative Volatility Model (Z-Score)

Rather than relying on subjective technical analysis, the engine calculates the mathematical probability of price crossing the Polymarket strike using a time-scaled Z-Score model.

* **Calculated Volatility:** Standard deviation derived from the preceding 10 minutes of one-minute klines.
* **Time Scaling:** Volatility is scaled by the square root of time remaining in the market window ().
* **Probability Output:** A cumulative distribution function (CDF) determines the statistical likelihood of a successful strike cross before market expiry.

#### 3. Automated Market Discovery

The "Auto-Slug Hunter" eliminates manual URL entry. It polls the Polymarket Gamma API to identify active 5-minute Bitcoin events. It evaluates candidate markets by their proximity to expiration and automatically locks onto the most imminent live window, resetting itself the moment the current market resolves.

#### 4. Local EV Gatekeeper

To optimize computational resources and API throughput, a local gatekeeper evaluates the Expected Value (EV) of every candle close. The system only triggers an external execution confirm when:

* The mathematical EV exceeds a +8% threshold.
* The model probability exceeds a 62% confidence level.
* Market liquidity and time-decay constraints (90s - 270s) are met.

---

### Key Features

* **Zero-Warmup Infiltration:** Prefills historical candlestick data via Binance REST API upon boot for instant trade readiness.
* **Exponential Backoff:** Integrated rate-limit handling for external API dependencies.
* **UTF-8 Enforcement:** Customized logging handlers to ensure data integrity across various terminal environments.
* **Capital Preservation Logic:** Automatic "SKIP" triggers for Doji structures, low-volatility environments, and extreme crowd pricing.

---

### Prerequisites

* Python 3.10+
* `websockets`
* `aiohttp`
* Google Gemini API Key (Flash 2.0 recommended for low-latency response)

---

### Installation and Usage

1. Clone the repository.
2. Install dependencies: `pip install websockets aiohttp`.
3. Configure `GEMINI_API_KEY` within the script.
4. Execute: `python poly_btc.py`.

---

### Disclaimer

This software is for educational and research purposes only. Quantitative trading involves significant risk. The developers are not responsible for financial losses incurred through the use of this engine.
