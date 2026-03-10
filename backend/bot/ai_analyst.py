"""Alpha-Z AI Analyst Offline Learning Loop.

This script queries the database for past trades, runs a performance analysis,
and asks the local LLM to propose an improved StrategyConfig.
It then validates the proposed rules by simulating them against historical trades.
If the new rules perform better, they are saved to ai_strategy_config.json
for dynamic reloading by the trading engine.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from typing import Any

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

import aiohttp
from backtest import load_resolved_trades, pnl_by_bucket, ResolvedTrade
from bot.strategy import StrategyConfig

# Default Ollama endpoint for llama3.2:3b
LLM_URL = "http://localhost:11434/v1/chat/completions"
MODEL_NAME = "llama3.2:3b-instruct-q4_K_M"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ai_analyst")

CONFIG_PATH = os.path.join(_BACKEND_DIR, "ai_strategy_config.json")
SUPPORTED_ANALYST_FIELDS = ("min_score_to_trade",)


def _normalized_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _merge_supported_strategy_changes(
    current_config: StrategyConfig,
    proposed_payload: dict[str, Any],
) -> tuple[StrategyConfig, list[str]]:
    current_dict = current_config.to_dict()
    merged_dict = dict(current_dict)
    unsupported_changes: list[str] = []

    for key, value in proposed_payload.items():
        if key in SUPPORTED_ANALYST_FIELDS:
            merged_dict[key] = value
            continue
        if _normalized_json(value) != _normalized_json(current_dict.get(key)):
            unsupported_changes.append(key)

    return StrategyConfig.from_dict(merged_dict), sorted(set(unsupported_changes))


def simulate_pnl_with_config(trades: list[ResolvedTrade], config: StrategyConfig) -> float:
    """
    Simulates PnL for the small subset of strategy rules that are currently
    supported by the offline analyst. At present this is only the score gate.
    """
    simulated_pnl = 0.0
    for trade in trades:
        score = getattr(trade, "score", 0)
        # Apply rudimentary filtering based on new rules
        if score < config.min_score_to_trade:
            # Under new config, this trade would not have been taken.
            continue
            
        simulated_pnl += trade.pnl_impact
        
    return simulated_pnl


async def generate_new_strategy(
    trades: list[ResolvedTrade],
    current_config: StrategyConfig,
) -> tuple[StrategyConfig | None, list[str]]:
    wins = sum(1 for t in trades if t.result == "WIN")
    losses = len(trades) - wins
    total_pnl = sum(t.pnl_impact for t in trades)
    score_pnl = pnl_by_bucket(trades, "score")
    
    prompt = f"""You are an elite quantitative researcher for the Alpha-Z binary options engine.
Your task is to optimize our StrategyConfig parameters to increase profitability.
Current performance:
- Total Trades: {len(trades)}
- Wins/Losses: {wins}/{losses}
- Total PnL: ${total_pnl:.2f}

PnL By Technical Score (0 to 4):
{json.dumps(score_pnl, indent=2)}

Our current strategy config:
{json.dumps(current_config.to_dict(), indent=2)}

Based on where we are losing money, propose a modified JSON config that adjusts ONLY the
supported analyst field(s): {", ".join(SUPPORTED_ANALYST_FIELDS)}.
Do not change any other StrategyConfig field.
You may output either a minimal JSON object like {{"min_score_to_trade": 2}} or a full
StrategyConfig payload, but only {", ".join(SUPPORTED_ANALYST_FIELDS)} will be validated.
Output ONLY valid JSON. Do not output markdown code blocks, just raw JSON.
"""
    logger.info("Querying LLM for new strategy...")
    aotimeout = aiohttp.ClientTimeout(total=300.0)
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(
                LLM_URL,
                json={
                    "model": MODEL_NAME,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.2, 
                    "format": "json" # Ollama structured JSON output
                },
                timeout=aotimeout
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()
                content = data["choices"][0]["message"]["content"]
                
                # Cleanup markdown formatting if model ignores prompt
                if content.startswith("```json"):
                    content = content[7:-3]
                elif content.startswith("```"):
                    content = content[3:-3]
                    
                new_dict = json.loads(content)
                normalized_config, unsupported_changes = _merge_supported_strategy_changes(current_config, new_dict)
                return normalized_config, unsupported_changes
        except asyncio.TimeoutError:
            logger.error("LLM request timed out after 300s")
            return None, []
        except Exception as e:
            logger.error(f"LLM request failed or returned invalid JSON: {e}")
            return None, []


async def run_analyst_loop(db_path: str):
    logger.info("Starting AI Analyst Learning Loop...")
    trades = load_resolved_trades(db_path)
    if not trades:
        logger.warning("No resolved trades found to learn from.")
        return
        
    current_config = StrategyConfig.load_managed(CONFIG_PATH)
    baseline_pnl = sum(t.pnl_impact for t in trades)
    
    logger.info(f"Loaded {len(trades)} trades. Baseline PnL: ${baseline_pnl:.2f}")

    new_config, unsupported_changes = await generate_new_strategy(trades, current_config)
    if not new_config:
        logger.warning("No valid config generated.")
        return
    if unsupported_changes:
        logger.warning(
            "Ignoring unsupported AI analyst changes: %s. Only validating %s.",
            ", ".join(unsupported_changes),
            ", ".join(SUPPORTED_ANALYST_FIELDS),
        )
    if all(getattr(new_config, field) == getattr(current_config, field) for field in SUPPORTED_ANALYST_FIELDS):
        logger.info(
            "AI analyst proposed no supported rule changes. Supported fields: %s",
            ", ".join(SUPPORTED_ANALYST_FIELDS),
        )
        return
        
    sim_pnl = simulate_pnl_with_config(trades, new_config)
    logger.info(f"Simulated PnL with new config: ${sim_pnl:.2f}")
    
    if sim_pnl > baseline_pnl:
        logger.info("New strategy is better! Saving to ai_strategy_config.json")
        with open(CONFIG_PATH, "w") as f:
            json.dump(new_config.to_dict(), f, indent=2)
    else:
        logger.info("New strategy rejected (Simulated PnL worse than or equal to baseline).")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=os.path.join(_BACKEND_DIR, "alpha_z_history.db"))
    args = parser.parse_args()
    
    asyncio.run(run_analyst_loop(args.db))

if __name__ == "__main__":
    main()
