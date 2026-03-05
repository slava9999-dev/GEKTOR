---
name: crypto-analyst-v3
description: Advanced crypto market intelligence and automated sentiment analysis. Integrates with MCP servers and external APIs to provide actionable trading insights.
version: 3.0.0
author: Red-Citadel-Core
triggers:
  - "анализ рынка"
  - "crypto sentiment"
  - "funding rate check"
  - "should I trade"
  - "market overview"
---

# 📊 Crypto Analyst V3 — Market Intelligence

## 🎯 Primary Objective

Transform Gerald into a tactical trading assistant by providing real-time data synthesis and sentiment-weighted signals.

## 🛠 Required Capabilities

1.  **Multi-Source Synthesis**:
    - Fetching data from Bybit/Binance (via existing Red Citadel logic).
    - Analyzing news and sentiment from X (Twitter) and specialized feeds.
2.  **Technical & Fundamental Hybrid**:
    - Correlating Funding Rates with price action.
    - Identifying "Smart Money" movements (CVD, Liquidations).
3.  **Strategic Recommendations**:
    - Evaluates "Trade/No Trade" based on current risk parameters.

## 🚦 Operational Workflow

1.  **Data Gathering**: Access MCP servers for CoinCap or Twelve Data if available.
2.  **Context Loading**: Pull latest trading strategies from `skills/vector-knowledge`.
3.  **Risk Analysis**: Cross-reference current market volatility with `GPUMonitor` (ensuring system is stable before heavy trading computations).
4.  **Final Signal**: Output a CoT-reasoned market outlook.

## 🛡 Security & Constraints

- **NEVER** execute trades directly without explicit confirmation strings.
- **ALWAYS** warn about the "Financial Advice" disclaimer.
- **Idempotency**: Ensure a single analysis doesn't lead to duplicate trade recommendations.

## 🧪 Examples

**User**: "Что по Бииткоину сегодня?"
**Result**: A structured report covering Price, Funding, Social Sentiment, and a "Risk Level" assessment.

---

_Status: Tactical | Sector: Quant Trading_
