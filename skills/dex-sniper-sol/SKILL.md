---
name: dex-sniper-sol
description: Ultra-fast Solana DEX sniping and liquidity monitoring. Optimized for Raydium and Pump.fun with Jito/MEV protection logic.
version: 1.0.0
author: Gerald-Sol-War-Machine
triggers:
  - "snipe token"
  - "найди гем"
  - "monitor pump.fun"
  - "raydium sniper"
  - "check liquidity"
---

# ⚡ DEX Sniper SOL — Tactical Liquidity Striker

## 🎯 Primary Objective

Navigate the high-volatility Solana meme-coin market to identify and (with Slava's approval) "snipe" high-potential tokens at launch.

## 🛠 Required Capabilities

1.  **New Listing Surveillance**:
    - Polling Raydium and Pump.fun for new pool creation.
2.  **Rug-Check & Security**:
    - Automatic analysis of contract ownership (Renounced?).
    - Liquidity Lock status check.
    - Developer history analysis (previous "ruggers").
3.  **Execution Logic**:
    - Calculated entry points using Jito Tips to ensure transaction priority.

## 🚦 Operational Workflow

1.  **Discovery**: Scan latest 10 tokens on Pump.fun.
2.  **Validation**: Run `cyber-sentry` logic on the token contract address.
3.  **Alerting**: "Слава, найден токен [NAME] с заблокированной ликвидностью и чистым кодом. Потенциал 10x. Снайпим?"
4.  **Action**: Execute buy order via `execute_python_code` using `solana-py` if approved.

## 🛡 Security & Constraints

- **NEVER** trade without `v0-protection` (rugged tokens are 99% of the market).
- **ALWAYS** set a hard stop-loss of 15% for sniped positions.
- **NEVER** risk more than 1% of the wallet on a single "gem".

## 🧪 Examples

**User**: "Счет на Pump.fun сегодня какой?"
**Result**: "Найдено 3 токена с ростом объема. Два — скам (админ не сжег ключи). Один — [TOKEN] выглядит чисто. Объем $50k за 5 минут."

---
_Status: Combat Ready | Sector: DEX Trading_
