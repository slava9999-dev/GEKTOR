# 🎯 GEKTOR v12.0 (APEX) — Macro-Radar Architecture

# 🏗️ SYSTEM STATUS
- **Core Version:** 12.0 (HARDENED)
- **Status:** Operational / Advisory Mode
- **System Target:** Institutional-Grade Signal Discovery (Market-Mirror)
- **Execution Mode:** AMPUTATED (Self-Aware Monitoring)

---

## 🛠️ KEY COMPONENTS

### 🔹 TradeSweeper (L3 Aggregation)
- Reconstructs fragmented trade streams into unified **Institutional Sweeps**.
- Identifies **Aggressor Signatures** (Icebergs, Impulse, Distribution).
- Includes **Pessimistic Sweep Fill (v12.0)** for risk-realistic advisory targets.

### 🔹 Dynamic Universe Shaker
- Background autonomous loop for **monitoring pool rebalancing** (15m interval).
- **Filters:** Turnover > $50M, Spread < 15bps (Liquidity-First Universe).
- Real-time **Shard Rebalancing** on the `NerveCenter` bus.

### 🔹 Macro-Radar Engine
- **Alpha-Neutral Discovery:** Beta-neutralizing CVD against BTC for isolated signals.
- **Stealth CUSUM Detector:** Identifies TWAP/VWAP accumulation via cumulative sum drift.
- **Spatial Basis Audit:** Validates Lead-Lag signals (Perp vs Spot divergence).

### 🔹 Aegis (Decay Sentinel)
- Persistent monitoring of **Signal Expectancy (WR & R-Multiple)** in Redis.
- **Auto-Halt Protocol:** Instantly disables advisory signals on mathematical Alpha decay.
- **Self-Excitation Filter:** Suppresses "feedback loops" (Shadow Liquidity Overdrive).

---

## 🔒 SECURITY & RESILIENCE
- **Zero-GIL Math:** All heavy statistical analysis (Z-Score, MAD, Variance Ratio) runs in `ProcessPoolExecutor`.
- **NerveCenter (Redis-Bus):** Ultra-low latency asynchronous synchronization across distributed shards.
- **StateReconciler:** Automatic REST-based gap filling on WebSocket disconnections.
- **Aegis-Halt:** Manual or automatic global kill-switch for market-breaking events.

---

## 🚀 GETTING STARTED

### 1. Requirements
- Python 3.11+
- PostgreSQL (TimescaleDB)
- Redis 7.0+
- Bybit V5 API Credentials

### 2. Deployment
```bash
# Install dependencies
pip install -r requirements.txt

# Run Pre-Flight Validation
python pre_flight.py

# Start APEX Engine
python main.py
```

### 3. Monitoring
- Institutional-grade alerts are dispatched via **Telegram Broadcaster**.
- Real-time performance tracking is stored in the `signals` table for post-trade analysis.

---

_Created: 2026-04-04 | Project: GEKTOR v12.0 (APEX) | Status: HARDENED_
