---
name: crypto-sentinel-pro
description: Advanced crypto market screener that monitors Bybit for volume anomalies, Open Interest (OI) spikes, and sends formatted Telegram alerts, replacing manual chart watching.
version: 1.0.0
author: Gerald-SuperBrain (Principal Engineer)
triggers:
  - "запусти сентинела"
  - "start sentinel"
  - "включи скринер"
  - "покажи аномалии рынка"
---

# 👁️‍🗨️ Crypto Sentinel PRO (Digashka Style)

## 🎯 Primary Objective

A background daemon that monitors crypto markets (primarily Bybit Linear Perpetual) 24/7 for:

1. Abnormal trading volumes (Volume Spikes).
2. Large influxes or outflows of Open Interest (OI Spikes).
3. Highly active/volatile coins.

When a trigger fires, it sends an actionable alert to the user's Telegram via `telegram-guard`.

## 🛠 Required Capabilities

1. **Volume Profiling**: Compares current 5m/15m volume against historical moving average.
2. **Open Interest Tracking**: Identifies sudden accumulation or distribution.
3. **Telegram Reporting**: Alerts using styled cards.

## 🚦 Architecture

- `sentinel_daemon.py`: The standalone async worker script that loops and tests conditions.
- `config.json`: User-defined thresholds (e.g. "Alert if 5m Volume > $5M and OI Change > 5%").

## 🛡 Alert Types Examples

- 🚨 **VOLUME ALERT**: $SOL +25% Volatility & Volume Spike $15M in 5 mins.
- ⚡ **OI ANOMALY**: $BTC Open Interest grew by 8% ($50M) while price is stagnant (Squeeze imminent).
