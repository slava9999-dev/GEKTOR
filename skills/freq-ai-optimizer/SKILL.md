---
name: freq-ai-optimizer
description: Self-evolving trading strategy optimizer. Uses machine learning (Ensemble/Neural) to adapt risk parameters and entry logic to current market regimes.
version: 1.0.0
author: Gerald-Quant-ML
triggers:
  - "optimize strategy"
  - "обучи модель"
  - "backtest check"
  - "adapt to market"
  - "strategy review"
---

# 🧠 Freq-AI Optimizer — Self-Learning Quant

## 🎯 Primary Objective

Ensure Gerald's trading strategies never become obsolete by continuously retraining on fresh data and optimizing hyper-parameters.

## 🛠 Required Capabilities

1.  **Strategy Backtesting**:
    - Running historical simulations on candle data (1m, 5m, 1h).
2.  **Market Regime Detection**:
    - Identifying Trend (Bull/Bear) vs. Ranging (Side) markets.
    - Adjusting indicators (EMA, RSI, Bollinger) automatically.
3.  **Model Retraining**:
    - Using `execute_python_code` to run `scikit-learn` or `XGBoost` on recent trade logs.

## 🚦 Operational Workflow

1.  **Audit**: Review last 10 successful and failed trades from Red Citadel logs.
2.  **Hypothesis**: "Current RSI settings are too sensitive for this volatility."
3.  **Backtest**: Run simulation with adjusted parameters.
4.  **Proposal**: "Слава, бэктест показал +12% к прибыли при смене RSI с 14 на 21. Применяем?"

## 🛡 Security & Constraints

- **NEVER** overfit to short-term noise (minimum 1000 candles for backtest).
- **ALWAYS** keep a 'Safe' baseline strategy to fall back on.

## 🧪 Examples

**User**: "Почему бот начал сливать?"
**Result**: "Анализ показал резкое падение волатильности. Стандартная стратегия тренда не работает в боковике. Включаю 'Side-Market' режим с более узкими тейк-профитами."

---
_Status: Quant Grade | Sector: Machine Learning_
