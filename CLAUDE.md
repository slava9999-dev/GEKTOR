# 🛑 GEKTOR APEX: CORE ARCHITECTURE MANIFESTO (v2.0 STRICT) 🛑

# 🎯 MISSION & IDENTITY
**GEKTOR — ЭТО ИСКЛЮЧИТЕЛЬНО АНАЛИТИЧЕСКИЙ РАДАР. ОН НЕ ТОРГУЕТ.**
Система предназначена для поиска **среднесрочных аномалий** (Medium-Term) и скрытой институциональной аккумуляции.

- **Status:** MISSION CONTROL [ACTIVE]
- **Identity:** Analytical Radar (Advisory Only)
- **Execution:** STRICTLY MANUAL (Operator via Telegram)
- **Horizon:** 4h to 2w (Medium-Term)

---

# 🚫 ANTI-PATTERNS (STRICTLY PROHIBITED)
1. ❌ **HFT/Scalping:** No 1m/1s bars. No micro-OFI for sub-minute entries.
2. ❌ **Auto-Execution:** No `ExecutionEngine`, `OrderManager`, or direct Broker API integration.
3. ❌ **Silent Failures:** Never use `except Exception: pass`. All errors must be logged and handled.
4. ❌ **Event Loop Blocks:** CPU-bound math must reside in `ProcessPoolExecutor`.
5. ❌ **Asset Siloing:** Never analyze a symbol without BTC/ETH Macro Regime context.

---

# 📋 DEVELOPMENT PROTOCOLS (GEKTOR v2.0)

## 1. High-Fidelity Macro Ingestion (Phase 1)
- **Raw Ticks to Buckets:** Transform WebSocket trades into **Adaptive Dollar Bars**.
- **Conflation:** Batch L2 updates to minimize processing overhead. No noise.

## 2. Quant Engine & VPIN (Phase 2)
- **VPIN:** Calculate volume-synchronized toxicity for medium-term accumulation.
- **FFD (Fractional Differentiation):** Preserve memory while achieving stationarity.

## 3. Meta-Labeling & Validation (Phase 3)
- **Purged K-Fold CV:** Eliminate data leakage and autocorrelation.
- **Embargoing:** A mandatory 1% temporal gap between training and testing sets.

## 4. Atomic State Persistence (Phase 4)
- **Memory Recovery:** All math state (VPIN, CUSUM) must survive container restarts.
- **Storage:** PostgreSQL/TimescaleDB (Signals) + Redis (Volatile telemetry).

## 5. Decision Support & Abort Mission (Phase 5)
- **Invalidation:** Generate `[🚨 ABORT MISSION]` alerts if the microstructural premise breaks before the macro goal is met.

---

# 🛠️ TECHNOLOGY STACK
- **Backend:** Python 3.11+ (Strict Typing)
- **Framework:** `asyncio` + `ProcessPoolExecutor` (Math)
- **Messaging:** Redis Pub/Sub (NerveCenter)
- **Monitoring:** Loguru with structured JSON for observability.

---

# 🚀 OPERATIONS
- **Start Up:** `python main.py` (Mode: ADVISORY)
- **Alerts:** Clean, one-way Telegram signal dispatch (Entry/Abort/Heartbeat).
- **Manifesto Source of Truth:** Read [GEKTOR_MANIFESTO.md](file:///c:/Gerald-superBrain/GEKTOR_MANIFESTO.md) before any architectural change.
