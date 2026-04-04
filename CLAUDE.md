# 🛡️ GEKTOR v12.0 (APEX) — Global Project Standards

# 🎯 MISSION & IDENTITY
GEKTOR (Macro-Radar) is an advanced, non-interactive institutional-grade advisory system designed for high-fidelity liquidity monitoring and signal discovery.

**Current Mode:** `MACRO_RADAR` (Advisory)
**Execution Mode:** `AMPUTATED` (Manual Execution Only)

---

# 📋 DEVELOPMENT PROTOCOLS

## 1. Zero-GIL Policy (Math Intensive)
- Use `ProcessPoolExecutor` for all heavy math (Median, MAD, Variance Ratio).
- Do NOT use `asyncio` for CPU-bound computations; it will freeze the event loop.
- Use `msgspec` for ultra-fast serialization between processes.

## 2. Low-Latency I/O Sharding
- Each shard (Bybit Symbol Group) runs in its own process.
- Use **Redis Pub/Sub** (NerveCenter) for inter-shard communication.
- Global `RealtimeStateManager` monitors `StatusChangeEvent` for live shard health.

## 3. High-Fidelity Data Ingestion
- **TradeSweeper**: Always reconstruct fragmented L3 trades into unified "Sweeps".
- **Pessimistic Fill**: Assume the worst fill (VWAP) when simulating advisor outcomes.
- **Basis Correction**: Always compare Perpetual price to Spot price (Lead-Lag validation).

---

# 🛠️ TECHNOLOGY STACK
- **Back-end:** Python 3.11+
- **Concurrency:** `asyncio` + `ProcessPoolExecutor` + `msgspec`
- **Infrastructure:**
  - **Redis:** Volatile telemetry, Pub/Sub, CUSUM/Sentinel state.
  - **PostgreSQL (TimescaleDB):** Deep historical bar and signal storage.
  - **ZeroMQ:** Ultra-low latency IPC between shard workers.

---

# 🔒 SAFETY & RESILIENCE
- **Decay Sentinel (Aegis):** Persistent monitoring of signal expectancy; auto-halt on Alpha decay.
- **Fail-Fast Boot:** Every module must validate API connectivity and DB state before entering the event loop.
- **Vitals Watchdog:** Monitors shard heartbeats; triggers auto-respawn on network partitions.

---

# 📝 CODE STANDARDS
- **Typing:** Strict `typing` and `pydantic` models for all data structures.
- **Error Handling:** Explicit logging with correlation IDs; no silent crashes.
- **Testing:** `pytest` for all core logic; simulate network gaps in unit tests.

---

# 🚀 OPERATIONS
- **Start Up:** `python main.py` or `./run_tekton.ps1`
- **Alerts:** Institutional-grade reporting via Telegram.
- **Persistence:** All signals are recorded to TimescaleDB for post-trade analysis.
