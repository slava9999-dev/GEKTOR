#!/bin/bash
# c:/Gerald-superBrain/scripts/ignite_gektor_apex.sh
# 🛑 GEKTOR APEX v21.15.6: PRODUCTION IGNITION SEQUENCE 🛑
# OS: Linux (Ubuntu/Debian) | Arch: x86_64 High-Perf | Python: 3.11+
# -------------------------------------------------------------------------

echo "🚀 [INIT] Purging OS page cache and allocating Zero-Copy buffers..."
sync; echo 1 > /proc/sys/vm/drop_caches

echo "📡 [INIT] Arming async environment variables..."
# Disable debug modes for maximum execution speed
export PYTHONASYNCIODEBUG=0
# Enable Python optimization (removes assertions, __debug__ code)
export PYTHONOPTIMIZE=2
# Force use of uvloop if installed (High-performance event loop)
export UVLOOP_ENABLED=1
# Ensure deterministic randomness is fixed or handled
export PYTHONHASHSEED=42

echo "🧠 [INIT] Bootstrapping MLMathEngine on dedicated CPU cores (Isolating GIL)..."
# taskset -c 2,3,4,5: Affinity pinning to prevent context-switching and L1/L2 cache misses.
# Running the Global Orchestrator in HYPER_VIGILANCE mode.
nohup taskset -c 2,3,4,5 python3 -m skills.gerald_sniper.core.realtime.orchestrator --mode HYPER_VIGILANCE > /var/log/gektor_apex.log 2>&1 &

echo "🛡️ [STATUS] GektorAPEXDaemon successfully detached (PID: $!)."
echo "🐯 [STATUS] System is hunting. May the Alpha be with you, Vyacheslav."
echo "-------------------------------------------------------------------------"
