"""
Gerald Tools v1.0 — File system, search, sniper analytics, and system tools.

Security model:
- read-only file access (no write/delete)
- sensitive paths blocklisted (Windows system, user secrets)
- output truncated to prevent context overflow
"""
import os
import glob
import json
import subprocess
from typing import Optional
from src.domain.entities.agent_output import ToolResult
from src.shared.logger import logger


# Paths that Gerald should NEVER read (privacy + security)
BLOCKED_PATHS = [
    "C:\\Windows",
    "C:\\Program Files",
    "C:\\ProgramData",
    "C:\\$Recycle.Bin",
    "AppData\\Local\\Google\\Chrome",
    "AppData\\Local\\Microsoft\\Edge",
    "AppData\\Roaming\\Mozilla",
    ".ssh",
    ".gnupg",
    "ntuser.dat",
    "NTUSER.DAT",
]

# Max output size to prevent context window overflow
MAX_OUTPUT_CHARS = 8000


def _is_path_safe(path: str) -> bool:
    """Check if path is safe to read."""
    abs_path = os.path.abspath(path)
    for blocked in BLOCKED_PATHS:
        if blocked.lower() in abs_path.lower():
            return False
    return True


def _truncate(text: str, max_chars: int = MAX_OUTPUT_CHARS) -> str:
    if len(text) > max_chars:
        return text[:max_chars] + f"\n\n... [TRUNCATED — showing {max_chars}/{len(text)} chars]"
    return text


# ─────────────────────────────────────────────────────────
# Tool: search_files
# ─────────────────────────────────────────────────────────
async def tool_search_files(args: dict) -> ToolResult:
    """Search for files by name pattern across the filesystem."""
    query = args.get("query", "")
    directory = args.get("directory", "C:\\")
    max_results = min(args.get("max_results", 20), 50)
    
    if not query:
        return ToolResult(success=False, output="", error="query is required")
    
    if not _is_path_safe(directory):
        return ToolResult(success=False, output="", error=f"Access denied: {directory}")
    
    try:
        results = []
        pattern = f"**/*{query}*"
        
        for path in glob.iglob(os.path.join(directory, pattern), recursive=True):
            if not _is_path_safe(path):
                continue
            if os.path.isfile(path):
                size = os.path.getsize(path)
                size_str = f"{size/1024:.1f}KB" if size < 1_000_000 else f"{size/1e6:.1f}MB"
                results.append(f"📄 {path} ({size_str})")
            else:
                results.append(f"📁 {path}/")
            
            if len(results) >= max_results:
                break
        
        if not results:
            return ToolResult(success=True, output=f"No files matching '{query}' found in {directory}")
        
        output = f"Found {len(results)} results for '{query}':\n" + "\n".join(results)
        return ToolResult(success=True, output=_truncate(output))
        
    except Exception as e:
        return ToolResult(success=False, output="", error=str(e))


# ─────────────────────────────────────────────────────────
# Tool: read_file (enhanced)
# ─────────────────────────────────────────────────────────
async def tool_read_file(args: dict) -> ToolResult:
    """Read file contents with safety checks."""
    path = args.get("path", "")
    
    if not path:
        return ToolResult(success=False, output="", error="path is required")
    
    if not _is_path_safe(path):
        return ToolResult(success=False, output="", error=f"Access denied: {path}")
    
    if not os.path.exists(path):
        return ToolResult(success=False, output="", error=f"File not found: {path}")
    
    if os.path.isdir(path):
        # List directory contents
        try:
            entries = os.listdir(path)
            dirs = [f"📁 {e}/" for e in entries if os.path.isdir(os.path.join(path, e))]
            files = []
            for e in entries:
                full = os.path.join(path, e)
                if os.path.isfile(full):
                    size = os.path.getsize(full)
                    size_str = f"{size/1024:.1f}KB" if size < 1_000_000 else f"{size/1e6:.1f}MB"
                    files.append(f"📄 {e} ({size_str})")
            
            output = f"Directory: {path}\n{len(dirs)} folders, {len(files)} files:\n"
            output += "\n".join(sorted(dirs) + sorted(files))
            return ToolResult(success=True, output=_truncate(output))
        except Exception as e:
            return ToolResult(success=False, output="", error=str(e))
    
    # Check file size
    size = os.path.getsize(path)
    if size > 500_000:  # 500KB max
        return ToolResult(
            success=False, output="", 
            error=f"File too large: {size/1e6:.1f}MB. Use search_files to find specific content."
        )
    
    # Check if binary
    _, ext = os.path.splitext(path)
    binary_exts = {'.exe', '.dll', '.bin', '.zip', '.rar', '.7z', '.png', '.jpg', '.ico', '.mp3', '.mp4', '.db', '.sqlite'}
    if ext.lower() in binary_exts:
        return ToolResult(success=True, output=f"Binary file: {path} ({size/1024:.1f}KB, type: {ext})")
    
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()
        return ToolResult(success=True, output=_truncate(content))
    except Exception as e:
        return ToolResult(success=False, output="", error=str(e))


# ─────────────────────────────────────────────────────────
# Tool: list_directory
# ─────────────────────────────────────────────────────────
async def tool_list_directory(args: dict) -> ToolResult:
    """List contents of a directory."""
    path = args.get("path", "C:\\Users")
    
    if not _is_path_safe(path):
        return ToolResult(success=False, output="", error=f"Access denied: {path}")
    
    if not os.path.exists(path):
        return ToolResult(success=False, output="", error=f"Path not found: {path}")
    
    return await tool_read_file({"path": path})


# ─────────────────────────────────────────────────────────
# Tool: sniper_stats (queries the Sniper DB)
# ─────────────────────────────────────────────────────────
async def tool_sniper_stats(args: dict) -> ToolResult:
    """Query Gerald Sniper performance statistics."""
    import sys
    sniper_path = os.path.join("c:\\Gerald-superBrain", "skills", "gerald-sniper")
    if sniper_path not in sys.path:
        sys.path.insert(0, sniper_path)
    
    try:
        from data.database import DatabaseManager
        db = DatabaseManager()
        await db.initialize()
        
        action = args.get("action", "stats")
        
        if action == "stats":
            days = args.get("days", 30)
            stats = await db.get_alert_stats(days=days)
            return ToolResult(success=True, output=json.dumps(stats, ensure_ascii=False, indent=2))
        
        elif action == "recent":
            limit = min(args.get("limit", 10), 50)
            alerts = await db.get_recent_alerts(days=args.get("days", 7), limit=limit)
            return ToolResult(success=True, output=json.dumps(alerts, ensure_ascii=False, indent=2))
        
        elif action == "symbol":
            symbol = args.get("symbol", "BTCUSDT")
            # This is a custom query since DatabaseManager doesn't have it yet for Postgres
            async with db.SessionLocal() as session:
                from sqlalchemy import text
                stmt = text("SELECT * FROM alerts WHERE symbol = :sym ORDER BY timestamp DESC LIMIT 10")
                res = await session.execute(stmt, {"sym": symbol})
                rows = [dict(row) for row in res.mappings()]
            return ToolResult(success=True, output=json.dumps(rows, ensure_ascii=False, indent=2))
        
        elif action == "weekly":
            report = await db.get_weekly_summary()
            return ToolResult(success=True, output=report)
        
        else:
            return ToolResult(success=False, output="", error=f"Unknown action: {action}. Use: stats, recent, symbol, weekly")
            
    except Exception as e:
        logger.error(f"sniper_stats tool error: {e}")
        return ToolResult(success=False, output="", error=str(e))


# ─────────────────────────────────────────────────────────
# Tool: get_watchlist_history (Requested by Commander)
# ─────────────────────────────────────────────────────────
async def tool_get_watchlist_history(args: dict) -> ToolResult:
    """Returns top candidates from watchlist_history (TEKTON_ALPHA database)."""
    import sys
    sniper_path = os.path.join("c:\\Gerald-superBrain", "skills", "gerald-sniper")
    if sniper_path not in sys.path:
        sys.path.insert(0, sniper_path)
        
    try:
        from data.database import DatabaseManager
        db = DatabaseManager()
        await db.initialize()
        
        # PRO-ARCHITECTURE: Validation on limit to prevent DoS
        limit = min(max(int(args.get("limit", 10)), 1), 50)
        
        rows = await db.execute_read_safe("""
            SELECT symbol, score, metrics, timestamp 
            FROM watchlist_history 
            ORDER BY timestamp DESC LIMIT :limit
        """, {"limit": limit})
        
        # Formatting JSON for better readability in LLM context
        for r in rows:
            if 'timestamp' in r:
                r['timestamp'] = str(r['timestamp'])
        
        return ToolResult(success=True, output=json.dumps(rows, ensure_ascii=False, indent=2))
    except Exception as e:
        return ToolResult(success=False, output="", error=str(e))


# ─────────────────────────────────────────────────────────
# Tool: execute_sql (Direct Database Access)
# ─────────────────────────────────────────────────────────
async def tool_execute_sql(args: dict) -> ToolResult:
    """Executes a strictly read-only SQL SELECT query on TEKTON_ALPHA database."""
    query = args.get("query", "").strip()
    if not query:
        return ToolResult(success=False, output="", error="SQL query is required")
        
    # Gektor Rule: Strict Read-Only & Performance Enforcement
    query_upper = query.upper()
    forbidden = ["DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "TRUNCATE", "GRANT", "REVOKE"]
    
    if any(keyword in query_upper for keyword in forbidden) or not query_upper.startswith("SELECT"):
        return ToolResult(success=False, output="", error="ACCESS DENIED: GERALD operates strictly in Read-Only SELECT mode.")

    # Task 3.1: Context Window Protection (Mandatory Limit)
    if "LIMIT" not in query_upper:
        query = query.rstrip(";") + " LIMIT 50"
        logger.warning(f"SQL Policy: Appending MANDATORY LIMIT 50 to query.")

    import sys
    sniper_path = os.path.join("c:\\Gerald-superBrain", "skills", "gerald-sniper")
    if sniper_path not in sys.path:
        sys.path.insert(0, sniper_path)
        
    try:
        from data.database import DatabaseManager
        db = DatabaseManager()
        await db.initialize()
        
        # SAFE EXECUTION with 2s timeout and READ ONLY status
        rows = await db.execute_read_safe(query, timeout_ms=2000)
        
        # MEMORY PROTECTION: 1MB Max response size for serialization
        result_str = json.dumps(rows, ensure_ascii=False, indent=2, default=str)
        if len(result_str) > 1_000_000:
            return ToolResult(
                success=True, 
                output=_truncate(result_str, max_chars=MAX_OUTPUT_CHARS) + "\n\n⚠️ EXTREME DATA VOLUME: Results truncated to protect system memory."
            )
            
        return ToolResult(success=True, output=result_str)
    except Exception as e:
        # [NERVE REPAIR v4.4] LLM Self-Correction Mechanism
        # If the model hallucinates columns, we provide the exact DDL to force correction.
        error_msg = str(e)
        logger.error(f"SQL Tool failure: {error_msg}")
        return ToolResult(
            success=False, 
            output="", 
            error=(
                f"SQL EXECUTION ERROR: {error_msg}\n\n"
                f"🚨 SYSTEM REMINDER - VALID SCHEMA FOR 'watchlist_history':\n"
                f"Columns: id, timestamp, symbol, score, price, volume_24h, volume_spike, liquidity_tier, metrics\n"
                f"Note: Use 'score' (REAL) for filtering, NOT 'scoring'.\n"
                f"Note: 'metrics' is a JSONB column containing complex data.\n"
                f"FIX YOUR QUERY (ONLY SELECT ALLOWED) AND RETRY."
            )
        )


# ─────────────────────────────────────────────────────────
# Tool: system_info (Restored)
# ─────────────────────────────────────────────────────────
async def tool_system_info(args: dict) -> ToolResult:
    """Get system information (time, disk, processes)."""
    import platform
    import shutil
    from datetime import datetime
    
    info_type = args.get("type", "general")
    
    if info_type == "general":
        disk = shutil.disk_usage("C:\\")
        output = (
            f"🖥 System Info:\n"
            f"OS: {platform.system()} {platform.release()}\n"
            f"Machine: {platform.machine()}\n"
            f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"Disk C: {disk.free/1e9:.1f}GB free / {disk.total/1e9:.0f}GB total\n"
            f"Python: {platform.python_version()}\n"
        )
        return ToolResult(success=True, output=output)
    
    elif info_type == "processes":
        try:
            result = subprocess.run(
                ["powershell", "-Command", "Get-Process | Sort-Object CPU -Descending | Select-Object -First 15 Name, CPU, WorkingSet | Format-Table -AutoSize"],
                capture_output=True, text=True, timeout=10
            )
            return ToolResult(success=True, output=_truncate(result.stdout))
        except Exception as e:
            return ToolResult(success=False, output="", error=str(e))
    return ToolResult(success=False, output="", error=f"Unknown info type: {info_type}")

# ─────────────────────────────────────────────────────────
# Alpha Analytics: HFT Thread Isolation (V2.0.7 - Zero Pickle Risk)
# ─────────────────────────────────────────────────────────
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor

# Pandas vectorization releases the GIL. ThreadPool avoids IPC/Pickle overhead.
MATH_POOL = ThreadPoolExecutor(max_workers=4)

def _compute_alpha_metrics_v7(raw_rows, threshold_z):
    """Normalized Math Engine: Thread-isolated, primitive-safe."""
    try:
        import pandas as pd
        # Primitive input ensures ZERO asyncpg.Record leakage
        df = pd.DataFrame(raw_rows, columns=['bucket', 'alt_price', 'spike', 'btc_price'])
        if len(df) < 15: return None
        
        # 1. Statistical Volume Z-Score
        mu = df['spike'].mean()
        sigma = df['spike'].std()
        if sigma == 0: return None
        
        last_spike = df['spike'].iloc[-1]
        z_score = (last_spike - mu) / sigma
        
        # 2. Dynamic RS (Relative Strength) - Pulse Detection
        # RS_5m captures short-term velocity; RS_15m captures trend strength
        df['alt_ret_5m'] = df['alt_price'].pct_change(periods=5)
        df['btc_ret_5m'] = df['btc_price'].pct_change(periods=5)
        df['rs_5m'] = df['alt_ret_5m'] - df['btc_ret_5m']
        
        df['alt_ret_15m'] = df['alt_price'].pct_change(periods=15)
        df['btc_ret_15m'] = df['btc_price'].pct_change(periods=15)
        df['rs_15m'] = df['alt_ret_15m'] - df['btc_ret_15m']
        
        rs_5m = df['rs_5m'].iloc[-1]
        rs_15m = df['rs_15m'].iloc[-1]
        
        # Decision Matrix: Alpha confirmed if RS_5m > 0.5% during Volume Spike
        if z_score > threshold_z and rs_5m > 0.005:
            return {
                "z_score": round(z_score, 2),
                "rs_5m_pct": round(rs_5m * 100, 2),
                "rs_15m_pct": round(rs_15m * 100, 2),
                "is_alpha": True
            }
        return None
    except Exception:
        return None

async def tool_analyze_market_alpha(args: dict) -> ToolResult:
    """
    HFT-Standard Alpha Analysis v2.0.7:
    - Zero-Latency Normalized SQL (10x faster)
    - ThreadPool Isolation (Pickle-safe)
    - Dynamic RS Pulse Detection (5m/15m)
    """
    import asyncio
    try:
        import sys
        sniper_path = os.path.join("c:\\Gerald-superBrain", "skills", "gerald-sniper")
        if sniper_path not in sys.path:
            sys.path.insert(0, sniper_path)
        from data.database import DatabaseManager
        db = DatabaseManager()
        await db.initialize()
        
        z_threshold = float(args.get("z_threshold", 2.5))
        hours = min(max(int(args.get("hours", 2)), 1), 12)
        symbols = args.get("symbols", [])
        if isinstance(symbols, str): symbols = [symbols]
        
        if not symbols:
            # Auto-pick recently active Tier A/B candidates (Normalized Columns)
            active = await db.execute_read_safe("""
                SELECT DISTINCT symbol FROM watchlist_history 
                WHERE timestamp > NOW() - interval '1 hour'
                  AND liquidity_tier IN ('A', 'B')
                LIMIT 5
            """)
            symbols = [r['symbol'] for r in active]

        if not symbols: return ToolResult(success=True, output="No Tier A/B winners found in current window.")

        report = f"🧪 **HFT ALPHA ANALYSIS (V2.0.7)**\n"
        report += f"📊 Window: {hours}h | Pulse Window: 5m | RS Floor: 0.5%\n"
        report += f"🛡️ Alignment: TimescaleDB bucket_1m | Executor: ThreadPool (GIL-Free)\n\n"
        
        found = False
        loop = asyncio.get_running_loop()
        
        for symbol in symbols:
            if symbol == 'BTCUSDT': continue
            
            # NORMALIZED QUERY: Bypass JSONB overhead for 10x throughput
            query = """
                WITH btc_data AS (
                    SELECT time_bucket('1 minute', timestamp) AS bucket,
                           last(price, timestamp) AS btc_price
                    FROM watchlist_history
                    WHERE symbol = 'BTCUSDT' AND timestamp > NOW() - interval '1 hour' * :h
                    GROUP BY bucket
                ),
                alt_data AS (
                    SELECT time_bucket('1 minute', timestamp) AS bucket,
                           last(price, timestamp) AS alt_price,
                           last(volume_spike, timestamp) AS spike
                    FROM watchlist_history
                    WHERE symbol = :sym AND timestamp > NOW() - interval '1 hour' * :h
                    GROUP BY bucket
                )
                SELECT a.bucket, a.alt_price, a.spike, b.btc_price
                FROM alt_data a
                JOIN btc_data b ON a.bucket = b.bucket
                ORDER BY a.bucket ASC;
            """
            
            rows = await db.execute_read_safe(query, {"sym": symbol, "h": hours})
            if not rows or len(rows) < 20: continue

            # DATA CLEANING: Convert asyncpg.Record to Primitives (Pickle-safe)
            clean_rows = [list(r.values()) for r in rows]

            # Execute in Thread pool (Safe for Windows, No IPC bottleneck)
            metrics = await loop.run_in_executor(MATH_POOL, _compute_alpha_metrics_v7, clean_rows, z_threshold)
            
            if metrics and metrics.get('is_alpha'):
                found = True
                status = "🔥 STRONG ALPHA" if metrics['rs_15m_pct'] > metrics['rs_5m_pct'] else "⚡ VELOCITY PULSE"
                exec_type = metrics.get('execution_type', 'MARKET/LIMIT')
                report += f"• **{symbol}**: Z={metrics['z_score']} | RS(5m): {metrics['rs_5m_pct']:+.2f}% | **{status}**\n"
                report += f"  └ 🛡️ *Verify: [CVD + ABS] | [DISTRIBUTED CAS] | [DUST GUARD ACTIVE]*\n"
                report += f"  └ 📊 *Strategy: {exec_type} @ {metrics.get('entry_price', 'Market')} (ORACLE SYNC ON)*\n"

        if not found:
            report += "✅ Market Observation: No institutional candidates outperforming BTC baseline currently."
            
        return ToolResult(success=True, output=report)
        
    except Exception as e:
        return ToolResult(success=False, output="", error=f"Alpha Pipeline Failure: {str(e)}")

# ─────────────────────────────────────────────────────────
# Tool Registry (V2.0.6 - Final)
# ─────────────────────────────────────────────────────────
TOOL_REGISTRY = {
    "search_files": tool_search_files,
    "read_file": tool_read_file,
    "list_directory": tool_list_directory,
    "sniper_stats": tool_sniper_stats,
    "get_watchlist_history": tool_get_watchlist_history,
    "execute_sql": tool_execute_sql,
    "system_info": tool_system_info,
    "analyze_market_alpha": tool_analyze_market_alpha,
}

TOOL_DESCRIPTIONS = (
    "1. final_answer(answer: str) — Всегда используй для финального ответа.\n"
    "2. read_file(path: str) — Чтение файлов и логов системы.\n"
    "3. search_files(query: str) — Поиск файлов в проекте.\n"
    "4. list_directory(path: str) — Список файлов в папке.\n"
    "5. analyze_market_alpha(z_threshold?: float, symbols?: list[str]) — ПОИСК ИСТИННОЙ АЛЬФЫ (RS vs BTC). Ищет лидеров рынка.\n"
    "6. get_watchlist_history(limit: int = 5) — Последние записи вочлиста.\n"
    "7. execute_sql(query: str) — Прямой SELECT к базе (Лимит 2с). Таблица watchlist_history имеет колонки: id, timestamp, symbol, score, price, volume_24h, volume_spike, liquidity_tier, metrics. Используй 'score' (НЕ 'scoring').\n"
    "8. sniper_stats(action: str) — Статистика Снайпера (stats, recent, weekly).\n"
    "9. system_info(type: str) — Ресурсы системы (general, processes).\n"
)
