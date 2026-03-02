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
    sys.path.insert(0, os.path.join("c:\\Gerald-superBrain", "skills", "gerald-sniper"))
    
    try:
        from data.database import DatabaseManager
        db = DatabaseManager("c:\\Gerald-superBrain\\skills\\gerald-sniper\\data_run\\sniper.db")
        await db.initialize()
        
        action = args.get("action", "stats")
        
        if action == "stats":
            days = args.get("days", 30)
            stats = await db.get_alert_stats(days=days)
            return ToolResult(success=True, output=json.dumps(stats, ensure_ascii=False, indent=2))
        
        elif action == "recent":
            limit = min(args.get("limit", 10), 20)
            alerts = await db.get_recent_alerts(days=args.get("days", 7))
            alerts = alerts[:limit]
            # Simplify output
            summary = []
            for a in alerts:
                summary.append({
                    "time": a.get("timestamp", "?")[:16],
                    "symbol": a.get("symbol"),
                    "direction": a.get("direction"),
                    "score": a.get("total_score"),
                    "result": a.get("result", "pending"),
                })
            return ToolResult(success=True, output=json.dumps(summary, ensure_ascii=False, indent=2))
        
        elif action == "symbol":
            symbol = args.get("symbol", "BTCUSDT")
            history = await db.get_symbol_alert_history(symbol, limit=10)
            return ToolResult(success=True, output=json.dumps(
                [{"time": a["timestamp"][:16], "dir": a["direction"], "score": a["total_score"], 
                  "result": a.get("result", "?")} for a in history],
                ensure_ascii=False, indent=2
            ))
        
        elif action == "weekly":
            report = await db.get_weekly_summary()
            return ToolResult(success=True, output=report)
        
        else:
            return ToolResult(success=False, output="", error=f"Unknown action: {action}. Use: stats, recent, symbol, weekly")
            
    except Exception as e:
        logger.error(f"sniper_stats tool error: {e}")
        return ToolResult(success=False, output="", error=str(e))


# ─────────────────────────────────────────────────────────
# Tool: system_info
# ─────────────────────────────────────────────────────────
async def tool_system_info(args: dict) -> ToolResult:
    """Get system information (time, disk, processes)."""
    import platform
    from datetime import datetime
    
    info_type = args.get("type", "general")
    
    if info_type == "general":
        import shutil
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
    
    else:
        return ToolResult(success=False, output="", error=f"Unknown type: {info_type}. Use: general, processes")


# ─────────────────────────────────────────────────────────
# Tool Registry
# ─────────────────────────────────────────────────────────
TOOL_REGISTRY = {
    "search_files": tool_search_files,
    "read_file": tool_read_file,
    "list_directory": tool_list_directory,
    "sniper_stats": tool_sniper_stats,
    "system_info": tool_system_info,
}

TOOL_DESCRIPTIONS = (
    "1. final_answer(answer: str) — Всегда используй для ответа Славе.\n"
    "2. read_file(path: str) — Чтение файлов и просмотр папок с компьютера Славы.\n"
    "3. search_files(query: str, directory?: str, max_results?: int) — Поиск файлов по имени на компьютере.\n"
    "4. list_directory(path: str) — Показать содержимое папки.\n"
    "5. sniper_stats(action: str, days?: int, symbol?: str, limit?: int) — "
    "Статистика Gerald Sniper. Действия: stats (общая за N дней), recent (последние алерты), "
    "symbol (история по монете), weekly (недельный отчёт).\n"
    "6. system_info(type: str) — Информация о системе. Типы: general, processes.\n"
    "7. execute_python_code(code: str) — Выполнение Python кода.\n"
)
