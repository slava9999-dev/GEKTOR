@echo off
REM ═══════════════════════════════════════════════════════
REM Gerald-SuperBrain: One-Click Startup
REM Starts Ollama check, sets env vars, launches gateway
REM ═══════════════════════════════════════════════════════

echo ============================================================
echo  Gerald-SuperBrain Startup
echo ============================================================
echo.

REM Check Ollama
echo [1/4] Checking Ollama...
ollama list >nul 2>&1
if errorlevel 1 (
    echo  ERROR: Ollama is not running! Start Ollama first.
    pause
    exit /b 1
)
echo  OK: Ollama running

REM Check Docker / ChromaDB
echo [2/4] Checking ChromaDB...
curl -s http://localhost:8000/api/v1/heartbeat >nul 2>&1
if errorlevel 1 (
    echo  WARNING: ChromaDB not responding on :8000
    echo  RAG search will not work. Start Docker?
) else (
    echo  OK: ChromaDB running
)

REM Set environment
echo [3/4] Setting environment...
set OLLAMA_API_KEY=ollama-local
echo  OK: OLLAMA_API_KEY set

REM Activate venv and start gateway
echo [4/4] Starting OpenClaw Gateway...
echo.
call C:\Gerald-superBrain\.venv\Scripts\activate.bat
cd /d C:\Gerald-superBrain
openclaw gateway run
