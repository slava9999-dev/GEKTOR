# 🛑 TEKTON_ALPHA: BOOTSTRAP PROTOCOL (v6.27)
$ErrorActionPreference = "Stop"

# Set Project Root
$PROJECT_ROOT = "c:/Gerald-superBrain"
$PYTHON_PATH = "$PROJECT_ROOT/.venv/Scripts/python.exe"
$VENV_ACTIVATE = "$PROJECT_ROOT/.venv/Scripts/Activate.ps1"

Write-Host "🟢 [STEP 1] Starting Nerve Center (Redis) with Persistence..." -ForegroundColor Green
Set-Location $PROJECT_ROOT
docker-compose up -d nerve_center
Start-Sleep -Seconds 3

Write-Host "🔵 [STEP 2] Starting Memory (Chroma DB)..." -ForegroundColor Blue
Start-Process powershell -ArgumentList "-NoExit", "-Command", "& { & $VENV_ACTIVATE; chroma run --host 0.0.0.0 --port 8001 }" -Title "CHROMA_DB"

Write-Host "🟣 [STEP 3] Starting Brain (HFT Bridge V2)..." -ForegroundColor Magenta
Start-Process powershell -ArgumentList "-NoExit", "-Command", "& { & $VENV_ACTIVATE; python $PROJECT_ROOT/bridge_v2.py }" -Title "BRIDGE_V2"

Write-Host "🟠 [STEP 4] Starting Radar (Sentinel Pro)..." -ForegroundColor Yellow
Start-Process powershell -ArgumentList "-NoExit", "-Command", "& { & $VENV_ACTIVATE; python $PROJECT_ROOT/skills/crypto-sentinel-pro/sentinel_wss.py }" -Title "RADAR"

Write-Host "🟡 [STEP 5] Starting Hedge Bridge (Binance Executor)..." -ForegroundColor Gray
Start-Process powershell -ArgumentList "-NoExit", "-Command", "& { & $VENV_ACTIVATE; python $PROJECT_ROOT/skills/gerald-sniper/scripts/binance_executor.py }" -Title "HEDGE_EXECUTOR"

Write-Host "🔴 [STEP 6] Starting Main Core (Sniper Main v6.27)..." -ForegroundColor Red
Start-Process powershell -ArgumentList "-NoExit", "-Command", "& { & $VENV_ACTIVATE; python $PROJECT_ROOT/skills/gerald-sniper/main.py }" -Title "SNIPER_CORE"

Write-Host "✅ ALL SYSTEMS INITIATED. OPERATOR STANDBY." -ForegroundColor Cyan
