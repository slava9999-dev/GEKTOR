# Gerald-SuperBrain: PowerShell Startup
# Run: .\start-gerald.ps1

Write-Host "=" * 60
Write-Host " Gerald-SuperBrain Startup" -ForegroundColor Cyan
Write-Host "=" * 60
Write-Host ""

# 1. Check Ollama
Write-Host "[1/4] Checking Ollama..." -NoNewline
try {
    $null = ollama list 2>&1
    Write-Host " OK" -ForegroundColor Green
} catch {
    Write-Host " FAIL - Start Ollama first!" -ForegroundColor Red
    exit 1
}

# 2. Check ChromaDB
Write-Host "[2/4] Checking ChromaDB..." -NoNewline
try {
    $r = Invoke-WebRequest -Uri "http://localhost:8000/api/v1/heartbeat" -TimeoutSec 3 -ErrorAction Stop
    Write-Host " OK" -ForegroundColor Green
} catch {
    Write-Host " WARNING - ChromaDB not responding (RAG unavailable)" -ForegroundColor Yellow
}

# 3. Set env
Write-Host "[3/4] Setting OLLAMA_API_KEY..." -NoNewline
$env:OLLAMA_API_KEY = "ollama-local"
Write-Host " OK" -ForegroundColor Green

# 4. Activate venv
Write-Host "[4/4] Activating venv + starting gateway..." -NoNewline
& C:\Gerald-superBrain\.venv\Scripts\Activate.ps1
Write-Host " GO!" -ForegroundColor Green
Write-Host ""

# Launch gateway
Set-Location C:\Gerald-superBrain
openclaw gateway run
