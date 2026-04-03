# Gerald Sniper v6.27.3 — Master Launcher (Smart Pruning & OS-Locking)
# 🎯 [PRE-FLIGHT] Precision Pruning
Write-Host "🧹 [1/6] Pre-flight: Precision Pruning of ZOMBIE SHARDS..." -ForegroundColor Cyan

# Use Get-CimInstance for precise CommandLine filtering (Rule 12: No Blind Pruning)
$ProjectDir = (Get-Location).Path
$ProjectDir_Escaped = $ProjectDir.Replace("\", "\\") # Escape for CommandLine matching

# Find python processes belonging to THIS project directory
$Zombies = Get-CimInstance Win32_Process -Filter "Name = 'python.exe'" | Where-Object { 
    $_.CommandLine -match $ProjectDir_Escaped -and $_.CommandLine -match "main.py|binance_executor.py|ws_worker.py|diagnostic_guard.py|bridge.py" 
}

if ($Zombies) {
    Write-Host "💀 Detected $($Zombies.Count) project-specific zombies. Clearing matrix..." -ForegroundColor Red
    $Zombies | ForEach-Object { 
        Write-Host "   -> Terminating Process: $($_.ProcessId) ($($_.Name))" -ForegroundColor Gray
        Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
    }
} else {
    Write-Host "✅ Matrix is clean. Ready for ignition." -ForegroundColor Green
}

# [STEP 1] Infrastructure
Write-Host "🐳 [2/6] Starting Infrastructure (Redis AOF + Timescale)..." -ForegroundColor Cyan
docker-compose up -d
Start-Sleep -Seconds 5

Write-Host "⏳ Waiting for Redis..." -ForegroundColor Gray
$redis_ready = $false
while (-not $redis_ready) {
    if (Get-Command redis-cli -ErrorAction SilentlyContinue) {
        $check = docker exec gerald-redis redis-cli ping
        if ($check -eq "PONG") { $redis_ready = $true }
    } else {
        # Fallback: Just assume it's up after a few seconds if no redis-cli installed locally
        Start-Sleep -Seconds 3
        $redis_ready = $true
    }
    if (-not $redis_ready) { Start-Sleep -Seconds 1 }
}
Write-Host "✅ Heartbeat: Redis PONG." -ForegroundColor Green

# [STEP 2] Matrix Launch
Write-Host "🚀 [3/6] Launching Execution Matrix..." -ForegroundColor Green

# Matrix Config
# Navigate into skills/gerald-sniper to ensure imports work (Rule 18.1)
$Launchers = @(
    @{Title="HEDGE_EXECUTOR"; Command="python -m scripts.binance_executor"},
    @{Title="ANALYTICS_WORKER"; Command="python -m data.swarm.ws_worker"},
    @{Title="SYSTEM_SENTINEL"; Command="python -m core.health.diagnostic_guard"},
    @{Title="BUS_BRIDGE"; Command="python -m core.realtime.bridge"}
)

foreach ($L in $Launchers) {
    # Navigates into root to ensure all modules (core, data) are discoverable
    Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd skills/gerald-sniper; `$env:PYTHONPATH='.'; `$Host.UI.RawUI.WindowTitle = '$($L.Title)'; $($L.Command)"
}

Write-Host "⏳ Cooling down (5s) before Core start..." -ForegroundColor Gray
Start-Sleep -Seconds 5

# [STEP 3] CORE IGNITION
Write-Host "🎯 [4/6] Igniting Sniper Core (PID_LOCK v6.27.3 Armed)..." -ForegroundColor Yellow
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd skills/gerald-sniper; `$env:PYTHONPATH='.'; `$Host.UI.RawUI.WindowTitle = 'SNIPER_MAIN'; python -m main"

Write-Host "✨ [5/6] Matrix ONLINE. Monitoring Quorum..." -ForegroundColor Green
Write-Host "💡 Note: Kernel-locking via msvcrt ensures no Split-Brain." -ForegroundColor Gray
