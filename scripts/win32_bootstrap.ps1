# Gerald Sniper Windows Bootstrap & Cleanup Script
# v1.0.0 (Audit 16.40: Zombie Prevention)

Write-Host "🧹 [Bootstrap] Initiating Windows Environment Cleanup..." -ForegroundColor Cyan

# 1. Kill any stray python processes running Sniper components
Write-Host "🔍 [1/3] Hunting Zombie Python processes..." -ForegroundColor Yellow
$sniper_procs = Get-Process python -ErrorAction SilentlyContinue | Where-Object { $_.CommandLine -like "*gerald-sniper*" -or $_.CommandLine -like "*bridge_v2*" }
if ($sniper_procs) {
    Write-Host "💀 Terminating $($sniper_procs.Count) zombie processes..." -ForegroundColor Red
    $sniper_procs | Stop-Process -Force
} else {
    Write-Host "✅ No zombies found." -ForegroundColor Green
}

# 2. Release DB Locks (SQLite)
# On Windows, if a process dies holding a handle, sometimes the .db-shm or .db-wal files stick around.
$db_path = "c:\Gerald-superBrain\skills\gerald-sniper\data_run\sniper.db"
if (Test-Path "$db_path-shm") {
    Write-Host "🔓 [2/3] Clearing SQLite SHM/WAL locks..." -ForegroundColor Yellow
    Remove-Item "$db_path-shm" -ErrorAction SilentlyContinue
    Remove-Item "$db_path-wal" -ErrorAction SilentlyContinue
} else {
    Write-Host "✅ Database is clear." -ForegroundColor Green
}

# 3. Verify Redis Bus (tekton-bus)
Write-Host "🔌 [3/3] Checking Redis Bus on port 6381..." -ForegroundColor Yellow
$redis_check = Test-NetConnection -ComputerName localhost -Port 6381 -InformationLevel Quiet
if ($redis_check) {
    Write-Host "✅ Redis Bus is RESPONDING." -ForegroundColor Green
} else {
    Write-Host "❌ Redis Bus is DOWN! Run 'docker run' command first." -ForegroundColor Red
}

Write-Host "`n🚀 [READY] Environment SANITIZED. You may now launch main.py." -ForegroundColor Cyan
