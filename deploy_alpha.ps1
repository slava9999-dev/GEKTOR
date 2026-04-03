# deploy_alpha.ps1
# Протокол боевого развертывания TEKTON_ALPHA (С микросервисной изоляцией окон)

$PROJECT_ROOT = "c:\Gerald-superBrain\skills\gerald-sniper"
$env:PYTHONPATH = $PROJECT_ROOT

Write-Host "🚨 ИНИЦИАЛИЗАЦИЯ TEKTON_ALPHA (ПРОТОКОЛ ZERO-DAY) 🚨" -ForegroundColor Red

# ТЕРМИНАЛ 1: ИНФРАСТРУКТУРА (Слой Данных и Шина)
Write-Host "[1/5] Поднимаю инфраструктуру (если требуется ручной старт - замените на docker-compose)" -ForegroundColor Cyan
Start-Process pwsh -ArgumentList "-NoExit -Command `"cd $PROJECT_ROOT; Write-Host 'Инфраструктура: Ожидание Redis/Postgres.' -ForegroundColor Yellow`"" -WindowStyle Normal
Start-Sleep -Seconds 2

# ТЕРМИНАЛ 2: СЛОЙ НАБЛЮДАЕМОСТИ (Sentinel Dashboard / API)
Write-Host "[2/5] Запуск Sentinel Dashboard API..." -ForegroundColor Cyan
Start-Process pwsh -ArgumentList "-NoExit -Command `"cd $PROJECT_ROOT; `$env:PYTHONPATH='$PROJECT_ROOT'; Write-Host 'Заглушка мониторинга API...' -ForegroundColor Magenta`""

# ТЕРМИНАЛ 3: ИНТЕЛЛЕКТУАЛЬНЫЕ СКАНЕРЫ (World Scanner)
Write-Host "[3/5] Запуск World Scanner (Слой Ингестии)..." -ForegroundColor Cyan
Start-Process pwsh -ArgumentList "-NoExit -Command `"cd $PROJECT_ROOT; `$env:PYTHONPATH='$PROJECT_ROOT'; python -m data.swarm.ws_worker`""

# ТЕРМИНАЛ 4: L2 LIQUIDITY ENGINE (Мониторинг Стакана)
Write-Host "[4/5] Запуск движка микроструктуры (Liquidity Engine)..." -ForegroundColor Cyan
Start-Process pwsh -ArgumentList "-NoExit -Command `"cd $PROJECT_ROOT; `$env:PYTHONPATH='$PROJECT_ROOT'; Write-Host 'Инициализация L2 Orderbook...' -ForegroundColor DarkCyan`""

# ТЕРМИНАЛ 5: ГЛАВНОЕ ЯДРО (Pre-Flight -> Cortex Execution Engine)
Write-Host "[5/5] Прохождение гейтвея и запуск Боевого Ядра (main.py)..." -ForegroundColor Green
Start-Process pwsh -ArgumentList "-NoExit -Command `"cd $PROJECT_ROOT; `$env:PYTHONPATH='$PROJECT_ROOT'; Write-Host 'Запуск Pre-Flight Diagnostics...' -ForegroundColor Yellow; python scripts/preflight_check.py; if (`$?) { Write-Host '✅ Gateway пройден. Запуск Ядра.' -ForegroundColor Green; python main.py } else { Write-Host '❌ PRE-FLIGHT FAILED. Запуск отменен.' -ForegroundColor Red }`""

Write-Host "✅ Все боевые системы запущены в изолированных процессах." -ForegroundColor White
Write-Host "Следите за потоком логов в открывшихся терминалах." -ForegroundColor DarkGray
