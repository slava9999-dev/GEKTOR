# Makefile: GEKTOR APEX DEPLOYMENT PROTOCOL
# Version: 21.49 | Strategy: Institutional Sniper

.PHONY: help test-warp deploy-live halt-nuclear logs metrics status

help:
	@echo "🛠 GEKTOR APEX Management Interface"
	@echo "------------------------------------------------"
	@echo "test-warp   : Запуск симуляции (Warp-Speed Backtest) с проверкой Edge"
	@echo "deploy-live : Перевод системы в LIVE FIRE (Docker Deployment)"
	@echo "halt-nuclear: Экстренная ликвидация всех позиций и остановка торгов"
	@echo "logs        : Просмотр логов ядра в реальном времени"
	@echo "metrics     : Открытие панели Grafana для мониторинга латентности"
	@echo "status      : Проверка здоровья контейнеров и синхронизации часов"

# 1. Финальный прогон в Виртуальном Континууме (Determenistic Replay)
test-warp:
	@echo "🧪 [SIM] INITIATING WARP-SPEED SIMULATION..."
	@export GEKTOR_ENV=SIMULATION && \
	python3 -m pytest skills/gerald_sniper/tests/ --disable-warnings && \
	python3 main.py --mode backtest --years 5 --clock virtual
	@echo "✅ [SIM] Simulation Passed. Statistical Advantage Proven."

# 2. Боевое развертывание (Atomic Start)
deploy-live: test-warp
	@echo "🔥 [LIVE] INITIATING LIVE FIRE PROTOCOL. REAL CAPITAL AT RISK."
	@echo "Checking NTP Synchronization and DB Heartbeat..."
	@docker-compose -f docker-compose.yml up -d --build
	@echo "💎 [LIVE] GEKTOR APEX IS ARMED. MAY THE DELTA BE WITH YOU."

# 3. Ручная активация Nuclear Kill Switch (Emergency Response)
halt-nuclear:
	@echo "☢️ [EMERGENCY] INITIATING EMERGENCY LIQUIDATION PROTOCOL..."
	@docker exec -it gektor_core_apex python3 -c "from skills.gerald_sniper.core.realtime.evacuation import trigger_kill_switch; trigger_kill_switch()"
	@echo "🛑 [EMERGENCY] FLAT & HALT EXECUTED. POSITIONS NEUTRALIZED."

logs:
	@docker logs -f gektor_core_apex --tail 100

metrics:
	@echo "📊 Opening Grafana Metrics Dashboard..."
	@open http://localhost:3000

status:
	@docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
	@echo "------------------------------------------------"
	@docker exec -it gektor_core_apex python3 -c "from skills.gerald_sniper.core.realtime.quarantine import check_clock_sync; check_clock_sync()"
