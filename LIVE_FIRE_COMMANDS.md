# 🏆 Gerald-SuperBrain: LIVE FIRE COMMAND CENTER (v5.23)

Follow this precise sequence to launch the full trading organism. Each terminal must be active and synchronized via the Redis Bus.

---

### 🟢 ТЕРМИНАЛ 0: ШИНА ДАННЫХ (REDIS BUS)
**Роль**: ACID-гарантии эпох, транзакционные стримы и межпроцессное общение.
```powershell
# Очистка и запуск шины с максимальной персистентностью
docker rm -f tekton-bus
docker run -d --name tekton-bus -p 6381:6379 redis:alpine redis-server --appendonly yes --appendfsync always --notify-keyspace-events KEA
```

---

### 🔵 ТЕРМИНАЛ 1: ВЕКТОРНАЯ ПАМЯТЬ (CHROMA DB)
**Роль**: RAG-контекст, история рыночных аномалий и долгосрочная память.
```powershell
& c:/Gerald-superBrain/.venv/Scripts/Activate.ps1
chroma run --host 0.0.0.0 --port 8001
```

---

### 🟣 ТЕРМИНАЛ 2: ОСНОВНОЙ МОЗГ (BRIDGE V2)
**Роль**: Telegram-интерфейс, координация агентов, прием внешних команд.
```powershell
& c:/Gerald-superBrain/.venv/Scripts/Activate.ps1
python bridge_v2.py
```

---

### 🟡 ТЕРМИНАЛ 3: МОСТ ОМЕГА (BINANCE HEDGE)
**Роль**: Исполнение хеджирующей ноги на Binance Futures, защита от "Ядовитых писем".
```powershell
& c:/Gerald-superBrain/.venv/Scripts/Activate.ps1
python skills/gerald-sniper/scripts/binance_executor.py
```

---

### 🔴 ТЕРМИНАЛ 4: СНАЙПЕР (SNIPER MAIN)
**Роль**: Математический анализ, Bybit Execution, Launch Sequence Protocol (LSP).
```powershell
& c:/Gerald-superBrain/.venv/Scripts/Activate.ps1
python skills/gerald-sniper/main.py
```
> **ВНИМАНИЕ**: После запуска Sniper выполнит 50 циклов разогрева. Дождись `🚀 [WARMUP_COMPLETE]`.

---

### 🟠 ТЕРМИНАЛ 5: РАДАР АНОМАЛИЙ (SENTINEL PRO)
**Роль**: Глобальный мониторинг ликвидаций и китовых объемов (Топ-300).
```powershell
& c:/Gerald-superBrain/.venv/Scripts/Activate.ps1
python skills/crypto-sentinel-pro/sentinel_wss.py
```

---

### 🛠 ПОВЕДЕНИЕ В COMBAT MODE
1. **Drift Detection**: Если бот видит расхождение баланса в 0.000001 BTC — он сам нажмет «Красную Кнопку» (Emergency Flatten).
2. **Kill Switch**: Если Binance забанит ключи — Sniper мгновенно закроет Bybit-ногу по рынку.
3. **Slippage Protection**: Все экстренные выходы идут через IOC-ордера с капом 5%.

**СИСТЕМА ТРАНЗАКЦИОННО ЗАМКНУТА. ЦЕЛЬ: 0.1 BTC. УДАЧНОЙ ОХОТЫ.** 🛑
