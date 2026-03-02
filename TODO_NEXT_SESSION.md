# TODO: Gerald AI Alert Analysis System

# Created: 2026-03-02

# Priority: HIGH

# Status: PLANNED

# Context: Следующая сессия разработки

## Цель

Подключить Gerald AI агент к базе данных Sniper-алертов, чтобы он мог:

1. Анализировать качество сигналов (win rate, PnL)
2. Проверять актив по фундаментальным метрикам перед ответом
3. Давать рекомендации по калибровке порогов
4. Генерировать еженедельные отчёты автоматически

## Задачи

### 1. Подключить insert_alert() в candle_manager.py [CRITICAL]

- Файл: `skills/gerald-sniper/core/candle_manager.py`
- В `_check_triggers_for_symbol()` после `sym_data['last_alert'][level_id] = now`
  нужно вызвать `db.insert_alert(...)` для записи каждого алерта в БД
- Проблема: CandleManager не имеет ссылки на DatabaseManager
- Решение: передать db_manager в конструктор CandleManager или через DI
- Данные для записи: symbol, direction, signal_type, level_price, entry_price,
  stop/target из risk_data, score, breakdown, radar metrics, btc_trend

### 2. Создать tool "sniper_analytics" для Gerald AI [HIGH]

- Файл: создать `src/domain/entities/tools/sniper_analytics.py`
- Подключить к agent.py как новый инструмент
- Функции tool:
  - `get_alert_stats(days=30)` — возвращает агрегированную статистику
  - `get_symbol_history(symbol)` — история алертов по монете
  - `get_recent_alerts(days=7)` — последние алерты для ревью
  - `get_weekly_summary()` — текстовый отчёт
- Gerald AI сможет вызывать этот tool когда ему задают вопросы типа:
  "Как работает снайпер?", "Какой win rate?", "Проверь SOLUSDT"

### 3. Фундаментальный cross-check через AI [MEDIUM]

- Когда пользователь спрашивает про конкретную монету,
  Gerald должен:
  1. Запросить историю алертов по монете (get_symbol_history)
  2. Запросить текущие метрики из radar (RVOL, OI, funding)
  3. Сформировать промпт: "Вот технический сетап + метрики.
     Оцени с позиции спекулянта и фундаменталиста"
  4. Выдать структурированный ответ с рекомендацией
- Опционально: подключить внешние API для фундаментальных данных
  (CoinGecko community metrics, on-chain data)

### 4. Еженедельный автоматический отчёт [MEDIUM]

- Файл: `skills/gerald-sniper/main.py`
- Добавить `weekly_report_loop()`:
  - Каждое воскресенье в 20:00 МСК (из config)
  - Вызывает `db.get_weekly_summary()`
  - Отправляет в Telegram через `send_telegram_alert()`
  - Опционально: если подключен AI Analyst, добавить AI-комментарий к отчёту

### 5. Реализовать update_alert_result через Telegram [LOW]

- Добавить команды в бот: `/win 123`, `/loss 123 -1.2%`, `/skip 123`
- Это позволит пользователю отмечать результаты сделок
- На основе этих данных Gerald AI сможет считать реальный win rate
- Для этого нужно сохранять alert_id и отправлять его в Telegram-сообщении

### 6. Auto-calibration suggestions [LOW]

- На основе get_alert_stats():
  - Если win_rate < 40% за месяц → предложить ужесточить min_score
  - Если alerts_per_month < 10 → предложить ослабить фильтры
  - Если конкретный symbol даёт >50% losses → добавить в watchlist blacklist
- Реализовать как воскресный пост-скрипт после weekly report

## Зависимости

- DatabaseManager.insert_alert() — ✅ ГОТОВО
- DatabaseManager.get_alert_stats() — ✅ ГОТОВО
- DatabaseManager.get_symbol_alert_history() — ✅ ГОТОВО
- DatabaseManager.get_weekly_summary() — ✅ ГОТОВО
- DatabaseManager.update_alert_result() — ✅ ГОТОВО
- CandleManager → DatabaseManager injection — ❌ НЕ СДЕЛАНО
- Agent tool registration — ❌ НЕ СДЕЛАНО
- Weekly report loop — ❌ НЕ СДЕЛАНО

## Архитектурные заметки

- Все analytics-методы в database.py уже async и готовы к использованию
- SQLite при 365 днях алертов (макс ~5500 строк) будет работать мгновенно
- Индексы на alerts.symbol, alerts.timestamp, alerts.result уже созданы
- PRAGMA optimize запускается после каждого cleanup
