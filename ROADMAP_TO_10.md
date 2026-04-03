Gerald Sniper — Окончательный роадмап v3.2
Дата: 2026-03-09
Основание: реальные прогоны, свежие runtime-баги, анализ логов, архитектурная ревизия без романтики и без поклонения “красивым идеям”

ЧАСТЬ 1. Где мы находимся на самом деле
Состояние системы
Gerald Sniper не мёртвый прототип, но и не готовый боевой инструмент.
Это уже живая система, у которой:

- есть рабочий radar,
- есть BTC context,
- есть level detection,
- есть Telegram alerts,
- есть WebSocket поток,
- есть отдельные подтверждённые триггеры,

но у неё пока нет достаточной operational-надёжности, чтобы считать её устойчивым 24/7 инструментом.

Что уже доказано логами
Работает:

- Radar стабильно запускается и сканирует ликвидные тикеры
- BTC context живой и меняется адекватно
- ROUND / WEEK_EXTREME / KDE уровни реально строятся
- Telegram-алерты уже уходили
- daily alert limit срабатывает
- stale cleanup работает
- graceful shutdown работает
- reconnect происходит
- detector pipeline жив
- actionable levels появляются
- logger scope bug уже пережили и локализовали
- runtime path не разваливается на старте

Не доказано до конца или доказано как нестабильное:

- closed-candle → trigger → guard → dedup → breaker path полностью и стабильно
- DB persistence как надёжный слой
- schema compatibility между кодом и SQLite
- отсутствие тихих ошибок в background tasks
- стабильный network fan-out на больших сканах
- устойчивая KDE-логика при грязных данных
- надёжное восстановление state после рестарта
- trade tracking как правдивое отражение реального движения цены

Что сломано или опасно

1. Сетевой слой нестабилен
   Timeout avalanches и пустые scans — не мелкий дефект, а operational blocker.
2. Persistence ненадёжна
   Уже был реальный runtime баг:
   sqlite3.OperationalError: table detected_levels has no column named source

Это значит:

- код и схема БД могут расходиться,
- система может работать “наполовину”,
- ошибки могут не валить процесс, а тихо ломать операционку.

3. Background task handling незрелый
   Были:
   Task exception was never retrieved

Это уже красный флаг уровня platform hygiene. 4. Data quality нестабильна
Исторические аномалии ломают ATR/KDE и создают фейковые уровни. 5. KDE пока не доказала стабильность
Она то есть, то исчезает, то выдаёт мусор, то выглядит полезной. 6. AI пока не проблема №1
LLM latency неприятна, но это не главный блокер.
Главный блокер — доверие к самим входным сигналам и инфраструктуре.

ЧАСТЬ 2. Базовые принципы v3.2
Принцип 1. Сначала operational correctness, потом “ум”
Нельзя строить AI-аналитику на системе, которая:

- может тихо не записывать данные,
- может терять background exceptions,
- может иметь несовместимую схему БД,
- может частично работать и делать вид, что всё хорошо.

Принцип 2. Система должна уметь ломаться честно
Если БД несовместима, приложение должно:

- либо мигрировать схему,
- либо падать на старте,
- но не жить в режиме “ну вроде что-то работает”.

Принцип 3. Никакой магии fire-and-forget без контроля
Любая background task должна:

- быть await-нута,
- или иметь callback на exception,
- или быть частью явного task registry.

Принцип 4. Данные не удаляем — данные изолируем
Подозрительные свечи не надо “стирать из истории”.
Их надо:

- помечать,
- исключать из чувствительных расчётов,
- но оставлять в raw data.

Принцип 5. AI — это усилитель, а не костыль
LLM не должен компенсировать:

- плохой trigger pipeline,
- плохую data quality,
- сетевую нестабильность,
- некачественную persistence.

Принцип 6. Checkpoint важнее ощущения прогресса
Переход к следующему этапу возможен только после фактического доказательства логами и тестами.

Принцип 7. Реальные сроки > красивые сроки
Любая оценка ниже “кажется, успею” умножается минимум на 1.5–2.
Потому что на Земле:

- сеть тупит,
- биржа врёт,
- SQLite вспоминает о себе в самый неподходящий момент,
- и Python всегда найдёт способ удивить.

ЧАСТЬ 3. Новый роадмап v3.2

СПРИНТ 0 — Operational Integrity
Цель: система должна быть честной, самопроверяемой и предсказуемой до начала дальнейшего развития.
Смысл: убрать класс ошибок “вроде работает, но на самом деле нет”.
Оценка: 2–4 дня

0.1 Schema versioning и migrations
Проблема
Код уже эволюционирует быстрее, чем SQLite-файл.
Мы уже словили несовместимость схемы.
Решение

- ввести schema_version / meta таблицу
- все изменения схемы оформлять миграциями
- на старте проверять версию схемы
- если схема несовместима:

либо мигрировать,
либо падать с понятным сообщением

Минимум реализации

- initialize() не просто “create if not exists”
- добавить список миграций
- сделать idempotent upgrade path
- логировать текущую и целевую версии

Checkpoint

- больше нет runtime ошибок вида no column named ...
- чистая БД и старая БД обе стартуют предсказуемо
- migration test проходит

  0.2 Background task safety
  Проблема
  Task exception was never retrieved — это operational debt, а не безобидный warning.
  Решение

- инвентаризация всех asyncio.create_task(...)
- для каждой задачи:

либо await,
либо task registry,
либо safe callback logging

- критичные задачи не должны быть fire-and-forget

Checkpoint

- в логах нет Task exception was never retrieved
- при падении background task есть понятный stack trace и контекст

  0.3 Boot self-check
  Проблема
  Система стартует даже в частично несовместимом состоянии.
  Решение
  На старте проверять:

- DB schema
- наличие нужных таблиц и колонок
- базовые config values
- Telegram token / режим пропуска
- REST/WS конфиг
- доступность директорий
- валидность важных параметров конфига

Checkpoint

- старт либо успешен и среда валидна,
- либо приложение честно останавливается с понятным сообщением

  0.4 Structured lifecycle logging
  Проблема
  Сейчас часть пайплайна видна, часть — нет.
  Сложно понять, “сигнала не было” или “сигнал умер между слоями”.
  Решение
  Добавить структурированные лог-события:

- level_created
- level_selected
- level_persisted
- candle_closed_processed
- trigger_candidate
- trigger_guard_blocked
- trigger_sent
- alert_persisted
- cooldown_restored

Checkpoint

- по одному symbol можно проследить путь сигнала end-to-end по логам

  0.5 Smoke test suite
  Проблема
  Unit tests полезны, но не ловят половину operational-поломок.
  Решение
  Минимальный smoke-набор:

- app boot test
- DB init + migration test
- trigger pipeline smoke
- schema compatibility test
- persistence write test

Checkpoint

- перед запуском основных изменений есть базовая сетка защиты

СПРИНТ 1 — Runtime Stability: сеть, данные, триггеры
Цель: система должна стабильно добывать данные, строить адекватные уровни и корректно проходить trigger path.
Оценка: 4–6 дней

1.1 Bounded concurrency scheduler для radar
Проблема
Timeout storm — это не только rate limit, это uncontrolled fan-out.
Решение
Переписать scan на:

- bounded concurrency (Semaphore)
- batching/windowing
- jitter
- timeout budget
- retry budget
- optional slowdown по rate-limit headers

Важно
Rate-limit headers — полезны, но это не волшебная таблетка.
Ядро решения — контролируемая параллельность.
Checkpoint

- нет timeout avalanches
- radar scan стабилен на watchlist реального размера
- количество пустых сканов стремится к нулю

  1.2 WS heartbeat + pong tracking
  Проблема
  Reconnects есть, но непонятно: это сеть, idle timeout или отсутствие контроля liveness.
  Решение

- ping loop
- last pong timestamp
- reconnect при отсутствии pong
- логирование причин реконнекта

Checkpoint

- reconnect rate снижается
- причины реконнектов понятны по логам

  1.3 Data anomaly gating
  Проблема
  Исторические шпильки и мусор ломают ATR, KDE и уровни.
  Решение
  Не удалять данные, а:

- маркировать suspicious candles
- исключать их из ATR baseline
- исключать их из KDE / extremes logic при необходимости
- вести метрику “грязности” символа

Примеры правил

- price jump vs rolling median
- wick/body anomaly
- single-candle outlier against rolling window

Checkpoint

- POWERUSDT-класс аномалий больше не создаёт фейковые уровни
- ATR перестаёт улетать в космос на мусоре

  1.4 Closed-candle trigger path proof
  Проблема
  Нельзя идти дальше, пока не доказан полный путь:
  уровень → закрытая свеча → guard → trigger/dedup/block.
  Решение
  Явно пройти и залогировать:

- closed candle accepted
- ATR validated
- proximity check
- level store hit
- trigger candidate created
- guard block reason / pass
- alert send
- cooldown apply

Checkpoint
Есть живые логи, подтверждающие:

- хотя бы несколько корректных block cases
- хотя бы несколько корректных send cases

  1.5 KDE stabilization после quality gating
  Проблема
  KDE нестабильна, но тюнинг без чистых данных — игра в рулетку.
  Решение
  После пункта 1.3:

- разрешить min_touches=1 только со штрафом
- логарифмический scoring по касаниям
- distance adaptation для действительно сильных уровней
- penalize weak single-touch KDE
- добавить тесты на repeatability

Checkpoint

- KDE не мигает хаотично между “0 и 6”
- score различает 15 и 24 touches
- мусорные пики не доминируют

  1.6 Round-number bias audit
  Проблема
  Исторически round levels имели риск искусственного усиления.
  Решение

- оставить жёсткий penalty
- добавить debug/log-only аудит для ROUND_NUMBER
- проверить, что финальный actionable path использует penalized score

Checkpoint

- round levels не доминируют shortlist без веской причины
- лог подтверждает penalized final score

СПРИНТ 2 — Durable Signal Workflow
Цель: сделать сигнал жизненным объектом, а не одноразовым сообщением в Telegram.
Оценка: 4–6 дней

2.1 Alert/state persistence
Проблема
Cooldowns и состояние сигналов не должны зависеть от uptime процесса.
Решение

- сохранять sent alerts
- восстанавливать cooldowns при старте
- хранить signal state machine:

DETECTED
ALERTED
TAKEN
SKIPPED
CLOSED
EXPIRED

Checkpoint

- рестарт не создаёт дублей
- state восстанавливается корректно

  2.2 Signal identity и trade identity
  Проблема
  Без устойчивого ID нельзя нормально трекать take/skip/result.
  Решение
  Каждому сигналу присваивать:

- signal_id
- symbol
- level snapshot
- trigger snapshot
- context snapshot

Checkpoint

- любой Telegram action может однозначно ссылаться на конкретный сигнал

  2.3 Event-driven Trade Tracker
  Проблема
  Polling — медленный и местами фальшивый способ трекинга.
  Решение

- price updates from WS
- stateful trade monitor
- явные close reasons:

TP
SL
MANUAL
EXPIRED
INVALIDATED

- защита от duplicate close

Checkpoint

- trade outcomes фиксируются быстро и однозначно
- нет двойных закрытий

  2.4 Telegram reliability layer
  Проблема
  Telegram должен быть не “удачно отправилось”, а управляемым транспортом.
  Решение

- error context logging
- retryable/non-retryable classification
- correlation with signal_id
- сохранение факта отправки / ошибки

Checkpoint

- пустых “Telegram API Exception:” больше нет
- каждая ошибка диагностируема

СПРИНТ 3 — Human + AI Augmentation
Цель: добавить AI и пользовательскую интерактивность только после того, как сигналовый слой стал надёжным.
Оценка: 3–5 дней

3.1 Async SignalAnalyst
Проблема
LLM может отвечать медленно, но не должен тормозить алерт.
Решение

- fast alert отправляется сразу
- AI анализ стартует отдельно
- AI ответ приходит reply-сообщением к исходному alert
- логируется latency и failure rate

Checkpoint

- сигнал доходит без ожидания LLM
- AI не ломает основной pipeline

  3.2 /take, /skip, /stats
  Проблема
  Пользовательские действия должны быть частью signal state machine, а не отдельной магией.
  Решение

- команды / кнопки на основе signal_id
- запись действий в БД
- привязка к Trade Tracker

Checkpoint

- взятие/скип отражается в state и статистике

  3.3 Weekly report — только на чистых данных
  Проблема
  AI summary на грязных данных создаёт красивую ложь.
  Решение
  Weekly report делать только если есть:

- достаточное количество закрытых кейсов
- валидные outcome labels
- нормальная целостность signal/trade records

Checkpoint

- отчёт строится на реальной статистике, а не на мусоре

СПРИНТ 4 — Рефакторинг ответственности
Цель: закрепить рост системы, чтобы новые фичи не ломали старые.
Оценка: 2–4 дня

4.1 Разделение CandleManager
Проблема
Слишком много ответственности в одном месте почти гарантирует новые баги.
Решение
Разделить на:

- candle_cache.py — WS, хранение, контекст
- trigger_pipeline.py — проверка закрытой свечи, proximity, trigger generation
- signal_state.py — lifecycle signal/trade
- persistence_service.py — запись и восстановление state

Checkpoint

- обязанности разделены
- новые изменения не требуют “трогать всё сразу”

  4.2 Тесты на критические seam’ы
  Решение
  Добавить тесты не вообще “на всё”, а на швы:

- migration tests
- trigger path tests
- trade close idempotency
- Telegram failure handling
- KDE scoring consistency

Checkpoint

- самые опасные места прикрыты тестами

ЧАСТЬ 4. Что НЕ делать в v3.2
Не делатьПочемуПоднимать DI containerдля solo-проекта это лишняя массаДелать интерфейсы под 5 будущих биржпока нет второй биржи — не надо играть в корпорациюВнедрять Prometheus/Grafana раньше времениTelegram + логов достаточно на текущем этапеПисать weekly AI summary до чистой state-моделиgarbage in, wisdom-looking garbage outЛезть в глубокий рефактор до operational fixesсначала честность системы, потом красотаДелать “автоторговлю” до доказанного trade trackerиначе это не торговля, а ускоренная форма сожаления

ЧАСТЬ 5. Реалистичная оценка сроков
Sprint 0
2–4 дня
Sprint 1
4–6 дней
Sprint 2
4–6 дней
Sprint 3
3–5 дней
Sprint 4
2–4 дня

Итого
15–25 дней, а не “магические 12”.
Если всё идёт гладко — ближе к 15–18.
Если Bybit, SQLite и рынок решат показать характер — ближе к 20–25.
Это и есть подход, приближённый к жизни на планете Земля.

ЧАСТЬ 6. Новые checkpoint’ы
После Sprint 0

- нет schema drift runtime errors
- нет silent background task failures
- старт делает self-check
- есть минимальный smoke suite

После Sprint 1

- radar стабилен
- WS liveness контролируется
- аномальные данные не ломают ATR/KDE
- closed-candle trigger path доказан логами
- round-number bias под контролем

После Sprint 2

- сигналы и cooldowns переживают рестарт
- signal/trade states устойчивы
- trade tracker закрывает сделки без дублей
- Telegram ошибки диагностируемы

После Sprint 3

- AI не блокирует алерт
- user actions встроены в state machine
- weekly report строится на чистой статистике

После Sprint 4

- архитектура устойчивее
- критические seam-тесты есть
- расширять систему стало безопаснее

ЧАСТЬ 7. Честная итоговая сводка
Если v3.2 будет выполнен
То результатом станет не “идеальный AI sniper”, а гораздо более ценная вещь:
Система, которая:

- честно стартует,
- честно пишет данные,
- честно сигналит,
- честно переживает рестарт,
- честно объясняет, почему отправила или не отправила сигнал,
- и только потом добавляет AI как усилитель, а не как макияж на проблемной операционке.

Финальный вывод
v3.2 отличается от v3.1 главным:
он ставит Operational Integrity раньше Network/Data/AI.
Это и есть реализм.
Потому что в реальной жизни систему почти никогда не убивает отсутствие красивой аналитики.
Её убивают:

- тихие фоновые ошибки,
- несовместимая схема БД,
- частично рабочие пайплайны,
- неуправляемая конкуренция,
- и вера, что “раз не упало — значит работает”.
