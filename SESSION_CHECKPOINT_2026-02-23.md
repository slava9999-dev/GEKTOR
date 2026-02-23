# SESSION CHECKPOINT — 2026-02-23

## 🎯 Статус: Gerald РАБОТАЕТ через OpenClaw

Gerald успешно отвечает текстом по-русски через OpenClaw + Ollama.

---

## ✅ Что сделано в этой сессии

### 1. Диагностика и исправление модели

| Модель                    | Проблема                             | Результат                  |
| ------------------------- | ------------------------------------ | -------------------------- |
| qwen2.5-coder:14b         | NO_REPLY (tool-calling loop)         | ❌ Не работает как primary |
| mistral-nemo:latest (12B) | CUDA OOM на 8GB VRAM                 | ❌ Не помещается           |
| mistral:latest (7B)       | Работает, но НЕ знает русский        | ⚠️ Только English          |
| **qwen2.5:7b**            | **Работает! Русский + tool-calling** | ✅ **PRIMARY**             |

### 2. Изменённые файлы

#### `~/.openclaw/openclaw.json`

- Primary model: `ollama/qwen2.5:7b` (alias: `gerald-main`)
- Fallbacks: `mistral:latest`, `qwen2.5-coder:14b`
- qwen2.5:7b params: num_ctx=16384, num_gpu=35, temp=0.7
- Все модели имеют настроенные params

#### `~/.openclaw/workspace/AGENTS.md`

- Сокращён с 7,783 → 1,087 символов (86% reduction)
- Добавлена инструкция: "ALWAYS respond in Russian"
- Добавлено: "For simple questions, respond with text — do NOT call tools"

#### `~/.openclaw/workspace/SOUL.md`

- Engine обновлён: qwen2.5:7b primary (было qwen2.5-coder:14b)
- Context window: 16k (было 16k — неверно, фактически было 8k)
- Thermal: убрана ссылка на mistral-nemo (OOM)

#### `~/.openclaw/workspace/IDENTITY.md`

- Заполнен (был пустым шаблоном)
- Gerald, Emoji 🧠, Russian primary, qwen2.5:7b

#### `~/.openclaw/agents/main/agent/models.json`

- ⚠️ Gateway перегенерирует этот файл! Ручные правки не сохраняются.
- contextWindow остаётся 128000 (метаданные OpenClaw, не num_ctx)

### 3. Скачанные модели Ollama

- qwen2.5:7b (4.7GB) — NEW, основная модель Gerald
- mistral:latest (4.1GB) — fallback
- mistral-nemo:latest (12B) — не используется (OOM)
- qwen2.5-coder:14b (8.9GB) — для тяжёлого кода через прямой API

---

## 🔴 Нерешённые проблемы

### 1. Контекст почти полностью занят промптом

- System prompt: ~13,248 tokens из 16,384
- Conversation budget: ~3,136 tokens (5-6 сообщений)
- **Нужно увеличить num_ctx до 32768** (Qwen 7B поддерживает, VRAM хватит)

### 2. ChromaDB RAG не работает как нужно

- Сервер ChromaDB должен работать на localhost:8000
- Нужен скрипт индексации файлов компьютера
- Gerald должен автоматически искать в RAG перед ответом

### 3. Gerald не знает содержимое компьютера

- Нужна многослойная память (L1-L6 architecture)
- L4 (ChromaDB RAG) — ключевой слой для "знания всего на компе"
- Нужен indexer pipeline: scan dirs → chunk files → embed → store in Chroma

### 4. Git не инициализирован

- `c:\Gerald-superBrain` — нет .git
- Все изменения незащищены

### 5. Bridge Daemon не протестирован

- bridge_daemon.py может быть устаревшим
- Нужно проверить совместимость с qwen2.5:7b

### 6. Tool-use не протестирован

- Gerald отвечает текстом ✅
- Но может ли он использовать read/exec/browser tools? Не проверено.

---

## 📐 Текущая архитектура памяти (TO-DO)

```
L1: Context (always loaded)     → SOUL.md, IDENTITY.md, AGENTS.md, USER.md
L2: Session (conversation)      → ~3k tokens (мало!)
L3: Curated (MEMORY.md)         → ~600 tokens
L4: RAG (ChromaDB)              → НЕ НАСТРОЕН — ключевой приоритет
L5: File System (read tool)     → Доступен через OpenClaw tools
L6: Web (search/fetch tools)    → Доступен через OpenClaw tools
```

---

## 🎯 Приоритеты следующей сессии

1. **Увеличить num_ctx до 32768** — удвоить бюджет контекста
2. **Настроить ChromaDB indexer** — проиндексировать все файлы
3. **Протестировать tool-use** — read, exec, memory tools
4. **Git init** — защитить изменения
5. **Оптимизировать промпт** — убрать лишнее из контекста

---

## 🔧 Команды для старта

```powershell
# Активация среды
& c:/Gerald-superBrain/.venv/Scripts/Activate.ps1
$env:OLLAMA_API_KEY = "ollama-local"

# Gateway (должен уже работать, pid 31104)
openclaw gateway run

# Тест Gerald
openclaw agent --agent main --session-id test --message "Привет Gerald!"

# Прямой тест Ollama (без OpenClaw)
curl -s http://localhost:11434/api/generate -d '{"model":"qwen2.5:7b","prompt":"Привет!","stream":false}' | python -m json.tool
```

---

## 🧪 Подтверждённый рабочий вывод

```
Model: qwen2.5:7b
Input: 13,248 tokens
Output: 155 tokens
stopReason: "stop"

Response: "Привет! Я Геральд — персональный интеллектуальный ассистент,
работающий внутри OpenClaw..."
```

_Checkpoint created: 2026-02-23T15:05 MSK_
