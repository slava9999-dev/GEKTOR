# 🧠 Gerald-SuperBrain

> **Personal JARVIS 2026** — Local AI super-brain powered by OpenClaw + Ollama

---

## 🎯 What Is This?

Gerald-SuperBrain is Вячеслав's personal AI assistant — a local, private, hyper-intelligent agent that combines:

- **OpenClaw** as the agent framework (skills, tools, messaging, gateway)
- **Ollama** for local LLM inference (qwen2.5-coder:14b optimized for 8GB VRAM)
- **ChromaDB** for persistent vector memory (RAG — Retrieval-Augmented Generation)
- **Advanced Reasoning** — forced CoT, ToT, self-critique on every complex task
- **Smart Context Management** — 8k/16k optimized window for zero-latency response

---

## 🚀 Quick Start

### Prerequisites

- [Ollama](https://ollama.ai) installed
- [OpenClaw](https://openclaw.ai) v2026.2.14+
- [Docker](https://docker.com) (for ChromaDB)
- Windows 10/11 with NVIDIA GPU (recommended for GPU inference)

### 1. Start ChromaDB

```bash
docker start chroma
# or if first time:
docker run -d -p 8000:8000 --name chroma \
  -v ~/gerald-memory:/chroma/chroma \
  chromadb/chroma:latest
```

### 2. Ensure Models Are Loaded

```bash
ollama list
# Should show:
# qwen2.5-coder:14b-q4_k_m
# mistral-nemo:latest
```

### 3. Start Gerald

```bash
openclaw gateway run
```

### 4. Talk to Gerald

```bash
openclaw agent --message "Gerald, расскажи о себе"
```

---

## 📐 Hardware-Optimized Context

Gerald runs with a **8,192 token context window** (Qwen 14B) or **16,384** (Mistral Nemo). This is specifically tuned for the **RTX 4070 8GB** to ensure zero swap-to-RAM and 100% GPU processing speed.

| Capability             | Strategy                               |
| ---------------------- | -------------------------------------- |
| **Codebase analysis**  | Recursive RAG via ChromaDB             |
| **Long conversations** | Automatic summarization/compaction     |
| **Deep reasoning**     | Multi-step CoT with targeted fragments |
| **RAG synthesis**      | Semantic retrieval of key context only |

### How to leverage it:

```
# Ask Gerald to analyze entire modules
"Gerald, загрузи весь модуль trading/ и найди все потенциальные баги"

# Ask for comprehensive plans
"Gerald, создай полный plan рефакторинга с учётом всех зависимостей"

# Deep debugging
"Gerald, вот 5 файлов — найди, почему функция X вызывается два раза"
```

---

## 🛠 Skills

| Skill                  | Purpose                                              |
| ---------------------- | ---------------------------------------------------- |
| **skill-master**       | Create, install, validate any skill from any source  |
| **vector-knowledge**   | ChromaDB RAG — store & search knowledge semantically |
| **model-manager**      | Auto-switch models + thermal management              |
| **antigravity-bridge** | Control Antigravity IDE agent from OpenClaw          |
| **self-improver**      | Learn from errors, evolve capabilities               |
| **reasoning-engine**   | Force deep reasoning (CoT, ToT, self-critique)       |

---

## 📁 Project Structure

```
Gerald-SuperBrain/
├── SOUL.md                    # Gerald's personality & identity
├── README-Gerald.md           # This file
├── gerald-setup.log           # Installation log
├── .openclaw/
│   └── config.json            # Project-level OpenClaw config
├── skills/
│   ├── skill-master/          # Meta-skill manager
│   │   └── SKILL.md
│   ├── vector-knowledge/      # ChromaDB RAG integration
│   │   └── SKILL.md
│   ├── model-manager/         # Model switching & thermal mgmt
│   │   └── SKILL.md
│   ├── antigravity-bridge/    # OpenClaw ↔ Antigravity bridge
│   │   └── SKILL.md
│   ├── self-improver/         # Self-analysis & evolution
│   │   └── SKILL.md
│   └── reasoning-engine/      # Advanced reasoning (CoT/ToT)
│       └── SKILL.md
├── memory/                    # → symlink to ~/gerald-memory
└── bridge/                    # Antigravity bridge files
    ├── inbox/
    ├── outbox/
    └── shared-context/
```

---

## 🔧 Configuration

### Models (in `.openclaw/config.json`)

| Parameter        | Value | Purpose                                   |
| ---------------- | ----- | ----------------------------------------- |
| `num_ctx`        | 8192  | 8k context window (hardware safe)         |
| `num_gpu_layers` | 26    | Balanced GPU offload (8GB VRAM optimized) |
| `num_threads`    | 8     | CPU thread count                          |
| `keep_alive`     | 20m   | Model stays loaded 20 min after last use  |
| `temperature`    | 0.65  | Slightly creative but reliable            |
| `top_p`          | 0.95  | Nucleus sampling threshold                |

### ChromaDB

- **URL:** `http://localhost:8000`
- **Storage:** `~/gerald-memory`
- **Collections:** `gerald-knowledge`, `skills-docs`, `projects`, `error-patterns`

---

## 🧪 Testing Gerald

```bash
# Self-test
openclaw agent --message "Gerald, расскажи о себе, покажи все skills, создай тестовый skill"

# Context window test
openclaw models test --context 100000

# RAG test
openclaw agent --message "Gerald, что ты помнишь о нашем последнем проекте?"

# Reasoning test
openclaw agent --message "Gerald, think hard: какая архитектура лучше для high-frequency trading?"
```

---

## 📊 Model Comparison

| Feature       | qwen2.5-coder:14b (Primary) | mistral-nemo (Heavy) |
| ------------- | --------------------------- | -------------------- |
| **Speed**     | ⚡ Fast                     | 🐢 Slower            |
| **Coding**    | ⭐⭐⭐⭐⭐                  | ⭐⭐⭐⭐             |
| **Reasoning** | ⭐⭐⭐⭐                    | ⭐⭐⭐⭐⭐           |
| **Context**   | 8k                          | 16k                  |
| **GPU Load**  | Medium (26 layers)          | Medium (28 layers)   |
| **Use Case**  | Daily driver                | Complex analysis     |

---

## 🔐 Security

- **100% Local** — No data leaves your machine
- **No cloud APIs** — All inference runs on Ollama locally
- **ChromaDB local** — Vector DB runs in Docker on localhost
- **Gateway auth** — Token-based authentication for gateway access

---

_Created: 2026-02-22 | Owner: Вячеслав | Engine: OpenClaw v2026.2.21_
