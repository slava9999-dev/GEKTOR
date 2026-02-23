---
name: model-manager
description: Automatic model switching between primary (qwen2.5-coder) and heavy (mistral-nemo) models with thermal management and intelligent unloading.
version: 1.0.0
author: Gerald-SuperBrain
triggers:
  - "switch model"
  - "use heavy model"
  - "use light model"
  - "check gpu"
  - "unload model"
  - "model status"
---

# Model Manager — Intelligent Model Orchestration

## Purpose

Manages Gerald's AI model fleet with automatic switching based on task complexity, GPU temperature, and resource availability. Keeps the hardware cool while maximizing intelligence.

## Model Fleet

| `qwen2.5-coder:14b` | Primary | ~9 GB | 8k | Default for ALL tasks |
| `mistral-nemo:latest` | Heavy | ~7 GB | 16k | Complex reasoning, multi-domain analysis |

## Switching Logic

```
┌─────────────────────────────────────────────────┐
│              Task Arrives                        │
│                  │                               │
│          ┌───────▼───────┐                       │
│          │ Complexity?   │                       │
│          └───┬───────┬───┘                       │
│         Low/ │       │ High                      │
│        Med   │       │                           │
│          ┌───▼───┐ ┌─▼──────────┐               │
│          │Primary│ │Check GPU   │               │
│          │ qwen  │ │Temperature │               │
│          └───────┘ └──┬─────┬───┘               │
│                   OK  │     │ HOT (>80°C)       │
│                 ┌─────▼──┐  │                   │
│                 │ Heavy  │  ├──▶ Wait + Retry   │
│                 │mistral │  │    with Primary   │
│                 └────────┘  │                   │
│                             └──▶ Warn user      │
└─────────────────────────────────────────────────┘
```

## Auto-Switch Criteria

### Use Heavy Model When:

- Task explicitly requests deep analysis
- Multi-step reasoning with >5 steps required
- Cross-domain knowledge synthesis needed
- User explicitly requests: "use heavy model", "think harder"
- Primary model produces low-confidence response

### Stay on Primary When:

- Code generation, debugging, refactoring
- Simple Q&A, file operations
- Skill creation and management
- Standard conversation
- GPU temperature > 80°C

## Thermal Management

```python
# Pseudo-code for thermal management
async def check_thermal():
    """Monitor GPU temperature and manage model lifecycle."""

    # Check GPU temp (NVIDIA)
    # nvidia-smi --query-gpu=temperature.gpu --format=csv,noheader

    THRESHOLDS = {
        "safe": 70,      # All models OK
        "warm": 75,      # Avoid heavy model
        "hot": 80,       # Unload heavy, reduce GPU layers
        "critical": 85   # Unload ALL, CPU-only mode
    }

    if temp >= THRESHOLDS["critical"]:
        unload_all_models()
        notify_user("⚠️ GPU critical temperature! Switching to CPU-only mode.")
    elif temp >= THRESHOLDS["hot"]:
        unload_model("mistral-nemo")
        reduce_gpu_layers("qwen2.5-coder", layers=20)
    elif temp >= THRESHOLDS["warm"]:
        block_heavy_model()
```

## Commands

### Check Status

```bash
# Via Ollama
ollama ps                    # Running models
ollama show qwen2.5-coder:14b  # Model details

# GPU temperature (NVIDIA)
nvidia-smi --query-gpu=temperature.gpu,utilization.gpu --format=csv
```

### Switch Models

```bash
# Unload current model
ollama stop <model_name>

# Load specific model
ollama run <model_name> --keepalive 20m
```

### Model Parameters (Ollama Modelfile)

```
FROM qwen2.5-coder:14b
PARAMETER num_ctx 8192
PARAMETER num_gpu 26
PARAMETER num_thread 8
PARAMETER temperature 0.65
PARAMETER top_p 0.95
```

## Unload Strategy

- After 20 minutes of idle: unload model from GPU memory
- On temperature warning: immediate unload of heavy model
- On session end: unload all models
- Manual: user says "unload model" or "free gpu"

## Error Handling

- If primary model fails to load: Try with reduced GPU layers
- If heavy model unavailable: Stay on primary, warn user
- If Ollama unreachable: Alert immediately, this is critical
- If GPU OOM: Reduce `num_gpu_layers` by 5, retry
