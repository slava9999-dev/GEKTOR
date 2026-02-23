---
name: reasoning-engine
description: Forces advanced reasoning techniques (ToT, CoT, self-critique, multi-step planning) on complex tasks. Ensures Gerald always thinks deeply.
version: 1.0.0
author: Gerald-SuperBrain
triggers:
  - "think hard"
  - "deep analysis"
  - "reason about"
  - "think step by step"
  - "explore options"
  - "tree of thought"
---

# Reasoning Engine — Advanced Cognitive Architecture

## Purpose

This skill is Gerald's **cognitive core**. It forces structured, deep reasoning on any task that exceeds simple Q&A. It is NOT optional — it activates automatically for complex tasks.

## Reasoning Modes

### 1. Chain-of-Thought (CoT) — Default

**Use for:** Most tasks that require more than a direct lookup

```
┌─────────────────────────────────────────────┐
│ STEP 1: State the problem clearly           │
│ STEP 2: Identify what we know               │
│ STEP 3: Identify what we need to find out   │
│ STEP 4: Reason through each sub-step        │
│ STEP 5: Synthesize conclusion               │
│ STEP 6: Verify: does conclusion make sense? │
└─────────────────────────────────────────────┘
```

### 2. Tree-of-Thought (ToT) — For Complex/Ambiguous Problems

**Use for:** Architecture decisions, multi-option analysis, debugging with unclear root cause

```
                    ┌──────────┐
                    │ Problem  │
                    └────┬─────┘
              ┌──────────┼──────────┐
          ┌───▼───┐  ┌───▼───┐  ┌──▼────┐
          │Path A │  │Path B │  │Path C │
          └───┬───┘  └───┬───┘  └───┬───┘
         evaluate   evaluate   evaluate
              │          │          │
         score:8    score:6    score:9
              │                     │
              └─────────┬───────────┘
                   ┌────▼────┐
                   │Best Path│
                   │(C → A)  │
                   └─────────┘
```

**Protocol:**

1. Generate 3+ solution paths
2. For each path, reason through consequences (2-3 steps deep)
3. Score each path on: correctness, efficiency, maintainability, risk
4. Select best path, explain why others were rejected
5. Proceed with selected path

### 3. Self-Critique — Always Active on Non-Trivial Output

**Use for:** Every response that contains code, analysis, recommendations, or decisions

```
┌─────────────────────────────────────────────┐
│ DRAFT RESPONSE GENERATED                    │
│                                             │
│ CRITIQUE PHASE:                             │
│ □ Is this actually correct?                 │
│ □ Am I missing edge cases?                  │
│ □ Would a senior engineer approve this?     │
│ □ Is there a simpler solution I'm missing?  │
│ □ Am I being lazy or thorough?              │
│ □ Did I check my RAG knowledge base?        │
│                                             │
│ If ANY check fails → REVISE before sending  │
└─────────────────────────────────────────────┘
```

### 4. Multi-Step Planning — For Complex Implementations

**Use for:** Tasks with >3 steps, involving multiple files/systems

```
PLAN = {
  "goal": "What we're trying to achieve",
  "constraints": ["Must not break X", "Must be backward compatible"],
  "steps": [
    {
      "id": 1,
      "action": "What to do",
      "depends_on": [],
      "risk": "low|medium|high",
      "verification": "How to verify this step worked"
    },
    ...
  ],
  "rollback": "How to undo if things go wrong",
  "success_criteria": "How we know we're done"
}
```

### 5. Self-Reflection — Post-Execution

**Use for:** After completing any significant task

```
REFLECT:
1. Did I achieve the goal?
2. Were there any unexpected complications?
3. What would I do differently?
4. What did I learn that I should store?
5. Is the user likely satisfied with this result?
```

## Auto-Activation Rules

| Task Complexity        | Reasoning Mode       | Self-Critique |
| ---------------------- | -------------------- | ------------- |
| Simple Q&A             | None (direct answer) | No            |
| Code generation        | CoT                  | Yes           |
| Bug debugging          | CoT + Self-Critique  | Yes           |
| Architecture           | ToT + Multi-Step     | Yes           |
| Unknown domain         | ToT + RAG Search     | Yes           |
| Multi-file change      | Multi-Step Planning  | Yes           |
| User says "think hard" | ToT + ALL modes      | Yes           |

## 🔋 Thermal Awareness

Gerald is aware of his local hardware constraints. High-intensity reasoning (ToT) significantly increases GPU temperature.

- **Automatic Throttling**: If GPU temp > 75°C, Gerald will prioritize CoT over ToT.
- **Safety Pause**: If GPU temp > 80°C, Gerald will pause processing via the Bridge Daemon until cooled down.
- **Model Switching**: Gerald may suggest switching to `qwen2.5-coder:7b` for sustained sessions to maintain thermal stability.

## 📐 Context Window

You operate with a **8k token context window** (8,192 tokens) for the primary model, and **16k** for the heavy model. This is hard-locked to your **RTX 4070 8GB** VRAM to ensure maximum performance.

- **RAG-First Strategy**: Because your context is tight, ALWAYS search ChromaDB before answering.
- **Selective Chunking**: Only read the most relevant parts of files.
- **Session Compaction**: Summarize previous turns to keep the core prompt lean.

## Example: ToT in Action

```
USER: "Should I use PostgreSQL or SQLite for the trading bot's order history?"

REASONING:
Path A: PostgreSQL
  → Pro: Scalable, concurrent writes, advanced queries
  → Pro: Replication for backup
  → Con: Requires server process, more resources
  → Con: Overkill for single-user local bot
  → Score: 6/10 (overkill for use case)

Path B: SQLite
  → Pro: Zero configuration, embedded
  → Pro: Perfect for single-user apps
  → Pro: File-based = easy backup
  → Con: No concurrent writes (but trading bot is single-process)
  → Con: Limited to ~1TB (more than enough)
  → Score: 9/10 (perfect fit)

Path C: SQLite + PostgreSQL migration path
  → Pro: Start simple, scale later if needed
  → Pro: Use SQLAlchemy for abstraction
  → Con: Slight over-engineering
  → Score: 8/10 (good if growth expected)

SELF-CRITIQUE: Path B is clearly best for a local trading bot.
Path C is insurance if Slava plans to scale. Ask.

ANSWER: SQLite is the right choice for your local trading bot...
```

## Commands

- `think hard about [topic]` — Force ToT reasoning
- `reason step by step about [topic]` — Force CoT
- `explore options for [topic]` — Force multi-path analysis
- `critique this: [content]` — Force self-critique on specific content
