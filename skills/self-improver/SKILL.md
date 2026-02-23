---
name: self-improver
description: Self-analysis and self-improvement engine — analyzes errors, patterns, and performance to continuously evolve Gerald's capabilities and skills.
version: 1.0.0
author: Gerald-SuperBrain
triggers:
  - "improve yourself"
  - "analyze errors"
  - "what did you learn"
  - "self-review"
  - "evolve"
---

# Self-Improver — Continuous Evolution Engine

## Purpose

Gerald doesn't just execute tasks — he learns from them. This skill enables:

- Post-task error analysis
- Pattern detection across sessions
- Automatic skill updates based on learned patterns
- Performance tracking and optimization suggestions

## Self-Improvement Loop

```
┌─────────────────────────────────────────────────┐
│                IMPROVEMENT CYCLE                │
│                                                 │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐   │
│  │ Execute  │──▶│ Analyze  │──▶│  Learn   │   │
│  │  Task    │   │ Results  │   │ Pattern  │   │
│  └──────────┘   └──────────┘   └────┬─────┘   │
│       ▲                             │          │
│       │         ┌──────────┐   ┌────▼─────┐   │
│       └─────────│  Apply   │◀──│  Update  │   │
│                 │ Changes  │   │  Skills  │   │
│                 └──────────┘   └──────────┘   │
│                                                 │
└─────────────────────────────────────────────────┘
```

## Analysis Framework

### After Every Significant Task

```
1. OUTCOME ANALYSIS
   - Did the task succeed? Partially? Fail?
   - What was the expected vs actual result?
   - How long did it take vs expected?

2. ERROR FORENSICS (if errors occurred)
   - Root cause identification
   - Was this error preventable?
   - Has this error type occurred before?
   - Store error pattern in ChromaDB

3. SKILL GAP ANALYSIS
   - Did any skill fail or underperform?
   - Is a new skill needed for this task type?
   - Should an existing skill be updated?

4. KNOWLEDGE UPDATE
   - What new facts were learned?
   - What assumptions were corrected?
   - Store insights in ChromaDB for future reference

5. META-REFLECTION
   - Was my reasoning approach optimal?
   - Should I have used a different model?
   - What would I do differently next time?
```

### Weekly Self-Review (Triggered Manually)

```
"self-review" or "improve yourself"

1. Aggregate all errors from the past week
2. Identify top 3 recurring patterns
3. Propose specific improvements for each
4. Update relevant skills with fixes
5. Generate improvement report
```

## Error Pattern Storage

Errors are stored in ChromaDB with rich metadata:

```json
{
  "collection": "error-patterns",
  "document": "Description of what went wrong and why",
  "metadata": {
    "error_type": "reasoning|execution|knowledge|tool",
    "severity": "low|medium|high|critical",
    "root_cause": "Description of root cause",
    "fix_applied": "What was done to fix it",
    "prevention": "How to prevent in future",
    "related_skill": "skill-name",
    "occurrence_count": 1,
    "first_seen": "2026-02-22",
    "last_seen": "2026-02-22"
  }
}
```

## Improvement Actions

### Automatic

- Update error patterns in ChromaDB (always)
- Increment occurrence counts for recurring errors
- Flag skills with >3 errors for review

### Semi-Automatic (Propose to User)

- Skill SKILL.md updates
- New skill creation for uncovered capabilities
- Config parameter adjustments

### Manual (User Approval Required)

- SOUL.md updates
- Model switching defaults
- Removing or replacing skills

## Metrics Tracked

- Tasks completed / failed ratio
- Average reasoning depth per task
- Model switch frequency
- Error rate by category
- Knowledge base growth rate
- Most/least used skills

## Commands

- `self-review` — Run full self-analysis
- `analyze errors` — Show recent error patterns
- `what did you learn` — Summarize recent insights
- `evolve` — Propose and apply improvements
- `show metrics` — Display performance metrics
