---
description: How to run regression tests before deploying or committing
---

# Regression Test Workflow

## Quick Smoke Test (10 seconds)

Run before every deploy or major commit:

```powershell
// turbo
pytest tests/test_smoke.py -v --tb=short
```

## Full Core Tests (30 seconds)

Run before every session ends:

```powershell
// turbo
pytest tests/test_core.py -v --tb=short
```

## Full Suite (1 minute)

Run for complete validation:

```powershell
// turbo
pytest tests/ -v --tb=short -x
```

## What Each Test Covers

### test_smoke.py — "Will it start?"

- All module imports work (no NameError)
- Config loads with all required fields
- No hardcoded tokens in source code
- .env has all required keys
- DeepSeek remains disabled

### test_core.py — "Will it work correctly?"

- ExponentialBackoff delays increase properly
- Scoring always returns 0-100
- Database CRUD operations work
- Alert formatters produce valid HTML
- WS manager accepts fallback URL
- Cleanup preserves recent alerts

## When to Run

- **Before every commit**: `pytest tests/test_smoke.py`
- **Before deploy/restart**: `pytest tests/ -v`
- **After major refactoring**: `pytest tests/ -v --tb=long`
