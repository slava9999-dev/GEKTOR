---
name: cyber-sentry
description: Automated security auditing and vulnerability detection. Analyzes code for secrets, insecure patterns, and prompt injection vectors.
version: 1.0.0
author: Gerald-SuperBrain-Sec
triggers:
  - "audit security"
  - "check for secrets"
  - "vulnerability scan"
  - "secure this code"
  - "is this safe"
---

# 🛡 Cyber Sentry — Security Audit Guard

## 🎯 Primary Objective

Ensure that all code and configurations in Gerald-SuperBrain follow the highest security standards (Zero Trust architecture).

## 🛠 Required Capabilities

1.  **Secret Detection**:
    - Scanning files for hardcoded API keys, tokens (Telegram, Exchange APIs), and passwords.
    - Verifying `.env` is in `.gitignore`.
2.  **Pattern Vulnerability Analysis**:
    - Checking for `eval()`, `exec()`, or uncontrolled `subprocess` calls (beyond the sandbox).
    - Validation of user inputs in the Bridge Daemon.
3.  **Sanitization**:
    - Ensuring all external data is treated as untrusted.

## 🚦 Operational Workflow

1.  **Static Scan**: Use `grep_search` with regex for common secret patterns (e.g., `sk-[a-zA-Z0-9]{48}`).
2.  **Configuration Check**: Verify that `src/infrastructure/sandbox/python_executor.py` current blacklist is effective against the code being audited.
3.  **Remediation**: Provide specific, copy-pasteable fixes to move secrets to `.env` or implement input validation.

## 🛡 Security & Constraints

- **NEVER** output a full secret in cleartext; only show the first 4 and last 4 chars.
- **ALWAYS** check for `PermissionError` when auditing system files.

## 🧪 Examples

**User**: "Проверь безопасность моста."
**Process**:

- Analyze `bridge_daemon.py`.
- Check input handling for Telegram messages.
- Check how `npx openclaw` is called.
  **Result**: Findings on potential command injection or leaked logs.

---

_Status: Production Ready | Sector: Cybersecurity_
