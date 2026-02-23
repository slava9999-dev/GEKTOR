---
name: antigravity-bridge
description: Bidirectional bridge between OpenClaw and Google Antigravity — control Antigravity sessions, share context, delegate tasks, and synchronize knowledge.
version: 1.0.0
author: Gerald-SuperBrain
triggers:
  - "ask antigravity"
  - "sync with antigravity"
  - "delegate to antigravity"
  - "bridge status"
---

# Antigravity Bridge — Gerald-IDE Symbiosis

## Purpose

This skill manages the communication between **Gerald (OpenClaw)** and the **Antigravity IDE Agent**. It allows you to control the IDE from Gerald and vice-versa via an asynchronous file-based protocol.

## Protocol: File-Based Inbox/Outbox

The bridge operates on two main directories:

- `bridge/inbox/`: Outgoing messages from Antigravity to Gerald (`*.msg`).
- `bridge/outbox/`: Responses from Gerald to Antigravity (`*.resp`).

### Messaging Flow:

1. **Antigravity** writes a `.msg` file to `inbox/`.
2. **Bridge Daemon** (`bridge/bridge_daemon.py`) detects the file.
3. **Daemon** invokes `openclaw agent --message "..."`.
4. **Gerald** processes the request and returns a result.
5. **Daemon** writes the result to `outbox/` as a `.resp` file.
6. **Antigravity** reads the response and updates its context.

## Operational Modes

### 1. Manual Mode

You can manually send messages by running:

```bash
# Ask Gerald something via the bridge
echo "Analyze this file" > bridge/inbox/task1.msg
```

### 2. Autonomous Mode (Daemon)

The `bridge_daemon.py` must be running in the background for real-time integration:

```bash
# Start the daemon
python bridge/bridge_daemon.py
```

## Shared Context

The directory `bridge/shared-context/` contains JSON snapshots of the current state:

- `cursor.json`: Current line and file in the IDE.
- `project_map.json`: High-level file tree.
- `last_error.log`: Recent terminal failures for Gerald to diagnose.

## Commands

- `bridge status` — Check health of the connection.
- `bridge sync` — Force a refresh of shared context.
- `bridge restart` — Restart the bridge daemon.

## Error Handling

- **Stale Status**: If `status.json` is older than 60s, the daemon is likely dead.
- **Corrupt Message**: If a `.msg` file is unreadable, it is moved to `bridge/errors/`.
- **Latency**: Large files may take 5-10s to process due to LLM inference time.
