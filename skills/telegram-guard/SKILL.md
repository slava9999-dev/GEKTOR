---
name: telegram-guard
description: Managed security and notification skill for Gerald. Sends alerts and heartbeat status to Slava's Telegram.
version: 1.0.0
author: Gerald-SuperBrain
triggers:
  - "send alert"
  - "telegram notify"
  - "report error to slava"
---

# Telegram Guard

## Purpose

Enables Gerald to reach Slava via Telegram for critical alerts, thermal warnings, or task completion notifications.

## Usage

`send alert "GPU temperature is high!"`
`telegram notify "Reflector found a fatal error in logs"`

## Configuration

Stored in `skills/telegram-guard/config.json` or `.env`.
Token: (hidden)
Chat ID: (auto-detected from first message or set in `.env`)
