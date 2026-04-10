"""
Gerald v4 — Anti-Miss Alert Adapter.

Redirects to unified formatter. This file exists for backward compatibility.
"""
from core.alerts.formatters import format_early_detection_alert


def format_anti_miss_alert(hit: dict) -> str:
    """
    v4: Delegates to unified formatter.
    Returns message text only (backward compat).
    For full (text, buttons) use format_early_detection_alert directly.
    """
    msg, _ = format_early_detection_alert(hit)
    return msg
