def log_guard_event(logger, event: str, **kwargs):
    """
    Structured guard/fallback logging helper.
    Must never raise.
    """
    try:
        parts = [f"{k}={v}" for k, v in sorted(kwargs.items())]
        logger.warning(f"[GUARD] {event} | " + " ".join(parts))
    except Exception:
        try:
            logger.warning(f"[GUARD] {event}")
        except Exception:
            pass
