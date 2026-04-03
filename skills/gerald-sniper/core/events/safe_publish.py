"""
Gerald v7.1 — Safe Publish Facade.

Replaces direct bus.publish() calls throughout the codebase.
Implements the Transactional Outbox pattern transparently:

    bus.publish(event)           →  UNSAFE (Redis dependency)
    safe_publish(event, session) →  SAFE   (DB-first, Redis-async)
    safe_publish(event)          →  SAFE   (Best-effort Redis, logged on failure)

Three operating modes:
    1. TRANSACTIONAL: Called with an active DB session → writes to outbox_events 
       table atomically. Relay worker publishes to Redis later.
    2. FIRE_AND_FORGET: Called without session → attempts Redis publish, 
       logs and continues on failure. Used for non-critical events.
    3. ATOMIC_OUTBOX: Writes to DB outbox only. Relay worker handles 
       Redis delivery only after successful DB commit.
"""

import asyncio
import traceback
from typing import Optional, TYPE_CHECKING
from loguru import logger
from pydantic import BaseModel

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

from core.events.nerve_center import bus


async def safe_publish(
    event: BaseModel,
    session: Optional["AsyncSession"] = None,
    *,
    dual_write: bool = True,
) -> bool:
    """
    Production-safe event publication with Transactional Outbox guarantee.
    
    Args:
        event: Pydantic event to publish.
        session: Optional active SQLAlchemy session. If provided, event is 
                 written to outbox_events within the ongoing transaction.
        dual_write: If True AND session is provided, also attempts immediate 
                    Redis publish for zero-latency delivery. Default: True.
    
    Returns:
        True if the event was successfully persisted/published.
        
    Behavior Matrix:
        session     → DB outbox only (atomic, relay handles Redis later)
        no session  → Redis only (best-effort, logs on failure)
    """
    channel = type(event).__name__
    
    # --- MODE 1: Transactional (DB-backed durability) ---
    if session is not None:
        try:
            from core.events.outbox_relay import write_outbox_event
            await write_outbox_event(session, event, channel=channel)
            return True
        except Exception as e:
            logger.error(
                f"❌ [SafePublish] Failed to write outbox event ({channel}): {e}"
            )
            return False
    
    # --- MODE 2: Fire-and-Forget (Redis only, non-critical) ---
    return await _try_redis_publish(event, channel, log_level="warning")


async def _try_redis_publish(
    event: BaseModel, 
    channel: str,
    log_level: str = "debug",
) -> bool:
    """
    Attempt Redis publish with graceful degradation.
    Enforces Stream-persistence for critical event types (Rule 16.5).
    """
    try:
        # Determine if this event type requires Stream-persistence (Delivery Guarantee)
        persistent_types = {
            "DetectorEvent", "SignalEvent", "ExecutionEvent", 
            "OrderUpdateEvent", "OrderExecutedEvent", 
            "PositionUpdateEvent", "EmergencyAlertEvent"
        }
        is_persistent = channel in persistent_types
        
        await bus.publish(event, persistent=is_persistent)
        return True
    except Exception as e:
        msg = (
            f"⚠️ [SafePublish] Redis publish failed for {channel}: "
            f"{type(e).__name__}: {e}"
        )
        if log_level == "warning":
            logger.warning(msg)
        else:
            logger.debug(msg)
        return False


async def safe_publish_critical(
    event: BaseModel,
    db_manager,
    *,
    channel: Optional[str] = None,
) -> bool:
    """
    Self-contained critical publish: Opens its own DB session for 
    maximum isolation. Use when you need guaranteed persistence but 
    don't have an existing transaction.
    
    This creates a micro-transaction:
        BEGIN → INSERT outbox_events → COMMIT → attempt Redis
    
    Args:
        event: Pydantic event to publish.
        db_manager: DatabaseManager instance.
        channel: Optional override for Redis channel name.
    """
    ch = channel or type(event).__name__
    
    try:
        from core.events.outbox_relay import write_outbox_event
        
        async with db_manager.SessionLocal() as session:
            await write_outbox_event(session, event, channel=ch)
            await session.commit()
        
        return True
        
    except Exception as e:
        logger.error(
            f"❌ [SafePublish] Critical publish FAILED for {ch}: {e}\n"
            f"{traceback.format_exc()}"
        )
        # Last resort: try Redis directly
        return await _try_redis_publish(event, ch, log_level="warning")
