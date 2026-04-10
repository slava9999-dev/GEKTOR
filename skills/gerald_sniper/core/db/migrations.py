# core/db/migrations.py
import aiosqlite
from datetime import datetime, timezone
from loguru import logger

CODE_SCHEMA_VERSION = 8

MIGRATIONS: dict[int, list[str]] = {
    1: [
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version    INTEGER PRIMARY KEY,
            applied_at TEXT    NOT NULL
        );
        """
    ],
    2: [
        """
        CREATE TABLE IF NOT EXISTS detected_levels (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol     TEXT    NOT NULL,
            timeframe  TEXT    NOT NULL DEFAULT '60',
            level_price REAL    NOT NULL,
            level_type  TEXT    NOT NULL,
            source     TEXT    NOT NULL DEFAULT '',
            touches    INTEGER NOT NULL DEFAULT 1,
            strength_score      REAL,
            confluence_score INTEGER NOT NULL DEFAULT 0,
            timestamp TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP,
            is_active INTEGER DEFAULT 1
        );
        """,
        "CREATE INDEX IF NOT EXISTS idx_dl_symbol ON detected_levels(symbol);",
        "CREATE INDEX IF NOT EXISTS idx_dl_created ON detected_levels(timestamp);"
    ],
    3: [
        """
        CREATE TABLE IF NOT EXISTS alert_log (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id   TEXT    NOT NULL UNIQUE,
            alert_type TEXT    NOT NULL,
            symbol     TEXT,
            level_id   TEXT,
            severity   TEXT    NOT NULL DEFAULT 'INFO',
            payload    TEXT,
            sent_at    TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    ],
    6: [
        """
        CREATE TABLE IF NOT EXISTS signal_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            symbol TEXT NOT NULL,
            state TEXT NOT NULL,
            entry_price REAL,
            exit_price REAL,
            pnl_usdt REAL,
            pnl_percent REAL,
            detectors TEXT,
            rejection_reason TEXT
        );
        """
    ],
    7: [
        """
        CREATE TABLE IF NOT EXISTS outbox (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT    NOT NULL,
            payload    TEXT    NOT NULL,
            status     TEXT    NOT NULL DEFAULT 'PENDING',
            created_at TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP,
            processed_at TEXT
        );
        """,
        "CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox(status);"
    ],
    8: [
        """
        CREATE TABLE IF NOT EXISTS monitoring_contexts (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id  TEXT    NOT NULL UNIQUE,
            symbol     TEXT    NOT NULL,
            state      TEXT    NOT NULL DEFAULT 'ACTIVE',
            context    TEXT    NOT NULL,
            created_at TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """,
        "CREATE INDEX IF NOT EXISTS idx_mc_symbol ON monitoring_contexts(symbol);",
        "CREATE INDEX IF NOT EXISTS idx_mc_state ON monitoring_contexts(state);"
    ]
}

async def ensure_schema(db_path: str) -> None:
    """ Asynchronous DB schema management using aiosqlite. """
    async with aiosqlite.connect(db_path) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version    INTEGER PRIMARY KEY,
                applied_at TEXT    NOT NULL
            );
        """)
        await db.commit()

        async with db.execute("SELECT COALESCE(MAX(version), 0) FROM schema_migrations") as cursor:
            row = await cursor.fetchone()
            current_version = row[0] if row else 0

        if current_version >= CODE_SCHEMA_VERSION:
            logger.info(f"💾 DB schema OK: v{current_version}")
            return

        logger.info(f"🔄 Applying migrations: {current_version} -> {CODE_SCHEMA_VERSION}")

        for v in range(current_version + 1, CODE_SCHEMA_VERSION + 1):
            stmts = MIGRATIONS.get(v, [])
            for stmt in stmts:
                try:
                    await db.execute(stmt)
                except aiosqlite.OperationalError as e:
                    err = str(e).lower()
                    if "already exists" in err or "duplicate column" in err:
                        logger.debug(f"Migration v{v} skip: {e}")
                    else:
                        raise
            
            await db.execute(
                "INSERT OR REPLACE INTO schema_migrations (version, applied_at) VALUES (?, ?);",
                (v, datetime.now(timezone.utc).isoformat())
            )
            await db.commit()
            logger.info(f"✅ Applied migration v{v}")

    logger.info(f"💾 DB schema up-to-date: v{CODE_SCHEMA_VERSION}")
