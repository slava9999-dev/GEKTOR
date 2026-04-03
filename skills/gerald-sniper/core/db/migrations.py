import aiosqlite
from datetime import datetime, timezone
from loguru import logger

CODE_SCHEMA_VERSION = 6

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
            level      REAL    NOT NULL,
            side       TEXT    NOT NULL,
            source     TEXT    NOT NULL DEFAULT '',
            touches    INTEGER NOT NULL DEFAULT 1,
            score      REAL,
            confluence INTEGER NOT NULL DEFAULT 0,
            created_at TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_dl_symbol ON detected_levels(symbol);
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_dl_created ON detected_levels(created_at);
        """
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
    4: [
        "ALTER TABLE detected_levels RENAME COLUMN level TO level_price;",
        "ALTER TABLE detected_levels RENAME COLUMN side TO level_type;",
        "ALTER TABLE detected_levels RENAME COLUMN score TO strength_score;",
        "ALTER TABLE detected_levels RENAME COLUMN confluence TO confluence_score;",
        "ALTER TABLE detected_levels RENAME COLUMN created_at TO timestamp;",
        "ALTER TABLE detected_levels ADD COLUMN is_active INTEGER DEFAULT 1;"
    ],
    5: [
        "ALTER TABLE detected_levels ADD COLUMN confluence_score INTEGER DEFAULT 0;"
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
    ]
}

async def ensure_schema(db_path: str) -> None:
    """
    Asynchronous DB schema management using aiosqlite.
    Matches Digash Sprints for versioning.
    """
    async with aiosqlite.connect(db_path) as db:
        # Create migrations table
        await db.execute("""
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version    INTEGER PRIMARY KEY,
                applied_at TEXT    NOT NULL
            );
        """)
        await db.commit()

        # Get current version
        async with db.execute("SELECT COALESCE(MAX(version), 0) FROM schema_migrations") as cursor:
            row = await cursor.fetchone()
            current_version = row[0] if row else 0

        if current_version >= CODE_SCHEMA_VERSION:
            logger.info(f"💾 DB schema OK: version={current_version}")
            return

        logger.info(f"🔄 Applying DB migrations: {current_version} -> {CODE_SCHEMA_VERSION}")

        for v in range(current_version + 1, CODE_SCHEMA_VERSION + 1):
            stmts = MIGRATIONS.get(v, [])
            for stmt in stmts:
                try:
                    # Specific column check for v4 renaming
                    if "RENAME COLUMN" in stmt:
                        table = stmt.split("ALTER TABLE ")[1].split(" ")[0]
                        old_col = stmt.split("RENAME COLUMN ")[1].split(" TO ")[0]
                        new_col = stmt.split(" TO ")[1].replace(";", "")
                        async with db.execute(f"PRAGMA table_info({table})") as cursor:
                            cols = [row[1] for row in await cursor.fetchall()]
                            if old_col not in cols:
                                logger.debug(f"Migration v{v} skip (column {old_col} missing/already renamed): {stmt}")
                                continue
                            if new_col in cols:
                                logger.debug(f"Migration v{v} skip (column {new_col} already exists): {stmt}")
                                continue

                    await db.execute(stmt)
                except aiosqlite.OperationalError as e:
                    # Status messages to handle already existing indexes or columns
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
            logger.info(f"✅ DB migration applied: v{v}")

    logger.info(f"💾 DB schema up-to-date: version={CODE_SCHEMA_VERSION}")
