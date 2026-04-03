import sqlite3
import os

db_path = r"skills/gerald-sniper/data_run/sniper.db"
if not os.path.exists(db_path):
    print(f"DB not found at {db_path}")
    exit(0)

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

try:
    print("Migrating detected_levels table...")
    cursor.execute("ALTER TABLE detected_levels ADD COLUMN source TEXT DEFAULT 'KDE'")
    conn.commit()
    print("Column 'source' added successully.")
except sqlite3.OperationalError as e:
    if "duplicate column name" in str(e).lower():
        print("Column 'source' already exists.")
    else:
        print(f"Operational error: {e}")
except Exception as e:
    print(f"Error during migration: {e}")
finally:
    conn.close()
