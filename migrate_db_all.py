import sqlite3
import os

db_paths = [
    r"data_run/sniper.db",
    r"skills/gerald-sniper/data_run/sniper.db"
]

for db_path in db_paths:
    if not os.path.exists(db_path):
        print(f"DB not found at {db_path}, skipping.")
        continue
    
    print(f"Migrating {db_path}...")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # Check if column exists first
        cursor.execute("PRAGMA table_info(detected_levels)")
        columns = [info[1] for info in cursor.fetchall()]
        
        if "source" not in columns:
            cursor.execute("ALTER TABLE detected_levels ADD COLUMN source TEXT DEFAULT 'KDE'")
            conn.commit()
            print(f"  Column 'source' added successfully.")
        else:
            print(f"  Column 'source' already exists.")
            
    except Exception as e:
        print(f"  Error migrating {db_path}: {e}")
    finally:
        conn.close()
