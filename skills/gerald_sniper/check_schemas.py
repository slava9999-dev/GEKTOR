import sqlite3

for db_name in ['sniper.db', 'outbox.db']:
    print(f"--- SCHEMA FOR {db_name} ---")
    try:
        conn = sqlite3.connect(f"data_run/{db_name}")
        c = conn.cursor()
        c.execute("SELECT sql FROM sqlite_master WHERE type='table';")
        for row in c.fetchall():
            if row[0]:
                print(row[0])
        conn.close()
    except Exception as e:
        print(f"Error reading {db_name}: {e}")
    print("\n")
