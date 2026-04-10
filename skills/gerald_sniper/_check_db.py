import sqlite3

conn = sqlite3.connect("./data_run/sniper.db")

tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
print("Tables:", [t[0] for t in tables])
print()

try:
    cols = conn.execute("PRAGMA table_info(detected_levels)").fetchall()
    print("detected_levels columns:")
    for c in cols:
        print(f"  {c[0]}: {c[1]} ({c[2]}) default={c[4]}")
except Exception as e:
    print(f"No detected_levels table: {e}")

print()
try:
    versions = conn.execute("SELECT * FROM schema_migrations ORDER BY version").fetchall()
    print("schema_migrations:")
    for v in versions:
        print(f"  v{v[0]} applied={v[1]}")
except Exception as e:
    print(f"No schema_migrations: {e}")

conn.close()
