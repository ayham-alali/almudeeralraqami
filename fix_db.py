import sqlite3
import os

db_path = 'almudeer.db'
if not os.path.exists(db_path):
    print(f"Error: {db_path} not found")
    exit(1)

conn = sqlite3.connect(db_path)
try:
    # 1. Ensure alembic_version table exists and is set to 002
    conn.execute('CREATE TABLE IF NOT EXISTS alembic_version (version_num VARCHAR(32) PRIMARY KEY)')
    conn.execute('DELETE FROM alembic_version')
    conn.execute("INSERT INTO alembic_version VALUES ('002_fix_customers_id')")
    
    # 2. Add columns to inbox_messages if missing
    cursor = conn.cursor()
    cursor.execute('PRAGMA table_info(inbox_messages)')
    cols = [c[1] for c in cursor.fetchall()]
    
    if 'deleted_at' not in cols:
        conn.execute('ALTER TABLE inbox_messages ADD COLUMN deleted_at TIMESTAMP')
        print("Added deleted_at to inbox_messages")

    # 3. Add columns to outbox_messages if missing
    cursor.execute('PRAGMA table_info(outbox_messages)')
    cols = [c[1] for c in cursor.fetchall()]
    
    if 'deleted_at' not in cols:
        conn.execute('ALTER TABLE outbox_messages ADD COLUMN deleted_at TIMESTAMP')
        print("Added deleted_at to outbox_messages")

    conn.commit()
    print("Database preparation successful")
except Exception as e:
    print(f"Error: {e}")
    conn.rollback()
finally:
    conn.close()
