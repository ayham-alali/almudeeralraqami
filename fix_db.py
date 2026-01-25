
import asyncio
import os
from db_helper import get_db, execute_sql, commit_db, DB_TYPE

async def run_fix():
    print(f"Connecting to database (Type: {DB_TYPE})...")
    async with get_db() as db:
        try:
            print("Attempting to add 'last_message_ai_summary' column to 'inbox_conversations'...")
            # For PostgreSQL, we check if column exists first or use a safer approach
            if DB_TYPE == "postgresql":
                 await execute_sql(db, "ALTER TABLE inbox_conversations ADD COLUMN IF NOT EXISTS last_message_ai_summary TEXT;")
            else:
                 # SQLite doesn't support IF NOT EXISTS in ALTER TABLE easily, but we already did it locally
                 await execute_sql(db, "ALTER TABLE inbox_conversations ADD COLUMN last_message_ai_summary TEXT;")
            
            await commit_db(db)
            print("SUCCESS: Column added.")
        except Exception as e:
            if "already exists" in str(e).lower() or "duplicate column name" in str(e).lower():
                print("NOTE: Column already exists.")
            else:
                print(f"ERROR: {e}")

if __name__ == "__main__":
    asyncio.run(run_fix())
