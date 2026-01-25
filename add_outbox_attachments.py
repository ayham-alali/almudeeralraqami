
import asyncio
import os
from db_helper import get_db, execute_sql, commit_db, DB_TYPE

async def run_fix():
    print(f"Connecting to database (Type: {DB_TYPE})...")
    async with get_db() as db:
        try:
            print("Checking/Adding 'attachments' column to 'outbox_messages'...")
            if DB_TYPE == "postgresql":
                 await execute_sql(db, "ALTER TABLE outbox_messages ADD COLUMN IF NOT EXISTS attachments TEXT;")
            else:
                 try:
                    await execute_sql(db, "ALTER TABLE outbox_messages ADD COLUMN attachments TEXT;")
                 except Exception as e:
                    if "duplicate column" in str(e).lower():
                        print("Column attachments already exists (SQLite).")
                    else:
                        raise e
            
            await commit_db(db)
            print("SUCCESS: 'attachments' column check/add completed.")
        except Exception as e:
            print(f"ERROR: {e}")

if __name__ == "__main__":
    asyncio.run(run_fix())
