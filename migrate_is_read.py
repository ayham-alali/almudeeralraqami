import asyncio
import os
from db_helper import get_db, execute_sql, commit_db, DB_TYPE

async def migrate():
    print(f"Starting migration... (DB Status: {DB_TYPE})")
    
    async with get_db() as db:
        try:
            # Add is_read column
            # Check if column exists first (SQLite doesn't support IF NOT EXISTS for columns in all versions, but ADD COLUMN usually fails safely or we catch it)
            
            print("Adding is_read column...")
            try:
                if DB_TYPE == "postgresql":
                    await execute_sql(db, "ALTER TABLE inbox_messages ADD COLUMN IF NOT EXISTS is_read BOOLEAN DEFAULT FALSE")
                else:
                    await execute_sql(db, "ALTER TABLE inbox_messages ADD COLUMN is_read BOOLEAN DEFAULT 0")
                print("Column added.")
            except Exception as e:
                print(f"Column might already exist: {e}")

            # Backfill: Mark all 'approved', 'sent', 'auto_replied', 'ignored' as read
            print("Backfilling is_read for processed messages...")
            if DB_TYPE == "postgresql":
                await execute_sql(
                    db, 
                    "UPDATE inbox_messages SET is_read = TRUE WHERE status IN ('approved', 'sent', 'auto_replied', 'ignored')"
                )
            else:
                await execute_sql(
                    db, 
                    "UPDATE inbox_messages SET is_read = 1 WHERE status IN ('approved', 'sent', 'auto_replied', 'ignored')"
                )
            
            # For 'analyzed' messages (waiting for approval), let's keep them as unread (is_read=0)
            # so the user sees the badge until they click it.
            # However, if we want to be safe, we can assume old ones are read? 
            # No, user wants to see badges. Best to leave them as 0.
            
            await commit_db(db)
            print("Migration completed successfully.")
            
        except Exception as e:
            print(f"Migration failed: {e}")

if __name__ == "__main__":
    asyncio.run(migrate())
