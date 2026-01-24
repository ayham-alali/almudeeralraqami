import asyncio
import os
import aiosqlite
from db_helper import get_db, fetch_one, DB_TYPE

async def verify_schema():
    print(f"Checking database type: {DB_TYPE}")
    
    missing_cols = [
        'reply_to_platform_id',
        'reply_to_body_preview',
        'reply_to_sender_name',
        'reply_to_id',
        'platform_message_id',
        'delivery_status',
        'original_sender'
    ]
    
    async with get_db() as db:
        if DB_TYPE == "postgresql":
            from database import DATABASE_URL
            import asyncpg
            conn = await asyncpg.connect(DATABASE_URL)
            try:
                print("Checking PostgreSQL schema...")
                for col in missing_cols:
                    res = await conn.fetchval(f"SELECT column_name FROM information_schema.columns WHERE table_name='outbox_messages' AND column_name='{col}'")
                    if res:
                        print(f" [OK] Column {col} exists")
                    else:
                        print(f" [FAIL] Column {col} MISSING")
            finally:
                await conn.close()
        else:
            print("Checking SQLite schema...")
            db.row_factory = aiosqlite.Row
            async with db.execute("PRAGMA table_info(outbox_messages)") as cursor:
                rows = await cursor.fetchall()
                existing_cols = [row['name'] for row in rows]
                for col in missing_cols:
                    if col in existing_cols:
                        print(f" [OK] Column {col} exists")
                    else:
                        print(f" [FAIL] Column {col} MISSING")

if __name__ == "__main__":
    asyncio.run(verify_schema())
