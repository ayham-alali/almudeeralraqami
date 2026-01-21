import asyncio
import asyncpg
import os

DATABASE_URL = ""

async def main():
    print("Connecting to database...")
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        print("Connected.")
        
        print("Creating index on inbox_messages(license_key_id, sender_id)...")
        # Use concurrent index creation if possible to avoid locking (Postgres specific)
        # But for this size/tool context, standard create is likely fine or we just risk a small lock.
        # IF NOT EXISTS handles idempotency.
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_inbox_sender_lookup 
            ON inbox_messages(license_key_id, sender_id)
        """)
        print("Index verified/created.")
        
        await conn.close()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
