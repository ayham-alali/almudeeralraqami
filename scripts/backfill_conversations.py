import asyncio
import os
import sys

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from db_pool import db_pool
from models.inbox import upsert_conversation_state
from db_helper import get_db, fetch_all

async def main():
    print("Initialize DB Pool...")
    await db_pool.initialize()

    print("Fetching all unique sender contacts...")
    
    async with get_db() as db:
        # Get all unique (license_id, sender_contact) pairs
        rows = await fetch_all(db, """
            SELECT DISTINCT license_key_id, sender_contact, sender_name, channel
            FROM inbox_messages
            WHERE sender_contact IS NOT NULL
        """)
        
    total = len(rows)
    print(f"Found {total} conversations to backfill.")
    
    count = 0
    for row in rows:
        lid = row["license_key_id"]
        contact = row["sender_contact"]
        name = row["sender_name"]
        channel = row["channel"]
        
        try:
            await upsert_conversation_state(lid, contact, name, channel)
            count += 1
            if count % 100 == 0:
                print(f"Processed {count}/{total}...")
        except Exception as e:
            print(f"Error processing {contact} for license {lid}: {e}")
            
    print(f"Backfill complete! Processed {count} conversations.")
    await db_pool.close()

if __name__ == "__main__":
    asyncio.run(main())
