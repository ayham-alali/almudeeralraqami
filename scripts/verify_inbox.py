import asyncio
import os
import sys

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from db_pool import db_pool
from models.inbox import get_inbox_conversations
from db_helper import get_db, fetch_one, fetch_all

async def main():
    print("Initialize DB Pool...")
    await db_pool.initialize()

    license_id = 4 
    # Let's try to find a license with data first
    async with get_db() as db:
        row = await fetch_one(db, "SELECT DISTINCT license_key_id FROM inbox_messages LIMIT 1")
        if row:
            # Check row format (dict or tuple)
            if isinstance(row, dict):
                license_id = row["license_key_id"]
            else:
                license_id = row[0]
            print(f"Using License ID: {license_id}")

    print(f"\n--- Testing get_inbox_conversations for License {license_id} ---")
    try:
        conversations = await get_inbox_conversations(license_id, limit=5)
        
        print(f"Found {len(conversations)} conversations.")
        for conv in conversations:
            print(f"  - Contact: {conv['sender_contact']}")
            print(f"    Name: {conv['sender_name']}")
            print(f"    Last Msg: {str(conv['body'])[:50]}...")
            print(f"    Status: {conv['status']}")
            print(f"    Unread: {conv['unread_count']}")
            print(f"    Count: {conv['message_count']}")
            print("    ---")
    except Exception as e:
        print(f"Error fetching conversations: {e}")
        import traceback
        traceback.print_exc()
        
    await db_pool.close()

if __name__ == "__main__":
    asyncio.run(main())
