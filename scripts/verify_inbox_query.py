
import asyncio
import os
import sys

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from db_pool import db_pool
from models.inbox import get_inbox_conversations
from db_helper import execute_sql, get_db, commit_db, DB_TYPE

async def main():
    print("Initialize DB Pool...")
    await db_pool.initialize()

    license_id = 999999
    
    print("Testing get_inbox_conversations...")
    try:
        # Just running the query is enough to verify syntax
        conversations = await get_inbox_conversations(license_id)
        print(f"SUCCESS: Retrieved {len(conversations)} conversations.")
    except Exception as e:
        print(f"FAILURE: get_inbox_conversations raised error: {e}")
        sys.exit(1)
        
    await db_pool.close()

if __name__ == "__main__":
    asyncio.run(main())
