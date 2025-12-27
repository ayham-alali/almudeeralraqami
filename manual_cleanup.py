
import asyncio
import os
import sys

# Add backend directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

from db_pool import db_pool
from models.inbox import fix_stale_inbox_status

async def main():
    print("Initializing database...")
    await db_pool.initialize()
    
    print("Running cleanup task for ALL conversations...")
    try:
        # Run with license_id=None to clean ALL records
        await fix_stale_inbox_status(None)
        print("Cleanup completed successfully!")
    except Exception as e:
        print(f"Error during cleanup: {e}")
    finally:
        await db_pool.close()

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
