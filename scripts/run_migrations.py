import asyncio
import os
import sys

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from migrations.manager import migration_manager
from migrations.inbox_conversations_table import migrate as migrate_inbox_conversations

async def main():
    print("Running migrations...")
    
    # Run standard SQL migrations
    await migration_manager.migrate()
    
    # Run Python-based migrations
    print("Running inbox_conversations migration...")
    try:
        await migrate_inbox_conversations()
        print("✅ Inbox conversations migration complete.")
    except Exception as e:
        print(f"❌ Inbox conversations migration failed: {e}")
        
    print("All migrations complete.")

if __name__ == "__main__":
    asyncio.run(main())
