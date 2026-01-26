"""
One-time migration script: Update tasks.color type to BIGINT
"""

import os
import asyncio
import asyncpg


async def run_migration():
    database_url = os.getenv("DATABASE_URL")
    
    if not database_url:
        print("ERROR: DATABASE_URL not set")
        return
    
    print("Connecting to PostgreSQL...")
    try:
        # Try with SSL first
        try:
            conn = await asyncpg.connect(database_url, ssl='require')
        except:
            conn = await asyncpg.connect(database_url)
            
        print("Altering tasks table...")
        await conn.execute("ALTER TABLE tasks ALTER COLUMN color TYPE BIGINT")
        print("SUCCESS: tasks.color updated to BIGINT")
        
        await conn.close()
    except Exception as e:
        print(f"ERROR: {e}")


if __name__ == "__main__":
    asyncio.run(run_migration())
