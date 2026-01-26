"""
Reproduction script: Verify BIGINT fix for color column
"""

import os
import asyncio
import asyncpg
import uuid


async def test_large_color():
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
            
        # 1. Ensure table exists (optional, handled by app, but good for standalone test)
        # 2. Get a valid license_key_id
        license_id = await conn.fetchval("SELECT id FROM license_keys LIMIT 1")
        if not license_id:
            print("ERROR: No license keys found in database. Please create one.")
            await conn.close()
            return

        # 3. Attempt to insert a task with a large color value
        task_id = str(uuid.uuid4())
        large_color = 4280391411 # The value from the error log
        
        print(f"Attempting to insert task with color {large_color}...")
        try:
            await conn.execute("""
                INSERT INTO tasks (id, license_key_id, title, color)
                VALUES ($1, $2, $3, $4)
            """, task_id, license_id, "Test Int32 Fix", large_color)
            
            print("SUCCESS: Task inserted with large color value!")
            
            # 4. Clean up
            await conn.execute("DELETE FROM tasks WHERE id = $1", task_id)
            print("Cleaned up test task.")
            
        except asyncpg.exceptions.NumericValueOutOfRangeError as e:
            print(f"FAILED: Still getting range error: {e}")
        except Exception as e:
            print(f"ERROR during insertion: {e}")
            
        await conn.close()
    except Exception as e:
        print(f"ERROR: {e}")


if __name__ == "__main__":
    asyncio.run(test_large_color())
