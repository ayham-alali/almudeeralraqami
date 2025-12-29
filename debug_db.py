
import asyncio
import os
from datetime import datetime

# Mock env vars
os.environ["DB_TYPE"] = "sqlite"
os.environ["DATABASE_PATH"] = "almudeer.db"

from db_helper import get_db, execute_sql, fetch_one, commit_db

async def debug_lifecycle():
    print(f"DEBUG: Testing Telegram Config Lifecycle in SQLite")
    
    # Use a dummy license ID
    license_id = 99999
    
    async with get_db() as db:
        # 1. CLEANUP
        await execute_sql(db, "DELETE FROM telegram_configs WHERE license_key_id = ?", [license_id])
        await execute_sql(db, "DELETE FROM license_keys WHERE id = ?", [license_id])
        
        # Create dummy license key
        await execute_sql(db, "INSERT INTO license_keys (id, key_hash, company_name) VALUES (?, ?, ?)", [license_id, "debug_hash", "Debug Co"])
        await commit_db(db)
        
        # 2. INSERT (Active)
        print("DEBUG: Inserting active config...")
        await execute_sql(
            db,
            """
            INSERT INTO telegram_configs (license_key_id, bot_token, is_active)
            VALUES (?, ?, ?)
            """,
            [license_id, "123:token", True] # Passing True (Python boolean)
        )
        await commit_db(db)
        
        # 3. VERIFY ACTIVE
        row = await fetch_one(db, "SELECT * FROM telegram_configs WHERE license_key_id = ?", [license_id])
        print(f"DEBUG: Inserted Row: is_active={row['is_active']} (type: {type(row['is_active'])})")
        
        # Test query from model
        is_active_value = "1" # Hardcoded for SQLite as per model logic
        query = f"SELECT * FROM telegram_configs WHERE license_key_id = ? AND is_active = {is_active_value}"
        result = await fetch_one(db, query, [license_id])
        print(f"DEBUG: get_telegram_config(active=True) result: {'FOUND' if result else 'NOT FOUND'}")
        
        if not result:
            print("ERROR: Could not find active config just inserted!")
            return

        # 4. UPDATE (Deactivate)
        print("DEBUG: Deactivating...")
        # Simulating the fix I applied
        await execute_sql(
            db, 
            "UPDATE telegram_configs SET is_active = ? WHERE license_key_id = ?", 
            [False, license_id]
        )
        await commit_db(db)
        
        # 5. VERIFY DEACTIVATED
        row = await fetch_one(db, "SELECT * FROM telegram_configs WHERE license_key_id = ?", [license_id])
        print(f"DEBUG: Updated Row: is_active={row['is_active']} (type: {type(row['is_active'])})")
        
        result = await fetch_one(db, query, [license_id])
        print(f"DEBUG: get_telegram_config(active=True) result: {'FOUND' if result else 'NOT FOUND'}")
        
        if result:
            print("FAILURE: Found config even though it should be inactive!")
        else:
            print("SUCCESS: Config correctly filtered out.")
            
        # Cleanup
        await execute_sql(db, "DELETE FROM telegram_configs WHERE license_key_id = ?", [license_id])
        await execute_sql(db, "DELETE FROM license_keys WHERE id = ?", [license_id])
        await commit_db(db)

if __name__ == "__main__":
    asyncio.run(debug_lifecycle())
