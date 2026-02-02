
import asyncio
import asyncpg

# DB URL provided by user
DATABASE_URL = ""

async def debug_and_clean(conn):
    print("--- Debugging & Cleaning ---")
    
    # Target User ID 1633171858 (Message Count 524)
    target_id = '1633171858'
    target_id_int = 1633171858
    
    # Check count with String
    rows_str = await conn.fetch("SELECT id FROM inbox_messages WHERE sender_id = $1", target_id)
    print(f"Query by String '{target_id}': Found {len(rows_str)} messages.")
    
    # Check count with Int (if casting works or column is int)
    try:
        rows_int = await conn.fetch("SELECT id FROM inbox_messages WHERE sender_id = $1", str(target_id_int)) # Passed as string to be safe if column is text
        # If column is text, passing int param might fail or not match.
    except Exception as e:
        print(f"Query by Int failed: {e}")

    # FORCE DELETE
    if rows_str:
        ids = [r['id'] for r in rows_str]
        print(f"Attempting to delete {len(ids)} messages...")
        
        # 0. Customer Messages (FK dependency)
        await conn.execute("DELETE FROM customer_messages WHERE inbox_message_id = ANY($1::int[])", ids)

        # 1. Outbox
        await conn.execute("DELETE FROM outbox_messages WHERE inbox_message_id = ANY($1::int[])", ids)
        
        # 2. Inbox
        result = await conn.execute("DELETE FROM inbox_messages WHERE id = ANY($1::int[])", ids)
        print(f"Result: {result}")
        
    # Also check for 'sender_contact' duplicates for this user
    # Contact is +963952319392
    contact = "+963952319392"
    rows_contact = await conn.fetch("SELECT id FROM inbox_messages WHERE sender_contact = $1", contact)
    print(f"Query by Contact '{contact}': Found {len(rows_contact)} messages.")
    
    if rows_contact:
         # Check if these were already deleted (subset of above?)
         current_ids = [r['id'] for r in rows_contact]
         # Filter out already fetched ones if needed, but DELETE with ID list is idempotent safe
         if current_ids:
             print(f"Deleting {len(current_ids)} messages by contact match...")
             await conn.execute("DELETE FROM outbox_messages WHERE inbox_message_id = ANY($1::int[])", current_ids)
             result = await conn.execute("DELETE FROM inbox_messages WHERE id = ANY($1::int[])", current_ids)
             print(f"Result: {result}")

async def main():
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        await debug_and_clean(conn)
        await conn.close()
    except Exception as e:
        print(f"Fatal error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
