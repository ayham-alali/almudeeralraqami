
import asyncio
import os
import sys

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from db_pool import db_pool
from migrations.fts_setup import setup_full_text_search
from models.inbox import save_inbox_message, create_outbox_message, search_messages, approve_outbox_message
from db_helper import execute_sql, get_db, commit_db, DB_TYPE

async def main():
    print("Initialize DB Pool...")
    await db_pool.initialize()

    print("Running FTS Setup...")
    await setup_full_text_search()
    
    # Test Data
    license_id = 999999
    
    print("Seed Data...")
    async with get_db() as db:
        # Ensure license exists for FK constraints
        try:
            if DB_TYPE == "postgresql":
                await execute_sql(db, """
                    INSERT INTO license_keys (id, key, company_name, contact_email, is_active, max_requests_per_day, requests_today, created_at, expires_at)
                    VALUES ($1, 'test_key', 'Test Company', 'test@test.com', true, 1000, 0, NOW(), NOW() + INTERVAL '1 year')
                    ON CONFLICT (id) DO NOTHING
                """, [license_id])
            else:
                await execute_sql(db, """
                    INSERT OR IGNORE INTO license_keys (id, key, company_name, contact_email, is_active, max_requests_per_day, requests_today, created_at, expires_at)
                    VALUES (?, 'test_key', 'Test Company', 'test@test.com', 1, 1000, 0, datetime('now'), datetime('now', '+1 year'))
                """, [license_id])
        except Exception as e:
            print(f"Warning inserting license: {e}")

        # Cleanup test data
        if DB_TYPE != "postgresql":
            await execute_sql(db, "DELETE FROM messages_fts WHERE license_id = ?", [license_id])
        
        await execute_sql(db, "DELETE FROM outbox_messages WHERE license_key_id = ?", [license_id])
        await execute_sql(db, "DELETE FROM inbox_messages WHERE license_key_id = ?", [license_id])
        await commit_db(db)
        
    # Insert Inbox Message (English)
    await save_inbox_message(
        license_id=license_id,
        channel="whatsapp",
        body="Hello world, this is a test message for FTS.",
        sender_name="John Doe",
        sender_contact="123456789",
        sender_id="john_doe_id"
    )
    
    # Insert Outbox Message (Arabic)
    outbox_id = await create_outbox_message(
        inbox_message_id=0,
        license_id=license_id,
        channel="whatsapp",
        body="مرحبا بك في نظام المدير الذكي",
        recipient_id="john_doe_id",
        recipient_email="123456789",
        attachments=None
    )
    # Approve it to make it searchable? 
    # Actually FTS trigger is on INSERT/UPDATE. create_outbox_message does INSERT.
    # But usually creates with status 'pending'.
    
    print("Waiting for data consistency (sqlite immediate usually)...")
    await asyncio.sleep(1)
    
    print("\n--- Testing Search 'world' ---")
    res = await search_messages(license_id, "world")
    print(f"Count: {res['count']}")
    for m in res['results']:
        print(f"Result: {m['body']} ({m['type']})")
        
    print("\n--- Testing Search 'المدير' ---")
    res = await search_messages(license_id, "المدير")
    print(f"Count: {res['count']}")
    for m in res['results']:
        print(f"Result: {m['body']} ({m['type']})")

    print("\n--- Testing Search Filter (sender_contact='123456789') ---")
    # Both messages linked to 123456789 (one as sender, one as recipient target)
    res = await search_messages(license_id, "test", sender_contact="123456789")
    print(f"Count: {res['count']}")
    
    res = await search_messages(license_id, "test", sender_contact="987654321") # Wrong contact
    print(f"Count (Should be 0): {res['count']}")

    print("\nDone.")
    await db_pool.close()

if __name__ == "__main__":
    asyncio.run(main())
