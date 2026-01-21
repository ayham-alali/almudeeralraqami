import asyncio
import asyncpg
import json

DATABASE_URL = ""

async def main():
    print("Connecting to database...")
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        print("Connected.")
        
        rows = await conn.fetch("""
            SELECT license_key_id, sender_name, sender_contact, message_count, channel
            FROM inbox_conversations
            WHERE sender_name LIKE '%أيهم%' OR sender_contact LIKE '%963968478904%'
        """)

        if not rows:
            print("No conversations found.")
        else:
            print(f"Found {len(rows)} conversation(s):")
            for r in rows:
                print(f"License: {r['license_key_id']}")
                print(f"Name: {r['sender_name']}")
                print(f"Contact (PK): '{r['sender_contact']}'")
                print(f"Count: {r['message_count']}")
                print("-" * 20)
                
        await conn.close()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
