
import asyncio
import asyncpg
import sys

# DB URL provided by user
DATABASE_URL = ""

async def inspect_telegram(conn, f):
    f.write("--- Inspecting Telegram Messages ---\n")
    # Get all distinct senders for Telegram
    rows = await conn.fetch("""
        SELECT 
            sender_id, 
            sender_contact, 
            sender_name, 
            COUNT(*) as msg_count 
        FROM inbox_messages 
        WHERE channel = 'telegram' 
        GROUP BY sender_id, sender_contact, sender_name
        ORDER BY msg_count DESC
        LIMIT 50
    """)
    
    f.write(f"{'Count':<5} | {'ID':<20} | {'Contact':<20} | {'Name'}\n")
    f.write("-" * 80 + "\n")
    for row in rows:
        f.write(f"{row['msg_count']:<5} | {str(row['sender_id']):<20} | {str(row['sender_contact']):<20} | {row['sender_name']}\n")

    # Also check active session IDs to compare
    sessions = await conn.fetch("SELECT license_key_id, user_id, phone_number FROM telegram_phone_sessions")
    f.write("\n--- Active Sessions ---\n")
    for s in sessions:
        f.write(f"License: {s['license_key_id']}, User ID: {s['user_id']}, Phone: {s['phone_number']}\n")

async def main():
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        with open("inspect_result.txt", "w", encoding="utf-8") as f:
             await inspect_telegram(conn, f)
        await conn.close()
        print("Inspection done.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
