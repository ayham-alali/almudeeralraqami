
import asyncio
import asyncpg
import sys

# DB URL provided by user
DATABASE_URL = ""

async def inspect(conn, f):
    f.write("--- Finding 'رؤية رقمية' ---\n")
    rows = await conn.fetch("""
        SELECT id, sender_id, sender_name, sender_contact, channel, body 
        FROM inbox_messages 
        WHERE sender_name LIKE '%رؤية رقمية%'
        ORDER BY id DESC
        LIMIT 20
    """)
    f.write(f"{'ID':<10} | {'Sender ID':<15} | {'Name':<20} | {'Contact':<20} | {'Body'}\n")
    for r in rows:
         f.write(f"{r['id']:<10} | {str(r['sender_id']):<15} | {r['sender_name']:<20} | {str(r['sender_contact']):<20} | {r['body'][:30]}\n")
    
    f.write("\n--- Checking 'inbox_conversations' Table/View ---\n")
    # Check if it has entries for the deleted ID (1633171858)
    try:
        rows_conv = await conn.fetch("""
            SELECT * FROM inbox_conversations 
            WHERE sender_id = '1633171858' OR sender_name LIKE '%رؤية رقمية%'
        """)
        f.write(f"Found {len(rows_conv)} entries in inbox_conversations.\n")
        for r in rows_conv:
            f.write(f"Conv ID: {r.get('id')}, Sender: {r.get('sender_name')}, Count: {r.get('message_count')}\n")
    except Exception as e:
        f.write(f"Error checking inbox_conversations: {e}\n")

async def main():
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        with open("inspect_channels.txt", "w", encoding="utf-8") as f:
             await inspect(conn, f)
        await conn.close()
        print("Inspection done.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
