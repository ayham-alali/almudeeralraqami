
import asyncio
import os
import asyncpg
import httpx

# DB URL provided by user
DATABASE_URL = ""

async def delete_messages_by_ids(conn, inbox_ids):
    if not inbox_ids:
        return 0
        
    # 0. Customer Messages (FK dependency)
    await conn.execute(
        "DELETE FROM customer_messages WHERE inbox_message_id = ANY($1::int[])",
        inbox_ids
    )

    # 1. Delete dependent outbox messages
    await conn.execute(
        "DELETE FROM outbox_messages WHERE inbox_message_id = ANY($1::int[])",
        inbox_ids
    )
    
    # 2. Delete inbox messages
    result = await conn.execute(
        "DELETE FROM inbox_messages WHERE id = ANY($1::int[])",
        inbox_ids
    )
    
    try:
        return int(result.split(" ")[1])
    except:
        return 0

async def cleanup_channels(conn):
    print("\n--- Cleaning up Channels (RoyaRaqamia) ---")
    # Identify messages from the channel
    rows = await conn.fetch("""
        SELECT id FROM inbox_messages 
        WHERE sender_name LIKE '%رؤية رقمية%' 
           OR sender_contact = 'RoyaRaqamiaAdmin'
    """)
    
    ids = [row['id'] for row in rows]
    if ids:
        deleted = await delete_messages_by_ids(conn, ids)
        print(f"Deleted {deleted} channel messages.")
    else:
        print("No channel messages found.")

async def cleanup_conversations_table(conn):
    print("\n--- Cleaning up inbox_conversations Table ---")
    # If inbox_conversations is a materialized view or table, we might need to delete from it manually
    # if triggers aren't set up.
    # We will try to delete any conversation that has 0 messages (if dependent on inbox_messages)
    # OR explicitly delete conversations matching our targets.
    
    # Check if table exists and is not a view (simple check by trying delete)
    try:
        # Delete conversations for "Saved Messages" style contacts (User IDs)
        # We need to find the user_ids again
        sessions = await conn.fetch("SELECT user_id FROM telegram_phone_sessions WHERE is_active = TRUE AND user_id IS NOT NULL")
        user_ids = [str(s['user_id']) for s in sessions]
        
        # Also 'RoyaRaqamiaAdmin'
        targets = user_ids + ['RoyaRaqamiaAdmin']
        
        # Delete by sender_id or contact? inbox_conversations usually has sender_contact
        # Let's try deleting by sender_contact matching these IDs OR specific names
        
        # 1. By Sender Contact (User IDs often stored as contact for saved messages if no phone)
        if targets:
            await conn.execute("DELETE FROM inbox_conversations WHERE sender_contact = ANY($1::text[])", targets)
            
        # 2. By Sender Name
        await conn.execute("DELETE FROM inbox_conversations WHERE sender_name LIKE '%رؤية رقمية%' OR sender_name LIKE '%None%'")
        
        print("Explicitly cleaned inbox_conversations table.")
        
    except Exception as e:
        print(f"Skipping inbox_conversations cleanup (might be a view or error): {e}")

async def cleanup_telegram(conn):
    print("--- Cleaning up Telegram Saved Messages ---")
    sessions = await conn.fetch("SELECT license_key_id, user_id FROM telegram_phone_sessions WHERE is_active = TRUE AND user_id IS NOT NULL")
    
    count = 0
    for session in sessions:
        lic_id = session['license_key_id']
        user_id = session['user_id']
        
        # Find IDs
        rows = await conn.fetch(
            """
            SELECT id FROM inbox_messages 
            WHERE license_key_id = $1 
              AND channel = 'telegram' 
              AND sender_id = $2
            """,
            lic_id, str(user_id)
        )
        
        ids = [row['id'] for row in rows]
        if ids:
            deleted = await delete_messages_by_ids(conn, ids)
            print(f"License {lic_id}: Deleted {deleted} Telegram self-messages (user_id={user_id})")
            count += deleted
            
    print(f"Total Telegram messages cleaned: {count}")

async def cleanup_whatsapp(conn):
    print("\n--- Cleaning up WhatsApp Saved Messages ---")
    configs = await conn.fetch("SELECT license_key_id, phone_number_id, access_token FROM whatsapp_configs WHERE is_active = TRUE")
    
    deleted_total = 0
    
    async with httpx.AsyncClient() as client:
        for config in configs:
            lic_id = config['license_key_id']
            pid = config['phone_number_id']
            token = config['access_token']
            
            phone_number = None
            try:
                resp = await client.get(
                    f"https://graph.facebook.com/v18.0/{pid}",
                    headers={"Authorization": f"Bearer {token}"}
                )
                if resp.status_code == 200:
                    data = resp.json()
                    display_phone = data.get("display_phone_number")
                    if display_phone:
                        phone_number = "".join(filter(str.isdigit, display_phone))
            except Exception as e:
                print(f"Error fetching WhatsApp phone for license {lic_id}: {e}")
                continue
            
            if not phone_number:
                continue
                
            variations = [phone_number, f"+{phone_number}", phone_number.lstrip("0")]
            
            # Find IDs
            rows = await conn.fetch(
                """
                SELECT id FROM inbox_messages
                WHERE license_key_id = $1
                  AND channel = 'whatsapp'
                  AND sender_id = ANY($2::text[])
                """,
                lic_id, variations
            )
            
            ids = [row['id'] for row in rows]
            if ids:
                deleted = await delete_messages_by_ids(conn, ids)
                print(f"License {lic_id}: Deleted {deleted} WhatsApp self-messages (phone={phone_number})")
                deleted_total += deleted
                
    print(f"Total WhatsApp messages cleaned: {deleted_total}")

async def main():
    print(f"Connecting to {DATABASE_URL}...")
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        print("Connected.")
        
        await cleanup_telegram(conn)
        await cleanup_whatsapp(conn)
        await cleanup_channels(conn)
        await cleanup_conversations_table(conn)
        
        await conn.close()
        print("\nCleanup complete.")
    except Exception as e:
        print(f"Fatal error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
