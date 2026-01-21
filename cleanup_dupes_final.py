import asyncio
import asyncpg
import json

DATABASE_URL = ""

async def main():
    print("Connecting to database...")
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        print("Connected.")
        
        # 1. Detect users with multiple identities (Same License & Sender ID, Different Contact)
        # We only care about rows where sender_id is set (Telegram/WhatsApp etc)
        print("Scanning for Identity Conflicts (Same Sender ID, Multiple Contacts)...")
        
        rows = await conn.fetch("""
            SELECT license_key_id, sender_id, array_agg(DISTINCT sender_contact) as contacts
            FROM inbox_messages
            WHERE sender_id IS NOT NULL AND sender_contact IS NOT NULL AND channel = 'telegram'
            GROUP BY license_key_id, sender_id
            HAVING count(DISTINCT sender_contact) > 1
        """)
        
        if not rows:
            print("No identity conflicts found.")
        
        for r in rows:
            license_id = r['license_key_id']
            sender_id = r['sender_id']
            contacts = r['contacts']
            
            print(f"\nConflict Found: License {license_id}, Sender ID {sender_id}")
            print(f"  Contacts: {contacts}")
            
            # Determine Canonical Contact
            # Priority: 
            # 1. Starts with '+' and rest is digits (Standard Phone)
            # 2. Is digits (Phone without +) -> we should normalize it theoretically, but here just pick best existing.
            # 3. Anything else (Username)
            
            canonical = None
            
            # Filter for pure phone numbers with +
            phones_plus = [c for c in contacts if c.startswith('+') and c[1:].isdigit()]
            # Filter for pure digits
            phones_raw = [c for c in contacts if c.isdigit()]
            
            if phones_plus:
                canonical = phones_plus[0] # Pick first valid phone
            elif phones_raw:
                canonical = phones_raw[0] # Pick first raw phone
            else:
                # Pick the longest one? Or just first?
                # Usually username vs name? 
                canonical = contacts[0]
            
            print(f"  -> Selected Canonical: {canonical}")
            
            # MERGE PROCESS
            for contact in contacts:
                if contact == canonical:
                    continue
                    
                print(f"  Merging '{contact}' into '{canonical}'...")
                
                # 1. Update Messages
                res = await conn.execute("""
                    UPDATE inbox_messages 
                    SET sender_contact = $1 
                    WHERE license_key_id = $2 AND sender_id = $3 AND sender_contact = $4
                """, canonical, license_id, sender_id, contact)
                print(f"    Messages moved: {res}")
                
                # 2. Cleanup Conversations
                # We need to remove the 'old' conversation.
                # BUT, check if the Canonical Conversation exists.
                # If both exist, we keep the Canonical one, and just ensure stats are right (maybe re-calc).
                # If Canonical doesn't exist (unlikely if we picked from existing messages), we might need to rename the old one?
                # Actually, since we updated messages, the old conversation (keyed by old contact) is now effectively empty/orphaned 
                # regarding those messages.
                
                # Check for Canonical Conversation
                sub_rows = await conn.fetch("""
                    SELECT 1 FROM inbox_conversations 
                    WHERE license_key_id = $1 AND sender_contact = $2
                """, license_id, canonical)
                
                canonical_exists = len(sub_rows) > 0
                
                if canonical_exists:
                    # Just delete the old one
                    res = await conn.execute("""
                        DELETE FROM inbox_conversations
                        WHERE license_key_id = $1 AND sender_contact = $2
                    """, license_id, contact)
                    print(f"    Old conversation deleted: {res}")
                else:
                    # Rename the old conversation to the new contact
                    # Only if new contact doesn't exist (we checked above)
                    res = await conn.execute("""
                        UPDATE inbox_conversations
                        SET sender_contact = $1
                        WHERE license_key_id = $2 AND sender_contact = $3
                    """, canonical, license_id, contact)
                    print(f"    Old conversation renamed to canonical: {res}")
            
            # 3. Recalculate Stats for Canonical
            count = await conn.fetchval("""
                SELECT COUNT(*) FROM inbox_messages 
                WHERE license_key_id = $1 AND sender_contact = $2 AND channel = 'telegram'
            """, license_id, canonical)
            
            # Update/Upsert conversation count?
            # It should exist by now.
            await conn.execute("""
                UPDATE inbox_conversations
                SET message_count = $1
                WHERE license_key_id = $2 AND sender_contact = $3
            """, count, license_id, canonical)
            print(f"  -> Updated canonical message count to {count}")

        print("\nCleanup complete.")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(main())
