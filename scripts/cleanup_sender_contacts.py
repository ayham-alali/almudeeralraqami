"""
Al-Mudeer - Sender Contact Cleanup Script
==========================================
This script fixes conversations where the same Telegram user has messages
stored with different sender_contact values (phone, username, or ID).

It normalizes conversations to use a canonical sender_contact and refreshes
the inbox_conversations cache.

Run with: python scripts/cleanup_sender_contacts.py
"""

import asyncio
import asyncpg

# Production database connection
DATABASE_URL = ""


async def find_mismatched_senders(conn):
    """Find sender_ids that have multiple different sender_contact values."""
    rows = await conn.fetch("""
        SELECT sender_id, license_key_id, 
               COUNT(DISTINCT sender_contact) as contact_count,
               array_agg(DISTINCT sender_contact) as contacts,
               MIN(created_at) as first_message_at
        FROM inbox_messages 
        WHERE sender_id IS NOT NULL 
        AND sender_contact IS NOT NULL
        AND deleted_at IS NULL
        GROUP BY sender_id, license_key_id
        HAVING COUNT(DISTINCT sender_contact) > 1
    """)
    return rows


async def normalize_sender_contact(conn, license_id: int, sender_id: str, all_contacts: list):
    """
    Normalize all messages from a sender_id to use the same sender_contact.
    Uses the FIRST (oldest) sender_contact as the canonical one.
    """
    # Get the canonical contact (first one used historically)
    canonical = await conn.fetchrow("""
        SELECT sender_contact 
        FROM inbox_messages 
        WHERE license_key_id = $1 AND sender_id = $2
        AND sender_contact IS NOT NULL
        ORDER BY created_at ASC
        LIMIT 1
    """, license_id, sender_id)
    
    if not canonical:
        return None
        
    canonical_contact = canonical["sender_contact"]
    
    # Update all messages to use canonical contact
    updated_inbox = await conn.execute("""
        UPDATE inbox_messages 
        SET sender_contact = $1
        WHERE license_key_id = $2 
        AND sender_id = $3
        AND sender_contact != $1
    """, canonical_contact, license_id, sender_id)
    
    # Update outbox messages (recipient_id matches sender_id)
    updated_outbox = await conn.execute("""
        UPDATE outbox_messages 
        SET recipient_email = $1
        WHERE license_key_id = $2 
        AND recipient_id = $3
        AND recipient_email != $1
    """, canonical_contact, license_id, sender_id)
    
    # Refresh the conversation cache - recalculate from source
    await refresh_conversation_cache(conn, license_id, canonical_contact)
    
    return canonical_contact


async def refresh_conversation_cache(conn, license_id: int, sender_contact: str):
    """Refresh the inbox_conversations cache for a specific conversation."""
    
    # Get latest inbox message
    latest_inbox = await conn.fetchrow("""
        SELECT id, body, received_at as created_at, status 
        FROM inbox_messages 
        WHERE license_key_id = $1 
        AND sender_contact = $2
        AND status != 'pending'
        AND deleted_at IS NULL
        ORDER BY created_at DESC LIMIT 1
    """, license_id, sender_contact)
    
    # Get unread count
    unread = await conn.fetchval("""
        SELECT COUNT(*) FROM inbox_messages 
        WHERE license_key_id = $1 
        AND sender_contact = $2
        AND status = 'analyzed' 
        AND deleted_at IS NULL
        AND (is_read IS FALSE OR is_read IS NULL)
    """, license_id, sender_contact)
    
    # Get message count
    msg_count = await conn.fetchval("""
        SELECT COUNT(*) FROM inbox_messages 
        WHERE license_key_id = $1 
        AND sender_contact = $2
        AND status != 'pending'
        AND deleted_at IS NULL
    """, license_id, sender_contact)
    
    if latest_inbox:
        # Upsert the conversation
        await conn.execute("""
            INSERT INTO inbox_conversations 
                (license_key_id, sender_contact, last_message_id, last_message_body, 
                 last_message_at, status, unread_count, message_count, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
            ON CONFLICT (license_key_id, sender_contact) DO UPDATE SET
                last_message_id = EXCLUDED.last_message_id,
                last_message_body = EXCLUDED.last_message_body,
                last_message_at = EXCLUDED.last_message_at,
                status = EXCLUDED.status,
                unread_count = EXCLUDED.unread_count,
                message_count = EXCLUDED.message_count,
                updated_at = EXCLUDED.updated_at
        """, license_id, sender_contact, latest_inbox['id'], latest_inbox['body'],
            latest_inbox['created_at'], latest_inbox['status'], unread, msg_count)


async def cleanup_orphan_conversations(conn):
    """Remove inbox_conversations entries that no longer have messages."""
    result = await conn.execute("""
        DELETE FROM inbox_conversations 
        WHERE last_message_id NOT IN (
            SELECT id FROM inbox_messages WHERE deleted_at IS NULL
        )
    """)
    return result


async def main():
    print("=" * 60)
    print("Al-Mudeer Sender Contact Cleanup Script")
    print("=" * 60)
    print()
    
    print("Connecting to production database...")
    conn = await asyncpg.connect(DATABASE_URL)
    print("   ✓ Connected")
    print()
    
    try:
        # Find mismatched senders
        print("1. Finding senders with multiple contact formats...")
        mismatches = await find_mismatched_senders(conn)
        
        if not mismatches:
            print("   ✓ No mismatches found! All sender_contacts are consistent.")
        else:
            print(f"   Found {len(mismatches)} sender(s) with multiple contact formats:")
            for m in mismatches:
                print(f"   - sender_id={m['sender_id']}, license={m['license_key_id']}")
                print(f"     contacts: {m['contacts']}")
            
            print()
            print("2. Normalizing sender_contacts...")
            for m in mismatches:
                canonical = await normalize_sender_contact(
                    conn,
                    m['license_key_id'], 
                    m['sender_id'], 
                    m['contacts']
                )
                if canonical:
                    print(f"   ✓ sender_id={m['sender_id']} → normalized to '{canonical}'")
        
        print()
        print("3. Cleaning up orphan conversations...")
        await cleanup_orphan_conversations(conn)
        print("   ✓ Done")
        
        print()
        print("=" * 60)
        print("Cleanup Complete!")
        print("=" * 60)
    
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())

