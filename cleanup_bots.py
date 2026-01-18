
import asyncio
import logging
from db_helper import get_db, execute_sql, fetch_all, commit_db

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cleanup_bots")

async def cleanup_bots():
    logger.info("Starting bot cleanup...")
    
    deleted_customers = 0
    deleted_messages = 0
    
    async with get_db() as db:
        # 1. Identify potential bot CONTACTS from inbox_messages
        # We look for "bot" or "api" in sender_name or sender_contact
        
        bot_contacts = set()
        
        rows = await fetch_all(db, """
            SELECT DISTINCT sender_contact, sender_name 
            FROM inbox_messages 
            WHERE 
                (lower(sender_name) LIKE '%bot%' OR lower(sender_name) LIKE '%api%')
                OR 
                (lower(sender_contact) LIKE '%bot%' OR lower(sender_contact) LIKE '%api%')
        """)
        
        for row in rows:
            contact = row["sender_contact"]
            name = row["sender_name"]
            
            # Additional safety check: Don't delete if it looks like a regular email that just happens to have "bot" (e.g. abbot@gmail.com)
            # But "api" is pretty suspicious.
            
            is_bot = False
            
            # Check name
            if name:
                name_lower = name.lower()
                if "api" in name_lower or " bot" in name_lower or name_lower.endswith("bot"):
                     is_bot = True
            
            # Check contact (username)
            if contact:
                contact_lower = contact.lower()
                if "api" in contact_lower or contact_lower.endswith("bot"):
                    is_bot = True
            
            if is_bot:
                logger.info(f"Identified bot: {name} ({contact})")
                if contact:
                    bot_contacts.add(contact)

        # 2. ALSO check customers table
        customer_rows = await fetch_all(db, """
            SELECT id, name, phone, email
            FROM customers
            WHERE
                (lower(name) LIKE '%bot%' OR lower(name) LIKE '%api%')
        """)
        
        bot_customer_ids = []
        for row in customer_rows:
            name = row["name"]
            if name:
                name_lower = name.lower()
                if "api" in name_lower or " bot" in name_lower or name_lower.endswith("bot"):
                    logger.info(f"Identified bot customer: {name} (ID: {row['id']})")
                    bot_customer_ids.append(row['id'])
                    if row['phone']: bot_contacts.add(row['phone'])
                    if row['email']: bot_contacts.add(row['email'])

        if not bot_contacts and not bot_customer_ids:
            logger.info("No explicit bots found in phase 1, proceeding to pattern cleanup.")
            # Do not return, continue to phase 3c


        # 3. DELETE ACTIONS
        
        # 3a. Cleanup based on identified CONTACTS (email/phone)
        for contact in bot_contacts:
            if not contact: continue
            
            # Find inbox messages to link to customer_messages and outbox
            msg_rows = await fetch_all(db, "SELECT id FROM inbox_messages WHERE sender_contact = ?", [contact])
            msg_ids = [r["id"] for r in msg_rows]
            
            if msg_ids:
                msg_ids_placeholder = ",".join([f"'{mid}'" for mid in msg_ids]) # Be careful with string formatting if IDs are ints, typically SQL placeholders are better but list matching is tricky
                # Actually, easier to loop or use IN clause. `execute_sql` param handling varies. 
                # Let's iterate for safety or delete by join if DB allows. 
                # Doing iterative cleanup for safety/simplicity with existing helper
                
                for mid in msg_ids:
                     await execute_sql(db, "DELETE FROM customer_messages WHERE inbox_message_id = ?", [mid])
                     await execute_sql(db, "DELETE FROM outbox_messages WHERE inbox_message_id = ?", [mid])
            
            # Delete messages
            await execute_sql(db, "DELETE FROM inbox_messages WHERE sender_contact = ?", [contact])
            deleted_messages += len(msg_ids)
            
            
            # Clean up purchases if any (optional but good for referential integrity if cascade isn't set)
            await execute_sql(db, "DELETE FROM purchases WHERE customer_id IN (SELECT id FROM customers WHERE phone = ? OR email = ?)", [contact, contact])

            # Delete related customer_messages (by customer) before deleting customer
            await execute_sql(db, """
                DELETE FROM customer_messages 
                WHERE customer_id IN (SELECT id FROM customers WHERE phone = ? OR email = ?)
            """, [contact, contact])

            # Delete customers by contact
            await execute_sql(db, "DELETE FROM customers WHERE phone = ? OR email = ?", [contact, contact])


        # 3b. Cleanup based on identified CUSTOMER IDs (that might not have had contact info matched above)
        for cid in bot_customer_ids:
             # Delete purchases
             await execute_sql(db, "DELETE FROM purchases WHERE customer_id = ?", [cid])
             
             # Delete customer_messages
             await execute_sql(db, "DELETE FROM customer_messages WHERE customer_id = ?", [cid])
             
             # Delete customer
             await execute_sql(db, "DELETE FROM customers WHERE id = ?", [cid])
             deleted_customers += 1


        # 3c. Cleanup based on identified NAMES (for cases where contact might be empty or var)
        # We collected names in the identification phase? No, let's collect them now or just use the query again.
        # Simpler: Just run a direct delete for the specific patterns we know are bad.
        
        # 3c. Cleanup based on identified NAMES (for cases where contact might be empty or var)
        # We collected names in the identification phase? No, let's collect them now or just use the query again.
        # Simpler: Just run a direct delete for the specific patterns we know are bad.
        
        # Expanded blocklist including user-requested promotional senders
        bad_name_patterns = [
            '%bot%', '%api%', 
            '%calendly%', '%submagic%', '%iconscout%', 
            '%no-reply%', '%noreply%', '%donotreply%', 
            '%newsletter%', '%bulletin%'
        ]
        
        for pattern in bad_name_patterns:
             logger.info(f"Cleaning up pattern: {pattern}")
             # Get IDs first to clean dependent tables
             name_rows = await fetch_all(db, "SELECT id FROM inbox_messages WHERE lower(sender_name) LIKE ?", [pattern])
             msg_ids_from_name = [r["id"] for r in name_rows]
             
             for mid in msg_ids_from_name:
                 await execute_sql(db, "DELETE FROM customer_messages WHERE inbox_message_id = ?", [mid])
                 await execute_sql(db, "DELETE FROM outbox_messages WHERE inbox_message_id = ?", [mid])
             
             # Delete from inbox
             await execute_sql(db, "DELETE FROM inbox_messages WHERE lower(sender_name) LIKE ?", [pattern])
             
             # Delete from customers
             await execute_sql(db, "DELETE FROM customers WHERE lower(name) LIKE ?", [pattern])

        await commit_db(db)
        
    logger.info(f"Cleanup complete. Removed bot traces for {len(bot_contacts)} contacts and {len(bot_customer_ids)} customer IDs.")

if __name__ == "__main__":
    asyncio.run(cleanup_bots())
