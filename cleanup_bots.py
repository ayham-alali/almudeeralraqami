
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
            logger.info("No bots found.")
            return

        # 3. DELETE ACTIONS
        
        # Delete from inbox_messages
        for contact in bot_contacts:
            if not contact: continue
            
            # Delete messages
            await execute_sql(db, "DELETE FROM inbox_messages WHERE sender_contact = ?", [contact])
            deleted_messages += 1 # This counts calls, not rows unfortunately, but good enough
            
            # Delete presence
            await execute_sql(db, "DELETE FROM customer_presence WHERE sender_contact = ?", [contact])
            
        # Delete from customers table
        for cid in bot_customer_ids:
             await execute_sql(db, "DELETE FROM customers WHERE id = ?", [cid])
             deleted_customers += 1

        # Also delete customers by contact if they weren't caught by ID
        for contact in bot_contacts:
             if not contact: continue
             await execute_sql(db, "DELETE FROM customers WHERE phone = ? OR email = ?", [contact, contact])

        await commit_db(db)
        
    logger.info(f"Cleanup complete. Removed bot traces for {len(bot_contacts)} contacts and {len(bot_customer_ids)} customer IDs.")

if __name__ == "__main__":
    asyncio.run(cleanup_bots())
