"""
Migration for Advanced Chat Features
Adds support for:
- Message reactions (new table)
- Voice messages (new columns)
- Message forwarding (new columns)
"""

from db_helper import get_db, execute_sql, commit_db, DB_TYPE
from logging_config import get_logger

logger = get_logger(__name__)


async def ensure_chat_features_schema():
    """
    Ensure all chat features schema changes are applied.
    Safe to run multiple times - uses IF NOT EXISTS / tries to add columns.
    """
    
    async with get_db() as db:
        # ============ 1. Message Reactions Table ============
        if DB_TYPE == "postgresql":
            await execute_sql(db, """
                CREATE TABLE IF NOT EXISTS message_reactions (
                    id SERIAL PRIMARY KEY,
                    message_id BIGINT NOT NULL,
                    license_id INTEGER NOT NULL,
                    user_type VARCHAR(20) DEFAULT 'agent',
                    emoji VARCHAR(10) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(message_id, license_id, user_type, emoji)
                )
            """)
            await execute_sql(db, """
                CREATE INDEX IF NOT EXISTS idx_reactions_message 
                ON message_reactions(message_id)
            """)
        else:
            # SQLite (INTEGER is 64-bit in SQLite, so it's fine)
            await execute_sql(db, """
                CREATE TABLE IF NOT EXISTS message_reactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_id INTEGER NOT NULL,
                    license_id INTEGER NOT NULL,
                    user_type TEXT DEFAULT 'agent',
                    emoji TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(message_id, license_id, user_type, emoji)
                )
            """)
            await execute_sql(db, """
                CREATE INDEX IF NOT EXISTS idx_reactions_message 
                ON message_reactions(message_id)
            """)
        
        await commit_db(db)
        logger.info("✅ message_reactions table verified")
        
        # ============ 2. Voice Message Columns ============
        voice_columns = [
            ("audio_url", "TEXT"),
            ("audio_duration", "INTEGER"),
            ("audio_transcript", "TEXT"),
        ]
        
        for col_name, col_type in voice_columns:
            try:
                if DB_TYPE == "postgresql":
                    await execute_sql(db, f"""
                        ALTER TABLE inbox_messages 
                        ADD COLUMN IF NOT EXISTS {col_name} {col_type}
                    """)
                else:
                    await execute_sql(db, f"""
                        ALTER TABLE inbox_messages 
                        ADD COLUMN {col_name} {col_type}
                    """)
                await commit_db(db)
            except Exception as e:
                if "duplicate" not in str(e).lower() and "already exists" not in str(e).lower():
                    logger.debug(f"Voice column {col_name}: {e}")
        
        logger.info("✅ Voice message columns verified")
        
        # ============ 3. Message Forwarding Columns ============
        forward_columns = [
            ("is_forwarded", "BOOLEAN DEFAULT FALSE" if DB_TYPE == "postgresql" else "INTEGER DEFAULT 0"),
            ("forwarded_from", "TEXT"),
            ("forwarded_message_id", "BIGINT" if DB_TYPE == "postgresql" else "INTEGER"),
        ]
        
        for col_name, col_type in forward_columns:
            try:
                if DB_TYPE == "postgresql":
                    await execute_sql(db, f"""
                        ALTER TABLE inbox_messages 
                        ADD COLUMN IF NOT EXISTS {col_name} {col_type}
                    """)
                else:
                    await execute_sql(db, f"""
                        ALTER TABLE inbox_messages 
                        ADD COLUMN {col_name} {col_type}
                    """)
                await commit_db(db)
            except Exception as e:
                if "duplicate" not in str(e).lower() and "already exists" not in str(e).lower():
                    logger.debug(f"Forward column {col_name}: {e}")
        
        logger.info("✅ Message forwarding columns verified")
        
        # ============ 4. Delivery Receipt Columns (Real WhatsApp/Telegram receipts) ============
        delivery_columns = [
            ("platform_message_id", "TEXT"),       # WhatsApp/Telegram message ID
            ("delivery_status", "TEXT"),           # sent, delivered, read, failed
            ("delivered_at", "TIMESTAMP"),         # When delivered to recipient's device
            ("read_at", "TIMESTAMP"),              # When recipient opened/read the message
        ]
        
        for col_name, col_type in delivery_columns:
            try:
                if DB_TYPE == "postgresql":
                    await execute_sql(db, f"""
                        ALTER TABLE outbox_messages 
                        ADD COLUMN IF NOT EXISTS {col_name} {col_type}
                    """)
                else:
                    await execute_sql(db, f"""
                        ALTER TABLE outbox_messages 
                        ADD COLUMN {col_name} {col_type}
                    """)
                await commit_db(db)
            except Exception as e:
                if "duplicate" not in str(e).lower() and "already exists" not in str(e).lower():
                    logger.debug(f"Delivery column {col_name}: {e}")
        
        logger.info("✅ Delivery receipt columns verified")
        
        # ============ 5. Presence Tracking Table ============
        if DB_TYPE == "postgresql":
            await execute_sql(db, """
                CREATE TABLE IF NOT EXISTS user_presence (
                    id SERIAL PRIMARY KEY,
                    license_id INTEGER NOT NULL UNIQUE,
                    is_online BOOLEAN DEFAULT FALSE,
                    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
        else:
            await execute_sql(db, """
                CREATE TABLE IF NOT EXISTS user_presence (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    license_id INTEGER NOT NULL UNIQUE,
                    is_online INTEGER DEFAULT 0,
                    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
        
        await commit_db(db)
        logger.info("✅ user_presence table verified")
        
        # ============ 5. Customer Presence Table (Real Last Seen from WhatsApp/Telegram) ============
        if DB_TYPE == "postgresql":
            await execute_sql(db, """
                CREATE TABLE IF NOT EXISTS customer_presence (
                    id SERIAL PRIMARY KEY,
                    license_id INTEGER NOT NULL,
                    sender_contact VARCHAR(255) NOT NULL,
                    channel VARCHAR(50),
                    is_online BOOLEAN DEFAULT FALSE,
                    last_seen TIMESTAMP,
                    last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(license_id, sender_contact)
                )
            """)
            await execute_sql(db, """
                CREATE INDEX IF NOT EXISTS idx_customer_presence_contact 
                ON customer_presence(license_id, sender_contact)
            """)
        else:
            await execute_sql(db, """
                CREATE TABLE IF NOT EXISTS customer_presence (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    license_id INTEGER NOT NULL,
                    sender_contact TEXT NOT NULL,
                    channel TEXT,
                    is_online INTEGER DEFAULT 0,
                    last_seen TIMESTAMP,
                    last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(license_id, sender_contact)
                )
            """)
            await execute_sql(db, """
                CREATE INDEX IF NOT EXISTS idx_customer_presence_contact 
                ON customer_presence(license_id, sender_contact)
            """)
        
        await commit_db(db)
        logger.info("✅ customer_presence table verified")
        
        logger.info("🎉 All chat features schema changes applied successfully!")


# Run migration when imported directly
if __name__ == "__main__":
    import asyncio
    asyncio.run(ensure_chat_features_schema())
