import logging
from db_helper import get_db, execute_sql, commit_db, DB_TYPE

logger = logging.getLogger(__name__)

async def migrate():
    """
    Create inbox_conversations table for optimized inbox loading.
    This is a denormalized table that caches the latest state of each conversation.
    """
    async with get_db() as db:
        if DB_TYPE == "postgresql":
            # PostgreSQL Schema
            await execute_sql(db, """
                CREATE TABLE IF NOT EXISTS inbox_conversations (
                    license_key_id INTEGER NOT NULL,
                    sender_contact TEXT NOT NULL,
                    sender_name TEXT,
                    channel TEXT,
                    last_message_id INTEGER,
                    last_message_body TEXT,
                    last_message_ai_summary TEXT,
                    last_message_at TIMESTAMP,
                    status TEXT DEFAULT 'pending',
                    unread_count INTEGER DEFAULT 0,
                    message_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (license_key_id, sender_contact)
                )
            """)
            
            # Indexes
            await execute_sql(db, """
                CREATE INDEX IF NOT EXISTS idx_conversations_updated ON inbox_conversations(license_key_id, updated_at DESC)
            """)
            await execute_sql(db, """
                CREATE INDEX IF NOT EXISTS idx_conversations_status ON inbox_conversations(license_key_id, status)
            """)
            
        else:
            # SQLite Schema
            await execute_sql(db, """
                CREATE TABLE IF NOT EXISTS inbox_conversations (
                    license_key_id INTEGER NOT NULL,
                    sender_contact TEXT NOT NULL,
                    sender_name TEXT,
                    channel TEXT,
                    last_message_id INTEGER,
                    last_message_body TEXT,
                    last_message_ai_summary TEXT,
                    last_message_at DATETIME,
                    status TEXT DEFAULT 'pending',
                    unread_count INTEGER DEFAULT 0,
                    message_count INTEGER DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (license_key_id, sender_contact)
                )
            """)
            
            # Indexes
            await execute_sql(db, """
                CREATE INDEX IF NOT EXISTS idx_conversations_updated ON inbox_conversations(license_key_id, updated_at DESC)
            """)
            await execute_sql(db, """
                CREATE INDEX IF NOT EXISTS idx_conversations_status ON inbox_conversations(license_key_id, status)
            """)

        await commit_db(db)
        logger.info("Created inbox_conversations table")
