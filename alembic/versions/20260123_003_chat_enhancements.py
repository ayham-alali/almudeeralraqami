"""Chat enhancements migration - adds threading, tracking, and optimization

Revision ID: 003_chat_enhancements
Revises: 002_fix_customers_id
Create Date: 2026-01-23

This migration adds support for:
1. Threaded messaging (reply_to context)
2. Message tracking (platform_status, platform_message_id)
3. Soft deletion (deleted_at)
4. Optimized conversation list (inbox_conversations table)
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import os

# revision identifiers, used by Alembic.
revision: str = '003_chat_enhancements'
down_revision: Union[str, None] = '002_fix_customers_id'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# Detect database type
DB_TYPE = os.getenv("DB_TYPE", "sqlite").lower()


def upgrade() -> None:
    """Enhance schema for advanced chat features"""
    
    # Helper for DB-specific syntax
    if DB_TYPE == "postgresql":
        timestamp_now = "TIMESTAMP DEFAULT NOW()"
    else:
        timestamp_now = "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
    
    # helper to check if column exists
    def column_exists(table, column):
        connection = op.get_bind()
        if DB_TYPE == "postgresql":
            res = connection.execute(sa.text(f"SELECT column_name FROM information_schema.columns WHERE table_name='{table}' AND column_name='{column}'"))
            return res.first() is not None
        else:
            res = connection.execute(sa.text(f"PRAGMA table_info({table})"))
            for row in res:
                if row[1] == column:
                    return True
            return False

    # 1. Add columns to inbox_messages
    cols_to_add = [
        ('reply_to_platform_id', 'TEXT'),
        ('reply_to_body_preview', 'TEXT'),
        ('reply_to_sender_name', 'TEXT'),
        ('reply_to_id', 'INTEGER'),
        ('platform_status', 'TEXT'),
        ('platform_message_id', 'TEXT'),
        ('deleted_at', 'TIMESTAMP'),
        ('original_sender', 'TEXT')
    ]
    
    for col_name, col_type in cols_to_add:
        if not column_exists('inbox_messages', col_name):
            op.execute(f"ALTER TABLE inbox_messages ADD COLUMN {col_name} {col_type}")
    
    # 2. Add columns to outbox_messages
    if not column_exists('outbox_messages', 'deleted_at'):
        op.execute("ALTER TABLE outbox_messages ADD COLUMN deleted_at TIMESTAMP")
    
    # 3. Create inbox_conversations optimization table
    op.execute(f"""
        CREATE TABLE IF NOT EXISTS inbox_conversations (
            license_key_id INTEGER NOT NULL,
            sender_contact TEXT NOT NULL,
            sender_name TEXT,
            channel TEXT,
            last_message_id INTEGER,
            last_message_body TEXT,
            last_message_at TIMESTAMP,
            status TEXT,
            unread_count INTEGER DEFAULT 0,
            message_count INTEGER DEFAULT 0,
            updated_at {timestamp_now},
            PRIMARY KEY (license_key_id, sender_contact),
            FOREIGN KEY (license_key_id) REFERENCES license_keys(id)
        )
    """)
    
    # 4. Add indexes for performance
    op.execute("CREATE INDEX IF NOT EXISTS idx_inbox_platform_msg_id ON inbox_messages(platform_message_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_outbox_platform_msg_id ON outbox_messages(platform_message_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_inbox_deleted_at ON inbox_messages(deleted_at)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_outbox_deleted_at ON outbox_messages(deleted_at)")


def downgrade() -> None:
    """Remove chat enhancement features (caution: destructive)"""
    
    # Removing columns from existing tables is complex in SQLite (requires temp table).
    # Since this is an agentic environment, we provide the PG version and a warning for SQLite.
    
    if DB_TYPE == "postgresql":
        op.execute("ALTER TABLE inbox_messages DROP COLUMN IF EXISTS reply_to_platform_id")
        op.execute("ALTER TABLE inbox_messages DROP COLUMN IF EXISTS reply_to_body_preview")
        op.execute("ALTER TABLE inbox_messages DROP COLUMN IF EXISTS reply_to_sender_name")
        op.execute("ALTER TABLE inbox_messages DROP COLUMN IF EXISTS reply_to_id")
        op.execute("ALTER TABLE inbox_messages DROP COLUMN IF EXISTS platform_status")
        op.execute("ALTER TABLE inbox_messages DROP COLUMN IF EXISTS platform_message_id")
        op.execute("ALTER TABLE inbox_messages DROP COLUMN IF EXISTS deleted_at")
        op.execute("ALTER TABLE inbox_messages DROP COLUMN IF EXISTS original_sender")
        op.execute("ALTER TABLE outbox_messages DROP COLUMN IF EXISTS deleted_at")
    
    op.execute("DROP TABLE IF EXISTS inbox_conversations")
    op.execute("DROP INDEX IF EXISTS idx_inbox_platform_msg_id")
    op.execute("DROP INDEX IF EXISTS idx_outbox_platform_msg_id")
    op.execute("DROP INDEX IF EXISTS idx_inbox_deleted_at")
    op.execute("DROP INDEX IF EXISTS idx_outbox_deleted_at")
