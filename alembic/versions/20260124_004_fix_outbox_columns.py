"""Fix missing columns in outbox_messages

Revision ID: 004_fix_outbox_columns
Revises: 003_chat_enhancements
Create Date: 2026-01-24

This migration adds the missing columns to outbox_messages that were
overlooked in the previous chat enhancements migration.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import os

# revision identifiers, used by Alembic.
revision: str = '004_fix_outbox_columns'
down_revision: Union[str, None] = '003_chat_enhancements'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# Detect database type
DB_TYPE = os.getenv("DB_TYPE", "sqlite").lower()


def upgrade() -> None:
    """Add missing columns to outbox_messages"""
    
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

    # Columns to add to outbox_messages
    cols_to_add = [
        ('reply_to_platform_id', 'TEXT'),
        ('reply_to_body_preview', 'TEXT'),
        ('reply_to_sender_name', 'TEXT'),
        ('reply_to_id', 'INTEGER'),
        ('platform_message_id', 'TEXT'),
        ('delivery_status', 'TEXT'),
        ('original_sender', 'TEXT')
    ]
    
    for col_name, col_type in cols_to_add:
        if not column_exists('outbox_messages', col_name):
            op.execute(f"ALTER TABLE outbox_messages ADD COLUMN {col_name} {col_type}")


def downgrade() -> None:
    """Remove the added columns (caution: destructive)"""
    if DB_TYPE == "postgresql":
        op.execute("ALTER TABLE outbox_messages DROP COLUMN IF EXISTS reply_to_platform_id")
        op.execute("ALTER TABLE outbox_messages DROP COLUMN IF EXISTS reply_to_body_preview")
        op.execute("ALTER TABLE outbox_messages DROP COLUMN IF EXISTS reply_to_sender_name")
        op.execute("ALTER TABLE outbox_messages DROP COLUMN IF EXISTS reply_to_id")
        op.execute("ALTER TABLE outbox_messages DROP COLUMN IF EXISTS platform_message_id")
        op.execute("ALTER TABLE outbox_messages DROP COLUMN IF EXISTS delivery_status")
        op.execute("ALTER TABLE outbox_messages DROP COLUMN IF EXISTS original_sender")
    # SQLite doesn't support DROP COLUMN easily, so we leave it as is for SQLite
