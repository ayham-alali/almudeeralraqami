"""Initial baseline migration - creates all existing tables

Revision ID: 001_initial
Revises: None
Create Date: 2024-12-17

This migration creates the baseline schema matching the existing
init_enhanced_tables() and init_customers_and_analytics() functions.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import os

# revision identifiers, used by Alembic.
revision: str = '001_initial'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# Detect database type
DB_TYPE = os.getenv("DB_TYPE", "sqlite").lower()


def upgrade() -> None:
    """Create all tables for Al-Mudeer application"""
    
    # Helper for DB-specific syntax
    if DB_TYPE == "postgresql":
        id_pk = "SERIAL PRIMARY KEY"
        timestamp_now = "TIMESTAMP DEFAULT NOW()"
    else:
        id_pk = "INTEGER PRIMARY KEY AUTOINCREMENT"
        timestamp_now = "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
    
    # ============ License Keys (if not exists) ============
    op.execute(f"""
        CREATE TABLE IF NOT EXISTS license_keys (
            id {id_pk},
            key_hash TEXT NOT NULL UNIQUE,
            key_encrypted TEXT,
            plan TEXT NOT NULL DEFAULT 'free',
            max_messages_per_day INTEGER DEFAULT 100,
            expires_at TIMESTAMP,
            is_active BOOLEAN DEFAULT TRUE,
            created_at {timestamp_now}
        )
    """)
    
    # ============ Email Configuration ============
    op.execute("""
        CREATE TABLE IF NOT EXISTS email_configs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            license_key_id INTEGER UNIQUE NOT NULL,
            email_address TEXT NOT NULL,
            imap_server TEXT NOT NULL,
            imap_port INTEGER DEFAULT 993,
            smtp_server TEXT NOT NULL,
            smtp_port INTEGER DEFAULT 587,
            access_token_encrypted TEXT,
            refresh_token_encrypted TEXT,
            token_expires_at TIMESTAMP,
            password_encrypted TEXT,
            is_active BOOLEAN DEFAULT TRUE,
            auto_reply_enabled BOOLEAN DEFAULT FALSE,
            check_interval_minutes INTEGER DEFAULT 5,
            last_checked_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (license_key_id) REFERENCES license_keys(id)
        )
    """)
    
    # ============ Telegram Bot Configuration ============
    op.execute("""
        CREATE TABLE IF NOT EXISTS telegram_configs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            license_key_id INTEGER UNIQUE NOT NULL,
            bot_token TEXT NOT NULL,
            bot_username TEXT,
            webhook_secret TEXT,
            is_active BOOLEAN DEFAULT TRUE,
            auto_reply_enabled BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (license_key_id) REFERENCES license_keys(id)
        )
    """)
    
    # ============ Unified Inbox ============
    op.execute("""
        CREATE TABLE IF NOT EXISTS inbox_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            license_key_id INTEGER NOT NULL,
            channel TEXT NOT NULL,
            channel_message_id TEXT,
            sender_id TEXT,
            sender_name TEXT,
            sender_contact TEXT,
            subject TEXT,
            body TEXT NOT NULL,
            received_at TIMESTAMP,
            intent TEXT,
            urgency TEXT,
            sentiment TEXT,
            language TEXT,
            dialect TEXT,
            ai_summary TEXT,
            ai_draft_response TEXT,
            status TEXT DEFAULT 'pending',
            processed_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (license_key_id) REFERENCES license_keys(id)
        )
    """)
    
    # ============ Outbox ============
    op.execute("""
        CREATE TABLE IF NOT EXISTS outbox_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            inbox_message_id INTEGER NOT NULL,
            license_key_id INTEGER NOT NULL,
            channel TEXT NOT NULL,
            recipient_id TEXT,
            recipient_email TEXT,
            subject TEXT,
            body TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            approved_at TIMESTAMP,
            sent_at TIMESTAMP,
            error_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (inbox_message_id) REFERENCES inbox_messages(id),
            FOREIGN KEY (license_key_id) REFERENCES license_keys(id)
        )
    """)
    
    # ============ Telegram Phone Sessions ============
    op.execute("""
        CREATE TABLE IF NOT EXISTS telegram_phone_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            license_key_id INTEGER UNIQUE NOT NULL,
            phone_number TEXT NOT NULL,
            session_data_encrypted TEXT NOT NULL,
            user_id TEXT,
            user_first_name TEXT,
            user_last_name TEXT,
            user_username TEXT,
            is_active BOOLEAN DEFAULT TRUE,
            last_synced_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (license_key_id) REFERENCES license_keys(id)
        )
    """)
    
    # ============ WhatsApp Configuration ============
    op.execute(f"""
        CREATE TABLE IF NOT EXISTS whatsapp_configs (
            id {id_pk},
            license_key_id INTEGER NOT NULL UNIQUE,
            phone_number_id TEXT NOT NULL,
            access_token TEXT NOT NULL,
            business_account_id TEXT,
            verify_token TEXT NOT NULL,
            webhook_secret TEXT,
            is_active BOOLEAN DEFAULT TRUE,
            auto_reply_enabled BOOLEAN DEFAULT FALSE,
            created_at {timestamp_now},
            updated_at TIMESTAMP,
            FOREIGN KEY (license_key_id) REFERENCES license_keys(id)
        )
    """)
    
    # ============ Customer Profiles ============
    op.execute(f"""
        CREATE TABLE IF NOT EXISTS customers (
            id {id_pk},
            license_key_id INTEGER NOT NULL,
            name TEXT,
            phone TEXT,
            email TEXT,
            company TEXT,
            notes TEXT,
            tags TEXT,
            total_messages INTEGER DEFAULT 0,
            last_contact_at TIMESTAMP,
            sentiment_score REAL DEFAULT 0,
            is_vip BOOLEAN DEFAULT FALSE,
            segment TEXT,
            lead_score INTEGER DEFAULT 0,
            created_at {timestamp_now},
            FOREIGN KEY (license_key_id) REFERENCES license_keys(id)
        )
    """)
    
    # ============ Analytics ============
    op.execute(f"""
        CREATE TABLE IF NOT EXISTS analytics (
            id {id_pk},
            license_key_id INTEGER NOT NULL,
            date DATE NOT NULL,
            messages_received INTEGER DEFAULT 0,
            messages_replied INTEGER DEFAULT 0,
            auto_replies INTEGER DEFAULT 0,
            avg_response_time_seconds INTEGER,
            positive_sentiment INTEGER DEFAULT 0,
            negative_sentiment INTEGER DEFAULT 0,
            neutral_sentiment INTEGER DEFAULT 0,
            time_saved_seconds INTEGER DEFAULT 0,
            FOREIGN KEY (license_key_id) REFERENCES license_keys(id),
            UNIQUE(license_key_id, date)
        )
    """)
    
    # ============ Notifications ============
    op.execute(f"""
        CREATE TABLE IF NOT EXISTS notifications (
            id {id_pk},
            license_key_id INTEGER NOT NULL,
            type TEXT NOT NULL,
            priority TEXT DEFAULT 'normal',
            title TEXT NOT NULL,
            message TEXT NOT NULL,
            link TEXT,
            is_read BOOLEAN DEFAULT FALSE,
            created_at {timestamp_now},
            FOREIGN KEY (license_key_id) REFERENCES license_keys(id)
        )
    """)
    
    # ============ Team Members ============
    op.execute(f"""
        CREATE TABLE IF NOT EXISTS team_members (
            id {id_pk},
            license_key_id INTEGER NOT NULL,
            email TEXT NOT NULL,
            name TEXT NOT NULL,
            password_hash TEXT,
            role TEXT NOT NULL DEFAULT 'agent',
            permissions TEXT,
            is_active BOOLEAN DEFAULT TRUE,
            last_login_at TIMESTAMP,
            created_at {timestamp_now},
            invited_by INTEGER,
            FOREIGN KEY (license_key_id) REFERENCES license_keys(id),
            FOREIGN KEY (invited_by) REFERENCES team_members(id),
            UNIQUE(license_key_id, email)
        )
    """)
    
    # ============ User Preferences ============
    op.execute("""
        CREATE TABLE IF NOT EXISTS user_preferences (
            license_key_id INTEGER PRIMARY KEY,
            dark_mode BOOLEAN DEFAULT FALSE,
            notifications_enabled BOOLEAN DEFAULT TRUE,
            notification_sound BOOLEAN DEFAULT TRUE,
            auto_reply_delay_seconds INTEGER DEFAULT 30,
            language TEXT DEFAULT 'ar',
            onboarding_completed BOOLEAN DEFAULT FALSE,
            tone TEXT DEFAULT 'formal',
            custom_tone_guidelines TEXT,
            business_name TEXT,
            industry TEXT,
            products_services TEXT,
            preferred_languages TEXT,
            reply_length TEXT,
            formality_level TEXT,
            FOREIGN KEY (license_key_id) REFERENCES license_keys(id)
        )
    """)
    
    # ============ Performance Indexes ============
    op.execute("CREATE INDEX IF NOT EXISTS idx_inbox_license_status ON inbox_messages(license_key_id, status)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_inbox_license_created ON inbox_messages(license_key_id, created_at)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_outbox_license_status ON outbox_messages(license_key_id, status)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_analytics_license_date ON analytics(license_key_id, date)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_customers_license_last_contact ON customers(license_key_id, last_contact_at)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_notifications_license_created ON notifications(license_key_id, created_at)")


def downgrade() -> None:
    """Drop all tables (use with caution!)"""
    tables = [
        "user_preferences",
        "team_members", 
        "notifications",
        "analytics",
        "customers",
        "whatsapp_configs",
        "telegram_phone_sessions",
        "outbox_messages",
        "inbox_messages",
        "telegram_configs",
        "email_configs",
        "license_keys",
    ]
    
    for table in tables:
        op.execute(f"DROP TABLE IF EXISTS {table}")
