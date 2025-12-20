"""
Al-Mudeer - Base Database Models
Database configuration, connection utilities, and table initialization
Supports both SQLite (development) and PostgreSQL (production)
"""

import os
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Any
import json
import asyncio

# Database configuration
DB_TYPE = os.getenv("DB_TYPE", "sqlite").lower()
DATABASE_PATH = os.getenv("DATABASE_PATH", "almudeer.db")
DATABASE_URL = os.getenv("DATABASE_URL")

# Import appropriate database driver
if DB_TYPE == "postgresql":
    try:
        import asyncpg
        POSTGRES_AVAILABLE = True
        aiosqlite = None
    except ImportError:
        raise ImportError(
            "PostgreSQL selected but asyncpg not installed. "
            "Install with: pip install asyncpg"
        )
else:
    import aiosqlite
    POSTGRES_AVAILABLE = False
    asyncpg = None


from db_helper import get_db, execute_sql, fetch_all, fetch_one, commit_db


# Helpers to generate SQL that works on both SQLite and PostgreSQL
ID_PK = "SERIAL PRIMARY KEY" if DB_TYPE == "postgresql" else "INTEGER PRIMARY KEY AUTOINCREMENT"
TIMESTAMP_NOW = "TIMESTAMP DEFAULT NOW()" if DB_TYPE == "postgresql" else "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"


async def init_enhanced_tables():
    """Initialize enhanced tables for Email & Telegram integration"""
    async with get_db() as db:
        
        # Email Configuration per license (OAuth 2.0 for Gmail)
        await execute_sql(db, """
            CREATE TABLE IF NOT EXISTS email_configs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                license_key_id INTEGER UNIQUE NOT NULL,
                email_address TEXT NOT NULL,
                imap_server TEXT NOT NULL,
                imap_port INTEGER DEFAULT 993,
                smtp_server TEXT NOT NULL,
                smtp_port INTEGER DEFAULT 587,
                -- OAuth 2.0 tokens (for Gmail)
                access_token_encrypted TEXT,
                refresh_token_encrypted TEXT,
                token_expires_at TIMESTAMP,
                -- Legacy password field (deprecated, kept for migration)
                password_encrypted TEXT,
                is_active BOOLEAN DEFAULT TRUE,
                auto_reply_enabled BOOLEAN DEFAULT FALSE,
                check_interval_minutes INTEGER DEFAULT 5,
                last_checked_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (license_key_id) REFERENCES license_keys(id)
            )
        """)
        
        # Add OAuth columns if they don't exist (migration for existing databases)
        try:
            await execute_sql(db, """
                ALTER TABLE email_configs ADD COLUMN access_token_encrypted TEXT
            """)
        except:
            pass  # Column already exists
        
        try:
            await execute_sql(db, """
                ALTER TABLE email_configs ADD COLUMN refresh_token_encrypted TEXT
            """)
        except:
            pass  # Column already exists
        
        try:
            await execute_sql(db, """
                ALTER TABLE email_configs ADD COLUMN token_expires_at TIMESTAMP
            """)
        except:
            pass  # Column already exists
        
        # Telegram Bot Configuration per license
        await execute_sql(db, """
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
        
        # Unified Inbox - All incoming messages
        await execute_sql(db, """
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
        
        # Outbox - Approved/Sent messages
        await execute_sql(db, """
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
        
        # Telegram Phone Sessions (MTProto for user accounts)
        await execute_sql(db, """
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
        
        # Telegram Chat Sessions
        await execute_sql(db, """
            CREATE TABLE IF NOT EXISTS telegram_chats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                license_key_id INTEGER NOT NULL,
                chat_id TEXT NOT NULL,
                chat_type TEXT,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                is_blocked BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (license_key_id) REFERENCES license_keys(id),
                UNIQUE(license_key_id, chat_id)
            )
        """)
        
        # Performance indexes for frequent queries
        await execute_sql(db, """
            CREATE INDEX IF NOT EXISTS idx_inbox_license_status
            ON inbox_messages(license_key_id, status)
        """)
        await execute_sql(db, """
            CREATE INDEX IF NOT EXISTS idx_inbox_license_created
            ON inbox_messages(license_key_id, created_at)
        """)
        await execute_sql(db, """
            CREATE INDEX IF NOT EXISTS idx_outbox_license_status
            ON outbox_messages(license_key_id, status)
        """)
        # Language/dialect quick filter
        await execute_sql(db, """
            CREATE INDEX IF NOT EXISTS idx_inbox_language
            ON inbox_messages(language, dialect)
        """)

        await commit_db(db)
        print("Enhanced tables initialized")


async def init_customers_and_analytics():
    """Initialize customers, analytics, notifications and related tables.

    Uses the generic db_helper layer so it works for both SQLite (dev)
    and PostgreSQL (production).
    """
    async with get_db() as db:
        # WhatsApp Configuration
        await execute_sql(db, f"""
            CREATE TABLE IF NOT EXISTS whatsapp_configs (
                id {ID_PK},
                license_key_id INTEGER NOT NULL UNIQUE,
                phone_number_id TEXT NOT NULL,
                access_token TEXT NOT NULL,
                business_account_id TEXT,
                verify_token TEXT NOT NULL,
                webhook_secret TEXT,
                is_active BOOLEAN DEFAULT TRUE,
                auto_reply_enabled BOOLEAN DEFAULT FALSE,
                created_at {TIMESTAMP_NOW},
                updated_at TIMESTAMP,
                FOREIGN KEY (license_key_id) REFERENCES license_keys(id)
            )
        """)

        # Team Members (Multi-User Support)
        await execute_sql(db, f"""
            CREATE TABLE IF NOT EXISTS team_members (
                id {ID_PK},
                license_key_id INTEGER NOT NULL,
                email TEXT NOT NULL,
                name TEXT NOT NULL,
                password_hash TEXT,
                role TEXT NOT NULL DEFAULT 'agent',
                permissions TEXT,
                is_active BOOLEAN DEFAULT TRUE,
                last_login_at TIMESTAMP,
                created_at {TIMESTAMP_NOW},
                invited_by INTEGER,
                FOREIGN KEY (license_key_id) REFERENCES license_keys(id),
                FOREIGN KEY (invited_by) REFERENCES team_members(id),
                UNIQUE(license_key_id, email)
            )
        """)

        # Team Activity Log
        await execute_sql(db, f"""
            CREATE TABLE IF NOT EXISTS team_activity_log (
                id {ID_PK},
                license_key_id INTEGER NOT NULL,
                team_member_id INTEGER,
                action TEXT NOT NULL,
                details TEXT,
                ip_address TEXT,
                created_at {TIMESTAMP_NOW},
                FOREIGN KEY (license_key_id) REFERENCES license_keys(id),
                FOREIGN KEY (team_member_id) REFERENCES team_members(id)
            )
        """)

        # Notifications (the main notifications table used by the dashboard)
        await execute_sql(db, f"""
            CREATE TABLE IF NOT EXISTS notifications (
                id {ID_PK},
                license_key_id INTEGER NOT NULL,
                type TEXT NOT NULL,
                priority TEXT DEFAULT 'normal',
                title TEXT NOT NULL,
                message TEXT NOT NULL,
                link TEXT,
                is_read BOOLEAN DEFAULT FALSE,
                created_at {TIMESTAMP_NOW},
                FOREIGN KEY (license_key_id) REFERENCES license_keys(id)
            )
        """)

        # Push Subscriptions (Web Push notifications for browsers/devices)
        await execute_sql(db, f"""
            CREATE TABLE IF NOT EXISTS push_subscriptions (
                id {ID_PK},
                license_key_id INTEGER NOT NULL,
                endpoint TEXT NOT NULL UNIQUE,
                subscription_info TEXT NOT NULL,
                user_agent TEXT,
                is_active BOOLEAN DEFAULT TRUE,
                created_at {TIMESTAMP_NOW},
                updated_at TIMESTAMP,
                FOREIGN KEY (license_key_id) REFERENCES license_keys(id)
            )
        """)

        # Customer Profiles
        await execute_sql(db, f"""
            CREATE TABLE IF NOT EXISTS customers (
                id {ID_PK},
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
                created_at {TIMESTAMP_NOW},
                FOREIGN KEY (license_key_id) REFERENCES license_keys(id)
            )
        """)
        
        # Add segment and lead_score columns if they don't exist (for existing databases)
        try:
            await execute_sql(db, "ALTER TABLE customers ADD COLUMN segment TEXT")
        except: pass
        try:
            await execute_sql(db, "ALTER TABLE customers ADD COLUMN lead_score INTEGER DEFAULT 0")
        except: pass

        # Link inbox messages to customers
        await execute_sql(db, """
            CREATE TABLE IF NOT EXISTS customer_messages (
                customer_id INTEGER,
                inbox_message_id INTEGER,
                PRIMARY KEY (customer_id, inbox_message_id),
                FOREIGN KEY (customer_id) REFERENCES customers(id),
                FOREIGN KEY (inbox_message_id) REFERENCES inbox_messages(id)
            )
        """)

        # Analytics/Metrics tracking
        await execute_sql(db, f"""
            CREATE TABLE IF NOT EXISTS analytics (
                id {ID_PK},
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

        # User preferences (UI + AI behavior / tone)
        await execute_sql(db, """
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

        # Performance indexes for analytics & customers
        await execute_sql(db, """
            CREATE INDEX IF NOT EXISTS idx_analytics_license_date
            ON analytics(license_key_id, date)
        """)
        await execute_sql(db, """
            CREATE INDEX IF NOT EXISTS idx_customers_license_last_contact
            ON customers(license_key_id, last_contact_at)
        """)
        await execute_sql(db, """
            CREATE INDEX IF NOT EXISTS idx_notifications_license_created
            ON notifications(license_key_id, created_at)
        """)

        await commit_db(db)
        print("Customers, Analytics & Notifications tables initialized")


# ============ Utility Functions ============

def simple_encrypt(text: str) -> str:
    """Encrypt sensitive data using enhanced security module"""
    try:
        from security import encrypt_sensitive_data
        return encrypt_sensitive_data(text)
    except ImportError:
        # Fallback to simple XOR if enhanced security not available
        key = os.getenv("ENCRYPTION_KEY", "almudeer-secret-key-2024")
        encrypted = []
        for i, char in enumerate(text):
            encrypted.append(chr(ord(char) ^ ord(key[i % len(key)])))
        return ''.join(encrypted)


def simple_decrypt(encrypted: str) -> str:
    """Decrypt sensitive data using enhanced security module"""
    try:
        from security import decrypt_sensitive_data
        return decrypt_sensitive_data(encrypted)
    except ImportError:
        # Fallback to simple XOR if enhanced security not available
        return simple_encrypt(encrypted)  # XOR is symmetric


def init_models():
    """Initialize models synchronously"""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.create_task(init_enhanced_tables())
            asyncio.create_task(init_customers_and_analytics())
        else:
            loop.run_until_complete(init_enhanced_tables())
            loop.run_until_complete(init_customers_and_analytics())
    except RuntimeError:
        asyncio.run(init_enhanced_tables())
        asyncio.run(init_customers_and_analytics())
