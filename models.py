"""
Al-Mudeer - Enhanced Database Models
Email & Telegram Integration for Superhuman Mode
Supports both SQLite (development) and PostgreSQL (production)
"""

import os
from datetime import datetime, timedelta
from typing import Optional, List
import json

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


async def init_enhanced_tables():
    """Initialize enhanced tables for Email & Telegram integration"""
    async with get_db() as db:
        
        # Email Configuration per license
        await execute_sql(db, """
            CREATE TABLE IF NOT EXISTS email_configs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                license_key_id INTEGER UNIQUE NOT NULL,
                email_address TEXT NOT NULL,
                imap_server TEXT NOT NULL,
                imap_port INTEGER DEFAULT 993,
                smtp_server TEXT NOT NULL,
                smtp_port INTEGER DEFAULT 587,
                password_encrypted TEXT NOT NULL,
                is_active BOOLEAN DEFAULT TRUE,
                auto_reply_enabled BOOLEAN DEFAULT FALSE,
                check_interval_minutes INTEGER DEFAULT 5,
                last_checked_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (license_key_id) REFERENCES license_keys(id)
            )
        """)
        
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

        await commit_db(db)
        print("Enhanced tables initialized")


# ============ Email Config Functions ============

async def save_email_config(
    license_id: int,
    email_address: str,
    imap_server: str,
    smtp_server: str,
    password: str,
    imap_port: int = 993,
    smtp_port: int = 587,
    auto_reply: bool = False,
    check_interval: int = 5
) -> int:
    """Save or update email configuration"""
    # Simple XOR encryption (for demo - use proper encryption in production!)
    encrypted_password = simple_encrypt(password)
    
    async with aiosqlite.connect(DATABASE_PATH) as db:
        # Check if config exists
        async with db.execute(
            "SELECT id FROM email_configs WHERE license_key_id = ?",
            (license_id,)
        ) as cursor:
            existing = await cursor.fetchone()
        
        if existing:
            await db.execute("""
                UPDATE email_configs SET
                    email_address = ?, imap_server = ?, imap_port = ?,
                    smtp_server = ?, smtp_port = ?, password_encrypted = ?,
                    auto_reply_enabled = ?, check_interval_minutes = ?
                WHERE license_key_id = ?
            """, (email_address, imap_server, imap_port, smtp_server, smtp_port,
                  encrypted_password, auto_reply, check_interval, license_id))
            await db.commit()
            return existing[0]
        else:
            cursor = await db.execute("""
                INSERT INTO email_configs 
                (license_key_id, email_address, imap_server, imap_port,
                 smtp_server, smtp_port, password_encrypted, auto_reply_enabled,
                 check_interval_minutes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (license_id, email_address, imap_server, imap_port, smtp_server,
                  smtp_port, encrypted_password, auto_reply, check_interval))
            await db.commit()
            return cursor.lastrowid


async def get_email_config(license_id: int) -> Optional[dict]:
    """Get email configuration for a license"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM email_configs WHERE license_key_id = ?",
            (license_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                config = dict(row)
                # Don't return the actual password
                config.pop('password_encrypted', None)
                return config
    return None


async def get_email_password(license_id: int) -> Optional[str]:
    """Get decrypted email password (internal use only)"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        async with db.execute(
            "SELECT password_encrypted FROM email_configs WHERE license_key_id = ?",
            (license_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return simple_decrypt(row[0])
    return None


# ============ Telegram Config Functions ============

async def save_telegram_config(
    license_id: int,
    bot_token: str,
    bot_username: str = None,
    auto_reply: bool = False
) -> int:
    """Save or update Telegram bot configuration"""
    import secrets
    webhook_secret = secrets.token_hex(16)
    
    async with aiosqlite.connect(DATABASE_PATH) as db:
        async with db.execute(
            "SELECT id FROM telegram_configs WHERE license_key_id = ?",
            (license_id,)
        ) as cursor:
            existing = await cursor.fetchone()
        
        if existing:
            await db.execute("""
                UPDATE telegram_configs SET
                    bot_token = ?, bot_username = ?, auto_reply_enabled = ?
                WHERE license_key_id = ?
            """, (bot_token, bot_username, auto_reply, license_id))
            await db.commit()
            return existing[0]
        else:
            cursor = await db.execute("""
                INSERT INTO telegram_configs 
                (license_key_id, bot_token, bot_username, webhook_secret, auto_reply_enabled)
                VALUES (?, ?, ?, ?, ?)
            """, (license_id, bot_token, bot_username, webhook_secret, auto_reply))
            await db.commit()
            return cursor.lastrowid


async def get_telegram_config(license_id: int) -> Optional[dict]:
    """Get Telegram configuration for a license"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM telegram_configs WHERE license_key_id = ?",
            (license_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                config = dict(row)
                # Mask the bot token
                if config.get('bot_token'):
                    token = config['bot_token']
                    config['bot_token_masked'] = token[:10] + '...' + token[-5:]
                    config.pop('bot_token', None)
                return config
    return None


async def get_whatsapp_config(license_id: int) -> Optional[dict]:
    """Get WhatsApp configuration for a license"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM whatsapp_configs WHERE license_key_id = ? AND is_active = 1",
            (license_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                config = dict(row)
                # Mask the access token
                if config.get('access_token'):
                    token = config['access_token']
                    config['access_token_masked'] = token[:10] + '...' + token[-5:] if len(token) > 15 else '***'
                return config
    return None


# ============ Inbox Functions ============

async def save_inbox_message(
    license_id: int,
    channel: str,
    body: str,
    sender_name: str = None,
    sender_contact: str = None,
    sender_id: str = None,
    subject: str = None,
    channel_message_id: str = None,
    received_at: datetime = None
) -> int:
    """Save incoming message to inbox"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cursor = await db.execute("""
            INSERT INTO inbox_messages 
            (license_key_id, channel, channel_message_id, sender_id, sender_name,
             sender_contact, subject, body, received_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (license_id, channel, channel_message_id, sender_id, sender_name,
              sender_contact, subject, body, 
              received_at.isoformat() if received_at else datetime.now().isoformat()))
        await db.commit()
        return cursor.lastrowid


async def update_inbox_analysis(
    message_id: int,
    intent: str,
    urgency: str,
    sentiment: str,
    summary: str,
    draft_response: str
):
    """Update inbox message with AI analysis"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute("""
            UPDATE inbox_messages SET
                intent = ?, urgency = ?, sentiment = ?,
                ai_summary = ?, ai_draft_response = ?,
                status = 'analyzed', processed_at = ?
            WHERE id = ?
        """, (intent, urgency, sentiment, summary, draft_response,
              datetime.now().isoformat(), message_id))
        await db.commit()


async def get_inbox_messages(
    license_id: int,
    status: str = None,
    channel: str = None,
    limit: int = 50
) -> List[dict]:
    """Get inbox messages for a license"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        
        query = "SELECT * FROM inbox_messages WHERE license_key_id = ?"
        params = [license_id]
        
        if status:
            query += " AND status = ?"
            params.append(status)
        
        if channel:
            query += " AND channel = ?"
            params.append(channel)
        
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        
        async with db.execute(query, params) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


async def update_inbox_status(message_id: int, status: str):
    """Update inbox message status"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "UPDATE inbox_messages SET status = ? WHERE id = ?",
            (status, message_id)
        )
        await db.commit()


# ============ Outbox Functions ============

async def create_outbox_message(
    inbox_message_id: int,
    license_id: int,
    channel: str,
    body: str,
    recipient_id: str = None,
    recipient_email: str = None,
    subject: str = None
) -> int:
    """Create outbox message for approval"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cursor = await db.execute("""
            INSERT INTO outbox_messages 
            (inbox_message_id, license_key_id, channel, recipient_id,
             recipient_email, subject, body)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (inbox_message_id, license_id, channel, recipient_id,
              recipient_email, subject, body))
        await db.commit()
        return cursor.lastrowid


async def approve_outbox_message(message_id: int, edited_body: str = None):
    """Approve an outbox message for sending"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        if edited_body:
            await db.execute("""
                UPDATE outbox_messages SET
                    body = ?, status = 'approved', approved_at = ?
                WHERE id = ?
            """, (edited_body, datetime.now().isoformat(), message_id))
        else:
            await db.execute("""
                UPDATE outbox_messages SET
                    status = 'approved', approved_at = ?
                WHERE id = ?
            """, (datetime.now().isoformat(), message_id))
        await db.commit()


async def mark_outbox_sent(message_id: int):
    """Mark outbox message as sent"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute("""
            UPDATE outbox_messages SET
                status = 'sent', sent_at = ?
            WHERE id = ?
        """, (datetime.now().isoformat(), message_id))
        await db.commit()


async def get_pending_outbox(license_id: int) -> List[dict]:
    """Get pending outbox messages"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT o.*, i.sender_name, i.body as original_message
            FROM outbox_messages o
            JOIN inbox_messages i ON o.inbox_message_id = i.id
            WHERE o.license_key_id = ? AND o.status IN ('pending', 'approved')
            ORDER BY o.created_at DESC
        """, (license_id,)) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


# ============ Utility Functions ============

def simple_encrypt(text: str) -> str:
    """Encrypt sensitive data using enhanced security module"""
    try:
        from security_enhanced import encrypt_sensitive_data
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
        from security_enhanced import decrypt_sensitive_data
        return decrypt_sensitive_data(encrypted)
    except ImportError:
        # Fallback to simple XOR if enhanced security not available
        return simple_encrypt(encrypted)  # XOR is symmetric


# Initialize on import
import asyncio

async def init_templates_and_customers():
    """Initialize templates and customer tables"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        # WhatsApp Configuration
        await db.execute("""
            CREATE TABLE IF NOT EXISTS whatsapp_configs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                license_key_id INTEGER NOT NULL UNIQUE,
                phone_number_id TEXT NOT NULL,
                access_token TEXT NOT NULL,
                business_account_id TEXT,
                verify_token TEXT NOT NULL,
                webhook_secret TEXT,
                is_active BOOLEAN DEFAULT TRUE,
                auto_reply_enabled BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP,
                FOREIGN KEY (license_key_id) REFERENCES license_keys(id)
            )
        """)
        
        # Team Members (Multi-User Support)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS team_members (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                license_key_id INTEGER NOT NULL,
                email TEXT NOT NULL,
                name TEXT NOT NULL,
                password_hash TEXT,
                role TEXT NOT NULL DEFAULT 'agent',
                permissions TEXT,
                is_active BOOLEAN DEFAULT TRUE,
                last_login_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                invited_by INTEGER,
                FOREIGN KEY (license_key_id) REFERENCES license_keys(id),
                FOREIGN KEY (invited_by) REFERENCES team_members(id),
                UNIQUE(license_key_id, email)
            )
        """)
        
        # Team Activity Log
        await db.execute("""
            CREATE TABLE IF NOT EXISTS team_activity_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                license_key_id INTEGER NOT NULL,
                team_member_id INTEGER,
                action TEXT NOT NULL,
                details TEXT,
                ip_address TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (license_key_id) REFERENCES license_keys(id),
                FOREIGN KEY (team_member_id) REFERENCES team_members(id)
            )
        """)
        
        # Notifications
        await db.execute("""
            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                license_key_id INTEGER NOT NULL,
                type TEXT NOT NULL,
                priority TEXT DEFAULT 'normal',
                title TEXT NOT NULL,
                message TEXT NOT NULL,
                link TEXT,
                is_read BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (license_key_id) REFERENCES license_keys(id)
            )
        """)
        
        # Quick Reply Templates
        await db.execute("""
            CREATE TABLE IF NOT EXISTS reply_templates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                license_key_id INTEGER NOT NULL,
                shortcut TEXT NOT NULL,
                title TEXT NOT NULL,
                body TEXT NOT NULL,
                category TEXT DEFAULT 'عام',
                use_count INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (license_key_id) REFERENCES license_keys(id),
                UNIQUE(license_key_id, shortcut)
            )
        """)
        
        # Customer Profiles
        await db.execute("""
            CREATE TABLE IF NOT EXISTS customers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (license_key_id) REFERENCES license_keys(id)
            )
        """)
        
        # Link inbox messages to customers
        await db.execute("""
            CREATE TABLE IF NOT EXISTS customer_messages (
                customer_id INTEGER,
                inbox_message_id INTEGER,
                PRIMARY KEY (customer_id, inbox_message_id),
                FOREIGN KEY (customer_id) REFERENCES customers(id),
                FOREIGN KEY (inbox_message_id) REFERENCES inbox_messages(id)
            )
        """)
        
        # Analytics/Metrics tracking
        await db.execute("""
            CREATE TABLE IF NOT EXISTS analytics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        
        # User preferences (dark mode, notifications, etc.)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS user_preferences (
                license_key_id INTEGER PRIMARY KEY,
                dark_mode BOOLEAN DEFAULT FALSE,
                notifications_enabled BOOLEAN DEFAULT TRUE,
                notification_sound BOOLEAN DEFAULT TRUE,
                auto_reply_delay_seconds INTEGER DEFAULT 30,
                language TEXT DEFAULT 'ar',
                onboarding_completed BOOLEAN DEFAULT FALSE,
                FOREIGN KEY (license_key_id) REFERENCES license_keys(id)
            )
        """)
        
        # Performance indexes for analytics & customers
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_analytics_license_date
            ON analytics(license_key_id, date)
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_customers_license_last_contact
            ON customers(license_key_id, last_contact_at)
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_notifications_license_created
            ON notifications(license_key_id, created_at)
        """)

        await db.commit()
        print("Templates, Customers & Analytics tables initialized")


# ============ Quick Reply Templates ============

async def get_templates(license_id: int) -> List[dict]:
    """Get all templates for a license"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM reply_templates WHERE license_key_id = ? ORDER BY use_count DESC",
            (license_id,)
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


async def save_template(
    license_id: int,
    shortcut: str,
    title: str,
    body: str,
    category: str = 'عام'
) -> int:
    """Save a new template"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cursor = await db.execute("""
            INSERT OR REPLACE INTO reply_templates 
            (license_key_id, shortcut, title, body, category)
            VALUES (?, ?, ?, ?, ?)
        """, (license_id, shortcut.lower(), title, body, category))
        await db.commit()
        return cursor.lastrowid


async def delete_template(license_id: int, template_id: int) -> bool:
    """Delete a template"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "DELETE FROM reply_templates WHERE id = ? AND license_key_id = ?",
            (template_id, license_id)
        )
        await db.commit()
        return True


async def increment_template_usage(template_id: int):
    """Increment template usage counter"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "UPDATE reply_templates SET use_count = use_count + 1 WHERE id = ?",
            (template_id,)
        )
        await db.commit()


# ============ Customer Profiles ============

async def get_or_create_customer(
    license_id: int,
    phone: str = None,
    email: str = None,
    name: str = None
) -> dict:
    """Get existing customer or create new one"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        
        # Try to find by phone or email
        if phone:
            async with db.execute(
                "SELECT * FROM customers WHERE license_key_id = ? AND phone = ?",
                (license_id, phone)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return dict(row)
        
        if email:
            async with db.execute(
                "SELECT * FROM customers WHERE license_key_id = ? AND email = ?",
                (license_id, email)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return dict(row)
        
        # Create new customer
        cursor = await db.execute("""
            INSERT INTO customers (license_key_id, name, phone, email)
            VALUES (?, ?, ?, ?)
        """, (license_id, name, phone, email))
        await db.commit()
        
        return {
            "id": cursor.lastrowid,
            "license_key_id": license_id,
            "name": name,
            "phone": phone,
            "email": email,
            "total_messages": 0,
            "is_vip": False
        }


async def get_customers(license_id: int, limit: int = 100) -> List[dict]:
    """Get all customers for a license"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT * FROM customers 
            WHERE license_key_id = ? 
            ORDER BY last_contact_at DESC
            LIMIT ?
        """, (license_id, limit)) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


async def get_customer(license_id: int, customer_id: int) -> Optional[dict]:
    """Get a specific customer"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM customers WHERE id = ? AND license_key_id = ?",
            (customer_id, license_id)
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None


async def update_customer(
    license_id: int,
    customer_id: int,
    **kwargs
) -> bool:
    """Update customer details"""
    allowed_fields = ['name', 'phone', 'email', 'company', 'notes', 'tags', 'is_vip']
    updates = {k: v for k, v in kwargs.items() if k in allowed_fields}
    
    if not updates:
        return False
    
    set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
    values = list(updates.values()) + [customer_id, license_id]
    
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(f"""
            UPDATE customers SET {set_clause}
            WHERE id = ? AND license_key_id = ?
        """, values)
        await db.commit()
        return True


async def increment_customer_messages(customer_id: int):
    """Increment customer message count and update last contact"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute("""
            UPDATE customers SET 
                total_messages = total_messages + 1,
                last_contact_at = ?
            WHERE id = ?
        """, (datetime.now().isoformat(), customer_id))
        await db.commit()


# ============ Analytics ============

async def update_daily_analytics(
    license_id: int,
    messages_received: int = 0,
    messages_replied: int = 0,
    auto_replies: int = 0,
    sentiment: str = None,
    time_saved_seconds: int = 0
):
    """Update daily analytics"""
    today = datetime.now().date().isoformat()
    
    async with aiosqlite.connect(DATABASE_PATH) as db:
        # Get or create today's record
        async with db.execute(
            "SELECT id FROM analytics WHERE license_key_id = ? AND date = ?",
            (license_id, today)
        ) as cursor:
            row = await cursor.fetchone()
        
        if row:
            # Update existing
            sentiment_field = ""
            if sentiment == "إيجابي":
                sentiment_field = ", positive_sentiment = positive_sentiment + 1"
            elif sentiment == "سلبي":
                sentiment_field = ", negative_sentiment = negative_sentiment + 1"
            elif sentiment == "محايد":
                sentiment_field = ", neutral_sentiment = neutral_sentiment + 1"
            
            await db.execute(f"""
                UPDATE analytics SET
                    messages_received = messages_received + ?,
                    messages_replied = messages_replied + ?,
                    auto_replies = auto_replies + ?,
                    time_saved_seconds = time_saved_seconds + ?
                    {sentiment_field}
                WHERE license_key_id = ? AND date = ?
            """, (messages_received, messages_replied, auto_replies, 
                  time_saved_seconds, license_id, today))
        else:
            # Create new
            pos = 1 if sentiment == "إيجابي" else 0
            neg = 1 if sentiment == "سلبي" else 0
            neu = 1 if sentiment == "محايد" else 0
            
            await db.execute("""
                INSERT INTO analytics 
                (license_key_id, date, messages_received, messages_replied,
                 auto_replies, positive_sentiment, negative_sentiment,
                 neutral_sentiment, time_saved_seconds)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (license_id, today, messages_received, messages_replied,
                  auto_replies, pos, neg, neu, time_saved_seconds))
        
        await db.commit()


async def get_analytics_summary(license_id: int, days: int = 30) -> dict:
    """
    Get analytics summary for dashboard.

    Uses the unified db_helper layer so it works with both SQLite and PostgreSQL.
    """
    # Calculate cutoff date in Python to keep SQL cross-database
    cutoff_date = (datetime.utcnow().date() - timedelta(days=days)).isoformat()

    async with get_db() as db:
        row = await fetch_one(
            db,
            """
            SELECT 
                SUM(messages_received) as total_received,
                SUM(messages_replied) as total_replied,
                SUM(auto_replies) as total_auto,
                SUM(positive_sentiment) as positive,
                SUM(negative_sentiment) as negative,
                SUM(neutral_sentiment) as neutral,
                SUM(time_saved_seconds) as time_saved
            FROM analytics 
            WHERE license_key_id = ?
              AND date >= ?
            """,
            [license_id, cutoff_date],
        )

    if row:
        data = row
        total_sentiment = (data.get("positive") or 0) + (data.get("negative") or 0) + (data.get("neutral") or 0)

        return {
            "total_messages": data.get("total_received") or 0,
            "total_replied": data.get("total_replied") or 0,
            "auto_replies": data.get("total_auto") or 0,
            "time_saved_hours": round((data.get("time_saved") or 0) / 3600, 1),
            "satisfaction_rate": round((data.get("positive") or 0) / max(total_sentiment, 1) * 100),
            "response_rate": round(
                (data.get("total_replied") or 0) / max(data.get("total_received") or 1, 1) * 100
            ),
        }

    return {
        "total_messages": 0,
        "total_replied": 0,
        "auto_replies": 0,
        "time_saved_hours": 0,
        "satisfaction_rate": 0,
        "response_rate": 0,
    }


# ============ User Preferences ============

async def get_preferences(license_id: int) -> dict:
    """Get user preferences"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM user_preferences WHERE license_key_id = ?",
            (license_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return dict(row)
        
        # Create default preferences
        await db.execute(
            "INSERT INTO user_preferences (license_key_id) VALUES (?)",
            (license_id,)
        )
        await db.commit()
        
        return {
            "license_key_id": license_id,
            "dark_mode": False,
            "notifications_enabled": True,
            "onboarding_completed": False
        }


async def update_preferences(license_id: int, **kwargs) -> bool:
    """Update user preferences"""
    allowed = ['dark_mode', 'notifications_enabled', 'notification_sound', 
               'auto_reply_delay_seconds', 'onboarding_completed']
    updates = {k: v for k, v in kwargs.items() if k in allowed}
    
    if not updates:
        return False
    
    set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
    values = list(updates.values()) + [license_id]
    
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(f"""
            INSERT INTO user_preferences (license_key_id) VALUES (?)
            ON CONFLICT(license_key_id) DO UPDATE SET {set_clause}
        """.replace("DO UPDATE SET", f"DO UPDATE SET {set_clause}"), 
        [license_id] + list(updates.values()))
        await db.commit()
        return True


# ============ Notifications ============

async def create_notification(
    license_id: int,
    notification_type: str,
    title: str,
    message: str,
    priority: str = "normal",
    link: str = None
) -> int:
    """Create a new notification"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cursor = await db.execute("""
            INSERT INTO notifications (license_key_id, type, priority, title, message, link)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (license_id, notification_type, priority, title, message, link))
        await db.commit()
        return cursor.lastrowid


async def get_notifications(license_id: int, unread_only: bool = False, limit: int = 50) -> List[dict]:
    """Get notifications for a user"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        
        query = "SELECT * FROM notifications WHERE license_key_id = ?"
        params = [license_id]
        
        if unread_only:
            query += " AND is_read = FALSE"
        
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        
        async with db.execute(query, params) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


async def get_unread_count(license_id: int) -> int:
    """Get count of unread notifications"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        async with db.execute(
            "SELECT COUNT(*) FROM notifications WHERE license_key_id = ? AND is_read = FALSE",
            (license_id,)
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0


async def mark_notification_read(license_id: int, notification_id: int) -> bool:
    """Mark a notification as read"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "UPDATE notifications SET is_read = TRUE WHERE id = ? AND license_key_id = ?",
            (notification_id, license_id)
        )
        await db.commit()
        return True


async def mark_all_notifications_read(license_id: int) -> bool:
    """Mark all notifications as read"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "UPDATE notifications SET is_read = TRUE WHERE license_key_id = ?",
            (license_id,)
        )
        await db.commit()
        return True


async def delete_old_notifications(days: int = 30):
    """Delete notifications older than specified days"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "DELETE FROM notifications WHERE created_at < datetime('now', ?)",
            (f'-{days} days',)
        )
        await db.commit()


# ============ Team Management ============

ROLES = {
    "owner": {
        "name": "المالك",
        "permissions": ["*"]  # All permissions
    },
    "admin": {
        "name": "مدير",
        "permissions": ["read", "write", "reply", "manage_templates", "manage_integrations", "view_analytics"]
    },
    "agent": {
        "name": "موظف",
        "permissions": ["read", "write", "reply", "use_templates"]
    },
    "viewer": {
        "name": "مشاهد",
        "permissions": ["read", "view_analytics"]
    }
}


async def create_team_member(
    license_id: int,
    email: str,
    name: str,
    role: str = "agent",
    invited_by: int = None,
    password_hash: str = None
) -> int:
    """Create a new team member"""
    if role not in ROLES:
        role = "agent"
    
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cursor = await db.execute("""
            INSERT INTO team_members 
            (license_key_id, email, name, role, invited_by, password_hash, permissions)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (license_id, email.lower(), name, role, invited_by, password_hash, 
              ",".join(ROLES[role]["permissions"])))
        await db.commit()
        return cursor.lastrowid


async def get_team_members(license_id: int) -> List[dict]:
    """Get all team members for a license"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT id, email, name, role, is_active, last_login_at, created_at
            FROM team_members 
            WHERE license_key_id = ?
            ORDER BY created_at ASC
        """, (license_id,)) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


async def get_team_member(license_id: int, member_id: int) -> Optional[dict]:
    """Get a specific team member"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT * FROM team_members 
            WHERE id = ? AND license_key_id = ?
        """, (member_id, license_id)) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None


async def get_team_member_by_email(license_id: int, email: str) -> Optional[dict]:
    """Get team member by email"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT * FROM team_members 
            WHERE email = ? AND license_key_id = ?
        """, (email.lower(), license_id)) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None


async def update_team_member(
    license_id: int,
    member_id: int,
    **kwargs
) -> bool:
    """Update team member details"""
    allowed = ['name', 'role', 'is_active', 'permissions']
    updates = {k: v for k, v in kwargs.items() if k in allowed}
    
    if not updates:
        return False
    
    # If role is being updated, also update permissions
    if 'role' in updates and updates['role'] in ROLES:
        updates['permissions'] = ",".join(ROLES[updates['role']]["permissions"])
    
    set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
    values = list(updates.values()) + [member_id, license_id]
    
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(f"""
            UPDATE team_members SET {set_clause}
            WHERE id = ? AND license_key_id = ?
        """, values)
        await db.commit()
        return True


async def delete_team_member(license_id: int, member_id: int) -> bool:
    """Delete a team member"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "DELETE FROM team_members WHERE id = ? AND license_key_id = ?",
            (member_id, license_id)
        )
        await db.commit()
        return True


async def check_permission(license_id: int, member_id: int, permission: str) -> bool:
    """Check if a team member has a specific permission"""
    member = await get_team_member(license_id, member_id)
    if not member:
        return False
    
    permissions = (member.get('permissions') or '').split(',')
    return '*' in permissions or permission in permissions


async def log_team_activity(
    license_id: int,
    member_id: int,
    action: str,
    details: str = None,
    ip_address: str = None
):
    """Log team member activity"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute("""
            INSERT INTO team_activity_log 
            (license_key_id, team_member_id, action, details, ip_address)
            VALUES (?, ?, ?, ?, ?)
        """, (license_id, member_id, action, details, ip_address))
        await db.commit()


async def get_team_activity(license_id: int, limit: int = 100) -> List[dict]:
    """Get team activity log"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT a.*, m.name as member_name
            FROM team_activity_log a
            LEFT JOIN team_members m ON a.team_member_id = m.id
            WHERE a.license_key_id = ?
            ORDER BY a.created_at DESC
            LIMIT ?
        """, (license_id, limit)) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


# Smart notification triggers
async def create_smart_notification(
    license_id: int,
    event_type: str,
    data: dict = None
):
    """Create smart notifications based on events"""
    data = data or {}
    
    notifications_map = {
        "new_message": {
            "type": "message",
            "priority": "normal",
            "title": "📨 رسالة جديدة",
            "message": f"رسالة جديدة من {data.get('sender', 'مرسل مجهول')}",
            "link": "/dashboard/inbox"
        },
        "urgent_message": {
            "type": "urgent",
            "priority": "high",
            "title": "🔴 رسالة عاجلة",
            "message": f"رسالة عاجلة تحتاج انتباهك من {data.get('sender', 'مرسل')}",
            "link": "/dashboard/inbox"
        },
        "negative_sentiment": {
            "type": "alert",
            "priority": "high",
            "title": "⚠️ عميل غاضب",
            "message": f"تم اكتشاف شكوى من {data.get('customer', 'عميل')}",
            "link": "/dashboard/inbox"
        },
        "vip_message": {
            "type": "vip",
            "priority": "high",
            "title": "⭐ رسالة من عميل VIP",
            "message": f"رسالة من عميل VIP: {data.get('customer', 'عميل مهم')}",
            "link": "/dashboard/inbox"
        },
        "milestone": {
            "type": "achievement",
            "priority": "normal",
            "title": "🎉 إنجاز جديد!",
            "message": data.get('message', 'لقد حققت إنجازاً جديداً!'),
            "link": "/dashboard/overview"
        },
        "daily_summary": {
            "type": "summary",
            "priority": "low",
            "title": "📊 ملخص اليوم",
            "message": f"عالجت {data.get('count', 0)} رسالة ووفرت {data.get('time_saved', 0)} دقيقة",
            "link": "/dashboard/overview"
        }
    }
    
    if event_type not in notifications_map:
        return None
    
    notif = notifications_map[event_type]
    
    return await create_notification(
        license_id=license_id,
        notification_type=notif["type"],
        title=notif["title"],
        message=notif["message"],
        priority=notif["priority"],
        link=notif.get("link")
    )


def init_models():
    """Initialize models synchronously"""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.create_task(init_enhanced_tables())
            asyncio.create_task(init_templates_and_customers())
        else:
            loop.run_until_complete(init_enhanced_tables())
            loop.run_until_complete(init_templates_and_customers())
    except RuntimeError:
        asyncio.run(init_enhanced_tables())
        asyncio.run(init_templates_and_customers())

