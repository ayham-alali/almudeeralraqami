"""
Al-Mudeer - License Key Database Management
Supports both SQLite (development) and PostgreSQL (production)
"""

import os
from datetime import datetime, timedelta
from typing import Optional
import hashlib
import secrets

# Database configuration
DB_TYPE = os.getenv("DB_TYPE", "sqlite").lower()
DATABASE_PATH = os.getenv("DATABASE_PATH", "almudeer.db")
DATABASE_URL = os.getenv("DATABASE_URL")

# Import appropriate database driver
if DB_TYPE == "postgresql":
    try:
        import asyncpg
        POSTGRES_AVAILABLE = True
    except ImportError:
        raise ImportError(
            "PostgreSQL selected but asyncpg not installed. "
            "Install with: pip install asyncpg"
        )
else:
    import aiosqlite
    POSTGRES_AVAILABLE = False


def _adapt_sql_for_db(sql: str) -> str:
    """Adapt SQL syntax for current database type"""
    if DB_TYPE == "postgresql":
        sql = sql.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
        sql = sql.replace("AUTOINCREMENT", "")
        sql = sql.replace("TIMESTAMP DEFAULT CURRENT_TIMESTAMP", "TIMESTAMP DEFAULT NOW()")
    return sql


async def init_database():
    """Initialize the database with required tables (supports both SQLite and PostgreSQL)"""
    if DB_TYPE == "postgresql" and POSTGRES_AVAILABLE:
        if not DATABASE_URL:
            raise ValueError("DATABASE_URL is required for PostgreSQL")
        conn = await asyncpg.connect(DATABASE_URL)
        try:
            await _init_postgresql_tables(conn)
            # Add new columns if they don't exist (for existing databases)
            await conn.execute("""
                ALTER TABLE license_keys 
                ADD COLUMN IF NOT EXISTS referral_code VARCHAR(50) UNIQUE,
                ADD COLUMN IF NOT EXISTS referred_by_id INTEGER REFERENCES license_keys(id),
                ADD COLUMN IF NOT EXISTS is_trial BOOLEAN DEFAULT FALSE,
                ADD COLUMN IF NOT EXISTS referral_count INTEGER DEFAULT 0,
                ADD COLUMN IF NOT EXISTS username VARCHAR(255) UNIQUE
            """)
        except Exception:
            pass
        finally:
            await conn.close()
    else:
        async with aiosqlite.connect(DATABASE_PATH) as db:
            await _init_sqlite_tables(db)
            # Migrations for existing SQLite tables
            try:
                await db.execute("ALTER TABLE license_keys ADD COLUMN referral_code TEXT UNIQUE")
            except Exception: pass
            try:
                await db.execute("ALTER TABLE license_keys ADD COLUMN referred_by_id INTEGER")
            except Exception: pass
            try:
                await db.execute("ALTER TABLE license_keys ADD COLUMN is_trial BOOLEAN DEFAULT FALSE")
            except Exception: pass
            try:
                await db.execute("ALTER TABLE license_keys ADD COLUMN referral_count INTEGER DEFAULT 0")
            except Exception: pass
            try:
                await db.execute("ALTER TABLE license_keys ADD COLUMN username TEXT UNIQUE")
            except Exception: pass
            await db.commit()
    
    # Initialize Service Tables
    try:
        from services.notification_service import init_notification_tables
        await init_notification_tables()
    except Exception as e:
        print(f"Warning: Failed to init notification tables: {e}")


async def _init_sqlite_tables(db):
    """Initialize SQLite tables"""
    # License keys table
    await db.execute("""
        CREATE TABLE IF NOT EXISTS license_keys (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            key_hash TEXT UNIQUE NOT NULL,
            license_key_encrypted TEXT,
            company_name TEXT NOT NULL,
            contact_email TEXT,
            username TEXT UNIQUE,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            expires_at TIMESTAMP,
            max_requests_per_day INTEGER DEFAULT 100,
            requests_today INTEGER DEFAULT 0,
            last_request_date DATE,
            referral_code TEXT UNIQUE,
            referred_by_id INTEGER,
            is_trial BOOLEAN DEFAULT FALSE,
            referral_count INTEGER DEFAULT 0,
            FOREIGN KEY (referred_by_id) REFERENCES license_keys(id)
        )
    """)
    
    # Usage logs table for analytics
    await db.execute("""
        CREATE TABLE IF NOT EXISTS usage_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            license_key_id INTEGER,
            action_type TEXT NOT NULL,
            input_preview TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (license_key_id) REFERENCES license_keys(id)
        )
    """)
    
    # CRM entries table
    await db.execute("""
        CREATE TABLE IF NOT EXISTS crm_entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            license_key_id INTEGER,
            sender_name TEXT,
            sender_contact TEXT,
            message_type TEXT,
            intent TEXT,
            extracted_data TEXT,
            original_message TEXT,
            draft_response TEXT,
            status TEXT DEFAULT 'جديد',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP,
            FOREIGN KEY (license_key_id) REFERENCES license_keys(id)
        )
    """)

    # Customers table (for detailed profile)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            license_key_id INTEGER,
            name TEXT,
            contact TEXT UNIQUE NOT NULL,
            type TEXT DEFAULT 'Regular',
            total_spend REAL DEFAULT 0.0,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP,
            FOREIGN KEY (license_key_id) REFERENCES license_keys(id)
        )
    """)

    # Orders table
    await db.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_ref TEXT UNIQUE NOT NULL,
            customer_contact TEXT,
            status TEXT DEFAULT 'Pending',
            total_amount REAL,
            items TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP,
            FOREIGN KEY (customer_contact) REFERENCES customers(contact)
        )
    """)

    # Update Events table (Analytics)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS update_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            event TEXT NOT NULL,
            from_build INTEGER,
            to_build INTEGER,
            device_id TEXT,
            device_type TEXT,
            license_key TEXT
        )
    """)

    # App Config table (Source of Truth for Versioning)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS app_config (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Version History table
    await db.execute("""
        CREATE TABLE IF NOT EXISTS version_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            version TEXT NOT NULL,
            build_number INTEGER NOT NULL,
            release_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            changelog_ar TEXT,
            changelog_en TEXT,
            changes_json TEXT
        )
    """)

    
    # Create indexes for performance
    await db.execute("""
        CREATE INDEX IF NOT EXISTS idx_license_key_hash 
        ON license_keys(key_hash)
    """)
    
    await db.execute("""
        CREATE INDEX IF NOT EXISTS idx_crm_license_id 
        ON crm_entries(license_key_id)
    """)
    
    await db.execute("""
        CREATE INDEX IF NOT EXISTS idx_crm_created_at 
        ON crm_entries(created_at)
    """)
    
    await db.execute("""
        CREATE INDEX IF NOT EXISTS idx_usage_logs_license_id 
        ON usage_logs(license_key_id)
    """)
    
    await db.execute("""
        CREATE INDEX IF NOT EXISTS idx_license_expires_at 
        ON license_keys(expires_at)
    """)
    
    await db.commit()


async def _init_postgresql_tables(conn):
    """Initialize PostgreSQL tables"""
    # License keys table
    await conn.execute(_adapt_sql_for_db("""
        CREATE TABLE IF NOT EXISTS license_keys (
            id SERIAL PRIMARY KEY,
            key_hash VARCHAR(255) UNIQUE NOT NULL,
            license_key_encrypted TEXT,
            company_name VARCHAR(255) NOT NULL,
            contact_email VARCHAR(255),
            username VARCHAR(255) UNIQUE,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT NOW(),
            expires_at TIMESTAMP,
            max_requests_per_day INTEGER DEFAULT 100,
            requests_today INTEGER DEFAULT 0,
            last_request_date DATE,
            referral_code VARCHAR(50) UNIQUE,
            referred_by_id INTEGER REFERENCES license_keys(id),
            is_trial BOOLEAN DEFAULT FALSE,
            referral_count INTEGER DEFAULT 0
        )
    """))
    
    # Usage logs table for analytics
    await conn.execute(_adapt_sql_for_db("""
        CREATE TABLE IF NOT EXISTS usage_logs (
            id SERIAL PRIMARY KEY,
            license_key_id INTEGER,
            action_type VARCHAR(255) NOT NULL,
            input_preview TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            FOREIGN KEY (license_key_id) REFERENCES license_keys(id)
        )
    """))
    
    # CRM entries table
    await conn.execute(_adapt_sql_for_db("""
        CREATE TABLE IF NOT EXISTS crm_entries (
            id SERIAL PRIMARY KEY,
            license_key_id INTEGER,
            sender_name VARCHAR(255),
            sender_contact VARCHAR(255),
            message_type VARCHAR(255),
            intent VARCHAR(255),
            extracted_data TEXT,
            original_message TEXT,
            draft_response TEXT,
            status VARCHAR(255) DEFAULT 'جديد',
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP,
            FOREIGN KEY (license_key_id) REFERENCES license_keys(id)
        )
    """))

    # Customers table (for detailed profile)
    await conn.execute(_adapt_sql_for_db("""
        CREATE TABLE IF NOT EXISTS customers (
            id SERIAL PRIMARY KEY,
            license_key_id INTEGER,
            name VARCHAR(255),
            contact VARCHAR(255) UNIQUE NOT NULL,
            phone VARCHAR(255),
            email VARCHAR(255),
            type VARCHAR(50) DEFAULT 'Regular',
            total_spend REAL DEFAULT 0.0,
            notes TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP,
            FOREIGN KEY (license_key_id) REFERENCES license_keys(id)
        )
    """))

    # Migration: Ensure 'contact' column exists if the table was created by older logic
    try:
        await conn.execute("ALTER TABLE customers ADD COLUMN IF NOT EXISTS contact VARCHAR(255) UNIQUE")
    except Exception:
        pass

    # Orders table
    await conn.execute(_adapt_sql_for_db("""
        CREATE TABLE IF NOT EXISTS orders (
            id SERIAL PRIMARY KEY,
            order_ref VARCHAR(255) UNIQUE NOT NULL,
            customer_contact VARCHAR(255),
            status VARCHAR(50) DEFAULT 'Pending',
            total_amount REAL,
            items TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP,
            FOREIGN KEY (customer_contact) REFERENCES customers(contact)
        )
    """))

    # Update Events table (Analytics)
    await conn.execute(_adapt_sql_for_db("""
        CREATE TABLE IF NOT EXISTS update_events (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP DEFAULT NOW(),
            event VARCHAR(255) NOT NULL,
            from_build INTEGER,
            to_build INTEGER,
            device_id VARCHAR(255),
            device_type VARCHAR(50),
            license_key VARCHAR(255)
        )
    """))

    # App Config table (Source of Truth for Versioning)
    await conn.execute(_adapt_sql_for_db("""
        CREATE TABLE IF NOT EXISTS app_config (
            key VARCHAR(255) PRIMARY KEY,
            value TEXT,
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """))

    # Version History table
    await conn.execute(_adapt_sql_for_db("""
        CREATE TABLE IF NOT EXISTS version_history (
            id SERIAL PRIMARY KEY,
            version VARCHAR(50) NOT NULL,
            build_number INTEGER NOT NULL,
            release_date TIMESTAMP DEFAULT NOW(),
            changelog_ar TEXT,
            changelog_en TEXT,
            changes_json TEXT
        )
    """))

    
    # Create indexes for performance
    await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_license_key_hash 
        ON license_keys(key_hash)
    """)
    
    await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_crm_license_id 
        ON crm_entries(license_key_id)
    """)
    
    await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_crm_created_at 
        ON crm_entries(created_at)
    """)
    
    await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_usage_logs_license_id 
        ON usage_logs(license_key_id)
    """)
    
    await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_license_expires_at 
        ON license_keys(expires_at)
    """)


def hash_license_key(key: str) -> str:
    """Hash a license key for secure storage"""
    return hashlib.sha256(key.encode()).hexdigest()


async def generate_license_key(
    company_name: str,
    days_valid: int = 365,
    max_requests: int = 50,
    is_trial: bool = False,
    referred_by_id: Optional[int] = None,
    username: Optional[str] = None
) -> str:
    """Generate a new license key and store it in the database"""
    # Generate a readable license key format: MUDEER-XXXX-XXXX-XXXX
    raw_key = f"MUDEER-{secrets.token_hex(2).upper()}-{secrets.token_hex(2).upper()}-{secrets.token_hex(2).upper()}"
    key_hash = hash_license_key(raw_key)
    
    # Generate a unique referral code (short)
    referral_code = secrets.token_hex(3).upper() # 6 characters
    
    # Encrypt the original key for storage
    from security import encrypt_sensitive_data
    encrypted_key = encrypt_sensitive_data(raw_key)
    
    expires_at = datetime.now() + timedelta(days=days_valid)
    
    if DB_TYPE == "postgresql" and POSTGRES_AVAILABLE:
        conn = await asyncpg.connect(DATABASE_URL)
        try:
            # Get the next ID manually to avoid sequence issues
            max_id = await conn.fetchval("SELECT COALESCE(MAX(id), 0) FROM license_keys")
            next_id = max_id + 1
            
            # Reset sequence if needed
            await conn.execute(f"SELECT setval('license_keys_id_seq', {next_id}, false)")
            
            await conn.execute("""
                INSERT INTO license_keys (id, key_hash, license_key_encrypted, company_name, expires_at, max_requests_per_day, is_trial, referred_by_id, referral_code, username)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            """, next_id, key_hash, encrypted_key, company_name, expires_at, max_requests, is_trial, referred_by_id, referral_code, username)
            
            # If referred, increment referrer's count
            if referred_by_id:
                await conn.execute("UPDATE license_keys SET referral_count = referral_count + 1 WHERE id = $1", referred_by_id)
        finally:
            await conn.close()
    else:
        async with aiosqlite.connect(DATABASE_PATH) as db:
            cursor = await db.execute("""
                INSERT INTO license_keys (key_hash, license_key_encrypted, company_name, expires_at, max_requests_per_day, is_trial, referred_by_id, referral_code, username)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (key_hash, encrypted_key, company_name, expires_at.isoformat(), max_requests, is_trial, referred_by_id, referral_code, username))
            
            # If referred, increment referrer's count
            if referred_by_id:
                await db.execute("UPDATE license_keys SET referral_count = referral_count + 1 WHERE id = ?", (referred_by_id,))
            
            await db.commit()
    
    return raw_key


async def get_license_key_by_id(license_id: int) -> Optional[str]:
    """Get the original license key by ID (decrypted)"""
    from security import decrypt_sensitive_data
    from logging_config import get_logger
    
    logger = get_logger(__name__)
    
    if DB_TYPE == "postgresql" and POSTGRES_AVAILABLE:
        conn = await asyncpg.connect(DATABASE_URL)
        try:
            row = await conn.fetchrow("""
                SELECT license_key_encrypted FROM license_keys WHERE id = $1
            """, license_id)
            
            if not row:
                logger.warning(f"Subscription {license_id} not found")
                return None
            
            if not row.get('license_key_encrypted'):
                logger.warning(f"License key encrypted field is NULL for subscription {license_id} - this is an old subscription created before encryption was added")
                return None
            
            encrypted_key = row['license_key_encrypted']
            try:
                decrypted = decrypt_sensitive_data(encrypted_key)
                return decrypted
            except Exception as e:
                logger.error(f"Failed to decrypt license key for subscription {license_id}: {e}")
                return None
        finally:
            await conn.close()
    else:
        async with aiosqlite.connect(DATABASE_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("""
                SELECT license_key_encrypted FROM license_keys WHERE id = ?
            """, (license_id,)) as cursor:
                row = await cursor.fetchone()
                
                if not row:
                    logger.warning(f"Subscription {license_id} not found")
                    return None
                
                if not row.get('license_key_encrypted'):
                    logger.warning(f"License key encrypted field is NULL for subscription {license_id} - this is an old subscription created before encryption was added")
                    return None
                
                encrypted_key = row['license_key_encrypted']
                try:
                    decrypted = decrypt_sensitive_data(encrypted_key)
                    return decrypted
                except Exception as e:
                    logger.error(f"Failed to decrypt license key for subscription {license_id}: {e}")
                    return None


async def validate_license_key(key: str) -> dict:
    """Validate a license key and return its details"""
    # Try cache first
    try:
        from cache import get_cached_license_validation, cache_license_validation
        cached_result = await get_cached_license_validation(key)
        if cached_result is not None:
            return cached_result
    except ImportError:
        # Cache not available, continue with DB lookup
        pass
    
    key_hash = hash_license_key(key)
    
    if DB_TYPE == "postgresql" and POSTGRES_AVAILABLE:
        conn = await asyncpg.connect(DATABASE_URL)
        try:
            row = await conn.fetchrow("""
                SELECT * FROM license_keys WHERE key_hash = $1
            """, key_hash)
            
            if not row:
                return {"valid": False, "error": "مفتاح الاشتراك غير صالح"}
            
            row_dict = dict(row)
        finally:
            await conn.close()
    else:
        async with aiosqlite.connect(DATABASE_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("""
                SELECT * FROM license_keys WHERE key_hash = ?
            """, (key_hash,)) as cursor:
                row = await cursor.fetchone()
                
                if not row:
                    return {"valid": False, "error": "مفتاح الاشتراك غير صالح"}
                
                row_dict = dict(row)
    
    # Check if active
    if not row_dict["is_active"]:
        return {"valid": False, "error": "تم تعطيل هذا الاشتراك"}
    
    # Check expiration
    if row_dict.get("expires_at"):
        if isinstance(row_dict["expires_at"], str):
            expires_at = datetime.fromisoformat(row_dict["expires_at"].replace('Z', '+00:00'))
        elif hasattr(row_dict["expires_at"], 'isoformat'):
            expires_at = row_dict["expires_at"]
        else:
            expires_at = datetime.fromisoformat(str(row_dict["expires_at"]))
        
        if datetime.now() > expires_at:
            return {"valid": False, "error": "انتهت صلاحية الاشتراك"}
    
    # Check daily rate limit
    today = datetime.now().date()
    last_request_date = None
    if row_dict.get("last_request_date"):
        if isinstance(row_dict["last_request_date"], str):
            last_request_date = datetime.fromisoformat(row_dict["last_request_date"].split('T')[0]).date()
        elif hasattr(row_dict["last_request_date"], 'date'):
            last_request_date = row_dict["last_request_date"].date()
        else:
            last_request_date = datetime.fromisoformat(str(row_dict["last_request_date"]).split('T')[0]).date()
    
    if last_request_date == today:
        if row_dict.get("requests_today", 0) >= row_dict.get("max_requests_per_day", 0):
            return {"valid": False, "error": "تم تجاوز الحد اليومي للطلبات"}
    
    # Prepare result
    expires_at_str = None
    if row_dict.get("expires_at"):
        if isinstance(row_dict["expires_at"], str):
            expires_at_str = row_dict["expires_at"]
        elif hasattr(row_dict["expires_at"], 'isoformat'):
            expires_at_str = row_dict["expires_at"].isoformat()
        else:
            expires_at_str = str(row_dict["expires_at"])
    
    result = {
        "valid": True,
        "license_id": row_dict["id"],
        "company_name": row_dict["company_name"],
        "created_at": str(row_dict["created_at"]) if row_dict.get("created_at") else None,
        "expires_at": expires_at_str,
        "is_trial": bool(row_dict.get("is_trial")),
        "referral_code": row_dict.get("referral_code"),
        "referral_count": row_dict.get("referral_count", 0),
        "username": row_dict.get("username"),
        "requests_remaining": row_dict.get("max_requests_per_day", 0) - (
            row_dict.get("requests_today", 0) if last_request_date == today else 0
        )
    }
    
    # Cache the result (5 minutes TTL)
    try:
        from cache import cache_license_validation
        await cache_license_validation(key, result, ttl=300)
    except ImportError:
        pass
    
    return result


async def increment_usage(license_id: int, action_type: str, input_preview: str = None):
    """Increment usage counter and log the action"""
    today = datetime.now().date().isoformat()
    
    if DB_TYPE == "postgresql" and POSTGRES_AVAILABLE:
        conn = await asyncpg.connect(DATABASE_URL)
        try:
            # Update request counter
            await conn.execute("""
                UPDATE license_keys 
                SET requests_today = CASE 
                    WHEN last_request_date = $1 THEN requests_today + 1 
                    ELSE 1 
                END,
                last_request_date = $1
                WHERE id = $2
            """, today, license_id)
            
            # Log the usage
            await conn.execute("""
                INSERT INTO usage_logs (license_key_id, action_type, input_preview)
                VALUES ($1, $2, $3)
            """, license_id, action_type, input_preview[:200] if input_preview else None)
        finally:
            await conn.close()
    else:
        async with aiosqlite.connect(DATABASE_PATH) as db:
            # Update request counter
            await db.execute("""
                UPDATE license_keys 
                SET requests_today = CASE 
                    WHEN last_request_date = ? THEN requests_today + 1 
                    ELSE 1 
                END,
                last_request_date = ?
                WHERE id = ?
            """, (today, today, license_id))
            
            # Log the usage
            await db.execute("""
                INSERT INTO usage_logs (license_key_id, action_type, input_preview)
                VALUES (?, ?, ?)
            """, (license_id, action_type, input_preview[:200] if input_preview else None))
            
            await db.commit()


async def save_crm_entry(
    license_id: int,
    sender_name: str,
    sender_contact: str,
    message_type: str,
    intent: str,
    extracted_data: str,
    original_message: str,
    draft_response: str
) -> int:
    """Save a CRM entry and return its ID"""
    if DB_TYPE == "postgresql" and POSTGRES_AVAILABLE:
        conn = await asyncpg.connect(DATABASE_URL)
        try:
            result = await conn.fetchval("""
                INSERT INTO crm_entries 
                (license_key_id, sender_name, sender_contact, message_type, intent, 
                 extracted_data, original_message, draft_response)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                RETURNING id
            """, license_id, sender_name, sender_contact, message_type, intent,
                  extracted_data, original_message, draft_response)
            return result
        finally:
            await conn.close()
    else:
        async with aiosqlite.connect(DATABASE_PATH) as db:
            cursor = await db.execute("""
                INSERT INTO crm_entries 
                (license_key_id, sender_name, sender_contact, message_type, intent, 
                 extracted_data, original_message, draft_response)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (license_id, sender_name, sender_contact, message_type, intent,
                  extracted_data, original_message, draft_response))
            await db.commit()
            return cursor.lastrowid


async def get_crm_entries(license_id: int, limit: int = 50) -> list:
    """Get CRM entries for a license"""
    if DB_TYPE == "postgresql" and POSTGRES_AVAILABLE:
        conn = await asyncpg.connect(DATABASE_URL)
        try:
            rows = await conn.fetch("""
                SELECT * FROM crm_entries 
                WHERE license_key_id = $1 
                ORDER BY created_at DESC 
                LIMIT $2
            """, license_id, limit)
            return [dict(row) for row in rows]
        finally:
            await conn.close()
    else:
        async with aiosqlite.connect(DATABASE_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("""
                SELECT * FROM crm_entries 
                WHERE license_key_id = ? 
                ORDER BY created_at DESC 
                LIMIT ?
            """, (license_id, limit)) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]


async def get_entry_by_id(entry_id: int, license_id: int) -> Optional[dict]:
    """Get a specific CRM entry"""
    if DB_TYPE == "postgresql" and POSTGRES_AVAILABLE:
        conn = await asyncpg.connect(DATABASE_URL)
        try:
            row = await conn.fetchrow("""
                SELECT * FROM crm_entries 
                WHERE id = $1 AND license_key_id = $2
            """, entry_id, license_id)
            return dict(row) if row else None
        finally:
            await conn.close()
    else:
        async with aiosqlite.connect(DATABASE_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("""
                SELECT * FROM crm_entries 
                WHERE id = ? AND license_key_id = ?
            """, (entry_id, license_id)) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None


# Initialize demo license key for testing
async def create_demo_license():
    """Create a demo license key if none exists"""
    if DB_TYPE == "postgresql" and POSTGRES_AVAILABLE:
        conn = await asyncpg.connect(DATABASE_URL)
        try:
            count = await conn.fetchval("SELECT COUNT(*) FROM license_keys")
        finally:
            await conn.close()
    else:
        async with aiosqlite.connect(DATABASE_PATH) as db:
            async with db.execute("SELECT COUNT(*) FROM license_keys") as cursor:
                count = (await cursor.fetchone())[0]
    
    if count == 0:
        # Create demo license
        demo_key = await generate_license_key(
            company_name="شركة تجريبية",
            days_valid=365,
            max_requests=1000
        )
        print(f"Demo License Key Created: {demo_key}")
        return demo_key
    return None


async def get_customer(contact: str) -> Optional[dict]:
    """Get customer details by contact (SQLite only for now for simplicity)"""
    # Assuming SQLite for tools MVP
    if DB_TYPE != "postgresql":
        async with aiosqlite.connect(DATABASE_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM customers WHERE contact = ?", (contact,)) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None
    return None

async def get_order_by_ref(order_ref: str) -> Optional[dict]:
    """Get order details by reference"""
    if DB_TYPE != "postgresql":
        async with aiosqlite.connect(DATABASE_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM orders WHERE order_ref = ?", (order_ref,)) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None
    return None

async def upsert_customer_lead(name: str, contact: str, notes: str) -> int:
    """Create or update a customer lead"""
    if DB_TYPE != "postgresql":
        async with aiosqlite.connect(DATABASE_PATH) as db:
            # Check if exists
            async with db.execute("SELECT id FROM customers WHERE contact = ?", (contact,)) as cursor:
                row = await cursor.fetchone()
                
            if row:
                # Update notes
                await db.execute("UPDATE customers SET notes = notes || '\n' || ? WHERE contact = ?", (notes, contact))
                await db.commit()
                return row[0]
            else:
                # Insert
                cursor = await db.execute("""
                    INSERT INTO customers (name, contact, type, notes) 
                    VALUES (?, ?, 'Lead', ?)
                """, (name, contact, notes))
                await db.commit()
                return cursor.lastrowid
    return 0


async def save_update_event(
    event: str,
    from_build: int,
    to_build: int,
    device_id: Optional[str] = None,
    device_type: Optional[str] = None,
    license_key: Optional[str] = None
):
    """Save an update event to the database"""
    if DB_TYPE == "postgresql" and POSTGRES_AVAILABLE:
        conn = await asyncpg.connect(DATABASE_URL)
        try:
            await conn.execute("""
                INSERT INTO update_events 
                (event, from_build, to_build, device_id, device_type, license_key)
                VALUES ($1, $2, $3, $4, $5, $6)
            """, event, from_build, to_build, device_id, device_type, license_key)
        finally:
            await conn.close()
    else:
        async with aiosqlite.connect(DATABASE_PATH) as db:
            await db.execute("""
                INSERT INTO update_events 
                (event, from_build, to_build, device_id, device_type, license_key)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (event, from_build, to_build, device_id, device_type, license_key))
            await db.commit()


async def get_update_events(limit: int = 100) -> list:
    """Get recent update events"""
    if DB_TYPE == "postgresql" and POSTGRES_AVAILABLE:
        conn = await asyncpg.connect(DATABASE_URL)
        try:
            rows = await conn.fetch("""
                SELECT * FROM update_events 
                ORDER BY timestamp DESC 
                LIMIT $1
            """, limit)
            return [dict(row) for row in rows]
        finally:
            await conn.close()
    else:
        async with aiosqlite.connect(DATABASE_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("""
                SELECT * FROM update_events 
                ORDER BY timestamp DESC 
                LIMIT ?
            """, (limit,)) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]


# ============ App Config & Versioning ============

async def get_app_config(key: str) -> Optional[str]:
    """Get a configuration value by key"""
    if DB_TYPE == "postgresql" and POSTGRES_AVAILABLE:
        conn = await asyncpg.connect(DATABASE_URL)
        try:
            return await conn.fetchval("SELECT value FROM app_config WHERE key = $1", key)
        finally:
            await conn.close()
    else:
        async with aiosqlite.connect(DATABASE_PATH) as db:
            async with db.execute("SELECT value FROM app_config WHERE key = ?", (key,)) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else None


async def set_app_config(key: str, value: str):
    """Set a configuration value"""
    if DB_TYPE == "postgresql" and POSTGRES_AVAILABLE:
        conn = await asyncpg.connect(DATABASE_URL)
        try:
            await conn.execute("""
                INSERT INTO app_config (key, value, updated_at) 
                VALUES ($1, $2, NOW())
                ON CONFLICT (key) DO UPDATE 
                SET value = EXCLUDED.value, updated_at = NOW()
            """, key, value)
        finally:
            await conn.close()
    else:
        async with aiosqlite.connect(DATABASE_PATH) as db:
            await db.execute("""
                INSERT INTO app_config (key, value, updated_at) 
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(key) DO UPDATE 
                SET value = excluded.value, updated_at = CURRENT_TIMESTAMP
            """, (key, value))
            await db.commit()


async def add_version_history(
    version: str,
    build_number: int,
    changelog_ar: str,
    changelog_en: str,
    changes_json: str
):
    """Add a new version to history"""
    if DB_TYPE == "postgresql" and POSTGRES_AVAILABLE:
        conn = await asyncpg.connect(DATABASE_URL)
        try:
            await conn.execute("""
                INSERT INTO version_history 
                (version, build_number, changelog_ar, changelog_en, changes_json)
                VALUES ($1, $2, $3, $4, $5)
            """, version, build_number, changelog_ar, changelog_en, changes_json)
        finally:
            await conn.close()
    else:
        async with aiosqlite.connect(DATABASE_PATH) as db:
            await db.execute("""
                INSERT INTO version_history 
                (version, build_number, changelog_ar, changelog_en, changes_json)
                VALUES (?, ?, ?, ?, ?)
            """, (version, build_number, changelog_ar, changelog_en, changes_json))
            await db.commit()


async def get_version_history_list(limit: int = 10) -> list:
    """Get recent version history"""
    if DB_TYPE == "postgresql" and POSTGRES_AVAILABLE:
        conn = await asyncpg.connect(DATABASE_URL)
        try:
            rows = await conn.fetch("""
                SELECT * FROM version_history 
                ORDER BY build_number DESC 
                LIMIT $1
            """, limit)
            return [dict(row) for row in rows]
        finally:
            await conn.close()
    else:
        async with aiosqlite.connect(DATABASE_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("""
                SELECT * FROM version_history 
                ORDER BY build_number DESC 
                LIMIT ?
            """, (limit,)) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]


# ============ Version Analytics ============

async def get_version_distribution() -> list:
    """Get distribution of users across build numbers based on update events"""
    if DB_TYPE == "postgresql" and POSTGRES_AVAILABLE:
        conn = await asyncpg.connect(DATABASE_URL)
        try:
            # Get latest build per device from update events
            rows = await conn.fetch("""
                WITH latest_builds AS (
                    SELECT DISTINCT ON (COALESCE(device_id, license_key))
                        COALESCE(device_id, license_key) as identifier,
                        from_build as build_number,
                        device_type,
                        timestamp
                    FROM update_events
                    WHERE from_build IS NOT NULL
                    ORDER BY COALESCE(device_id, license_key), timestamp DESC
                )
                SELECT 
                    build_number,
                    device_type,
                    COUNT(*) as user_count
                FROM latest_builds
                GROUP BY build_number, device_type
                ORDER BY build_number DESC
            """)
            return [dict(row) for row in rows]
        finally:
            await conn.close()
    else:
        async with aiosqlite.connect(DATABASE_PATH) as db:
            db.row_factory = aiosqlite.Row
            # SQLite version using subquery
            async with db.execute("""
                SELECT 
                    from_build as build_number,
                    device_type,
                    COUNT(DISTINCT COALESCE(device_id, license_key)) as user_count
                FROM update_events
                WHERE from_build IS NOT NULL
                GROUP BY from_build, device_type
                ORDER BY from_build DESC
            """) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]


async def get_update_funnel(days: int = 30) -> dict:
    """Get update funnel metrics (viewed -> clicked -> installed)"""
    from datetime import datetime, timedelta
    
    cutoff = datetime.now() - timedelta(days=days)
    cutoff_str = cutoff.isoformat()
    
    if DB_TYPE == "postgresql" and POSTGRES_AVAILABLE:
        conn = await asyncpg.connect(DATABASE_URL)
        try:
            row = await conn.fetchrow("""
                SELECT 
                    COUNT(*) FILTER (WHERE event = 'viewed') as views,
                    COUNT(*) FILTER (WHERE event = 'clicked_update') as clicks,
                    COUNT(*) FILTER (WHERE event = 'clicked_later') as laters,
                    COUNT(*) FILTER (WHERE event = 'installed') as installs,
                    COUNT(DISTINCT COALESCE(device_id, license_key)) as unique_devices
                FROM update_events
                WHERE timestamp >= $1
            """, cutoff)
            return dict(row) if row else {}
        finally:
            await conn.close()
    else:
        async with aiosqlite.connect(DATABASE_PATH) as db:
            async with db.execute("""
                SELECT 
                    SUM(CASE WHEN event = 'viewed' THEN 1 ELSE 0 END) as views,
                    SUM(CASE WHEN event = 'clicked_update' THEN 1 ELSE 0 END) as clicks,
                    SUM(CASE WHEN event = 'clicked_later' THEN 1 ELSE 0 END) as laters,
                    SUM(CASE WHEN event = 'installed' THEN 1 ELSE 0 END) as installs,
                    COUNT(DISTINCT COALESCE(device_id, license_key)) as unique_devices
                FROM update_events
                WHERE timestamp >= ?
            """, (cutoff_str,)) as cursor:
                row = await cursor.fetchone()
                if row:
                    return {
                        "views": row[0] or 0,
                        "clicks": row[1] or 0,
                        "laters": row[2] or 0,
                        "installs": row[3] or 0,
                        "unique_devices": row[4] or 0
                    }
                return {}


async def get_time_to_update_metrics() -> dict:
    """Calculate median and average time from update release to adoption"""
    if DB_TYPE == "postgresql" and POSTGRES_AVAILABLE:
        conn = await asyncpg.connect(DATABASE_URL)
        try:
            # Get time between 'viewed' and 'installed' events per device
            row = await conn.fetchrow("""
                WITH update_times AS (
                    SELECT 
                        COALESCE(device_id, license_key) as identifier,
                        MIN(CASE WHEN event = 'viewed' THEN timestamp END) as first_view,
                        MIN(CASE WHEN event = 'installed' THEN timestamp END) as installed_at
                    FROM update_events
                    WHERE event IN ('viewed', 'installed')
                    GROUP BY identifier
                    HAVING MIN(CASE WHEN event = 'installed' THEN timestamp END) IS NOT NULL
                )
                SELECT 
                    COUNT(*) as total_updates,
                    AVG(EXTRACT(EPOCH FROM (installed_at - first_view))) as avg_seconds,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (
                        ORDER BY EXTRACT(EPOCH FROM (installed_at - first_view))
                    ) as median_seconds
                FROM update_times
                WHERE first_view IS NOT NULL
            """)
            if row:
                return {
                    "total_updates": row["total_updates"] or 0,
                    "avg_hours": round((row["avg_seconds"] or 0) / 3600, 1),
                    "median_hours": round((row["median_seconds"] or 0) / 3600, 1)
                }
            return {"total_updates": 0, "avg_hours": 0, "median_hours": 0}
        finally:
            await conn.close()
    else:
        # SQLite doesn't have PERCENTILE_CONT, return simpler metrics
        async with aiosqlite.connect(DATABASE_PATH) as db:
            async with db.execute("""
                SELECT COUNT(DISTINCT device_id) as total_updates
                FROM update_events
                WHERE event = 'installed'
            """) as cursor:
                row = await cursor.fetchone()
                return {
                    "total_updates": row[0] if row else 0,
                    "avg_hours": 0,
                    "median_hours": 0,
                    "note": "Detailed metrics available with PostgreSQL"
                }
