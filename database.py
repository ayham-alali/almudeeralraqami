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
        finally:
            await conn.close()
    else:
        async with aiosqlite.connect(DATABASE_PATH) as db:
            await _init_sqlite_tables(db)


async def _init_sqlite_tables(db):
    """Initialize SQLite tables"""
    # License keys table
    await db.execute("""
        CREATE TABLE IF NOT EXISTS license_keys (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            key_hash TEXT UNIQUE NOT NULL,
            company_name TEXT NOT NULL,
            contact_email TEXT,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            expires_at TIMESTAMP,
            max_requests_per_day INTEGER DEFAULT 100,
            requests_today INTEGER DEFAULT 0,
            last_request_date DATE
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
            company_name VARCHAR(255) NOT NULL,
            contact_email VARCHAR(255),
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT NOW(),
            expires_at TIMESTAMP,
            max_requests_per_day INTEGER DEFAULT 100,
            requests_today INTEGER DEFAULT 0,
            last_request_date DATE
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
    contact_email: str = None,
    days_valid: int = 365,
    max_requests: int = 100
) -> str:
    """Generate a new license key and store it in the database"""
    # Generate a readable license key format: MUDEER-XXXX-XXXX-XXXX
    raw_key = f"MUDEER-{secrets.token_hex(2).upper()}-{secrets.token_hex(2).upper()}-{secrets.token_hex(2).upper()}"
    key_hash = hash_license_key(raw_key)
    
    expires_at = datetime.now() + timedelta(days=days_valid)
    
    if DB_TYPE == "postgresql" and POSTGRES_AVAILABLE:
        conn = await asyncpg.connect(DATABASE_URL)
        try:
            await conn.execute("""
                INSERT INTO license_keys (key_hash, company_name, contact_email, expires_at, max_requests_per_day)
                VALUES ($1, $2, $3, $4, $5)
            """, key_hash, company_name, contact_email, expires_at, max_requests)
        finally:
            await conn.close()
    else:
        async with aiosqlite.connect(DATABASE_PATH) as db:
            await db.execute("""
                INSERT INTO license_keys (key_hash, company_name, contact_email, expires_at, max_requests_per_day)
                VALUES (?, ?, ?, ?, ?)
            """, (key_hash, company_name, contact_email, expires_at.isoformat(), max_requests))
            await db.commit()
    
    return raw_key


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
    if row_dict["expires_at"]:
        expires_at = datetime.fromisoformat(row_dict["expires_at"]) if isinstance(row_dict["expires_at"], str) else row_dict["expires_at"]
        if datetime.now() > expires_at:
            return {"valid": False, "error": "انتهت صلاحية الاشتراك"}
    
    # Check daily rate limit
    today = datetime.now().date().isoformat()
    if row_dict["last_request_date"] == today:
        if row_dict["requests_today"] >= row_dict["max_requests_per_day"]:
            return {"valid": False, "error": "تم تجاوز الحد اليومي للطلبات"}
    
    # Prepare result
    result = {
        "valid": True,
        "license_id": row_dict["id"],
        "company_name": row_dict["company_name"],
        "expires_at": row_dict["expires_at"],
        "requests_remaining": row_dict["max_requests_per_day"] - (
            row_dict["requests_today"] if row_dict["last_request_date"] == today else 0
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
            contact_email="demo@example.com",
            days_valid=365,
            max_requests=1000
        )
        print(f"Demo License Key Created: {demo_key}")
        return demo_key
    return None
