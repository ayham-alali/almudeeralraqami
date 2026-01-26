"""
Database migration utilities
Prepares for SQLite → PostgreSQL migration
"""

import os
import aiosqlite
from typing import List, Dict


class MigrationManager:
    """Manages database schema migrations"""
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or os.getenv("DATABASE_PATH", "almudeer.db")
        self.migrations: List[Dict] = []
        self.db_type = os.getenv("DB_TYPE", "sqlite").lower()
    
    def register_migration(self, version: int, name: str, up_sql: str, down_sql: str = None):
        """Register a migration"""
        self.migrations.append({
            "version": version,
            "name": name,
            "up": up_sql,
            "down": down_sql
        })
        self.migrations.sort(key=lambda x: x["version"])
    
    async def create_migrations_table(self):
        """Create migrations tracking table"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await db.commit()
    
    async def get_applied_migrations(self) -> List[int]:
        """Get list of applied migration versions"""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT version FROM schema_migrations ORDER BY version")
            rows = await cursor.fetchall()
            return [row[0] for row in rows]
    
    async def apply_migration(self, migration: Dict):
        """Apply a single migration"""
        async with aiosqlite.connect(self.db_path) as db:
            # Check if already applied
            cursor = await db.execute(
                "SELECT version FROM schema_migrations WHERE version = ?",
                (migration["version"],)
            )
            if await cursor.fetchone():
                return False
            
            # Special handling for version 2 (add license_key_encrypted column)
            if migration["version"] == 2:
                # Check if column already exists
                cursor = await db.execute("PRAGMA table_info(license_keys)")
                columns = await cursor.fetchall()
                column_names = [col[1] for col in columns]
                
                if "license_key_encrypted" not in column_names:
                    await db.execute("ALTER TABLE license_keys ADD COLUMN license_key_encrypted TEXT")
                    await db.commit()
            else:
                # Apply migration normally
                await db.executescript(migration["up"])
            
            # Record migration
            await db.execute(
                "INSERT INTO schema_migrations (version, name) VALUES (?, ?)",
                (migration["version"], migration["name"])
            )
            await db.commit()
            return True
    
    async def migrate(self):
        """Apply all pending migrations"""
        # Skip SQLite migrations when using PostgreSQL
        # PostgreSQL schema is managed by setup_railway_postgres.py
        if self.db_type == "postgresql":
            return 0
        
        await self.create_migrations_table()
        applied = await self.get_applied_migrations()
        
        applied_count = 0
        for migration in self.migrations:
            if migration["version"] not in applied:
                if await self.apply_migration(migration):
                    applied_count += 1
                    print(f"✅ Applied migration {migration['version']}: {migration['name']}")
        
        if applied_count == 0:
            print("✅ No pending migrations")
        
        return applied_count


# Initialize migration manager
migration_manager = MigrationManager()

# Register initial migrations
migration_manager.register_migration(
    version=1,
    name="add_database_indexes",
    up_sql="""
        CREATE INDEX IF NOT EXISTS idx_license_key_hash ON license_keys(key_hash);
        CREATE INDEX IF NOT EXISTS idx_crm_license_id ON crm_entries(license_key_id);
        CREATE INDEX IF NOT EXISTS idx_crm_created_at ON crm_entries(created_at);
        CREATE INDEX IF NOT EXISTS idx_usage_logs_license_id ON usage_logs(license_key_id);
        CREATE INDEX IF NOT EXISTS idx_license_expires_at ON license_keys(expires_at);
    """
)

# Migration to add license_key_encrypted column
migration_manager.register_migration(
    version=2,
    name="add_license_key_encrypted_column",
    up_sql="""
        -- Add license_key_encrypted column if it doesn't exist
        -- SQLite doesn't support IF NOT EXISTS for ALTER TABLE ADD COLUMN
        -- So we'll use a try-catch approach in the migration manager
        -- For PostgreSQL, we'll handle it separately
    """
)

# Migration to add language and dialect columns
migration_manager.register_migration(
    version=3,
    name="add_language_and_dialect_columns",
    up_sql="""
        -- Add language and dialect columns to inbox_messages
        -- These columns are used for language analytics
    """
)

# Migration to add user_preferences columns
migration_manager.register_migration(
    version=4,
    name="add_user_preferences_columns",
    up_sql="""
        -- Add missing columns to user_preferences table
        -- These columns are needed for AI tone configuration
        -- Implementation handled by ensure_user_preferences_columns on startup as it is complex
    """
)

# Migration to add device_id to fcm_tokens
migration_manager.register_migration(
    version=5,
    name="add_device_id_to_fcm_tokens",
    up_sql="""
        -- Add device_id column to fcm_tokens table
    """
)



async def ensure_inbox_columns():
    """Ensure inbox_messages has language and dialect columns (run on startup)."""
    from db_helper import get_db, execute_sql, commit_db, DB_TYPE
    
    async with get_db() as db:
        if DB_TYPE == "postgresql":
            # PostgreSQL - check if column exists and add if not
            try:
                await execute_sql(db, """
                    ALTER TABLE inbox_messages ADD COLUMN IF NOT EXISTS language TEXT
                """)
                await execute_sql(db, """
                    ALTER TABLE inbox_messages ADD COLUMN IF NOT EXISTS dialect TEXT
                """)
                await execute_sql(db, """
                    ALTER TABLE inbox_messages ADD COLUMN IF NOT EXISTS attachments TEXT
                """)
                await commit_db(db)
            except Exception as e:
                # Column might already exist
                pass
        else:
            # SQLite - try to add column, ignore error if exists
            try:
                await execute_sql(db, "ALTER TABLE inbox_messages ADD COLUMN language TEXT")
                await commit_db(db)
            except:
                pass
            try:
                await execute_sql(db, "ALTER TABLE inbox_messages ADD COLUMN dialect TEXT")
                await commit_db(db)
            except:
                pass
            try:
                await execute_sql(db, "ALTER TABLE inbox_messages ADD COLUMN attachments TEXT")
                await commit_db(db)
            except:
                pass
        
        # Ensure is_read column exists (Boolean)
        if DB_TYPE == "postgresql":
            try:
                await execute_sql(db, """
                    ALTER TABLE inbox_messages ADD COLUMN IF NOT EXISTS is_read BOOLEAN DEFAULT FALSE
                """)
                await commit_db(db)
            except Exception as e:
                pass
        else:
            try:
                await execute_sql(db, "ALTER TABLE inbox_messages ADD COLUMN is_read BOOLEAN DEFAULT 0")
                await commit_db(db)
            except:
                pass


async def ensure_outbox_columns():
    """Ensure outbox_messages has required columns (run on startup)."""
    from db_helper import get_db, execute_sql, commit_db, DB_TYPE
    
    async with get_db() as db:
        if DB_TYPE == "postgresql":
            try:
                await execute_sql(db, "ALTER TABLE outbox_messages ADD COLUMN IF NOT EXISTS attachments TEXT")
                await commit_db(db)
            except:
                pass
        else:
            try:
                await execute_sql(db, "ALTER TABLE outbox_messages ADD COLUMN attachments TEXT")
                await commit_db(db)
            except:
                pass


async def ensure_user_preferences_columns():
    """Ensure user_preferences has all required columns (run on startup)."""
    from db_helper import get_db, execute_sql, commit_db, DB_TYPE
    from logging_config import get_logger
    
    logger = get_logger(__name__)
    
    # List of columns that should exist in user_preferences
    columns_to_add = [
        ("tone", "TEXT DEFAULT 'formal'"),
        ("custom_tone_guidelines", "TEXT"),
        ("business_name", "TEXT"),
        ("industry", "TEXT"),
        ("products_services", "TEXT"),
        ("preferred_languages", "TEXT"),
        ("reply_length", "TEXT"),
        ("formality_level", "TEXT"),
    ]
    
    async with get_db() as db:
        for col_name, col_type in columns_to_add:
            try:
                if DB_TYPE == "postgresql":
                    await execute_sql(db, f"""
                        ALTER TABLE user_preferences ADD COLUMN IF NOT EXISTS {col_name} {col_type}
                    """)
                else:
                    # SQLite - try to add column, ignore error if exists
                    await execute_sql(db, f"ALTER TABLE user_preferences ADD COLUMN {col_name} {col_type}")
                await commit_db(db)
            except Exception as e:
                # Column already exists or other error - log and continue
                if "duplicate column" not in str(e).lower() and "already exists" not in str(e).lower():
                    logger.debug(f"Note: user_preferences.{col_name} check: {e}")
                pass
    
async def ensure_inbox_conversations_pk():
    """Ensure inbox_conversations has a primary key on (license_key_id, sender_contact)."""
    from db_helper import get_db, execute_sql, commit_db, DB_TYPE
    from logging_config import get_logger
    
    logger = get_logger(__name__)
    
    async with get_db() as db:
        if DB_TYPE == "postgresql":
            try:
                # Check if PK exists
                # This query is robust for PostgreSQL
                row = await db.fetchrow("""
                    SELECT 1
                    FROM pg_index i
                    JOIN pg_class c ON c.oid = i.indrelid
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relname = 'inbox_conversations'
                    AND n.nspname = 'public'
                    AND i.indisprimary
                """)
                
                if not row:
                    logger.info("Adding missing Primary Key to inbox_conversations...")
                    # 1. Cleanup duplicates just in case
                    await execute_sql(db, """
                        DELETE FROM inbox_conversations a USING (
                            SELECT MIN(ctid) as ctid, license_key_id, sender_contact
                            FROM inbox_conversations 
                            GROUP BY license_key_id, sender_contact HAVING COUNT(*) > 1
                        ) b
                        WHERE a.license_key_id = b.license_key_id 
                        AND a.sender_contact = b.sender_contact 
                        AND a.ctid <> b.ctid
                    """)
                    
                    # 2. Add PK
                    await execute_sql(db, """
                        ALTER TABLE inbox_conversations 
                        ADD PRIMARY KEY (license_key_id, sender_contact)
                    """)
                    await commit_db(db)
                    logger.info("Successfully added Primary Key to inbox_conversations")
            except Exception as e:
                logger.error(f"Error ensuring inbox_conversations PK: {e}")
        else:
            # SQLite handles PK in CREATE TABLE IF NOT EXISTS in the migration script.
            # But we can verify it if we really wanted to. Usually not needed for SQLite as it was correct from start.
            pass

