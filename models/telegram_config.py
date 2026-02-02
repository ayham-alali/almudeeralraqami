"""
Al-Mudeer - Telegram Configuration Models
Bot configuration and phone session management (MTProto)
"""

import secrets
from datetime import datetime
from typing import Optional

from db_helper import get_db, execute_sql, fetch_one, commit_db, DB_TYPE
from .base import simple_encrypt, simple_decrypt


async def save_telegram_config(
    license_id: int,
    bot_token: str,
    bot_username: str = None,
    auto_reply: bool = False
) -> int:
    """Save or update Telegram bot configuration (SQLite & PostgreSQL compatible)."""
    webhook_secret = secrets.token_hex(16)

    async with get_db() as db:
        existing = await fetch_one(
            db,
            "SELECT id FROM telegram_configs WHERE license_key_id = ?",
            [license_id],
        )

        if existing:
            await execute_sql(
                db,
                """
                UPDATE telegram_configs SET
                    bot_token = ?, bot_username = ?, auto_reply_enabled = ?
                WHERE license_key_id = ?
                """,
                [bot_token, bot_username, auto_reply, license_id],
            )
            await commit_db(db)
            return existing["id"]

        await execute_sql(
            db,
            """
            INSERT INTO telegram_configs 
                (license_key_id, bot_token, bot_username, webhook_secret, auto_reply_enabled)
            VALUES (?, ?, ?, ?, ?)
            """,
            [license_id, bot_token, bot_username, webhook_secret, auto_reply],
        )
        row = await fetch_one(
            db,
            """
            SELECT id FROM telegram_configs
            WHERE license_key_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            [license_id],
        )
        await commit_db(db)
        return row["id"] if row else 0


async def get_telegram_config(license_id: int, include_inactive: bool = True) -> Optional[dict]:
    """Get Telegram configuration for a license (SQLite & PostgreSQL compatible).
    
    Args:
        license_id: The license key ID
        include_inactive: If False, only returns active configs. Default True for backward compatibility.
    
    Note: bot_token is masked for security in API responses.
    Use get_telegram_bot_token() for internal backend use.
    """
    async with get_db() as db:
        if include_inactive:
            query = "SELECT * FROM telegram_configs WHERE license_key_id = ?"
        else:
            # Use DB_TYPE to handle boolean differences
            is_active_value = "TRUE" if DB_TYPE == "postgresql" else "1"
            query = f"SELECT * FROM telegram_configs WHERE license_key_id = ? AND is_active = {is_active_value}"
        config = await fetch_one(
            db,
            query,
            [license_id],
        )
        if config and config.get("bot_token"):
            token = config["bot_token"]
            config["bot_token_masked"] = token[:10] + "..." + token[-5:]
            config.pop("bot_token", None)
        return config


async def update_telegram_config_settings(
    license_id: int,
    auto_reply: bool = None
) -> bool:
    """Update Telegram bot settings."""
    async with get_db() as db:
        updates = []
        params = []
        
        if auto_reply is not None:
            updates.append("auto_reply_enabled = ?")
            params.append(auto_reply)
            
        if not updates:
            return False
            
        params.append(license_id)
        query = f"UPDATE telegram_configs SET {', '.join(updates)} WHERE license_key_id = ?"
        
        await execute_sql(db, query, params)
        await commit_db(db)
        return True


async def get_telegram_bot_token(license_id: int) -> Optional[str]:
    """Get the actual bot token for internal use (e.g., API calls to Telegram).
    
    This returns the unmasked token - only use within backend code, never expose to API.
    """
    async with get_db() as db:
        # Don't filter by is_active in SQL - check in Python to avoid boolean issues
        config = await fetch_one(
            db,
            "SELECT bot_token, is_active FROM telegram_configs WHERE license_key_id = ?",
            [license_id],
        )
        if config and config.get("bot_token"):
            # Check is_active - handle both boolean and int representations
            is_active = config.get("is_active")
            if is_active in (True, 1, "1", "true", "TRUE"):
                return config["bot_token"]
        return None


# ============ Telegram Phone Sessions Functions ============

async def save_telegram_phone_session(
    license_id: int,
    phone_number: str,
    session_string: str,
    user_id: str = None,
    user_first_name: str = None,
    user_last_name: str = None,
    user_username: str = None,
    auto_reply: bool = False
) -> int:
    """Save or update Telegram phone session (MTProto)."""
    # Encrypt session data
    encrypted_session = simple_encrypt(session_string)
    
    async with get_db() as db:
        # Check if session exists
        existing = await fetch_one(
            db,
            "SELECT id FROM telegram_phone_sessions WHERE license_key_id = ?",
            [license_id],
        )
        
        now = datetime.now() if DB_TYPE == "postgresql" else datetime.now().isoformat()
        
        if existing:
            await execute_sql(
                db,
                """
                UPDATE telegram_phone_sessions SET
                    phone_number = ?,
                    session_data_encrypted = ?,
                    user_id = ?,
                    user_first_name = ?,
                    user_last_name = ?,
                    user_username = ?,
                    auto_reply_enabled = ?,
                    is_active = TRUE,
                    updated_at = ?
                WHERE license_key_id = ?
                """,
                [
                    phone_number,
                    encrypted_session,
                    user_id,
                    user_first_name,
                    user_last_name,
                    user_username,
                    auto_reply,
                    now,
                    license_id,
                ],
            )
            await commit_db(db)
            return existing["id"]
        
        await execute_sql(
            db,
            """
            INSERT INTO telegram_phone_sessions 
                (license_key_id, phone_number, session_data_encrypted,
                 user_id, user_first_name, user_last_name, user_username, is_active, auto_reply_enabled, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, TRUE, ?, ?, ?)
            """,
            [
                license_id,
                phone_number,
                encrypted_session,
                user_id,
                user_first_name,
                user_last_name,
                user_username,
                auto_reply,
                now,
                now,
            ],
        )
        row = await fetch_one(
            db,
            """
            SELECT id FROM telegram_phone_sessions
            WHERE license_key_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            [license_id],
        )
        await commit_db(db)
        return row["id"] if row else 0


async def get_telegram_phone_session(license_id: int) -> Optional[dict]:
    """Get Telegram phone session for a license (without decrypted session data)."""
    async with get_db() as db:
        row = await fetch_one(
            db,
            "SELECT * FROM telegram_phone_sessions WHERE license_key_id = ? AND is_active = TRUE",
            [license_id],
        )
        if row:
            # Don't return encrypted session data
            row.pop("session_data_encrypted", None)
            # Mask phone number for display
            if row.get("phone_number"):
                phone = row["phone_number"]
                if len(phone) > 6:
                    row["phone_number_masked"] = phone[:3] + "***" + phone[-3:]
        return row


async def get_telegram_phone_session_data(license_id: int) -> Optional[str]:
    """Get decrypted Telegram phone session string (internal use only)."""
    async with get_db() as db:
        row = await fetch_one(
            db,
            "SELECT session_data_encrypted FROM telegram_phone_sessions WHERE license_key_id = ? AND is_active = TRUE",
            [license_id],
        )
        if row and row.get("session_data_encrypted"):
            return simple_decrypt(row["session_data_encrypted"])
    return None


async def deactivate_telegram_phone_session(license_id: int) -> bool:
    """Delete Telegram phone session."""
    async with get_db() as db:
        await execute_sql(
            db,
            "DELETE FROM telegram_phone_sessions WHERE license_key_id = ?",
            [license_id],
        )
        await commit_db(db)
        return True


async def update_telegram_phone_session_sync_time(license_id: int) -> bool:
    """Update last_synced_at timestamp."""
    now = datetime.now() if DB_TYPE == "postgresql" else datetime.now().isoformat()
    
    async with get_db() as db:
        await execute_sql(
            db,
            "UPDATE telegram_phone_sessions SET last_synced_at = ? WHERE license_key_id = ?",
            [now, license_id],
        )
        await commit_db(db)
        return True


async def update_telegram_phone_session_settings(
    license_id: int,
    auto_reply: bool = None
) -> bool:
    """Update Telegram phone session settings."""
    async with get_db() as db:
        updates = []
        params = []
        
        if auto_reply is not None:
            updates.append("auto_reply_enabled = ?")
            params.append(auto_reply)
            
        if not updates:
            return False
            
        params.append(license_id)
        # Ensure we only update active sessions
        query = f"UPDATE telegram_phone_sessions SET {', '.join(updates)} WHERE license_key_id = ? AND is_active = TRUE"
        
        await execute_sql(db, query, params)
        await commit_db(db)
        return True


async def get_whatsapp_config(license_id: int) -> Optional[dict]:
    """Get WhatsApp configuration for a license (SQLite & PostgreSQL compatible)."""
    async with get_db() as db:
        # Use TRUE for PostgreSQL, 1 for SQLite
        is_active_value = "TRUE" if DB_TYPE == "postgresql" else "1"
        config = await fetch_one(
            db,
            f"SELECT * FROM whatsapp_configs WHERE license_key_id = ? AND is_active = {is_active_value}",
            [license_id],
        )
        if config and config.get("access_token"):
            token = config["access_token"]
            config["access_token_masked"] = (
                token[:10] + "..." + token[-5:] if len(token) > 15 else "***"
            )
        return config


async def update_whatsapp_config_settings(
    license_id: int,
    auto_reply: bool = None
) -> bool:
    """Update WhatsApp configuration settings."""
    async with get_db() as db:
        updates = []
        params = []
        
        if auto_reply is not None:
            updates.append("auto_reply_enabled = ?")
            params.append(auto_reply)
            
        if not updates:
            return False
            
        params.append(license_id)
        
        # Consistent is_active check
        from db_helper import DB_TYPE
        is_active_val = "TRUE" if DB_TYPE == "postgresql" else "1"
        query = f"UPDATE whatsapp_configs SET {', '.join(updates)} WHERE license_key_id = ? AND is_active = {is_active_val}"

        await execute_sql(db, query, params)
        await commit_db(db)
        return True

# ============ Telegram Entity Persistence Functions ============

async def save_telegram_entity(
    license_id: int,
    entity_id: str,
    access_hash: str,
    entity_type: str,
    username: str = None,
    phone: str = None
) -> bool:
    """Save or update persistent Telegram entity information (access_hash)."""
    async with get_db() as db:
        now = datetime.now() if DB_TYPE == "postgresql" else datetime.now().isoformat()
        
        # Try to insert first (Optimistic approach)
        try:
            await execute_sql(
                db,
                """
                INSERT INTO telegram_entities 
                    (license_key_id, entity_id, access_hash, entity_type, username, phone, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [license_id, str(entity_id), str(access_hash), entity_type, username, phone, now]
            )
        except Exception as e:
            # Check for unique constraint violation (duplicate key)
            # SQLite: "UNIQUE constraint failed"
            # Postgres: "duplicate key value violates unique constraint"
            err_msg = str(e).lower()
            if "unique" in err_msg or "duplicate key" in err_msg:
                # Race condition hit - update instead
                await execute_sql(
                    db,
                    """
                    UPDATE telegram_entities SET
                        access_hash = ?,
                        entity_type = ?,
                        username = ?,
                        phone = ?,
                        updated_at = ?
                    WHERE license_key_id = ? AND entity_id = ?
                    """,
                    [str(access_hash), entity_type, username, phone, now, license_id, str(entity_id)]
                )
            else:
                raise e
            
        await commit_db(db)
        return True


async def get_telegram_entity(license_id: int, entity_id: str) -> Optional[dict]:
    """Get persistent entity info (including access_hash) for a license and ID."""
    async with get_db() as db:
        return await fetch_one(
            db,
            "SELECT * FROM telegram_entities WHERE license_key_id = ? AND entity_id = ?",
            [license_id, str(entity_id)]
        )
