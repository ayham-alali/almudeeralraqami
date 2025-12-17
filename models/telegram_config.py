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


async def get_telegram_config(license_id: int) -> Optional[dict]:
    """Get Telegram configuration for a license (SQLite & PostgreSQL compatible)."""
    async with get_db() as db:
        config = await fetch_one(
            db,
            "SELECT * FROM telegram_configs WHERE license_key_id = ?",
            [license_id],
        )
        if config and config.get("bot_token"):
            token = config["bot_token"]
            config["bot_token_masked"] = token[:10] + "..." + token[-5:]
            config.pop("bot_token", None)
        return config


# ============ Telegram Phone Sessions Functions ============

async def save_telegram_phone_session(
    license_id: int,
    phone_number: str,
    session_string: str,
    user_id: str = None,
    user_first_name: str = None,
    user_last_name: str = None,
    user_username: str = None
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
                 user_id, user_first_name, user_last_name, user_username, is_active, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, TRUE, ?, ?)
            """,
            [
                license_id,
                phone_number,
                encrypted_session,
                user_id,
                user_first_name,
                user_last_name,
                user_username,
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
    """Deactivate Telegram phone session."""
    async with get_db() as db:
        await execute_sql(
            db,
            "UPDATE telegram_phone_sessions SET is_active = FALSE WHERE license_key_id = ?",
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


async def get_whatsapp_config(license_id: int) -> Optional[dict]:
    """Get WhatsApp configuration for a license (SQLite & PostgreSQL compatible)."""
    async with get_db() as db:
        config = await fetch_one(
            db,
            "SELECT * FROM whatsapp_configs WHERE license_key_id = ? AND is_active = 1",
            [license_id],
        )
        if config and config.get("access_token"):
            token = config["access_token"]
            config["access_token_masked"] = (
                token[:10] + "..." + token[-5:] if len(token) > 15 else "***"
            )
        return config
