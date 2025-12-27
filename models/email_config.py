"""
Al-Mudeer - Email Configuration Models
Gmail OAuth 2.0 configuration and token management
"""

import os
from datetime import datetime, timezone
from typing import Optional

from db_helper import get_db, execute_sql, fetch_one, commit_db, DB_TYPE
from .base import simple_encrypt, simple_decrypt


async def save_email_config(
    license_id: int,
    email_address: str,
    access_token: str = None,
    refresh_token: str = None,
    token_expires_at: datetime = None,
    imap_server: str = "imap.gmail.com",
    smtp_server: str = "smtp.gmail.com",
    imap_port: int = 993,
    smtp_port: int = 587,
    auto_reply: bool = False,
    check_interval: int = 5
) -> int:
    """Save or update email configuration with OAuth 2.0 tokens (Gmail only)."""

    # Encrypt OAuth tokens
    encrypted_access_token = simple_encrypt(access_token) if access_token else None
    encrypted_refresh_token = simple_encrypt(refresh_token) if refresh_token else None

    # For PostgreSQL (asyncpg), pass a real datetime object.
    # For SQLite, store ISO string for readability/backward compatibility.
    if token_expires_at:
        # Normalize to naive UTC datetime for PostgreSQL, ISO for SQLite
        if token_expires_at.tzinfo is not None:
            token_expires_at = token_expires_at.astimezone(timezone.utc).replace(tzinfo=None)
        if DB_TYPE == "postgresql":
            expires_value = token_expires_at
        else:
            expires_value = token_expires_at.isoformat()
    else:
        expires_value = None

    async with get_db() as db:
        # Check if config exists
        existing = await fetch_one(
            db,
            "SELECT id FROM email_configs WHERE license_key_id = ?",
            [license_id],
        )

        if existing:
            await execute_sql(
                db,
                """
                UPDATE email_configs SET
                    email_address = ?, imap_server = ?, imap_port = ?,
                    smtp_server = ?, smtp_port = ?,
                    access_token_encrypted = ?, refresh_token_encrypted = ?,
                    token_expires_at = ?,
                    password_encrypted = ?,
                    auto_reply_enabled = ?, check_interval_minutes = ?
                WHERE license_key_id = ?
                """,
                [
                    email_address,
                    imap_server,
                    imap_port,
                    smtp_server,
                    smtp_port,
                    encrypted_access_token,
                    encrypted_refresh_token,
                    expires_value,
                    "",  # Empty string for OAuth (legacy password field)
                    auto_reply,
                    check_interval,
                    license_id,
                ],
            )
            await commit_db(db)
            return existing["id"]

        await execute_sql(
            db,
            """
            INSERT INTO email_configs 
                (license_key_id, email_address, imap_server, imap_port,
                 smtp_server, smtp_port, access_token_encrypted, refresh_token_encrypted,
                 token_expires_at, password_encrypted, auto_reply_enabled, check_interval_minutes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                license_id,
                email_address,
                imap_server,
                imap_port,
                smtp_server,
                smtp_port,
                encrypted_access_token,
                encrypted_refresh_token,
                expires_value,
                "",  # Empty string for OAuth (legacy password field)
                auto_reply,
                check_interval,
            ],
        )
        row = await fetch_one(
            db,
            """
            SELECT id FROM email_configs
            WHERE license_key_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            [license_id],
        )
        await commit_db(db)
        return row["id"] if row else 0


async def get_email_config(license_id: int, include_inactive: bool = True) -> Optional[dict]:
    """Get email configuration for a license (SQLite & PostgreSQL compatible).
    
    Args:
        license_id: The license key ID
        include_inactive: If False, only returns active configs. Default True for backward compatibility.
    """
    async with get_db() as db:
        if include_inactive:
            query = "SELECT * FROM email_configs WHERE license_key_id = ?"
        else:
            # Use DB_TYPE to handle boolean differences
            is_active_value = "TRUE" if DB_TYPE == "postgresql" else "1"
            query = f"SELECT * FROM email_configs WHERE license_key_id = ? AND is_active = {is_active_value}"
        row = await fetch_one(
            db,
            query,
            [license_id],
        )
        if row:
            # Don't return encrypted tokens
            row.pop("access_token_encrypted", None)
            row.pop("refresh_token_encrypted", None)
            row.pop("password_encrypted", None)  # Legacy field
        return row


async def get_email_oauth_tokens(license_id: int) -> Optional[dict]:
    """Get decrypted OAuth tokens for email (internal use only)."""
    async with get_db() as db:
        row = await fetch_one(
            db,
            """SELECT access_token_encrypted, refresh_token_encrypted, token_expires_at
               FROM email_configs WHERE license_key_id = ?""",
            [license_id],
        )
        if row:
            result = {}
            if row.get("access_token_encrypted"):
                result["access_token"] = simple_decrypt(row["access_token_encrypted"])
            if row.get("refresh_token_encrypted"):
                result["refresh_token"] = simple_decrypt(row["refresh_token_encrypted"])
            if row.get("token_expires_at"):
                result["token_expires_at"] = row["token_expires_at"]
            return result if result else None
    return None


async def update_email_config_settings(
    license_id: int,
    auto_reply: bool = None,
    check_interval: int = None,
    is_active: bool = None
) -> bool:
    """Update email configuration settings without changing tokens"""
    async with get_db() as db:
        updates = []
        params = []
        
        if auto_reply is not None:
            updates.append("auto_reply_enabled = ?")
            params.append(auto_reply)
        
        if check_interval is not None:
            updates.append("check_interval_minutes = ?")
            params.append(check_interval)
        
        if is_active is not None:
            updates.append("is_active = ?")
            params.append(is_active)
        
        if not updates:
            return False
        
        params.append(license_id)
        query = f"UPDATE email_configs SET {', '.join(updates)} WHERE license_key_id = ?"
        
        await execute_sql(db, query, params)
        await commit_db(db)
        return True


# Deprecated - kept for backward compatibility
async def get_email_password(license_id: int) -> Optional[str]:
    """Get decrypted email password (deprecated - use OAuth tokens instead)."""
    async with get_db() as db:
        row = await fetch_one(
            db,
            "SELECT password_encrypted FROM email_configs WHERE license_key_id = ?",
            [license_id],
        )
        if row and row.get("password_encrypted"):
            return simple_decrypt(row["password_encrypted"])
    return None
