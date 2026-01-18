"""
Al-Mudeer - User Preferences Model
Handles user settings, tone, language, and notification preferences.
"""

import json
from typing import Optional, List, Union
from db_helper import get_db, execute_sql, fetch_one, commit_db, DB_TYPE
from logging_config import get_logger

logger = get_logger(__name__)

async def get_preferences(license_id: int) -> dict:
    """
    Get user preferences.
    Handles backward compatibility for preferred_languages (CSV vs JSON).
    """
    async with get_db() as db:
        row = await fetch_one(
            db,
            "SELECT * FROM user_preferences WHERE license_key_id = ?",
            [license_id]
        )
        if row:
            prefs = dict(row)
            # CRITICAL: Ensure notifications_enabled defaults to True if NULL in DB
            if prefs.get("notifications_enabled") is None:
                prefs["notifications_enabled"] = True
            
            # Smart handling of preferred_languages
            # It might be stored as "ar,en" (Legacy) or "[\"ar\", \"en\"]" (New JSON)
            raw_langs = prefs.get("preferred_languages")
            if raw_langs:
                try:
                    if raw_langs.strip().startswith("["):
                        # It's likely JSON
                        prefs["preferred_languages"] = json.loads(raw_langs)
                    else:
                        # Fallback to CSV splitting
                        prefs["preferred_languages"] = [l.strip() for l in raw_langs.split(",") if l.strip()]
                except Exception:
                    # On error, treat as simple string or empty list
                    prefs["preferred_languages"] = [str(raw_langs)]
            
            return prefs

        # Create default preferences including AI tone defaults
        # We store defaults as JSON for consistency with new standard
        default_langs_json = json.dumps(["ar"])
        
        await execute_sql(
            db,
            """
            INSERT INTO user_preferences (
                license_key_id,
                tone,
                language,
                preferred_languages,
                notifications_enabled
            ) VALUES (?, 'formal', 'ar', ?, ?)
            """,
            [license_id, default_langs_json, True]
        )
        await commit_db(db)

        return {
            "license_key_id": license_id,
            "dark_mode": False,
            "notifications_enabled": True,
            "notification_sound": True,
            "auto_reply_delay_seconds": 30,
            "language": "ar",
            "onboarding_completed": False,
            "tone": "formal",
            "custom_tone_guidelines": None,
            "preferred_languages": ["ar"], # Return list directly
            "reply_length": None,
            "formality_level": None,
        }


async def update_preferences(license_id: int, **kwargs) -> bool:
    """
    Update user preferences.
    automatically serializes lists to JSON strings.
    """
    allowed = [
        'dark_mode',
        'notifications_enabled',
        'notification_sound',
        'auto_reply_delay_seconds',
        'onboarding_completed',
        # AI / workspace tone & business profile
        'tone',
        'custom_tone_guidelines',
        'preferred_languages',
        'reply_length',
        'formality_level',
    ]
    updates = {k: v for k, v in kwargs.items() if k in allowed}
    
    logger.info(f"update_preferences called: license_id={license_id}, updates={updates}")
    
    if not updates:
        logger.info("No updates to apply")
        return False
        
    # Pre-process updates: Serialize lists to JSON
    for k, v in updates.items():
        if k == 'preferred_languages' and isinstance(v, list):
            updates[k] = json.dumps(v)
    
    set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
    update_values = list(updates.values())

    async with get_db() as db:
        # Use UPSERT pattern - INSERT with ON CONFLICT UPDATE
        # PostgreSQL and SQLite both support this syntax
        if DB_TYPE == "postgresql":
            # PostgreSQL: use EXCLUDED reference for cleaner syntax
            set_clause_pg = ", ".join(f"{k} = EXCLUDED.{k}" for k in updates.keys())
            cols = ", ".join(["license_key_id"] + list(updates.keys()))
            placeholders = ", ".join(["?"] * (1 + len(updates)))
            sql = f"""
                INSERT INTO user_preferences ({cols}) VALUES ({placeholders})
                ON CONFLICT(license_key_id) DO UPDATE SET {set_clause_pg}
                """
            logger.info(f"PostgreSQL UPSERT SQL: {sql.strip()}, params: {[license_id] + update_values}")
            await execute_sql(
                db,
                sql,
                [license_id] + update_values
            )
        else:
            # SQLite: use standard ON CONFLICT DO UPDATE SET with explicit values
            cols = ", ".join(["license_key_id"] + list(updates.keys()))
            placeholders = ", ".join(["?"] * (1 + len(updates)))
            await execute_sql(
                db,
                f"""
                INSERT INTO user_preferences ({cols}) VALUES ({placeholders})
                ON CONFLICT(license_key_id) DO UPDATE SET {set_clause}
                """,
                [license_id] + update_values + update_values  # Values for INSERT + UPDATE
            )
        await commit_db(db)
        logger.info("Preferences update completed successfully")
        return True


async def delete_preferences(license_id: int, db=None) -> bool:
    """Delete user preferences"""
    if db:
        await execute_sql(
            db,
            "DELETE FROM user_preferences WHERE license_key_id = ?",
            [license_id]
        )
        return True

    async with get_db() as new_db:
        await execute_sql(
            new_db,
            "DELETE FROM user_preferences WHERE license_key_id = ?",
            [license_id]
        )
        await commit_db(new_db)
        return True
