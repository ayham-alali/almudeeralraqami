"""
Al-Mudeer - FCM Mobile Push Service
Handles Firebase Cloud Messaging for mobile app push notifications

Supports both:
- FCM HTTP v1 API (recommended, uses service account)
- Legacy HTTP API (deprecated fallback, uses server key)
"""

import os
import json
import time
import httpx
from typing import Optional, List, Dict, Any
from logging_config import get_logger

logger = get_logger(__name__)

# === FCM Configuration ===
# Legacy API (deprecated - will be removed by Google)
FCM_SERVER_KEY = os.getenv("FCM_SERVER_KEY")

# V1 API (recommended) - requires service account
FCM_PROJECT_ID = os.getenv("FCM_PROJECT_ID")  # Firebase project ID
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")  # Path to service account JSON

# Check if v1 API is available
FCM_V1_AVAILABLE = False
_cached_access_token = None
_token_expiry = None

try:
    from google.oauth2 import service_account
    from google.auth.transport.requests import Request
    import datetime
    
    creds_valid = False
    if GOOGLE_APPLICATION_CREDENTIALS:
        if os.path.exists(GOOGLE_APPLICATION_CREDENTIALS):
            creds_valid = True
        elif GOOGLE_APPLICATION_CREDENTIALS.strip().startswith("{"):
            creds_valid = True
            
    if not creds_valid and os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON"):
        creds_valid = True

    FCM_V1_AVAILABLE = bool(FCM_PROJECT_ID and creds_valid)
    
    if FCM_V1_AVAILABLE:
        logger.info(f"FCM: v1 API configured for project '{FCM_PROJECT_ID}'")
    else:
        if not FCM_PROJECT_ID:
            logger.info("FCM: FCM_PROJECT_ID not set, will use legacy API")
        if not creds_valid:
            logger.info("FCM: GOOGLE_APPLICATION_CREDENTIALS not set or invalid, will use legacy API")
except ImportError:
    logger.warning("FCM: google-auth not installed. Install with: pip install google-auth")
    logger.info("FCM: Will use legacy API if FCM_SERVER_KEY is set")


def _get_access_token() -> Optional[str]:
    """Get OAuth2 access token for FCM v1 API."""
    global _cached_access_token, _token_expiry
    
    if not FCM_V1_AVAILABLE:
        return None
    
    try:
        from google.oauth2 import service_account
        from google.auth.transport.requests import Request
        import datetime
        
        # Check if cached token is still valid (with 5 min buffer)
        if _cached_access_token and _token_expiry:
            if datetime.datetime.utcnow() < _token_expiry - datetime.timedelta(minutes=5):
                return _cached_access_token
        
        # Get credentials - try file first, then JSON env var
        credentials = None
        
        # Option 1: File path
        if GOOGLE_APPLICATION_CREDENTIALS and os.path.exists(GOOGLE_APPLICATION_CREDENTIALS):
            credentials = service_account.Credentials.from_service_account_file(
                GOOGLE_APPLICATION_CREDENTIALS,
                scopes=['https://www.googleapis.com/auth/firebase.messaging']
            )
            logger.debug("FCM: Using credentials from file")

        # Option 2: JSON content in GOOGLE_APPLICATION_CREDENTIALS (Railway/Docker)
        elif GOOGLE_APPLICATION_CREDENTIALS and GOOGLE_APPLICATION_CREDENTIALS.strip().startswith("{"):
            import json as json_module
            service_account_info = json_module.loads(GOOGLE_APPLICATION_CREDENTIALS)
            credentials = service_account.Credentials.from_service_account_info(
                service_account_info,
                scopes=['https://www.googleapis.com/auth/firebase.messaging']
            )
            logger.debug("FCM: Using credentials from GOOGLE_APPLICATION_CREDENTIALS env var")
        
        # Option 3: JSON content in dedicated env var
        elif os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON"):
            import json as json_module
            service_account_info = json_module.loads(os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON"))
            credentials = service_account.Credentials.from_service_account_info(
                service_account_info,
                scopes=['https://www.googleapis.com/auth/firebase.messaging']
            )
            logger.debug("FCM: Using credentials from JSON env var")
        
        if not credentials:
            logger.warning("FCM: No valid credentials found")
            return None
        
        credentials.refresh(Request())
        
        # Log the identity we are using
        try:
            if hasattr(credentials, 'service_account_email'):
                logger.info(f"FCM Auth: Using service account email: {credentials.service_account_email}")
            if hasattr(credentials, 'project_id'):
                logger.info(f"FCM Auth: Using project_id from credentials: {credentials.project_id}")
                if credentials.project_id != FCM_PROJECT_ID:
                    logger.warning(f"FCM Auth MISMATCH: Credentials project ({credentials.project_id}) != FCM_PROJECT_ID ({FCM_PROJECT_ID})")
        except Exception as e:
            logger.warning(f"FCM Auth: Could not log credential details: {e}")

        _cached_access_token = credentials.token
        _token_expiry = credentials.expiry
        
        logger.debug("FCM: OAuth2 access token refreshed")
        return _cached_access_token
        
    except Exception as e:
        logger.error(f"FCM: Failed to get access token: {e}")
        return None


async def ensure_fcm_tokens_table():
    """Ensure fcm_tokens table exists."""
    from db_helper import get_db, execute_sql, commit_db, DB_TYPE
    
    id_type = "SERIAL PRIMARY KEY" if DB_TYPE == "postgresql" else "INTEGER PRIMARY KEY AUTOINCREMENT"
    ts_default = "TIMESTAMP DEFAULT NOW()" if DB_TYPE == "postgresql" else "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
    
    async with get_db() as db:
        try:
            await execute_sql(db, f"""
                CREATE TABLE IF NOT EXISTS fcm_tokens (
                    id {id_type},
                    license_key_id INTEGER NOT NULL,
                    token TEXT NOT NULL,
                    device_id TEXT,
                    platform TEXT DEFAULT 'android',
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at {ts_default},
                    updated_at TIMESTAMP,
                    FOREIGN KEY (license_key_id) REFERENCES license_keys(id)
                )
            """)
            
            await execute_sql(db, """
                CREATE INDEX IF NOT EXISTS idx_fcm_token
                ON fcm_tokens(token)
            """)
            
            await execute_sql(db, """
                CREATE INDEX IF NOT EXISTS idx_fcm_license
                ON fcm_tokens(license_key_id) 
            """)
            
            await execute_sql(db, """
                CREATE INDEX IF NOT EXISTS idx_fcm_device_id
                ON fcm_tokens(device_id)
            """)

            await commit_db(db)
            logger.info("FCM: fcm_tokens table verified")
        except Exception as e:
            logger.error(f"FCM: Verify table failed: {e}")

async def ensure_device_id_column():
    """Ensure device_id column exists (migration helper)."""
    from db_helper import get_db, execute_sql, commit_db, DB_TYPE
    
    async with get_db() as db:
        try:
            if DB_TYPE == "postgresql":
                await execute_sql(db, "ALTER TABLE fcm_tokens ADD COLUMN IF NOT EXISTS device_id TEXT")
                await execute_sql(db, "CREATE INDEX IF NOT EXISTS idx_fcm_device_id ON fcm_tokens(device_id)")
            else:
                try:
                    await execute_sql(db, "ALTER TABLE fcm_tokens ADD COLUMN device_id TEXT")
                    await execute_sql(db, "CREATE INDEX IF NOT EXISTS idx_fcm_device_id ON fcm_tokens(device_id)")
                except:
                    pass
            await commit_db(db)
        except Exception as e:
            logger.error(f"FCM: Failed to add device_id column: {e}")


async def save_fcm_token(
    license_id: int,
    token: str,
    platform: str = "android",
    device_id: Optional[str] = None
) -> int:
    """
    Save a new FCM token for a license.
    Uses device_id for deduplication if provided.
    """
    from db_helper import get_db, fetch_one, execute_sql, commit_db
    
    async with get_db() as db:
        existing = None
        
        # Strategy 1: Match by device_id (preferred)
        if device_id:
            existing = await fetch_one(
                db,
                "SELECT id FROM fcm_tokens WHERE device_id = ? AND license_key_id = ?",
                [device_id, license_id]
            )
            
        # Strategy 2: Match by token (fallback/migration)
        if not existing:
             existing = await fetch_one(
                db,
                "SELECT id FROM fcm_tokens WHERE token = ?",
                [token]
            )
        
        if existing:
            # Update existing token
            await execute_sql(
                db,
                """
                UPDATE fcm_tokens 
                SET license_key_id = ?, token = ?, platform = ?, device_id = ?, is_active = TRUE, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                [license_id, token, platform, device_id, existing["id"]]
            )
            
            # Aggressive cleanup: If we have a device_id, deactivate all tokens for this license that DON'T have a device_id
            if device_id:
                await execute_sql(
                    db,
                    "UPDATE fcm_tokens SET is_active = FALSE WHERE license_key_id = ? AND device_id IS NULL",
                    [license_id]
                )
            
            await commit_db(db)
            logger.info(f"FCM: Token updated for license {license_id} (device_id: {device_id})")
            return existing["id"]
        
        # Create new token
        try:
            await execute_sql(
                db,
                """
                INSERT INTO fcm_tokens (license_key_id, token, platform, device_id)
                VALUES (?, ?, ?, ?)
                """,
                [license_id, token, platform, device_id]
            )
        except Exception as e:
            # Handle race condition: if another request inserted this token simultaneously
            if "duplicate" in str(e).lower() or "unique" in str(e).lower():
                logger.debug(f"FCM: Race condition detected for token {token[:20]}..., falling back to update")
                # Re-fetch the ID to update the existing record
                if device_id:
                    row = await fetch_one(db, "SELECT id FROM fcm_tokens WHERE device_id = ? AND license_key_id = ?", [device_id, license_id])
                else:
                    row = await fetch_one(db, "SELECT id FROM fcm_tokens WHERE token = ?", [token])
                
                if row:
                    await execute_sql(
                        db,
                        "UPDATE fcm_tokens SET license_key_id = ?, token = ?, platform = ?, device_id = ?, is_active = TRUE, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                        [license_id, token, platform, device_id, row["id"]]
                    )
                    await commit_db(db)
                    return row["id"]
            
            # If it's another error, re-raise it
            raise e
        
        # Aggressive cleanup for new tokens too
        if device_id:
             await execute_sql(
                db,
                "UPDATE fcm_tokens SET is_active = FALSE WHERE license_key_id = ? AND device_id IS NULL",
                [license_id]
            )
        
        # Return ID
        if device_id:
             row = await fetch_one(db, "SELECT id FROM fcm_tokens WHERE device_id = ? AND license_key_id = ?", [device_id, license_id])
        else:
             row = await fetch_one(db, "SELECT id FROM fcm_tokens WHERE token = ?", [token])

        await commit_db(db)
        logger.info(f"FCM: New token registered for license {license_id}")
        return row["id"] if row else 0


async def remove_fcm_token(token: str) -> bool:
    """Remove an FCM token."""
    from db_helper import get_db, execute_sql, commit_db
    
    async with get_db() as db:
        await execute_sql(
            db,
            "DELETE FROM fcm_tokens WHERE token = ?",
            [token]
        )
        await commit_db(db)
        logger.info(f"FCM: Token removed")
        return True


async def send_fcm_notification(
    token: str,
    title: str,
    body: str,
    data: Optional[dict] = None,
    link: Optional[str] = None,
    badge_count: int = 1,
    ttl_seconds: int = 86400,  # Default: 24 hours
    sound: str = "default",  # Customizable notification sound
    image: Optional[str] = None  # Notification image URL
) -> bool:
    """
    Send push notification to a single FCM token.
    
    Uses FCM HTTP v1 API if configured (recommended), 
    otherwise falls back to legacy HTTP API (deprecated).
    
    Args:
        token: FCM device token
        title: Notification title
        body: Notification body
        data: Optional custom data payload
        link: Optional deep link URL
        badge_count: iOS badge count (default: 1)
        ttl_seconds: Time-to-live in seconds (default: 24 hours)
        sound: Notification sound name (default: "default")
    """
    # Try v1 API first
    if FCM_V1_AVAILABLE:
        result = await _send_fcm_v1(token, title, body, data, link, badge_count, ttl_seconds, sound, image)
        if result is not None:  # None means v1 failed, try legacy
            return result
    
    # Fallback to legacy API
    return await _send_fcm_legacy(token, title, body, data, link, badge_count, ttl_seconds, sound, image)


async def _send_fcm_v1(
    token: str,
    title: str,
    body: str,
    data: Optional[dict] = None,
    link: Optional[str] = None,
    badge_count: int = 1,
    ttl_seconds: int = 86400,
    sound: str = "default",
    image: Optional[str] = None
) -> Optional[bool]:
    """
    Send notification via FCM HTTP v1 API.
    Returns None if v1 API is unavailable or failed (to trigger fallback).
    """
    access_token = _get_access_token()
    if not access_token:
        return None
    
    try:
        # Build v1 API payload
        message_data = data.copy() if data else {}
        if link:
            message_data["link"] = link
        
        if image:
             message_data["sender_image"] = image
        
        # Convert all data values to strings (FCM v1 requirement)
        message_data = {k: str(v) for k, v in message_data.items()}
        
        payload = {
            "message": {
                "token": token,
                "notification": {
                    "title": title,
                    "body": body
                },
                "android": {
                    "priority": "high",
                    "ttl": f"{ttl_seconds}s",  # TTL in string format with 's' suffix
                    "notification": {
                        "sound": sound,
                        "click_action": "FLUTTER_NOTIFICATION_CLICK"
                    }
                },
                "apns": {
                    "headers": {
                        "apns-expiration": str(int(time.time()) + ttl_seconds)  # Unix timestamp when notification expires
                    },
                    "payload": {
                        "aps": {
                            "sound": sound,
                            "badge": badge_count  # Dynamic badge count
                        }
                    }
                },
                "data": message_data
            }
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"https://fcm.googleapis.com/v1/projects/{FCM_PROJECT_ID}/messages:send",
                json=payload,
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json"
                },
                timeout=10.0
            )
            
            if response.status_code == 200:
                logger.info(f"FCM v1: Notification sent: {title[:30]}...")
                return True
            elif response.status_code == 404:
                # Token not found/expired - mark as failed
                logger.warning(f"FCM v1: Token not found (expired)")
                return False
            elif response.status_code == 401 or response.status_code == 403:
                # Auth error or Permission denied - clear cached token to force refresh
                global _cached_access_token
                _cached_access_token = None
                
                # Log detailed identity info to debug IAM issues
                try:
                    from google.oauth2 import service_account
                    # Re-load creds just to inspect them
                    creds = None
                    if GOOGLE_APPLICATION_CREDENTIALS and GOOGLE_APPLICATION_CREDENTIALS.strip().startswith("{"):
                        import json as json_module
                        info = json_module.loads(GOOGLE_APPLICATION_CREDENTIALS)
                        creds = service_account.Credentials.from_service_account_info(info)
                    
                    if creds and hasattr(creds, 'service_account_email'):
                        logger.error(f"FCM v1 AUTH ERROR ({response.status_code}): Using account '{creds.service_account_email}'. This account lacks 'cloudmessaging.messages.create' permission.")
                    else:
                        logger.error(f"FCM v1 AUTH ERROR ({response.status_code}): Could not determine local service account email.")
                except Exception as e:
                    logger.error(f"FCM v1: Error debugging auth identity: {e}")
                
                logger.warning(f"FCM v1: Auth/Permission failed ({response.status_code}), clearing cache and falling back")
                return None
            else:
                logger.error(f"FCM v1: HTTP error {response.status_code}: {response.text}")
                return None
                
    except Exception as e:
        logger.error(f"FCM v1: Error sending notification: {e}")
        return None


async def _send_fcm_legacy(
    token: str,
    title: str,
    body: str,
    data: Optional[dict] = None,
    link: Optional[str] = None,
    badge_count: int = 1,
    ttl_seconds: int = 86400,
    sound: str = "default",
    image: Optional[str] = None
) -> bool:
    """
    Send notification via legacy FCM HTTP API (deprecated).
    """
    if not FCM_SERVER_KEY:
        logger.warning("FCM: Neither v1 API nor legacy server key configured")
        return False
    
    try:
        payload = {
            "to": token,
            "notification": {
                "title": title,
                "body": body,
                "sound": sound,
                "click_action": "FLUTTER_NOTIFICATION_CLICK",
                "badge": badge_count,  # Dynamic badge for iOS
                "image": image
            },
            "data": data or {},
            "priority": "high",
            "time_to_live": ttl_seconds  # TTL for legacy API
        }
        
        if link:
            payload["data"]["link"] = link
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                "https://fcm.googleapis.com/fcm/send",
                json=payload,
                headers={
                    "Authorization": f"key={FCM_SERVER_KEY}",
                    "Content-Type": "application/json"
                },
                timeout=10.0
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get("success", 0) > 0:
                    logger.info(f"FCM legacy: Notification sent: {title[:30]}...")
                    return True
                else:
                    logger.warning(f"FCM legacy: Notification failed: {result}")
                    return False
            else:
                logger.error(f"FCM legacy: HTTP error {response.status_code}: {response.text}")
                return False
                
    except Exception as e:
        logger.error(f"FCM legacy: Error sending notification: {e}")
        return False


async def send_fcm_to_license(
    license_id: int,
    title: str,
    body: str,
    data: Optional[dict] = None,
    link: Optional[str] = None,
    badge_count: Optional[int] = None,  # If None, will be calculated
    ttl_seconds: int = 86400,
    sound: str = "default",  # Customizable notification sound
    image: Optional[str] = None,
    notification_id: Optional[int] = None
) -> int:
    """
    Send push notification to all mobile devices for a license.
    
    Returns the number of successful sends.
    
    Args:
        license_id: License key ID
        title: Notification title
        body: Notification body
        data: Optional custom data payload
        link: Optional deep link URL
        badge_count: iOS badge count (if None, calculates from unread notifications)
        ttl_seconds: Time-to-live in seconds (default: 24 hours)
    """
    from db_helper import get_db, fetch_all, fetch_one, execute_sql, commit_db
    
    # Check if user has notifications enabled
    try:
        from models.preferences import get_preferences
        prefs = await get_preferences(license_id)
        if not prefs.get("notifications_enabled", True):
            logger.info(f"FCM: Notifications disabled for license {license_id}, skipping push")
            return 0
    except Exception as e:
        # If we can't check preferences, proceed with sending (fail-open)
        logger.warning(f"FCM: Could not check notification preferences: {e}")
    
    sent_count = 0
    expired_ids = []
    
    # Calculate badge count from unread notifications if not provided
    if badge_count is None:
        async with get_db() as db:
            unread_row = await fetch_one(
                db,
                """
                SELECT COUNT(*) as unread_count FROM notifications
                WHERE license_key_id = ? AND is_read = FALSE
                """,
                [license_id]
            )
            badge_count = unread_row["unread_count"] if unread_row else 1
            # Ensure badge is at least 1 for new notification
            if badge_count < 1:
                badge_count = 1
    
    async with get_db() as db:
        rows = await fetch_all(
            db,
            """
            SELECT id, token, device_id, platform FROM fcm_tokens
            WHERE license_key_id = ? AND is_active = TRUE
            ORDER BY device_id DESC, updated_at DESC
            """,
            [license_id]
        )
        
        if not rows:
            return 0
        
        # Deduplicate by device_id
        # Since we order by device_id DESC, non-nulls come first (usually). 
        # Actually in SQL NULLs ordering depends on DB. 
        # We'll stick to a set-based deduplication in python.
        
        processed_device_ids = set()
        unique_tokens = []
        
        for row in rows:
            device_id = row.get("device_id")
            token = row.get("token")
            
            # If we have a device ID, ensure we haven't sent to it already
            if device_id:
                if device_id in processed_device_ids:
                    continue
                processed_device_ids.add(device_id)
            
            # Simple token deduplication (just in case)
            if any(r["token"] == token for r in unique_tokens):
                continue
                
            unique_tokens.append(row)

        for row in unique_tokens:
            # Prepare data with tracking IDs
            tracking_data = data.copy() if data else {}
            if notification_id:
                tracking_data["notification_id"] = str(notification_id)
                
                # Track delivery in database
                try:
                    from services.notification_service import track_notification_delivery
                    analytics_id = await track_notification_delivery(
                        license_id=license_id,
                        notification_id=notification_id,
                        platform=row.get("platform", "android"),
                        notification_type=tracking_data.get("type", "general")
                    )
                    tracking_data["analytics_id"] = str(analytics_id)
                except Exception as e:
                    logger.warning(f"FCM: Failed to track delivery: {e}")

            success = await send_fcm_notification(
                token=row["token"],
                title=title,
                body=body,
                data=tracking_data,
                link=link,
                badge_count=badge_count,
                ttl_seconds=ttl_seconds,
                sound=sound,
                image=image
            )
            
            if success:
                sent_count += 1
            else:
                expired_ids.append(row["id"])
        
        # Mark failed tokens as inactive
        if expired_ids:
            placeholders = ",".join("?" for _ in expired_ids)
            await execute_sql(
                db,
                f"UPDATE fcm_tokens SET is_active = FALSE WHERE id IN ({placeholders})",
                expired_ids
            )
            await commit_db(db)
            logger.info(f"FCM: Marked {len(expired_ids)} tokens as inactive")
    
    return sent_count


async def send_fcm_topic(
    topic: str,
    title: str,
    body: str,
    data: Optional[dict] = None,
    image: Optional[str] = None
) -> bool:
    """
    Send a notification to an FCM topic (efficient for broadcasts).
    """
    try:
        # Prepare data payload
        fcm_data = data.copy() if data else {}
        
        # Build FCM message for the topic
        message = messaging.Message(
            notification=messaging.Notification(
                title=title,
                body=body,
                image=image
            ),
            data=fcm_data,
            topic=topic
        )
        
        # Send via v1 API (preferred)
        messaging.send(message)
        logger.info(f"FCM: Sent notification to topic {topic}")
        return True
    except Exception as e:
        logger.error(f"FCM Topic error: {e}")
        return False


async def cleanup_expired_tokens(days_inactive: int = 30) -> int:
    """
    Cleanup FCM tokens that have been inactive for too long.
    
    Removes tokens where:
    - is_active = FALSE AND updated_at is older than days_inactive days
    - Tokens that have never been updated and are older than days_inactive days
    
    Args:
        days_inactive: Number of days of inactivity before cleanup (default: 30)
    
    Returns:
        Number of tokens deleted
    """
    from db_helper import get_db, execute_sql, fetch_one, commit_db, DB_TYPE
    from datetime import datetime, timedelta
    
    cutoff_date = datetime.utcnow() - timedelta(days=days_inactive)
    
    async with get_db() as db:
        try:
            # Count tokens to be deleted
            if DB_TYPE == "postgresql":
                count_row = await fetch_one(
                    db,
                    """
                    SELECT COUNT(*) as count FROM fcm_tokens
                    WHERE is_active = FALSE 
                    AND (updated_at < $1 OR (updated_at IS NULL AND created_at < $2))
                    """,
                    [cutoff_date, cutoff_date]
                )
            else:
                count_row = await fetch_one(
                    db,
                    """
                    SELECT COUNT(*) as count FROM fcm_tokens
                    WHERE is_active = 0 
                    AND (updated_at < ? OR (updated_at IS NULL AND created_at < ?))
                    """,
                    [cutoff_date.isoformat(), cutoff_date.isoformat()]
                )
            
            count = count_row["count"] if count_row else 0
            
            if count == 0:
                logger.info("FCM Cleanup: No expired tokens to remove")
                return 0
            
            # Delete expired tokens
            if DB_TYPE == "postgresql":
                await execute_sql(
                    db,
                    """
                    DELETE FROM fcm_tokens
                    WHERE is_active = FALSE 
                    AND (updated_at < $1 OR (updated_at IS NULL AND created_at < $2))
                    """,
                    [cutoff_date, cutoff_date]
                )
            else:
                await execute_sql(
                    db,
                    """
                    DELETE FROM fcm_tokens
                    WHERE is_active = 0 
                    AND (updated_at < ? OR (updated_at IS NULL AND created_at < ?))
                    """,
                    [cutoff_date.isoformat(), cutoff_date.isoformat()]
                )
            
            await commit_db(db)
            logger.info(f"FCM Cleanup: Removed {count} expired tokens (inactive > {days_inactive} days)")
            return count
            
        except Exception as e:
            logger.error(f"FCM Cleanup: Error cleaning up tokens: {e}")
            return 0

