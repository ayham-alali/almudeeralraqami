"""
Al-Mudeer - FCM Mobile Push Service
Handles Firebase Cloud Messaging for mobile app push notifications

Supports both:
- FCM HTTP v1 API (recommended, uses service account)
- Legacy HTTP API (deprecated fallback, uses server key)
"""

import os
import json
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
    
    FCM_V1_AVAILABLE = bool(FCM_PROJECT_ID and GOOGLE_APPLICATION_CREDENTIALS)
    if FCM_V1_AVAILABLE:
        logger.info(f"FCM: v1 API configured for project '{FCM_PROJECT_ID}'")
    else:
        if not FCM_PROJECT_ID:
            logger.info("FCM: FCM_PROJECT_ID not set, will use legacy API")
        if not GOOGLE_APPLICATION_CREDENTIALS:
            logger.info("FCM: GOOGLE_APPLICATION_CREDENTIALS not set, will use legacy API")
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
        
        # Option 2: JSON content in env var (for Railway/Docker)
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
                    token TEXT NOT NULL UNIQUE,
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
            
            await commit_db(db)
            logger.info("FCM: fcm_tokens table verified")
        except Exception as e:
            logger.error(f"FCM: Verify table failed: {e}")


async def save_fcm_token(
    license_id: int,
    token: str,
    platform: str = "android"
) -> int:
    """Save a new FCM token for a license."""
    from db_helper import get_db, fetch_one, execute_sql, commit_db
    
    async with get_db() as db:
        # Check if token already exists
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
                SET license_key_id = ?, platform = ?, is_active = TRUE, updated_at = CURRENT_TIMESTAMP
                WHERE token = ?
                """,
                [license_id, platform, token]
            )
            await commit_db(db)
            logger.info(f"FCM: Token updated for license {license_id}")
            return existing["id"]
        
        # Create new token
        await execute_sql(
            db,
            """
            INSERT INTO fcm_tokens (license_key_id, token, platform)
            VALUES (?, ?, ?)
            """,
            [license_id, token, platform]
        )
        
        row = await fetch_one(
            db,
            "SELECT id FROM fcm_tokens WHERE token = ?",
            [token]
        )
        await commit_db(db)
        logger.info(f"FCM: Token registered for license {license_id}")
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
    link: Optional[str] = None
) -> bool:
    """
    Send push notification to a single FCM token.
    
    Uses FCM HTTP v1 API if configured (recommended), 
    otherwise falls back to legacy HTTP API (deprecated).
    """
    # Try v1 API first
    if FCM_V1_AVAILABLE:
        result = await _send_fcm_v1(token, title, body, data, link)
        if result is not None:  # None means v1 failed, try legacy
            return result
    
    # Fallback to legacy API
    return await _send_fcm_legacy(token, title, body, data, link)


async def _send_fcm_v1(
    token: str,
    title: str,
    body: str,
    data: Optional[dict] = None,
    link: Optional[str] = None
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
                    "notification": {
                        "sound": "default",
                        "click_action": "FLUTTER_NOTIFICATION_CLICK"
                    }
                },
                "apns": {
                    "payload": {
                        "aps": {
                            "sound": "default",
                            "badge": 1
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
            elif response.status_code == 401:
                # Auth error - clear cached token and fallback
                global _cached_access_token
                _cached_access_token = None
                logger.warning("FCM v1: Auth failed, falling back to legacy")
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
    link: Optional[str] = None
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
                "sound": "default",
                "click_action": "FLUTTER_NOTIFICATION_CLICK"
            },
            "data": data or {},
            "priority": "high"
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
    link: Optional[str] = None
) -> int:
    """
    Send push notification to all mobile devices for a license.
    
    Returns the number of successful sends.
    """
    from db_helper import get_db, fetch_all, execute_sql, commit_db
    
    sent_count = 0
    expired_ids = []
    
    async with get_db() as db:
        rows = await fetch_all(
            db,
            """
            SELECT id, token FROM fcm_tokens
            WHERE license_key_id = ? AND is_active = TRUE
            """,
            [license_id]
        )
        
        if not rows:
            return 0
        
        for row in rows:
            success = await send_fcm_notification(
                token=row["token"],
                title=title,
                body=body,
                data=data,
                link=link
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
