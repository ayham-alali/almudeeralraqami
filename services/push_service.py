"""
Al-Mudeer - Web Push Notification Service
Handles VAPID keys and sending push notifications to subscribed browsers/devices
"""

import os
import json
from typing import Optional, List
from logging_config import get_logger

logger = get_logger(__name__)

# VAPID keys for Web Push (generate once and store in environment)
# Generate with: from pywebpush import webpush; webpush.generate_vapid_keys()
VAPID_PRIVATE_KEY = os.getenv("VAPID_PRIVATE_KEY")
VAPID_PUBLIC_KEY = os.getenv("VAPID_PUBLIC_KEY")
VAPID_CLAIMS_EMAIL = os.getenv("VAPID_CLAIMS_EMAIL", "mailto:admin@almudeer.com")

# Check if pywebpush is available
try:
    from pywebpush import webpush, WebPushException
    WEBPUSH_AVAILABLE = True
except ImportError:
    WEBPUSH_AVAILABLE = False
    logger.warning("pywebpush not installed. Web Push notifications disabled. Install with: pip install pywebpush")


def get_vapid_public_key() -> Optional[str]:
    """Get the VAPID public key for frontend subscription."""
    return VAPID_PUBLIC_KEY


def generate_vapid_keys() -> dict:
    """
    Generate new VAPID keys. Run this once and add to environment variables.
    
    Usage:
        python -c "from services.push_service import generate_vapid_keys; print(generate_vapid_keys())"
    """
    if not WEBPUSH_AVAILABLE:
        return {"error": "pywebpush not installed"}
    
    from py_vapid import Vapid
    
    vapid = Vapid()
    vapid.generate_keys()
    
    private_key = vapid.private_key.private_bytes(
        encoding=__import__('cryptography.hazmat.primitives.serialization', fromlist=['Encoding']).Encoding.PEM,
        format=__import__('cryptography.hazmat.primitives.serialization', fromlist=['PrivateFormat']).PrivateFormat.PKCS8,
        encryption_algorithm=__import__('cryptography.hazmat.primitives.serialization', fromlist=['NoEncryption']).NoEncryption()
    ).decode('utf-8')
    
    public_key = vapid.public_key.public_bytes(
        encoding=__import__('cryptography.hazmat.primitives.serialization', fromlist=['Encoding']).Encoding.PEM,
        format=__import__('cryptography.hazmat.primitives.serialization', fromlist=['PublicFormat']).PublicFormat.SubjectPublicKeyInfo
    ).decode('utf-8')
    
    return {
        "private_key": private_key,
        "public_key": public_key,
        "instructions": "Add these to your .env file as VAPID_PRIVATE_KEY and VAPID_PUBLIC_KEY"
    }


async def send_push_notification(
    subscription_info: dict,
    title: str,
    message: str,
    link: Optional[str] = None,
    tag: Optional[str] = None,
    priority: str = "normal"
) -> bool:
    """
    Send a push notification to a single subscription.
    
    Args:
        subscription_info: Browser push subscription object {endpoint, keys: {p256dh, auth}}
        title: Notification title
        message: Notification body
        link: Optional URL to open on click
        tag: Optional tag for notification grouping
        priority: Notification priority (low, normal, high, urgent)
    """
    if not WEBPUSH_AVAILABLE:
        logger.warning("Cannot send push: pywebpush not installed")
        return False
    
    if not VAPID_PRIVATE_KEY or not VAPID_PUBLIC_KEY:
        logger.warning("Cannot send push: VAPID keys not configured")
        return False
    
    try:
        payload = json.dumps({
            "title": title,
            "message": message,
            "body": message,  # Alternative key used by some SW implementations
            "link": link,
            "tag": tag or "almudeer-notification",
            "priority": priority,
        }, ensure_ascii=False)
        
        webpush(
            subscription_info=subscription_info,
            data=payload,
            vapid_private_key=VAPID_PRIVATE_KEY,
            vapid_claims={"sub": VAPID_CLAIMS_EMAIL}
        )
        
        logger.info(f"Push notification sent: {title[:30]}...")
        return True
        
    except WebPushException as e:
        logger.error(f"Push notification failed: {e}")
        # If subscription is expired/invalid, return False so caller can clean up
        if e.response and e.response.status_code in (404, 410):
            logger.info("Push subscription expired or unsubscribed")
        return False
    except Exception as e:
        logger.error(f"Error sending push notification: {e}", exc_info=True)
        return False


async def send_push_to_license(
    license_id: int,
    title: str,
    message: str,
    link: Optional[str] = None,
    tag: Optional[str] = None,
    priority: str = "normal"
) -> int:
    """
    Send push notification to all subscribed devices for a license.
    
    Returns the number of successful sends.
    """
    from db_helper import get_db, fetch_all, execute_sql, commit_db
    
    sent_count = 0
    expired_ids = []
    
    async with get_db() as db:
        # Get all push subscriptions for this license
        rows = await fetch_all(
            db,
            """
            SELECT id, subscription_info FROM push_subscriptions
            WHERE license_key_id = ? AND is_active = TRUE
            """,
            [license_id]
        )
        
        if not rows:
            return 0
        
        for row in rows:
            try:
                subscription_info = json.loads(row["subscription_info"])
                success = await send_push_notification(
                    subscription_info=subscription_info,
                    title=title,
                    message=message,
                    link=link,
                    tag=tag,
                    priority=priority
                )
                
                if success:
                    sent_count += 1
                else:
                    # Mark subscription as inactive if it failed
                    expired_ids.append(row["id"])
            except Exception as e:
                logger.warning(f"Error processing subscription {row['id']}: {e}")
                expired_ids.append(row["id"])
        
        # Clean up expired subscriptions
        if expired_ids:
            placeholders = ",".join("?" for _ in expired_ids)
            await execute_sql(
                db,
                f"UPDATE push_subscriptions SET is_active = FALSE WHERE id IN ({placeholders})",
                expired_ids
            )
            await commit_db(db)
            logger.info(f"Marked {len(expired_ids)} expired push subscriptions as inactive")
    
    return sent_count


async def save_push_subscription(
    license_id: int,
    subscription_info: dict,
    user_agent: Optional[str] = None
) -> int:
    """Save a new push subscription for a license."""
    from db_helper import get_db, fetch_one, execute_sql, commit_db
    
    subscription_json = json.dumps(subscription_info, ensure_ascii=False)
    endpoint = subscription_info.get("endpoint", "")
    
    async with get_db() as db:
        # Check if this endpoint already exists
        existing = await fetch_one(
            db,
            "SELECT id FROM push_subscriptions WHERE endpoint = ?",
            [endpoint]
        )
        
        if existing:
            # Update existing subscription
            await execute_sql(
                db,
                """
                UPDATE push_subscriptions 
                SET subscription_info = ?, is_active = TRUE, updated_at = CURRENT_TIMESTAMP
                WHERE endpoint = ?
                """,
                [subscription_json, endpoint]
            )
            await commit_db(db)
            return existing["id"]
        
        # Create new subscription
        await execute_sql(
            db,
            """
            INSERT INTO push_subscriptions (license_key_id, endpoint, subscription_info, user_agent)
            VALUES (?, ?, ?, ?)
            """,
            [license_id, endpoint, subscription_json, user_agent]
        )
        
        row = await fetch_one(
            db,
            "SELECT id FROM push_subscriptions WHERE endpoint = ?",
            [endpoint]
        )
        await commit_db(db)
        return row["id"] if row else 0


async def remove_push_subscription(endpoint: str) -> bool:
    """Remove a push subscription by endpoint."""
    from db_helper import get_db, execute_sql, commit_db
    
    async with get_db() as db:
        await execute_sql(
            db,
            "DELETE FROM push_subscriptions WHERE endpoint = ?",
            [endpoint]
        )
        await commit_db(db)
        return True
