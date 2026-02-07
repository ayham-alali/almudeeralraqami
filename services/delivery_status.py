"""
Al-Mudeer - Message Delivery Status Service
Tracks real delivery receipts from WhatsApp and Telegram.

WhatsApp delivery statuses:
- sent: Message sent to WhatsApp servers
- delivered: Message delivered to recipient's device
- read: Recipient opened/read the message
- failed: Message delivery failed

Telegram delivery statuses:
- sent: Message sent to Telegram servers
- (Telegram Bot API doesn't provide delivery/read receipts for bots)
"""

from datetime import datetime
from typing import Optional, Dict, Any
from db_helper import get_db, execute_sql, fetch_one, commit_db, DB_TYPE
from logging_config import get_logger

logger = get_logger(__name__)


async def save_platform_message_id(
    outbox_id: int,
    platform_message_id: str
) -> bool:
    """
    Save the platform message ID after sending.
    This is used to match incoming status webhooks to our messages.
    """
    try:
        async with get_db() as db:
            await execute_sql(
                db,
                """
                UPDATE outbox_messages 
                SET platform_message_id = ?, delivery_status = 'sent'
                WHERE id = ?
                """,
                [platform_message_id, outbox_id]
            )
            await commit_db(db)
        
        logger.debug(f"Saved platform message ID {platform_message_id} for outbox {outbox_id}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to save platform message ID: {e}")
        return False


async def update_delivery_status(
    platform_message_id: str,
    status: str,
    timestamp: Optional[datetime] = None
) -> bool:
    """
    Update delivery status based on webhook callback.
    
    Args:
        platform_message_id: The message ID from WhatsApp/Telegram
        status: One of 'sent', 'delivered', 'read', 'failed'
        timestamp: When the status change occurred
    """
    if timestamp is None:
        timestamp = datetime.utcnow()
    
    ts_value = timestamp if DB_TYPE == "postgresql" else timestamp.isoformat()
    
    try:
        async with get_db() as db:
            # Find the outbox message by platform_message_id
            row = await fetch_one(
                db,
                "SELECT id, license_key_id, inbox_message_id, delivery_status, recipient_email, recipient_id FROM outbox_messages WHERE platform_message_id = ?",
                [platform_message_id]
            )
            
            if not row:
                logger.debug(f"No message found for platform ID {platform_message_id}")
                return False
            
            outbox_id = row["id"]
            license_id = row["license_key_id"]
            inbox_message_id = row.get("inbox_message_id")
            current_status = row.get("delivery_status", "")
            
            # Determine the conversation identifier (sender_contact) for status mapping
            sender_contact = None
            if inbox_message_id:
                inbox_msg = await fetch_one(db, "SELECT sender_contact FROM inbox_messages WHERE id = ?", [inbox_message_id])
                if inbox_msg:
                    sender_contact = inbox_msg["sender_contact"]
            if not sender_contact:
                sender_contact = row["recipient_email"] or row["recipient_id"]

            # Status progression: sent -> failed
            # We only track 'sent' (single check) and 'failed'.
            
            status_order = {"sent": 1, "failed": 0}
            
            current_order = status_order.get(current_status, 0)
            new_order = status_order.get(status, 0)
            
            # If status is not in our list (e.g. read, delivered), ignore it
            if status not in status_order:
                 return True

            if new_order <= current_order and status != "failed":
                logger.debug(f"Skipping status update (not a progression): {current_status} -> {status}")
                return True
            
            # Update the status
            await execute_sql(
                db,
                "UPDATE outbox_messages SET delivery_status = ? WHERE id = ?",
                [status, outbox_id]
            )
            
            await commit_db(db)
            
            # Update the optimized conversation state
            if sender_contact:
                from models.inbox import upsert_conversation_state
                await upsert_conversation_state(license_id, sender_contact)

            logger.info(f"Updated delivery status for outbox {outbox_id}: {current_status} -> {status}")
            
            # Broadcast status update via WebSocket
            try:
                from services.websocket_manager import broadcast_message_status_update
                await broadcast_message_status_update(
                    license_id,
                    {
                        "outbox_id": outbox_id,
                        "sender_contact": sender_contact, # Critical for mobile app mapping
                        "inbox_message_id": inbox_message_id,
                        "platform_message_id": platform_message_id,
                        "status": status,
                        "timestamp": ts_value if isinstance(ts_value, str) else ts_value.isoformat()
                    }
                )
            except Exception as ws_error:
                logger.debug(f"WebSocket broadcast failed (non-critical): {ws_error}")
            
            return True
            
    except Exception as e:
        logger.error(f"Failed to update delivery status: {e}")
        return False


async def get_message_delivery_status(outbox_id: int) -> Dict[str, Any]:
    """
    Get the current delivery status of a message.
    """
    try:
        async with get_db() as db:
            row = await fetch_one(
                db,
                """
                SELECT delivery_status, sent_at, platform_message_id
                FROM outbox_messages
                WHERE id = ?
                """,
                [outbox_id]
            )
        
        if not row:
            return {"status": "unknown"}
        
        return {
            "status": row.get("delivery_status") or "sent",
            "sent_at": row.get("sent_at"),
            "platform_message_id": row.get("platform_message_id")
        }
        
    except Exception as e:
        logger.error(f"Failed to get delivery status: {e}")
        return {"status": "error"}


def get_delivery_status_icon(status: str, platform: str) -> str:
    """
    Get the appropriate icon indicator for a delivery status.
    
    WhatsApp:
    - sent: Single gray check ✓
    - delivered: Double gray checks ✓✓
    - read: Double blue checks ✓✓ (blue)
    
    Telegram:
    - sent: Single check ✓
    - read: Double checks ✓✓
    """
    if platform == "whatsapp":
        return {
            "sent": "single_gray",
            "delivered": "double_gray",
            "read": "double_blue",
            "failed": "failed"
        }.get(status, "single_gray")
    elif platform == "telegram":
        return {
            "sent": "single_check",
            "read": "double_check",
            "failed": "failed"
        }.get(status, "single_check")
    else:
        return {
            "sent": "single_check",
            "read": "double_check"
        }.get(status, "single_check")
