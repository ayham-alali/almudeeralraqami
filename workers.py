"""
Al-Mudeer - Background Workers
Automatic message polling and processing for Email, WhatsApp, and Telegram
"""

import asyncio
import os
import random
import hashlib
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Set

from logging_config import get_logger
from db_helper import (
    get_db,
    fetch_one,
    fetch_all,
    execute_sql,
    commit_db,
)

logger = get_logger(__name__)

# Database configuration
DB_TYPE = os.getenv("DB_TYPE", "sqlite").lower()
DATABASE_PATH = os.getenv("DATABASE_PATH", "almudeer.db")
DATABASE_URL = os.getenv("DATABASE_URL")

# Import appropriate database driver
if DB_TYPE == "postgresql":
    try:
        import asyncpg
        POSTGRES_AVAILABLE = True
        aiosqlite = None
    except ImportError:
        raise ImportError(
            "PostgreSQL selected but asyncpg not installed. "
            "Install with: pip install asyncpg"
        )
else:
    import aiosqlite
    POSTGRES_AVAILABLE = False

# Import services
from services.telegram_service import TelegramService
from services.whatsapp_service import WhatsAppService
from services.gmail_oauth_service import GmailOAuthService
from services.gmail_api_service import GmailAPIService
from services.telegram_phone_service import TelegramPhoneService

# Import models
from models import (
    get_email_config, get_email_oauth_tokens,
    get_telegram_config,
    get_whatsapp_config,
    save_inbox_message,
    update_inbox_analysis,
    get_inbox_messages,
    create_outbox_message,
    approve_outbox_message,
    mark_outbox_sent,
    get_telegram_phone_session_data,
    get_or_create_customer,
    update_customer_lead_score,
    increment_customer_messages,
    update_telegram_phone_session_sync_time,
    deactivate_telegram_phone_session,
)
from agent import process_message
from message_filters import apply_filters


class MessagePoller:
    """Background worker for polling messages from all channels"""
    
    def __init__(self):
        self.running = False
        self.tasks: Dict[int, asyncio.Task] = {}
        # Track recently processed message hashes to avoid duplicate AI calls
        self._processed_hashes: Set[str] = set()
        self._hash_cache_max_size = 1000  # Limit memory usage
        # Lightweight in‑memory status used by /api/integrations/workers/status
        self.status: Dict[str, Dict[str, Optional[str]]] = {
            "email_polling": {
                "last_check": None,
                "status": "stopped",
                "next_check": None,
            },
            "telegram_polling": {
                "last_check": None,
                "status": "stopped",
            },
        }
    
    async def start(self):
        """Start all polling workers"""
        self.running = True
        self.status["email_polling"]["status"] = "running"
        self.status["telegram_polling"]["status"] = "running"
        logger.info("Starting message polling workers...")
        
        # Start polling loop
        asyncio.create_task(self._polling_loop())
    
    async def stop(self):
        """Stop all polling workers"""
        self.running = False
        for task in self.tasks.values():
            task.cancel()
        self.tasks.clear()
        self.status["email_polling"]["status"] = "stopped"
        self.status["telegram_polling"]["status"] = "stopped"
        logger.info("Stopped message polling workers")
    
    async def _polling_loop(self):
        """Main polling loop - runs every minute"""
        while self.running:
            try:
                # Get all active licenses with integrations
                active_licenses = await self._get_active_licenses()
                now_iso = datetime.utcnow().isoformat()
                self.status["email_polling"]["last_check"] = now_iso
                self.status["telegram_polling"]["last_check"] = now_iso
                
                for license_id in active_licenses:
                    # Stagger polling: random delay between licenses to spread AI load
                    await asyncio.sleep(random.uniform(1.0, 3.0))
                    # Poll each integration type
                    asyncio.create_task(self._poll_email(license_id))
                    asyncio.create_task(self._poll_telegram(license_id))
                    # WhatsApp uses webhooks, so no polling needed
                
                # Wait 120 seconds before next poll (doubled to reduce AI API usage)
                next_ts = (datetime.utcnow() + timedelta(seconds=120)).isoformat()
                self.status["email_polling"]["next_check"] = next_ts
                await asyncio.sleep(120)
                
            except Exception as e:
                logger.error(f"Error in polling loop: {e}", exc_info=True)
                self.status["email_polling"]["status"] = "error"
                self.status["telegram_polling"]["status"] = "error"
                await asyncio.sleep(120)  # Wait before retry
    
    async def _get_active_licenses(self) -> List[int]:
        """Get list of license IDs with active integrations"""
        licenses = []
        
        try:
            if DB_TYPE == "postgresql" and POSTGRES_AVAILABLE:
                if not DATABASE_URL:
                    logger.warning("DATABASE_URL not set for PostgreSQL")
                    return []
                conn = await asyncpg.connect(DATABASE_URL)
                try:
                    # Get licenses with email configs
                    rows = await conn.fetch("""
                        SELECT DISTINCT license_key_id 
                        FROM email_configs 
                        WHERE is_active = TRUE
                    """)
                    licenses.extend([row['license_key_id'] for row in rows])
                    
                    # Get licenses with telegram configs
                    rows = await conn.fetch("""
                        SELECT DISTINCT license_key_id 
                        FROM telegram_configs 
                        WHERE is_active = TRUE
                    """)
                    licenses.extend([row['license_key_id'] for row in rows])
                finally:
                    await conn.close()
            else:
                async with aiosqlite.connect(DATABASE_PATH) as db:
                    # Get licenses with email configs
                    async with db.execute("""
                        SELECT DISTINCT license_key_id 
                        FROM email_configs 
                        WHERE is_active = 1
                    """) as cursor:
                        rows = await cursor.fetchall()
                        licenses.extend([row[0] for row in rows])
                    
                    # Get licenses with telegram bot configs
                    async with db.execute("""
                        SELECT DISTINCT license_key_id 
                        FROM telegram_configs 
                        WHERE is_active = 1
                    """) as cursor:
                        rows = await cursor.fetchall()
                        licenses.extend([row[0] for row in rows])
                    
                    # Get licenses with telegram phone sessions
                    async with db.execute("""
                        SELECT DISTINCT license_key_id 
                        FROM telegram_phone_sessions 
                        WHERE is_active = 1
                    """) as cursor:
                        rows = await cursor.fetchall()
                        licenses.extend([row[0] for row in rows])
        
        except Exception as e:
            logger.error(f"Error getting active licenses: {e}")
        
        return list(set(licenses))  # Remove duplicates
    
    async def _poll_email(self, license_id: int):
        """Poll email for new messages using Gmail API"""
        try:
            config = await get_email_config(license_id)
            if not config or not config.get("is_active"):
                return
            
            # Check if it's time to poll (based on check_interval_minutes)
            last_checked = config.get("last_checked_at")
            check_interval = config.get("check_interval_minutes", 5)
            
            if last_checked:
                # Support both string (SQLite) and datetime (PostgreSQL) values
                if isinstance(last_checked, str):
                    try:
                        last_checked_dt = datetime.fromisoformat(last_checked.replace("Z", "+00:00"))
                    except ValueError:
                        # Fallback: try parsing generic string representation
                        last_checked_dt = datetime.fromisoformat(str(last_checked))
                elif hasattr(last_checked, "isoformat"):
                    # Already a datetime-like object
                    last_checked_dt = last_checked
                else:
                    last_checked_dt = datetime.fromisoformat(str(last_checked))

                if datetime.utcnow() - last_checked_dt < timedelta(minutes=check_interval):
                    return  # Too soon to check again
            
            # Get OAuth tokens
            tokens = await get_email_oauth_tokens(license_id)
            if not tokens or not tokens.get("access_token"):
                logger.warning(f"No OAuth tokens found for license {license_id}")
                return
            
            # Initialize Gmail API service
            oauth_service = GmailOAuthService()
            gmail_service = GmailAPIService(
                tokens["access_token"],
                tokens.get("refresh_token"),
                oauth_service
            )
            
            # Get our own email address to filter out self-messages
            # This prevents AI from processing emails WE sent
            our_email_address = config.get("email_address", "").lower()
            
            # Fetch new emails using Gmail API (last 72 hours)
            # Increased limit to 200 to capture more messages per poll
            emails = await gmail_service.fetch_new_emails(since_hours=72, limit=200)
            
            # Get recent messages for duplicate detection
            # Use higher limit to avoid missing duplicates when inbox is large
            recent_messages = await get_inbox_messages(license_id, limit=500)
            
            # Process each email
            for email_data in emails:
                # CRITICAL: Skip emails sent BY US to prevent AI loop
                sender_email = (email_data.get("sender_contact") or "").lower()
                if our_email_address and sender_email == our_email_address:
                    logger.debug(f"Skipping self-sent email from {sender_email}")
                    continue
                
                # Check if we already have this message
                existing = await self._check_existing_message(
                    license_id, "email", email_data.get("channel_message_id")
                )
                
                if existing:
                    continue  # Already processed
                
                # Apply filters
                message_dict = {
                    "body": email_data["body"],
                    "sender_contact": email_data.get("sender_contact"),
                    "sender_name": email_data.get("sender_name"),
                    "subject": email_data.get("subject"),
                    "channel": "email"
                }
                
                should_process, filter_reason = await apply_filters(
                    message_dict, license_id, recent_messages
                )
                
                if not should_process:
                    logger.info(f"Message filtered: {filter_reason}")
                    continue
                
                # Save to inbox
                msg_id = await save_inbox_message(
                    license_id=license_id,
                    channel="email",
                    body=email_data["body"],
                    sender_name=email_data["sender_name"],
                    sender_contact=email_data["sender_contact"],
                    subject=email_data.get("subject"),
                    channel_message_id=email_data.get("channel_message_id"),
                    received_at=email_data.get("received_at")
                )
                
                # Analyze with AI
                await self._analyze_and_process_message(
                    msg_id,
                    email_data["body"],
                    license_id,
                    config.get("auto_reply_enabled", False),
                    "email",
                    email_data.get("sender_contact"),
                    email_data.get("sender_name")
                )
            
            # Update last_checked_at
            await self._update_email_last_checked(license_id)
            
        except Exception as e:
            logger.error(f"Error polling email for license {license_id}: {e}", exc_info=True)
    
    async def _poll_telegram(self, license_id: int):
        """Poll Telegram for new messages for phone-number sessions (MTProto)."""
        try:
            # Get Telegram phone session string (if any)
            session_string = await get_telegram_phone_session_data(license_id)
            if not session_string:
                # No phone session configured for this license
                return

            phone_service = TelegramPhoneService()

            # Fetch recent messages from Telegram phone account (last 72 hours)
            # Increased limit to 200 to capture more messages per poll
            try:
                messages = await phone_service.get_recent_messages(
                    session_string=session_string,
                    since_hours=72,
                    limit=200,
                )
            except Exception as e:
                # If the underlying Telethon client or session is invalid, avoid
                # spamming errors on every poll and disable the session so the
                # user can re-link it from the dashboard.
                msg = str(e)
                logger.warning(
                    f"Telegram phone session appears invalid for license {license_id}: {msg}. "
                    f"Deactivating session so the user can re-connect."
                )
                try:
                    await deactivate_telegram_phone_session(license_id)
                except Exception as de:
                    logger.error(
                        f"Failed to deactivate Telegram phone session for license {license_id}: {de}",
                        exc_info=True,
                    )
                return

            if not messages:
                return

            # Get recent inbox messages for duplicate detection
            # Use higher limit to avoid missing duplicates when inbox is large
            recent_messages = await get_inbox_messages(license_id, limit=500)

            for msg in messages:
                # Check if we already have this message
                existing = await self._check_existing_message(
                    license_id, "telegram", msg.get("channel_message_id")
                )
                if existing:
                    continue

                # Apply filters
                message_dict = {
                    "body": msg["body"],
                    "sender_contact": msg.get("sender_contact"),
                    "sender_name": msg.get("sender_name"),
                    "subject": msg.get("subject"),
                    "channel": "telegram",
                }

                should_process, filter_reason = await apply_filters(
                    message_dict, license_id, recent_messages
                )

                if not should_process:
                    logger.info(f"Telegram phone message filtered: {filter_reason}")
                    continue

                # Save to inbox as Telegram channel
                msg_id = await save_inbox_message(
                    license_id=license_id,
                    channel="telegram",
                    body=msg["body"],
                    sender_name=msg.get("sender_name"),
                    sender_contact=msg.get("sender_contact"),
                    sender_id=msg.get("sender_id"),
                    subject=msg.get("subject"),
                    channel_message_id=msg.get("channel_message_id"),
                    received_at=msg.get("received_at"),
                )

                # Analyze with AI (auto-reply disabled by default for phone sessions)
                await self._analyze_and_process_message(
                    msg_id,
                    msg["body"],
                    license_id,
                    False,  # auto_reply for Telegram phone can be added later
                    "telegram",
                    msg.get("sender_contact"),
                    msg.get("sender_name")
                )

            # Update last sync time
            await update_telegram_phone_session_sync_time(license_id)

        except Exception as e:
            logger.error(f"Error polling Telegram phone for license {license_id}: {e}", exc_info=True)
    
    def _get_message_hash(self, body: str, sender: Optional[str] = None) -> str:
        """
        Create a hash to detect duplicate/similar messages.
        Uses first 500 chars of body to reduce false negatives from signatures.
        """
        content = f"{sender or 'unknown'}:{body[:500].strip().lower()}"
        return hashlib.md5(content.encode()).hexdigest()
    
    def _is_duplicate_content(self, body: str, sender: Optional[str] = None) -> bool:
        """
        Check if we've recently processed a similar message.
        This prevents duplicate AI calls for the same content.
        """
        msg_hash = self._get_message_hash(body, sender)
        
        if msg_hash in self._processed_hashes:
            logger.debug(f"Duplicate content detected, skipping AI: {msg_hash[:8]}...")
            return True
        
        # Add to cache and limit size
        self._processed_hashes.add(msg_hash)
        if len(self._processed_hashes) > self._hash_cache_max_size:
            # Remove oldest entries (convert to list, remove first half)
            self._processed_hashes = set(list(self._processed_hashes)[self._hash_cache_max_size // 2:])
        
        return False
    
    async def _check_existing_message(self, license_id: int, channel: str, channel_message_id: Optional[str]) -> bool:
        """Check if a message already exists in inbox"""
        if not channel_message_id:
            return False
        
        try:
            async with get_db() as db:
                row = await fetch_one(
                    db,
                    "SELECT id FROM inbox_messages WHERE license_key_id = ? AND channel = ? AND channel_message_id = ?",
                    [license_id, channel, channel_message_id],
                )
                return row is not None
        except Exception as e:
            logger.error(f"Error checking existing message: {e}")
            return False
    
    async def _analyze_and_process_message(
        self,
        message_id: int,
        body: str,
        license_id: int,
        auto_reply: bool,
        channel: str,
        recipient: Optional[str] = None,
        sender_name: Optional[str] = None
    ):
        """Analyze message with AI and optionally auto-reply"""
        try:
            # Check for duplicate content to avoid wasting AI quota
            if self._is_duplicate_content(body, sender_name):
                logger.info(f"Skipping AI for message {message_id}: duplicate content detected")
                return
            
            # Process with AI agent
            result = await process_message(body)
            
            if not result["success"]:
                logger.warning(f"AI processing failed for message {message_id}: {result.get('error')}")
                return
            
            data = result["data"]

            # Update inbox with analysis (including language/dialect)
            await update_inbox_analysis(
                message_id=message_id,
                intent=data["intent"],
                urgency=data["urgency"],
                sentiment=data["sentiment"],
                language=data.get("language"),
                dialect=data.get("dialect"),
                summary=data["summary"],
                draft_response=data["draft_response"],
            )
            
            # Link message to customer and update lead score
            try:
                # Get message details to extract sender info
                async with get_db() as db:
                    message = await fetch_one(
                        db,
                        "SELECT sender_name, sender_contact FROM inbox_messages WHERE id = ?",
                        [message_id]
                    )
                    
                    if message:
                        sender_contact = message.get("sender_contact") or recipient
                        sender_name = message.get("sender_name") or sender_name
                        
                        if sender_contact:
                            # Extract email or phone from contact
                            email = None
                            phone = None
                            if "@" in sender_contact:
                                email = sender_contact
                            elif sender_contact.replace("+", "").replace("-", "").replace(" ", "").isdigit():
                                phone = sender_contact
                            
                            # Get or create customer
                            customer = await get_or_create_customer(
                                license_id=license_id,
                                phone=phone,
                                email=email,
                                name=sender_name
                            )
                            
                            if customer and customer.get("id"):
                                customer_id = customer["id"]
                                
                                # Increment message count
                                await increment_customer_messages(customer_id)
                                
                                # Link message to customer (check if exists first to avoid duplicates)
                                existing = await fetch_one(
                                    db,
                                    "SELECT 1 FROM customer_messages WHERE customer_id = ? AND inbox_message_id = ?",
                                    [customer_id, message_id]
                                )
                                if not existing:
                                    from db_helper import DB_TYPE
                                    if DB_TYPE == "postgresql":
                                        await execute_sql(
                                            db,
                                            """
                                            INSERT INTO customer_messages (customer_id, inbox_message_id)
                                            VALUES (?, ?)
                                            ON CONFLICT (customer_id, inbox_message_id) DO NOTHING
                                            """,
                                            [customer_id, message_id]
                                        )
                                    else:
                                        await execute_sql(
                                            db,
                                            """
                                            INSERT OR IGNORE INTO customer_messages (customer_id, inbox_message_id)
                                            VALUES (?, ?)
                                            """,
                                            [customer_id, message_id]
                                        )
                                    await commit_db(db)
                                
                                # Update lead score based on analysis
                                await update_customer_lead_score(
                                    license_id=license_id,
                                    customer_id=customer_id,
                                    intent=data.get("intent"),
                                    sentiment=data.get("sentiment"),
                                    sentiment_score=0.0  # Could be calculated from sentiment history
                                )
            except Exception as crm_error:
                logger.warning(f"Error updating CRM for message {message_id}: {crm_error}")
            
            # Auto-reply if enabled
            if auto_reply and data["draft_response"]:
                await self._auto_reply(
                    message_id=message_id,
                    license_id=license_id,
                    channel=channel,
                    response_body=data["draft_response"],
                    recipient=recipient
                )
        
        except Exception as e:
            logger.error(f"Error analyzing message {message_id}: {e}", exc_info=True)
    
    async def _auto_reply(
        self,
        message_id: int,
        license_id: int,
        channel: str,
        response_body: str,
        recipient: Optional[str] = None
    ):
        """Automatically send a reply"""
        try:
            # Get message details
            messages = await get_inbox_messages(license_id, limit=1000)
            message = next((m for m in messages if m["id"] == message_id), None)
            
            if not message:
                return
            
            # Create outbox entry
            outbox_id = await create_outbox_message(
                inbox_message_id=message_id,
                license_id=license_id,
                channel=channel,
                body=response_body,
                recipient_id=message.get("sender_id"),
                recipient_email=message.get("sender_contact") or recipient,
                subject=f"Re: {message.get('subject', '')}" if message.get("subject") else None
            )
            
            # Approve and send
            await approve_outbox_message(outbox_id)
            await self._send_message(outbox_id, license_id, channel)
        
        except Exception as e:
            logger.error(f"Error in auto-reply for message {message_id}: {e}", exc_info=True)
    
    async def _send_message(self, outbox_id: int, license_id: int, channel: str):
        """Send an approved message"""
        try:
            # Get outbox message (works for both SQLite and PostgreSQL)
            async with get_db() as db:
                rows = await fetch_all(
                    db,
                    """
                    SELECT o.*, i.sender_name, i.body as original_message, i.sender_contact, i.sender_id
                    FROM outbox_messages o
                    JOIN inbox_messages i ON o.inbox_message_id = i.id
                    WHERE o.id = ? AND o.license_key_id = ?
                    """,
                    [outbox_id, license_id],
                )

            if not rows:
                return
            
            message = rows[0]
            
            if channel == "email":
                # Send via Gmail API using OAuth
                tokens = await get_email_oauth_tokens(license_id)
                
                if tokens and tokens.get("access_token"):
                    oauth_service = GmailOAuthService()
                    gmail_service = GmailAPIService(
                        tokens["access_token"],
                        tokens.get("refresh_token"),
                        oauth_service
                    )
                    
                    await gmail_service.send_message(
                        to_email=message["recipient_email"],
                        subject=message.get("subject", "رد على رسالتك"),
                        body=message["body"],
                        reply_to_message_id=message.get("inbox_message_id")
                    )
                    
                    await mark_outbox_sent(outbox_id)
                    logger.info(f"Sent email reply for outbox {outbox_id}")
            
            elif channel == "telegram":
                # First, try Telegram Phone (MTProto) if we have a phone session
                session_string = await get_telegram_phone_session_data(license_id)
                sent = False
                
                if session_string:
                    try:
                        phone_service = TelegramPhoneService()
                        # Use sender_id as recipient since that's the chat/user we're replying to
                        recipient = message.get("recipient_id") or message.get("sender_id")
                        if recipient:
                            await phone_service.send_message(
                                session_string=session_string,
                                recipient_id=str(recipient),
                                text=message["body"]
                            )
                            await mark_outbox_sent(outbox_id)
                            logger.info(f"Sent Telegram phone reply for outbox {outbox_id}")
                            sent = True
                    except Exception as e:
                        logger.warning(f"Failed to send via Telegram phone for outbox {outbox_id}: {e}")
                
                # Fall back to Bot API if phone session didn't work or doesn't exist
                if not sent:
                    async with get_db() as db:
                        row = await fetch_one(
                            db,
                            "SELECT bot_token FROM telegram_configs WHERE license_key_id = ?",
                            [license_id],
                        )
                        if row and row.get("bot_token"):
                            telegram_service = TelegramService(row["bot_token"])
                            await telegram_service.send_message(
                                chat_id=message["recipient_id"],
                                text=message["body"]
                            )
                            
                            await mark_outbox_sent(outbox_id)
                            logger.info(f"Sent Telegram bot reply for outbox {outbox_id}")

            
            elif channel == "whatsapp":
                config = await get_whatsapp_config(license_id)
                
                if config:
                    whatsapp_service = WhatsAppService(
                        phone_number_id=config["phone_number_id"],
                        access_token=config["access_token"]
                    )
                    
                    result = await whatsapp_service.send_message(
                        to=message["recipient_id"],
                        message=message["body"]
                    )
                    
                    if result["success"]:
                        await mark_outbox_sent(outbox_id)
                        logger.info(f"Sent WhatsApp reply for outbox {outbox_id}")
        
        except Exception as e:
            logger.error(f"Error sending message {outbox_id}: {e}", exc_info=True)
    
    async def _update_email_last_checked(self, license_id: int):
        """Update last_checked_at timestamp for email config"""
        try:
            # For PostgreSQL we should store a real datetime object.
            # For SQLite we keep using ISO strings for backward compatibility.
            from db_helper import DB_TYPE  # Local import to avoid circulars
            now_value = datetime.utcnow() if DB_TYPE == "postgresql" else datetime.utcnow().isoformat()

            async with get_db() as db:
                await execute_sql(
                    db,
                    """
                    UPDATE email_configs 
                    SET last_checked_at = ? 
                    WHERE license_key_id = ?
                    """,
                    [now_value, license_id],
                )
                await commit_db(db)
        except Exception as e:
            logger.error(f"Error updating last_checked_at: {e}")


# Global poller instance
_poller: Optional[MessagePoller] = None


async def start_message_polling():
    """Start the message polling service"""
    global _poller
    if _poller is None:
        _poller = MessagePoller()
        await _poller.start()
    return _poller


async def stop_message_polling():
    """Stop the message polling service"""
    global _poller
    if _poller:
        await _poller.stop()
        _poller = None


def get_worker_status() -> Dict[str, Dict[str, Optional[str]]]:
    """
    Lightweight status snapshot for background workers.

    This is intentionally simple and read-only so the frontend dashboard can
    show whether polling is running without depending on internal details.
    """
    status = "running" if _poller is not None and _poller.running else "stopped"
    now = datetime.utcnow().isoformat() + "Z"

    # Shape is aligned with frontend WorkerStatus type (email_polling, telegram_polling)
    return {
        "email_polling": {
            "last_check": now,
            "status": status,
            "next_check": None,
        },
        "telegram_polling": {
            "last_check": now,
            "status": status,
        },
    }


# ============ Subscription Reminder Worker ============

_subscription_reminder_task: Optional[asyncio.Task] = None


async def check_subscription_reminders():
    """
    Check for subscriptions expiring in 3 days and send notifications.
    Runs once per day.
    """
    from models import create_notification
    
    try:
        async with get_db() as db:
            # Find subscriptions expiring in exactly 3 days
            if DB_TYPE == "postgresql":
                # PostgreSQL: use CURRENT_DATE + INTERVAL
                rows = await fetch_all(
                    db,
                    """
                    SELECT id, company_name, expires_at, contact_email
                    FROM license_keys 
                    WHERE is_active = TRUE 
                    AND DATE(expires_at) = CURRENT_DATE + INTERVAL '3 days'
                    """,
                    []
                )
            else:
                # SQLite: use date arithmetic
                rows = await fetch_all(
                    db,
                    """
                    SELECT id, company_name, expires_at, contact_email
                    FROM license_keys 
                    WHERE is_active = 1 
                    AND DATE(expires_at) = DATE('now', '+3 days')
                    """,
                    []
                )
            
            if not rows:
                logger.info("No subscriptions expiring in 3 days")
                return
            
            # Send reminder notifications
            for row in rows:
                license_id = row["id"]
                company_name = row.get("company_name", "Unknown")
                
                try:
                    await create_notification(
                        license_id=license_id,
                        notification_type="subscription_expiring",
                        title="⚠️ اشتراكك ينتهي قريباً",
                        message=f"اشتراكك في المدير ينتهي خلال 3 أيام. يرجى تجديد الاشتراك لضمان استمرار الخدمة.",
                        priority="high",
                        link="/dashboard/settings"
                    )
                    logger.info(f"Sent subscription reminder to license {license_id} ({company_name})")
                except Exception as e:
                    logger.warning(f"Failed to send reminder to license {license_id}: {e}")
                    
    except Exception as e:
        logger.error(f"Error checking subscription reminders: {e}", exc_info=True)


async def _subscription_reminder_loop():
    """Background loop that runs once per day to check subscription reminders."""
    while True:
        try:
            await check_subscription_reminders()
        except Exception as e:
            logger.error(f"Error in subscription reminder loop: {e}", exc_info=True)
        
        # Wait 24 hours before next check
        await asyncio.sleep(24 * 60 * 60)


async def start_subscription_reminders():
    """Start the subscription reminder background task."""
    global _subscription_reminder_task
    if _subscription_reminder_task is None:
        _subscription_reminder_task = asyncio.create_task(_subscription_reminder_loop())
        logger.info("Started subscription reminder worker")


async def stop_subscription_reminders():
    """Stop the subscription reminder background task."""
    global _subscription_reminder_task
    if _subscription_reminder_task:
        _subscription_reminder_task.cancel()
        _subscription_reminder_task = None
        logger.info("Stopped subscription reminder worker")
