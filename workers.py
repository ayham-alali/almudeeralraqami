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
    get_telegram_phone_session,
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
    
    # Rate limits per user to protect Gemini free tier (15 RPM, 1500 RPD)
    # With 3 API calls per message: 500 messages/day total, 50/user for 10 users
    MAX_MESSAGES_PER_USER_PER_DAY = int(os.getenv("MAX_MESSAGES_PER_USER_DAY", "50"))
    MAX_MESSAGES_PER_USER_PER_MINUTE = int(os.getenv("MAX_MESSAGES_PER_USER_MINUTE", "1"))
    
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
        
        # Keep references to background tasks to prevent garbage collection
        self.background_tasks: Set[asyncio.Task] = set()
        
        # Limit concurrent AI requests to prevent hitting free tier rate limits
        # Using 1 concurrent request max to be extra safe with Gemini free tier
        self.ai_semaphore = asyncio.Semaphore(1)
        
        # Per-user rate limiting tracking
        # Structure: {license_id: {"daily_count": int, "daily_reset": datetime, "minute_count": int, "minute_reset": datetime}}
        self._user_rate_limits: Dict[int, Dict[str, Any]] = {}
    
    def _check_user_rate_limit(self, license_id: int) -> tuple[bool, str]:
        """
        Check if user is within rate limits.
        Returns (allowed, reason) tuple.
        """
        now = datetime.utcnow()
        
        # Initialize tracking for new user
        if license_id not in self._user_rate_limits:
            self._user_rate_limits[license_id] = {
                "daily_count": 0,
                "daily_reset": now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1),
                "minute_count": 0,
                "minute_reset": now + timedelta(minutes=1),
            }
        
        limits = self._user_rate_limits[license_id]
        
        # Reset daily counter if new day
        if now >= limits["daily_reset"]:
            limits["daily_count"] = 0
            limits["daily_reset"] = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        
        # Reset minute counter if minute passed
        if now >= limits["minute_reset"]:
            limits["minute_count"] = 0
            limits["minute_reset"] = now + timedelta(minutes=1)
        
        # Check daily limit
        if limits["daily_count"] >= self.MAX_MESSAGES_PER_USER_PER_DAY:
            remaining = (limits["daily_reset"] - now).total_seconds() / 3600
            return False, f"Daily limit reached ({self.MAX_MESSAGES_PER_USER_PER_DAY}/day). Resets in {remaining:.1f}h"
        
        # Check minute limit
        if limits["minute_count"] >= self.MAX_MESSAGES_PER_USER_PER_MINUTE:
            remaining = (limits["minute_reset"] - now).total_seconds()
            return False, f"Minute limit reached ({self.MAX_MESSAGES_PER_USER_PER_MINUTE}/min). Resets in {remaining:.0f}s"
        
        return True, ""
    
    def _increment_user_rate_limit(self, license_id: int):
        """Increment rate limit counters for a user."""
        if license_id in self._user_rate_limits:
            self._user_rate_limits[license_id]["daily_count"] += 1
            self._user_rate_limits[license_id]["minute_count"] += 1
            logger.debug(
                f"License {license_id} rate limit: "
                f"{self._user_rate_limits[license_id]['daily_count']}/{self.MAX_MESSAGES_PER_USER_PER_DAY} daily, "
                f"{self._user_rate_limits[license_id]['minute_count']}/{self.MAX_MESSAGES_PER_USER_PER_MINUTE} per min"
            )
    
    async def start(self):
        """Start all polling workers"""
        self.running = True
        self.status["email_polling"]["status"] = "running"
        self.status["telegram_polling"]["status"] = "running"
        logger.info("Starting message polling workers...")
        
        # Start polling loop
        # Start polling loop
        task = asyncio.create_task(self._polling_loop())
        self.background_tasks.add(task)
        task.add_done_callback(self.background_tasks.discard)
    
    async def stop(self):
        """Stop all polling workers"""
        self.running = False
        for task in self.background_tasks:
            task.cancel()
        self.background_tasks.clear()
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
                    # Stagger polling: increased delay between licenses to spread AI load
                    await asyncio.sleep(random.uniform(3.0, 8.0))
                    # Poll each integration type
                    t1 = asyncio.create_task(self._poll_email(license_id))
                    self.background_tasks.add(t1)
                    t1.add_done_callback(self.background_tasks.discard)
                    
                    t2 = asyncio.create_task(self._poll_telegram(license_id))
                    self.background_tasks.add(t2)
                    t2.add_done_callback(self.background_tasks.discard)
                    # WhatsApp uses webhooks, so no polling needed
                    
                    # Retry pending messages (those with placeholder responses from failed AI)
                    t3 = asyncio.create_task(self._retry_pending_messages(license_id))
                    self.background_tasks.add(t3)
                    t3.add_done_callback(self.background_tasks.discard)
                
                # Wait 300 seconds (5 minutes) before next poll - optimized for Gemini free tier
                # This ensures we stay well within 15 RPM limit even with 10 users
                next_ts = (datetime.utcnow() + timedelta(seconds=300)).isoformat()
                self.status["email_polling"]["next_check"] = next_ts
                await asyncio.sleep(300)
                
            except Exception as e:
                logger.error(f"Error in polling loop: {e}", exc_info=True)
                self.status["email_polling"]["status"] = "error"
                self.status["telegram_polling"]["status"] = "error"
                await asyncio.sleep(300)  # Wait before retry
    
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
    
    async def _retry_pending_messages(self, license_id: int):
        """Retry AI analysis for messages with placeholder responses"""
        try:
            # Find messages with pending placeholder response
            placeholder = "⏳ جاري تحليل الرسالة تلقائياً..."
            
            async with get_db() as db:
                # Query messages with placeholder ai_draft_response
                rows = await fetch_all(
                    db,
                    """
                    SELECT id, body, sender_contact, sender_name, channel
                    FROM inbox_messages
                    WHERE license_key_id = ?
                      AND (ai_draft_response = ? OR ai_draft_response IS NULL OR ai_draft_response = '')
                      AND created_at > datetime('now', '-24 hours')
                    ORDER BY created_at DESC
                    LIMIT 5
                    """,
                    [license_id, placeholder]
                )
                
                if not rows:
                    return
                
                logger.info(f"License {license_id}: Found {len(rows)} pending messages to retry")
                
                for row in rows:
                    message_id = row.get("id") or row[0]
                    body = row.get("body") or row[1]
                    sender_contact = row.get("sender_contact") or row[2]
                    sender_name = row.get("sender_name") or row[3]
                    channel = row.get("channel") or row[4]
                    
                    # Check rate limit before retrying
                    allowed, reason = self._check_user_rate_limit(license_id)
                    if not allowed:
                        logger.debug(f"Rate limit for license {license_id}: {reason}. Retry skipped.")
                        break  # Stop retrying for this license
                    
                    # Add delay between retries
                    await asyncio.sleep(random.uniform(5.0, 10.0))
                    
                    # Re-analyze the message
                    await self._analyze_and_process_message(
                        message_id=message_id,
                        body=body,
                        license_id=license_id,
                        auto_reply=False,  # Don't auto-reply on retry
                        channel=channel,
                        recipient=sender_contact,
                        sender_name=sender_name
                    )
                    
                    logger.info(f"Retried AI analysis for message {message_id}")
                    
        except Exception as e:
            logger.error(f"Error retrying pending messages for license {license_id}: {e}")
    
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
            
            # Calculate since_hours based on when the channel was connected
            # This ensures we ONLY fetch messages received after the channel was connected
            config_created_at = config.get("created_at")
            
            if config_created_at:
                # Parse created_at to datetime
                if isinstance(config_created_at, str):
                    try:
                        created_dt = datetime.fromisoformat(config_created_at.replace("Z", "+00:00"))
                        # Remove timezone for comparison with utcnow()
                        if created_dt.tzinfo:
                            created_dt = created_dt.replace(tzinfo=None)
                    except ValueError:
                        created_dt = None
                elif hasattr(config_created_at, "isoformat"):
                    created_dt = config_created_at
                    if hasattr(created_dt, 'tzinfo') and created_dt.tzinfo:
                        created_dt = created_dt.replace(tzinfo=None)
                else:
                    created_dt = None
                
                if created_dt:
                    # Calculate hours since channel was connected
                    hours_since_connected = (datetime.utcnow() - created_dt).total_seconds() / 3600
                    # Add 1 hour buffer to catch any edge cases
                    since_hours = int(hours_since_connected) + 1
                else:
                    # Fallback: if no created_at, only fetch last 1 hour
                    since_hours = 1
            else:
                # No created_at means new config, only fetch last 1 hour
                since_hours = 1
            
            # Fetch new emails using Gmail API
            # Limit to 200 to capture enough messages per poll
            emails = await gmail_service.fetch_new_emails(since_hours=since_hours, limit=200)
            
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

            # Get session info for created_at timestamp
            session_info = await get_telegram_phone_session(license_id)
            
            # Calculate since_hours based on when the channel was connected
            # This ensures we ONLY fetch messages received after the channel was connected
            session_created_at = session_info.get("created_at") if session_info else None
            
            if session_created_at:
                # Parse created_at to datetime
                if isinstance(session_created_at, str):
                    try:
                        created_dt = datetime.fromisoformat(session_created_at.replace("Z", "+00:00"))
                        if created_dt.tzinfo:
                            created_dt = created_dt.replace(tzinfo=None)
                    except ValueError:
                        created_dt = None
                elif hasattr(session_created_at, "isoformat"):
                    created_dt = session_created_at
                    if hasattr(created_dt, 'tzinfo') and created_dt.tzinfo:
                        created_dt = created_dt.replace(tzinfo=None)
                else:
                    created_dt = None
                
                if created_dt:
                    hours_since_connected = (datetime.utcnow() - created_dt).total_seconds() / 3600
                    # Add 1 hour buffer to catch any edge cases
                    since_hours = int(hours_since_connected) + 1
                else:
                    # Fallback: if no created_at, only fetch last 1 hour
                    since_hours = 1
            else:
                # No created_at means new config, only fetch last 1 hour
                since_hours = 1

            phone_service = TelegramPhoneService()

            # Fetch recent messages from Telegram phone account
            # Limit to 200 to capture enough messages per poll
            try:
                messages = await phone_service.get_recent_messages(
                    session_string=session_string,
                    since_hours=since_hours,
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
            
            # Check per-user rate limits (Gemini protection)
            allowed, reason = self._check_user_rate_limit(license_id)
            if not allowed:
                logger.warning(f"Rate limit for license {license_id}: {reason}. Message {message_id} queued.")
                # Message will be processed later when limits reset
                # For now, we skip AI processing - the message is saved but not analyzed
                return
            # Fetch conversation history for context-aware AI responses
            conversation_history = ""
            if recipient:  # recipient contains sender_contact
                try:
                    from models import get_recent_conversation
                    conversation_history = await get_recent_conversation(
                        license_id=license_id,
                        sender_contact=recipient,
                        limit=5
                    )
                    if conversation_history:
                        logger.debug(f"Loaded conversation history for {recipient}: {len(conversation_history)} chars")
                except Exception as hist_e:
                    logger.warning(f"Failed to load conversation history: {hist_e}")
            
            # Process with AI agent
            try:
                # Use semaphore to limit global concurrency for free LLM tiers
                async with self.ai_semaphore:
                    # Add timeout to prevent hanging if AI service stalls
                    result = await asyncio.wait_for(
                        process_message(
                            message=body,
                            sender_name=sender_name,
                            sender_contact=recipient,
                            conversation_history=conversation_history
                        ),
                        timeout=60.0
                    )
            except asyncio.TimeoutError:
                logger.warning(f"AI processing timed out for message {message_id}")
                return
            except Exception as ai_e:
                logger.error(f"AI processing error for message {message_id}: {ai_e}")
                return
            
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
            
            # Increment rate limit counter AFTER successful AI processing
            self._increment_user_rate_limit(license_id)
            
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
                                
                                # === Auto-Purchase Detection ===
                                # If intent is order-related and we find money amounts, auto-create a pending purchase
                                intent = data.get("intent", "").lower()
                                order_intents = ["طلب", "طلب خدمة", "order", "شراء", "اشتراك"]
                                
                                if any(oi in intent for oi in order_intents):
                                    # Extract entities to find money and product info
                                    from analysis_advanced import extract_entities
                                    entities = extract_entities(body)
                                    
                                    money = entities.get("money", [])
                                    quantities = entities.get("quantity", [])
                                    
                                    if money:
                                        # Create auto-purchase for detected amounts
                                        from models.purchases import create_purchase
                                        for m in money[:1]:  # Only first amount detected
                                            try:
                                                amount_str = m.get("amount", "0").replace(",", "")
                                                amount = float(amount_str)
                                                
                                                # Try to extract product name from message
                                                # Look for common product indicators
                                                product_name = "طلب من المحادثة"
                                                product_patterns = [
                                                    r'(?:اشتراك|خدمة|منتج|طلب)\s+([^\d\n,،]{3,30})',
                                                    r'(?:أريد|أبغى|بدي)\s+([^\d\n,،]{3,30})',
                                                ]
                                                import re
                                                for pattern in product_patterns:
                                                    match = re.search(pattern, body)
                                                    if match:
                                                        product_name = match.group(1).strip()[:50]
                                                        break
                                                
                                                # Auto-create pending purchase
                                                await create_purchase(
                                                    license_id=license_id,
                                                    customer_id=customer_id,
                                                    product_name=product_name,
                                                    amount=amount,
                                                    currency="SYP",  # Default to SYP
                                                    status="pending",  # Pending for human review
                                                    notes=f"تم إنشاؤها تلقائياً من الرسالة - {body[:100]}..."
                                                )
                                                logger.info(f"Auto-created pending purchase for customer {customer_id}: {amount} SYP")
                                            except Exception as pe:
                                                logger.warning(f"Error auto-creating purchase: {pe}")
                                # === End Auto-Purchase Detection ===
                                
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
                # Send via Telegram Phone (MTProto) ONLY
                session_string = await get_telegram_phone_session_data(license_id)
                
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
                    except Exception as e:
                        logger.warning(f"Failed to send via Telegram phone for outbox {outbox_id}: {e}")
                else:
                    logger.warning(f"No active Telegram phone session for license {license_id}")

            elif channel == "telegram_bot":
                 # Send via Telegram Bot API ONLY
                try:
                    async with get_db() as db:
                        row = await fetch_one(
                            db,
                            "SELECT bot_token FROM telegram_configs WHERE license_key_id = ?",
                            [license_id],
                        )
                        if row and row.get("bot_token"):
                            from services.telegram_service import TelegramService
                            telegram_service = TelegramService(row["bot_token"])
                            await telegram_service.send_message(
                                chat_id=message["recipient_id"],
                                text=message["body"]
                            )
                            
                            await mark_outbox_sent(outbox_id)
                            logger.info(f"Sent Telegram bot reply for outbox {outbox_id}")
                        else:
                            logger.warning(f"No bot token found for license {license_id}")
                except Exception as e:
                    logger.error(f"Failed to send via Telegram bot for outbox {outbox_id}: {e}")

            
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
