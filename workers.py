"""
Al-Mudeer - Background Workers
Automatic message polling and processing for Email, WhatsApp, and Telegram
"""

import asyncio
import os
import random
import hashlib
import base64
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Set, Any

from logging_config import get_logger
from models.task_queue import fetch_next_task, complete_task, fail_task
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
from services.backfill_service import get_backfill_service
from cache import cache

# Import models
from models import (
    get_email_config, get_email_oauth_tokens,
    get_telegram_config,
    get_whatsapp_config,
    save_inbox_message,
    update_inbox_status,
    update_inbox_analysis,
    get_preferences,
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
        
        # Track message retry counts to prevent infinite retry loops
        # Structure: {message_id: retry_count}
        self._retry_counts: Dict[int, int] = {}
        
        # Track recent message hashes for duplicate detection
        self._recent_message_hashes: Set[str] = set()
        
        # Track messages retried this poll cycle (cleared each cycle)
        # Prevents rapid retry loops within the same 5-minute cycle
        self._retried_this_cycle: Set[int] = set()
        
        # Per-user rate limiting is now handled via Redis/CacheManager
    
    async def _check_user_rate_limit(self, license_id: int) -> tuple[bool, str]:
        """
        Check if user is within rate limits using Redis.
        Returns (allowed, reason) tuple.
        """
        daily_key = f"rate_limit:daily:{license_id}"
        minute_key = f"rate_limit:minute:{license_id}"
        
        # Get current counts (default to 0)
        daily_count = await cache.get(daily_key) or 0
        minute_count = await cache.get(minute_key) or 0
        
        # Check daily limit
        if int(daily_count) >= self.MAX_MESSAGES_PER_USER_PER_DAY:
            return False, f"Daily limit reached ({self.MAX_MESSAGES_PER_USER_PER_DAY}/day)"
        
        # Check minute limit
        if int(minute_count) >= self.MAX_MESSAGES_PER_USER_PER_MINUTE:
            return False, f"Minute limit reached ({self.MAX_MESSAGES_PER_USER_PER_MINUTE}/min)"
        
        return True, ""
    
    async def _increment_user_rate_limit(self, license_id: int):
        """Increment rate limit counters for a user in Redis."""
        daily_key = f"rate_limit:daily:{license_id}"
        minute_key = f"rate_limit:minute:{license_id}"
        
        # Increment daily
        d_val = await cache.increment(daily_key)
        if d_val == 1:
            await cache.expire(daily_key, 86400) # 24 hours
            
        # Increment minute
        m_val = await cache.increment(minute_key)
        if m_val == 1:
            await cache.expire(minute_key, 60) # 1 minute
            
        logger.debug(
            f"License {license_id} rate limit: "
            f"{d_val}/{self.MAX_MESSAGES_PER_USER_PER_DAY} daily, "
            f"{m_val}/{self.MAX_MESSAGES_PER_USER_PER_MINUTE} per min"
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
                # Clear retry tracking at the start of each cycle
                # This allows messages to be retried in this new 5-min window
                self._retried_this_cycle.clear()
                
                # Get all active licenses with integrations
                active_licenses = await self._get_active_licenses()
                now_iso = datetime.utcnow().isoformat()
                self.status["email_polling"]["last_check"] = now_iso
                self.status["telegram_polling"]["last_check"] = now_iso
                
                for license_id in active_licenses:
                    # Stagger polling: increased delay between licenses to spread AI load
                    # 10-15s gap ensures we stay under Gemini's 15 RPM limit across users
                    await asyncio.sleep(random.uniform(10.0, 15.0))
                    # Poll each integration type
                    t1 = asyncio.create_task(self._poll_email(license_id))
                    self.background_tasks.add(t1)
                    t1.add_done_callback(self.background_tasks.discard)
                    
                    t2 = asyncio.create_task(self._poll_telegram(license_id))
                    self.background_tasks.add(t2)
                    t2.add_done_callback(self.background_tasks.discard)
                    # WhatsApp uses webhooks, so no polling needed
                    
                    await self._retry_pending_messages(license_id)
                    
                    # Poll Telegram delivery statuses (read receipts)
                    # We run this less frequently or just part of the loop
                    # Using create_task to run concurrently
                    t3 = asyncio.create_task(self._poll_telegram_outbox_status(license_id))
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
            # Check global rate limiter cooldown first
            # This prevents multiple licenses from queuing up requests when we're already rate limited
            from services.llm_provider import get_rate_limiter
            rate_limiter = get_rate_limiter()
            
            if rate_limiter.is_in_cooldown():
                remaining = rate_limiter.get_cooldown_remaining()
                logger.debug(f"License {license_id}: Global rate limit cooldown active ({remaining:.1f}s), skipping retries")
                return

            # Find messages with pending placeholder response
            placeholder = "⏳ جاري تحليل الرسالة تلقائياً..."
            
            async with get_db() as db:
                # Query messages with placeholder ai_draft_response
                # Use different datetime syntax for PostgreSQL vs SQLite
                if DB_TYPE == "postgresql":
                    query = """
                        SELECT id, body, sender_contact, sender_name, channel
                        FROM inbox_messages
                        WHERE license_key_id = $1
                          AND (ai_draft_response = $2 OR ai_draft_response IS NULL OR ai_draft_response = '')
                          AND created_at > NOW() - INTERVAL '24 hours'
                        ORDER BY created_at DESC
                        LIMIT 1
                    """
                else:
                    query = """
                        SELECT id, body, sender_contact, sender_name, channel
                        FROM inbox_messages
                        WHERE license_key_id = ?
                          AND (ai_draft_response = ? OR ai_draft_response IS NULL OR ai_draft_response = '')
                          AND created_at > datetime('now', '-24 hours')
                        ORDER BY created_at DESC
                        LIMIT 1
                    """
                logger.debug(f"License {license_id}: Querying pending messages with DB_TYPE={DB_TYPE}")
                rows = await fetch_all(db, query, [license_id, placeholder])
                logger.debug(f"License {license_id}: Query returned {len(rows) if rows else 0} rows")
                
                if not rows:
                    return
                
                logger.info(f"License {license_id}: Found {len(rows)} pending messages to retry")
                
                for row in rows:
                    message_id = row.get("id")
                    body = row.get("body")
                    sender_contact = row.get("sender_contact")
                    sender_name = row.get("sender_name")
                    channel = row.get("channel")
                    
                    # Skip if already retried this cycle (prevents rapid retry loops)
                    if message_id in self._retried_this_cycle:
                        logger.debug(f"Skipping message {message_id}: already retried this cycle")
                        continue
                    
                    # Check rate limit before retrying
                    allowed, reason = await self._check_user_rate_limit(license_id)
                    if not allowed:
                        logger.debug(f"Rate limit for license {license_id}: {reason}. Retry skipped.")
                        break  # Stop retrying for this license
                    
                    # Mark as retried this cycle
                    self._retried_this_cycle.add(message_id)
                    
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
            logger.error(f"Error retrying pending messages for license {license_id}: {type(e).__name__}: {e}", exc_info=True)
    
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
            
            # Check for backfill trigger (first time loading)
            backfill_service = get_backfill_service()
            is_backfill = await backfill_service.should_trigger_backfill(license_id, "email")
            
            # Calculate since_hours based on when the channel was connected
            config_created_at = config.get("created_at")
            
            if is_backfill:
                # Fetch 30 days history for backfill
                logger.info(f"Triggering historical backfill for license {license_id} (email)")
                since_hours = backfill_service.backfill_days * 24
            elif config_created_at:
                # Standard polling: calculate hours since connected
                try:
                    if isinstance(config_created_at, str):
                        created_dt = datetime.fromisoformat(config_created_at.replace('Z', '+00:00'))
                    elif isinstance(config_created_at, datetime):
                        created_dt = config_created_at
                    else:
                        # Fallback if unknown type
                        created_dt = datetime.now(timezone.utc) - timedelta(hours=24)

                    # Handle offset-naive vs offset-aware
                    if created_dt.tzinfo is None:
                        created_dt = created_dt.replace(tzinfo=timezone.utc)
                    
                    now = datetime.now(timezone.utc)
                    hours_since_connected = (now - created_dt).total_seconds() / 3600
                    
                    # Fetch messages since connection (plus 1 hour buffer)
                    # But cap at 24 hours for regular polling to avoid huge fetches if system was down
                    since_hours = min(int(hours_since_connected) + 1, 720) # cap at 30 days anyway
                except Exception as e:
                    logger.warning(f"Error parsing created_at: {e}")
                    since_hours = 24
            else:
                since_hours = 1
                
            # Update last checked timestamp
            self.status["email_polling"]["last_check"] = datetime.utcnow().isoformat()
            
            # Fetch emails
            # If backfill, fetch more (e.g. 500), otherwise standard limit
            limit = 500 if is_backfill else 200
            # Fetch messages
            if is_backfill:
                # Smart Backfill: Fetch threads that are UNREPLIED (last message not from us)
                # This ensures we don't import old conversations we already finished
                backfill_days = int(os.getenv("BACKFILL_DAYS", "30"))
                emails = await gmail_service.fetch_unreplied_threads(days=backfill_days, limit=100)
                logger.info(f"Backfill: Fetched {len(emails)} unreplied emails")
            else:
                emails = await gmail_service.fetch_new_emails(since_hours=since_hours, limit=limit)

            
            if not emails:
                return

            # If backfill is active, queue ALL fetched messages and skip standard processing
            if is_backfill:
                backfill_messages = []
                for email_data in emails:
                    # Apply filters (Spam, Promo, Bot, etc.)
                    # We skip duplicate check for backfill (pass empty list)
                    filter_msg = {
                        "body": email_data.get("body", "") or email_data.get("snippet", ""),
                        "sender_contact": email_data.get("sender"),
                        "sender_name": email_data.get("sender_name"),
                        "subject": email_data.get("subject"),
                        "channel": "email",
                        "attachments": email_data.get("attachments", []),
                    }
                    should_process, reason = await apply_filters(filter_msg, license_id, [])
                    if not should_process:
                        logger.debug(f"Skipping backfill email from {filter_msg['sender_contact']}: {reason}")
                        continue

                    # Extract attachments if present (Gmail service returns simplified attachment objects)
                    attachments = []
                    if "attachments" in email_data and email_data["attachments"]:
                        # Ensure attachments are JSON serializable
                        attachments = email_data["attachments"]
                    
                    backfill_messages.append({
                        "body": email_data.get("body", "") or email_data.get("snippet", ""),
                        "channel_message_id": email_data.get("id"),
                        "sender_contact": email_data.get("sender"),
                        "sender_name": email_data.get("sender_name"),
                        "subject": email_data.get("subject"),
                        "received_at": datetime.fromtimestamp(email_data.get("internalDate", 0)/1000) if email_data.get("internalDate") else None,
                        "attachments": attachments
                    })
                
                queued = await backfill_service.schedule_historical_messages(
                    license_id=license_id,
                    channel="email",
                    messages=backfill_messages
                )
                
                if queued > 0:
                    logger.info(f"Queued {queued} email messages for backfill. Skipping immediate processing.")
                    return

            # Process standard emails (non-backfill)
            processed_count = 0
            
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
                    "channel": "email",
                    "attachments": email_data.get("attachments", []),
                }
                
                should_process, filter_reason = await apply_filters(
                    message_dict, license_id, recent_messages
                )
                
                if not should_process:
                    logger.info(f"Message filtered: {filter_reason}")
                    continue
                
                # Process attachments
                attachments = email_data.get("attachments", [])
                for att in attachments:
                    try:
                        # Download attachment data (if not already present)
                        if att.get("file_id") and not att.get("base64"):
                            data = await gmail_service.get_attachment_data(
                                email_data.get("channel_message_id"), 
                                att["file_id"]
                            )
                            if data:
                                att["base64"] = base64.b64encode(data).decode("utf-8")
                    except Exception as e:
                        logger.warning(f"Failed to download email attachment {att.get('file_name')}: {e}")

                # Save to inbox
                msg_id = await save_inbox_message(
                    license_id=license_id,
                    channel="email",
                    body=email_data["body"],
                    sender_name=email_data["sender_name"],
                    sender_contact=email_data["sender_contact"],
                    sender_id=None,
                    subject=email_data.get("subject"),
                    channel_message_id=email_data.get("channel_message_id"),
                    received_at=email_data.get("received_at"),
                    attachments=attachments
                )
                
                # Analyze with AI
                await self._analyze_and_process_message(
                    message_id=msg_id,
                    body=email_data["body"],
                    license_id=license_id,
                    auto_reply=config.get("auto_reply_enabled", False),
                    channel="email",
                    recipient=email_data.get("sender_contact"),
                    sender_name=email_data.get("sender_name"),
                    channel_message_id=email_data.get("channel_message_id"),
                    attachments=attachments
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
            
            # Check for backfill trigger (first time loading)
            backfill_service = get_backfill_service()
            is_backfill = await backfill_service.should_trigger_backfill(license_id, "telegram")
            
            if is_backfill:
                logger.info(f"Triggering historical backfill for license {license_id} (telegram)")
                since_hours = backfill_service.backfill_days * 24

            phone_service = TelegramPhoneService()

            # Get recent inbox messages for duplicate detection
            # Use higher limit to avoid missing duplicates when inbox is large
            recent_limit = 200
            recent_messages = await get_inbox_messages(license_id, limit=recent_limit)

            # Extract exclude_ids for optimization
            exclude_ids = [msg["channel_message_id"] for msg in recent_messages if msg.get("channel_message_id")]

            # Fetch messages with optimization
            try:
                # If backfill, use larger limit
                limit = 500 if is_backfill else 200
                messages = await phone_service.get_recent_messages(
                    session_string=session_string,
                    since_hours=since_hours,
                    limit=limit,
                    exclude_ids=exclude_ids,
                    skip_replied=is_backfill  # Skip dialogs where last message is ours (already replied)
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

            # If backfill is active, queue ALL fetched messages and skip standard processing
            if is_backfill:
                backfill_messages = []
                for msg in messages:
                    # Apply filters (Spam, Bot, etc.)
                    filter_msg = {
                        "body": msg.get("body", ""),
                        "sender_contact": msg.get("sender_contact"),
                        "sender_name": msg.get("sender_name"),
                        "subject": msg.get("subject"),
                        "attachments": msg.get("attachments", []),
                    }
                    should_process, reason = await apply_filters(filter_msg, license_id, [])
                    if not should_process:
                        logger.debug(f"Skipping backfill telegram from {filter_msg['sender_contact']}: {reason}")
                        continue

                    # Map to backfill format (keys are mostly same)
                    backfill_messages.append({
                        "body": msg.get("body", ""),
                        "channel_message_id": msg.get("channel_message_id"),
                        "sender_contact": msg.get("sender_contact"),
                        "sender_name": msg.get("sender_name"),
                        "sender_id": msg.get("sender_id"),
                        "subject": msg.get("subject"),
                        "received_at": msg.get("received_at"),
                        "attachments": msg.get("attachments")
                    })
                
                queued = await backfill_service.schedule_historical_messages(
                    license_id=license_id,
                    channel="telegram",
                    messages=backfill_messages
                )
                
                if queued > 0:
                    logger.info(f"Queued {queued} telegram messages for backfill. Skipping immediate processing.")
                    return

            # Group messages by sender for burst handling
            # Structure: {sender_contact: [msg_data, ...]}
            grouped_messages: Dict[str, List[Dict]] = {}
            saved_messages_map: Dict[str, int] = {}  # channel_message_id -> db_id
            
            for msg in messages:
                # Check existance
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
                    "sender_id": msg.get("sender_id"),
                    "subject": msg.get("subject"),
                    "channel": "telegram",
                    "attachments": msg.get("attachments", []),
                    "is_group": msg.get("is_group"),
                    "is_channel": msg.get("is_channel"),
                }

                should_process, filter_reason = await apply_filters(
                    message_dict, license_id, recent_messages
                )

                if not should_process:
                    logger.info(f"Telegram phone message filtered: {filter_reason}")
                    continue

                # Save to inbox
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
                    attachments=msg.get("attachments")
                )
                
                # Add to map and groups
                saved_messages_map[msg["channel_message_id"]] = msg_id
                
                sender_key = msg.get("sender_contact") or "unknown"
                if sender_key not in grouped_messages:
                    grouped_messages[sender_key] = []
                
                # Store msg data with DB ID
                msg["db_id"] = msg_id
                grouped_messages[sender_key].append(msg)

            # Process groups (Burst Handling)
            for sender_key, group in grouped_messages.items():
                if not group:
                    continue
                
                # Sort by time (oldest first) to reconstruct conversation
                group.sort(key=lambda x: x.get("received_at", datetime.min))
                
                if len(group) == 1:
                    # Single message case
                    msg = group[0]
                    await self._analyze_and_process_message(
                        msg["db_id"],
                        msg["body"],
                        license_id,
                        session_info.get("auto_reply_enabled", False) if session_info else False, 
                        "telegram",
                        msg.get("sender_contact"),
                        msg.get("sender_name"),
                        msg.get("channel_message_id"),
                        attachments=msg.get("attachments")
                    )
                else:
                    # Burst case - merge messages
                    # We process only the LATEST message, but include context from others
                    latest_msg = group[-1]
                    
                    # Combine bodies
                    combined_body = ""
                    for m in group:
                        timestamp = m.get("received_at").strftime("%H:%M") if m.get("received_at") else ""
                        body_text = m['body'] or "[ملف مرفق]"
                        combined_body += f"[{timestamp}] {body_text}\n"
                    
                    combined_body = combined_body.strip()
                    logger.info(f"Burst detected for {sender_key}: merged {len(group)} messages")
                    
                    # Mark previous messages as 'analyzed' with special note
                    for m in group[:-1]:
                        # Use update_inbox_analysis to set status without triggering AI
                        try:
                            await update_inbox_analysis(
                                message_id=m["db_id"],
                                intent="merged",
                                urgency="low",
                                sentiment="neutral",
                                language=None,
                                dialect=None,
                                summary="تم دمج الرسالة مع الرد التالي",
                                draft_response=""
                            )
                        except Exception as e:
                            logger.error(f"Failed to mark merged message {m['db_id']}: {e}")

                    # Process the latest message with combined context
                    # Pass original attachments from ALL messages
                    all_attachments = []
                    for m in group:
                        if m.get("attachments"): 
                            all_attachments.extend(m["attachments"])
                    
                    await self._analyze_and_process_message(
                        latest_msg["db_id"],
                        combined_body, # Use combined body for AI understanding
                        license_id,
                        session_info.get("auto_reply_enabled", False) if session_info else False,
                        "telegram",
                        latest_msg.get("sender_contact"),
                        latest_msg.get("sender_name"),
                        latest_msg.get("channel_message_id"),
                        attachments=all_attachments
                    )

            # Update last sync time
            await update_telegram_phone_session_sync_time(license_id)

        except Exception as e:
            logger.error(f"Error polling Telegram phone for license {license_id}: {e}", exc_info=True)
    
    def _get_message_hash(self, body: str, sender: Optional[str] = None, channel_message_id: Optional[str] = None) -> str:
        """
        Create a hash to detect duplicate messages.
        Uses channel_message_id if available (exact duplicate), otherwise full body + sender.
        """
        if channel_message_id:
            # Exact duplicate detection using platform message ID
            content = f"msg_id:{channel_message_id}"
        else:
            # Fallback to full body hash (for platforms without message IDs)
            content = f"{sender or 'unknown'}:{body.strip().lower()}"
        return hashlib.md5(content.encode()).hexdigest()
    
    def _is_duplicate_content(self, body: str, sender: Optional[str] = None, channel_message_id: Optional[str] = None) -> bool:
        """
        Check if we've recently processed the EXACT same message.
        Only returns True if channel_message_id matches (same message received twice).
        Different messages with similar content are NOT considered duplicates.
        """
        # Only check for duplicates if we have a channel_message_id
        # This prevents marking unique messages with similar content as duplicates
        if not channel_message_id:
            return False  # Can't determine duplicate without message ID
        
        msg_hash = self._get_message_hash(body, sender, channel_message_id)
        
        if msg_hash in self._processed_hashes:
            logger.debug(f"Exact duplicate message detected (same channel_message_id): {channel_message_id}")
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
        sender_name: Optional[str] = None,
        channel_message_id: Optional[str] = None,
        attachments: Optional[List[Dict[str, Any]]] = None
    ):
        """
        Analyze message with AI and optionally auto-reply.
        Refactored to use centralized analysis_service to prevent logic duplication.
        All AI, CRM, Auto-purchase, and Notification logic is now in process_inbox_message_logic.
        """
        try:
            # Check for duplicate content (channel_message_id) 
            if self._is_duplicate_content(body, sender_name, channel_message_id):
                logger.info(f"Skipping AI for message {message_id}: exact duplicate")
                from models import update_inbox_analysis
                try:
                    await update_inbox_analysis(
                        message_id=message_id,
                        intent="duplicate",
                        urgency="low",
                        sentiment="neutral",
                        language=None,
                        dialect=None,
                        summary="تم تخطي التحليل: محتوى مكرر",
                        draft_response=""
                    )
                except Exception as e:
                    logger.error(f"Failed to update duplicate message status: {e}")
                return

            from services.analysis_service import process_inbox_message_logic
            
            # Delegate to the centralized logic
            await process_inbox_message_logic(
                message_id=message_id,
                body=body,
                license_id=license_id,
                auto_reply=auto_reply,
                attachments=attachments
            )
            
            await self._increment_user_rate_limit(license_id)

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
            
            # Track auto-reply in analytics
            from models import update_daily_analytics
            await update_daily_analytics(
                license_id=license_id,
                auto_replies=1
            )
            logger.info(f"Auto-reply sent for message {message_id}")
        
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
            
            # Extract Audio Tag
            import re
            body = message["body"]
            audio_path = None
            
            audio_match = re.search(r'\[AUDIO: (.*?)\]', body)
            if audio_match:
                audio_path = audio_match.group(1).strip()
                # Remove tag from body for text sending
                body = body.replace(audio_match.group(0), "").strip()
            
            sent_anything = False
            
            # SEND TEXT PART (only if NO audio - audio-only for natural human-like response)
            if body and not audio_path:  # Skip text when audio is present
                
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
                            body=body, # Use stripped body
                            reply_to_message_id=message.get("inbox_message_id")
                        )
                        sent_anything = True
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
                                    text=body # Use stripped body
                                )
                                sent_anything = True
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
                                    text=body # Use stripped body
                                )
                                sent_anything = True
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
                            message=body # Use stripped body
                        )
                        
                        if result["success"]:
                            sent_anything = True
                            logger.info(f"Sent WhatsApp reply for outbox {outbox_id}")
                            
                            # Save platform message ID for delivery receipt tracking
                            wa_message_id = result.get("message_id")
                            if wa_message_id:
                                try:
                                    from services.delivery_status import save_platform_message_id
                                    await save_platform_message_id(outbox_id, wa_message_id)
                                except Exception as e:
                                    logger.warning(f"Failed to save WA message ID: {e}")


            # SEND AUDIO PART (All Channels)
            if audio_path:
                try:
                    if channel == "whatsapp":
                        config = await get_whatsapp_config(license_id)
                        if config:
                            whatsapp_service = WhatsAppService(
                                phone_number_id=config["phone_number_id"],
                                access_token=config["access_token"]
                            )
                            
                            # Upload media
                            media_id = await whatsapp_service.upload_media(audio_path)
                            
                            if media_id:
                                # Send audio message
                                await asyncio.sleep(1)  # Small delay to ensure order
                                await whatsapp_service.send_audio_message(
                                    to=message["recipient_id"],
                                    media_id=media_id
                                )
                                sent_anything = True
                                logger.info(f"Sent WhatsApp audio reply for outbox {outbox_id}")
                    
                    elif channel == "telegram_bot":
                        async with get_db() as db:
                            row = await fetch_one(
                                db,
                                "SELECT bot_token FROM telegram_configs WHERE license_key_id = ?",
                                [license_id],
                            )
                            if row and row.get("bot_token"):
                                telegram_service = TelegramService(row["bot_token"])
                                await asyncio.sleep(1)  # Small delay to ensure order
                                await telegram_service.send_voice(
                                    chat_id=message["recipient_id"],
                                    audio_path=audio_path
                                )
                                sent_anything = True
                                logger.info(f"Sent Telegram Bot audio reply for outbox {outbox_id}")
                    
                    elif channel == "telegram":
                        session_string = await get_telegram_phone_session_data(license_id)
                        if session_string:
                            phone_service = TelegramPhoneService()
                            recipient = message.get("recipient_id") or message.get("sender_id")
                            if recipient:
                                await asyncio.sleep(1)  # Small delay to ensure order
                                await phone_service.send_voice(
                                    session_string=session_string,
                                    recipient_id=str(recipient),
                                    audio_path=audio_path
                                )
                                sent_anything = True
                                logger.info(f"Sent Telegram Phone audio reply for outbox {outbox_id}")
                    
                    elif channel == "email":
                        # For email, audio is sent as attachment (handled in GmailAPIService)
                        # TODO: Add audio attachment support to GmailAPIService if needed
                        logger.info(f"Email audio attachments not yet implemented for outbox {outbox_id}")
                        
                except Exception as e:
                    logger.error(f"Failed to send audio for channel {channel}: {e}")
            
            # Mark outbox as sent if anything was sent
            if sent_anything:
                await mark_outbox_sent(outbox_id)
        
        except Exception as e:
            logger.error(f"Error sending message {outbox_id}: {e}", exc_info=True)
    
    async def _poll_telegram_outbox_status(self, license_id: int):
        """Poll Telegram Phone outbox messages for read receipts"""
        try:
            # Get Telegram phone session string
            session_string = await get_telegram_phone_session_data(license_id)
            if not session_string:
                return

            # Find outbox messages that are 'sent' or 'delivered' (not 'read' or 'failed')
            # and imply 'telegram' channel
            # Calculate 24h cutoff in Python for DB compatibility
            cutoff = datetime.utcnow() - timedelta(hours=24)
            cutoff_value = cutoff if DB_TYPE == "postgresql" else cutoff.isoformat()

            async with get_db() as db:
                rows = await fetch_all(
                    db,
                    """
                    SELECT id, platform_message_id, delivery_status, created_at
                    FROM outbox_messages
                    WHERE license_key_id = ? 
                      AND channel = 'telegram'
                      AND delivery_status IN ('sent', 'delivered')
                      AND platform_message_id IS NOT NULL
                      AND created_at > ?
                    """,
                    [license_id, cutoff_value]
                )
            
            if not rows:
                return

            platform_ids = [row["platform_message_id"] for row in rows]
            phone_service = TelegramPhoneService()
            
            # Identify status
            statuses = await phone_service.get_messages_read_status(
                session_string=session_string,
                channel_message_ids=platform_ids
            )
            
            # Update statuses
            from services.delivery_status import update_delivery_status
            
            count = 0
            for platform_id, status in statuses.items():
                if status == "read":
                    updated = await update_delivery_status(platform_id, "read")
                    if updated:
                        count += 1
            
            if count > 0:
                logger.info(f"Updated {count} Telegram messages to READ for license {license_id}")

        except Exception as e:
            logger.error(f"Error polling Telegram outbox status: {e}")

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


# ============ FCM Token Cleanup Worker ============

_token_cleanup_task: Optional[asyncio.Task] = None


async def _token_cleanup_loop():
    """Background loop that runs once per day to clean up expired FCM tokens."""
    while True:
        try:
            from services.fcm_mobile_service import cleanup_expired_tokens
            # Cleanup tokens inactive for > 30 days
            cleaned = await cleanup_expired_tokens(days_inactive=30)
            if cleaned > 0:
                logger.info(f"Daily Cleanup: Removed {cleaned} expired FCM tokens")
        except Exception as e:
            logger.error(f"Error in token cleanup loop: {e}", exc_info=True)
        
        # Wait 24 hours before next check
        # Add random jitter to avoid thundering herd if we had multiple instances
        await asyncio.sleep(24 * 60 * 60 + random.randint(0, 3600))


async def start_token_cleanup_worker():
    """Start the token cleanup background task."""
    global _token_cleanup_task
    if _token_cleanup_task is None:
        _token_cleanup_task = asyncio.create_task(_token_cleanup_loop())
        logger.info("Started FCM token cleanup worker")


async def stop_token_cleanup_worker():
    """Stop the token cleanup background task."""
    global _token_cleanup_task
    if _token_cleanup_task:
        _token_cleanup_task.cancel()
        _token_cleanup_task = None
        logger.info("Stopped FCM token cleanup worker")

# ============ Task Queue Worker ============

class TaskWorker:
     """
     Persistent Worker for DB-backed Task Queue.
     """
     def __init__(self, worker_id: str = "worker-main"):
         self.worker_id = worker_id
         self.running = False
         self._loop_task = None
         
     async def start(self):
         self.running = True
         logger.info(f"TaskWorker {self.worker_id} started")
         self._loop_task = asyncio.create_task(self._process_loop())
         
     async def stop(self):
         self.running = False
         if self._loop_task:
             self._loop_task.cancel()
             try:
                 await self._loop_task
             except: pass
         logger.info(f"TaskWorker {self.worker_id} stopped")
 
     async def _process_loop(self):
         while self.running:
             try:
                 # 1. Fetch Task
                 task = await fetch_next_task(self.worker_id)
                 
                 if not task:
                     await asyncio.sleep(1.0) # Idle wait
                     continue
                 
                 task_id = task["id"]
                 task_type = task["task_type"]
                 payload = task["payload"]
                 
                 logger.info(f"Processing task {task.get('id')}: {task_type}")
                 
                 # 2. Execute Logic
                 try:
                     result = None
                     if task_type == "analyze_message":
                          from services.analysis_service import process_inbox_message_logic
                          result = await process_inbox_message_logic(
                              message_id=payload.get("message_id"),
                              body=payload.get("body"),
                              license_id=payload.get("license_id"),
                              auto_reply=payload.get("auto_reply", False),
                              telegram_chat_id=payload.get("telegram_chat_id"),
                              attachments=payload.get("attachments")
                          )
                     elif task_type == "analyze":
                          # Generic analyze from main.py endpoint
                          from agent import process_message
                          result = await process_message(
                              message=payload.get("message"),
                              message_type=payload.get("message_type"),
                              sender_name=payload.get("sender_name"),
                              sender_contact=payload.get("sender_contact"),
                          )
                     
                     # 3. Complete
                     await complete_task(task_id)
                     logger.info(f"Task {task_id} completed")
                     
                 except Exception as e:
                     logger.error(f"Task {task_id} failed: {e}", exc_info=True)
                     await fail_task(task_id, str(e))
                     
             except Exception as outer_e:
                 logger.error(f"Worker loop error: {outer_e}")
                 await asyncio.sleep(5.0)
