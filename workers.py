"""
Al-Mudeer - Background Workers
Automatic message polling and processing for Email, WhatsApp, and Telegram
"""

import asyncio
import os
from datetime import datetime, timedelta
from typing import Optional, Dict, List
from logging_config import get_logger

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
from services.email_service import EmailService
from services.telegram_service import TelegramService
from services.whatsapp_service import WhatsAppService

# Import models
from models import (
    get_email_config, get_email_password,
    get_telegram_config,
    get_whatsapp_config,
    save_inbox_message,
    update_inbox_analysis,
    get_inbox_messages,
    create_outbox_message,
    approve_outbox_message,
    mark_outbox_sent
)
from agent import process_message
from message_filters import apply_filters


class MessagePoller:
    """Background worker for polling messages from all channels"""
    
    def __init__(self):
        self.running = False
        self.tasks: Dict[int, asyncio.Task] = {}
    
    async def start(self):
        """Start all polling workers"""
        self.running = True
        logger.info("Starting message polling workers...")
        
        # Start polling loop
        asyncio.create_task(self._polling_loop())
    
    async def stop(self):
        """Stop all polling workers"""
        self.running = False
        for task in self.tasks.values():
            task.cancel()
        self.tasks.clear()
        logger.info("Stopped message polling workers")
    
    async def _polling_loop(self):
        """Main polling loop - runs every minute"""
        while self.running:
            try:
                # Get all active licenses with integrations
                active_licenses = await self._get_active_licenses()
                
                for license_id in active_licenses:
                    # Poll each integration type
                    asyncio.create_task(self._poll_email(license_id))
                    asyncio.create_task(self._poll_telegram(license_id))
                    # WhatsApp uses webhooks, so no polling needed
                
                # Wait 60 seconds before next poll
                await asyncio.sleep(60)
                
            except Exception as e:
                logger.error(f"Error in polling loop: {e}", exc_info=True)
                await asyncio.sleep(60)  # Wait before retry
    
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
                    
                    # Get licenses with telegram configs
                    async with db.execute("""
                        SELECT DISTINCT license_key_id 
                        FROM telegram_configs 
                        WHERE is_active = 1
                    """) as cursor:
                        rows = await cursor.fetchall()
                        licenses.extend([row[0] for row in rows])
        
        except Exception as e:
            logger.error(f"Error getting active licenses: {e}")
        
        return list(set(licenses))  # Remove duplicates
    
    async def _poll_email(self, license_id: int):
        """Poll email for new messages"""
        try:
            config = await get_email_config(license_id)
            if not config or not config.get("is_active"):
                return
            
            # Check if it's time to poll (based on check_interval_minutes)
            last_checked = config.get("last_checked_at")
            check_interval = config.get("check_interval_minutes", 5)
            
            if last_checked:
                last_checked_dt = datetime.fromisoformat(last_checked)
                if datetime.now() - last_checked_dt < timedelta(minutes=check_interval):
                    return  # Too soon to check again
            
            password = await get_email_password(license_id)
            if not password:
                logger.warning(f"No password found for license {license_id}")
                return
            
            email_service = EmailService(
                email_address=config["email_address"],
                password=password,
                imap_server=config["imap_server"],
                smtp_server=config["smtp_server"],
                imap_port=config["imap_port"],
                smtp_port=config["smtp_port"]
            )
            
            # Fetch new emails
            emails = await email_service.fetch_new_emails(since_hours=24, limit=50)
            
            # Get recent messages for duplicate detection
            recent_messages = await get_inbox_messages(license_id, limit=50)
            
            # Process each email
            for email_data in emails:
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
                    email_data.get("sender_contact")
                )
            
            # Update last_checked_at
            await self._update_email_last_checked(license_id)
            
        except Exception as e:
            logger.error(f"Error polling email for license {license_id}: {e}", exc_info=True)
    
    async def _poll_telegram(self, license_id: int):
        """Poll Telegram for new messages (if using polling instead of webhook)"""
        # Telegram primarily uses webhooks, but we can implement polling as fallback
        # For now, this is a placeholder - webhooks are preferred
        pass
    
    async def _check_existing_message(self, license_id: int, channel: str, channel_message_id: Optional[str]) -> bool:
        """Check if a message already exists in inbox"""
        if not channel_message_id:
            return False
        
        try:
            async with aiosqlite.connect(DATABASE_PATH) as db:
                async with db.execute("""
                    SELECT id FROM inbox_messages 
                    WHERE license_key_id = ? AND channel = ? AND channel_message_id = ?
                """, (license_id, channel, channel_message_id)) as cursor:
                    row = await cursor.fetchone()
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
        recipient: Optional[str] = None
    ):
        """Analyze message with AI and optionally auto-reply"""
        try:
            # Process with AI agent
            result = await process_message(body)
            
            if not result["success"]:
                logger.warning(f"AI processing failed for message {message_id}: {result.get('error')}")
                return
            
            data = result["data"]
            
            # Update inbox with analysis
            await update_inbox_analysis(
                message_id=message_id,
                intent=data["intent"],
                urgency=data["urgency"],
                sentiment=data["sentiment"],
                summary=data["summary"],
                draft_response=data["draft_response"]
            )
            
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
            # Get outbox message
            async with aiosqlite.connect(DATABASE_PATH) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("""
                    SELECT o.*, i.sender_name, i.body as original_message, i.sender_contact, i.sender_id
                    FROM outbox_messages o
                    JOIN inbox_messages i ON o.inbox_message_id = i.id
                    WHERE o.id = ? AND o.license_key_id = ?
                """, (outbox_id, license_id)) as cursor:
                    row = await cursor.fetchone()
            
            if not row:
                return
            
            message = dict(row)
            
            if channel == "email":
                config = await get_email_config(license_id)
                password = await get_email_password(license_id)
                
                if config and password:
                    email_service = EmailService(
                        email_address=config["email_address"],
                        password=password,
                        imap_server=config["imap_server"],
                        smtp_server=config["smtp_server"],
                        imap_port=config["imap_port"],
                        smtp_port=config["smtp_port"]
                    )
                    
                    await email_service.send_email(
                        to_email=message["recipient_email"],
                        subject=message.get("subject", "رد على رسالتك"),
                        body=message["body"]
                    )
                    
                    await mark_outbox_sent(outbox_id)
                    logger.info(f"Sent email reply for outbox {outbox_id}")
            
            elif channel == "telegram":
                config = await get_telegram_config(license_id)
                
                if config:
                    telegram_service = TelegramService(config["bot_token"])
                    await telegram_service.send_message(
                        chat_id=message["recipient_id"],
                        text=message["body"]
                    )
                    
                    await mark_outbox_sent(outbox_id)
                    logger.info(f"Sent Telegram reply for outbox {outbox_id}")
            
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
            async with aiosqlite.connect(DATABASE_PATH) as db:
                await db.execute("""
                    UPDATE email_configs 
                    SET last_checked_at = ? 
                    WHERE license_key_id = ?
                """, (datetime.now().isoformat(), license_id))
                await db.commit()
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

