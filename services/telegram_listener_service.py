"""
Al-Mudeer - Telegram Listener Service
Persistent service for real-time Telegram events (Typing, Recording, etc.)
"""
import asyncio
import logging
import os
from typing import Dict, Set
from telethon import TelegramClient, events
from telethon.sessions import StringSession

from logging_config import get_logger


from db_helper import fetch_all, get_db, fetch_one
import base64

# Load environment variables
TELEGRAM_API_ID = os.getenv("TELEGRAM_API_ID")
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH")

from models.base import simple_decrypt

logger = get_logger(__name__)

class TelegramListenerService:
    """
    Manages persistent Telethon clients for multiple sessions.
    Listens for real-time updates like typing indicators.
    """
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(TelegramListenerService, cls).__new__(cls)
            cls._instance.clients = {}  # license_id -> TelegramClient
            cls._instance.running = False
            cls._instance.monitor_task = None
        return cls._instance

    def __init__(self):
        # Already initialized in __new__
        pass

    async def start(self):
        """Start the listener service"""
        if self.running:
            return
            
        logger.info("Starting Telegram Listener Service...")
        self.running = True
        self.monitor_task = asyncio.create_task(self._monitor_sessions())

    async def stop(self):
        """Stop the listener service and all clients"""
        logger.info("Stopping Telegram Listener Service...")
        self.running = False
        
        if self.monitor_task:
            self.monitor_task.cancel()
            try:
                await self.monitor_task
            except asyncio.CancelledError:
                pass
        
        # Disconnect all clients
        keys = list(self.clients.keys())
        for license_id in keys:
            await self._stop_client(license_id)

    async def _monitor_sessions(self):
        """Periodically check for active sessions and start listeners"""
        while self.running:
            try:
                await self._sync_sessions()
            except Exception as e:
                logger.error(f"Error syncing Telegram sessions: {e}")
            
            # Check every minute for new/removed sessions
            await asyncio.sleep(60)

    async def _sync_sessions(self):
        """Sync active DB sessions with running clients"""
        # Fetch all active sessions
        async with get_db() as db:
            try:
                query = """
                    SELECT license_key_id, session_data_encrypted, phone_number 
                    FROM telegram_phone_sessions 
                    WHERE is_active = TRUE
                """
                rows = await fetch_all(db, query)
            except Exception as e:
                logger.error(f"DB Error fetching sessions: {e}")
                return

        active_license_ids = set()
        
        for row in rows:
            license_id = row.get("license_key_id") or row[0]
            encrypted_data = row.get("session_data_encrypted") or row[1]
            phone_number = row.get("phone_number") or row[2]
            
            try:
                session_string = simple_decrypt(encrypted_data)
            except Exception as e:
                logger.error(f"Failed to decrypt session for license {license_id}: {e}")
                continue

            active_license_ids.add(license_id)
            
            # If not running, start client
            if license_id not in self.clients:
                await self._start_client(license_id, session_string, phone_number)

        # Stop clients for sessions that are no longer active
        current_clients = list(self.clients.keys())
        for license_id in current_clients:
            if license_id not in active_license_ids:
                logger.info(f"Session for license {license_id} no longer active. Stopping client.")
                await self._stop_client(license_id)

    async def _start_client(self, license_id: int, session_string: str, phone_number: str):
        """Start a single Telegram client and attach listeners"""
        try:
            logger.info(f"Starting Telegram client for license {license_id} ({phone_number})")
            
            client = TelegramClient(
                StringSession(session_string),
                TELEGRAM_API_ID,
                TELEGRAM_API_HASH
            )
            
            await client.connect()
            
            if not await client.is_user_authorized():
                logger.warning(f"Session unauthorized for license {license_id}. Skipping.")
                await client.disconnect()
                return

            # Attach Event Handlers
            
            # 1. User Update (Typing/Recording/Status)
            @client.on(events.UserUpdate)
            async def handler(event):
                try:
                    # We only care about online status if we were to implement it fully, 
                    # but for now we are removing typing/recording.
                    pass
                        
                except Exception as e:
                    logger.debug(f"Error handling Telegram UserUpdate: {e}")

            # 2. New Message (Incoming)
            @client.on(events.NewMessage(incoming=True))
            async def msg_handler(event):
                try:
                    if not event.is_private:
                        return

                    # 1. Extract basic info
                    sender = await event.get_sender()
                    if not sender: return
                    
                    # sender_contact should follow the same format as MessagePoller: phone or tg:12345
                    sender_contact = getattr(sender, 'phone', None)
                    if sender_contact:
                        if not sender_contact.startswith("+"):
                            sender_contact = "+" + sender_contact
                    else:
                        sender_contact = f"tg:{sender.id}"
                    
                    first = getattr(sender, 'first_name', '') or ''
                    last = getattr(sender, 'last_name', '') or ''
                    sender_name = f"{first} {last}".strip() or "Telegram User"
                    
                    body = event.message.message or ""
                    channel_message_id = str(event.message.id)
                    
                    # 2. Deduplication check
                    async with get_db() as db:
                        existing = await fetch_one(
                            db,
                            "SELECT id FROM inbox_messages WHERE license_key_id = ? AND channel = ? AND channel_message_id = ?",
                            [license_id, "telegram", channel_message_id]
                        )
                        if existing:
                            return

                    # 3. Apply Filters
                    from message_filters import apply_filters
                    from models.inbox import get_inbox_messages
                    recent_messages = await get_inbox_messages(license_id, limit=20)
                    
                    filter_msg = {
                        "body": body,
                        "sender_contact": sender_contact,
                        "sender_name": sender_name,
                        "sender_id": str(event.sender_id) if hasattr(event, 'sender_id') else None,
                        "is_group": event.is_group,
                        "is_channel": event.is_channel,
                        "channel": "telegram"
                    }
                    
                    should_process, reason = await apply_filters(filter_msg, license_id, recent_messages)
                    if not should_process:
                        logger.info(f"Telegram real-time message filtered: {reason}")
                        return

                    # 4. Handle Media (Photo/Voice)
                    attachments = []
                    if event.message.media:
                        try:
                            # Skip huge files > 5MB
                            size = 0
                            if hasattr(event.message.media, "document") and event.message.media.document:
                                size = event.message.media.document.size
                            
                            if size < 5 * 1024 * 1024:
                                file_bytes = await event.message.download_media(file=bytes)
                                if file_bytes:
                                    mime_type = "application/octet-stream"
                                    if hasattr(event.message.media, "photo"):
                                        mime_type = "image/jpeg"
                                    elif hasattr(event.message.media, "document"):
                                        mime_type = event.message.media.document.mime_type
                                        
                                    b64_data = base64.b64encode(file_bytes).decode('utf-8')
                                    attachments.append({
                                        "type": mime_type,
                                        "base64": b64_data,
                                        "filename": f"tg_file_{channel_message_id}"
                                    })
                        except Exception as media_e:
                            logger.debug(f"Failed to download real-time media: {media_e}")

                    # 5. Save to Inbox
                    from models.inbox import save_inbox_message
                    msg_id = await save_inbox_message(
                        license_id=license_id,
                        channel="telegram",
                        body=body,
                        sender_name=sender_name,
                        sender_contact=sender_contact,
                        sender_id=str(sender.id),
                        channel_message_id=channel_message_id,
                        received_at=event.message.date,
                        attachments=attachments
                    )
                    
                    if msg_id:
                        logger.info(f"Saved real-time Telegram message {msg_id} for license {license_id}")
                        
                        # 6. Trigger AI Analysis
                        # Local import to avoid circular dependency
                        from routes.chat_routes import analyze_inbox_message
                        
                        # Get auto-reply preference
                        async with get_db() as db:
                            row = await fetch_one(
                                db,
                                "SELECT auto_reply_enabled FROM telegram_phone_sessions WHERE license_key_id = ? AND is_active = TRUE",
                                [license_id]
                            )
                            auto_reply = bool(row["auto_reply_enabled"]) if row else False

                        asyncio.create_task(
                            analyze_inbox_message(
                                message_id=msg_id,
                                body=body,
                                license_id=license_id,
                                auto_reply=auto_reply,
                                telegram_chat_id=str(sender.id),
                                attachments=attachments
                            )
                        )
                        
                except Exception as e:
                    logger.error(f"Error in Telegram real-time message handler: {e}")

            # Start receiving updates in background
            # Telethon clients run in loop automatically once connected and handlers attached?
            # No, we assume client stays connected. 
            
            self.clients[license_id] = client
            logger.info(f"Telegram client started for license {license_id}")

        except Exception as e:
            logger.error(f"Failed to start Telegram client for license {license_id}: {e}")

    async def _stop_client(self, license_id: int):
        """Stop and remove a client"""
        if license_id in self.clients:
            client = self.clients[license_id]
            try:
                await client.disconnect()
            except Exception as e:
                logger.error(f"Error disconnecting client {license_id}: {e}")
            del self.clients[license_id]

# Global access
_listener_service = None

def get_telegram_listener():
    global _listener_service
    if _listener_service is None:
        _listener_service = TelegramListenerService()
    return _listener_service
