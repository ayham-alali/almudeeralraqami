"""
Al-Mudeer - Telegram Listener Service
Persistent service for real-time Telegram events (Typing, Recording, etc.)
"""
import asyncio
import logging
import os
from typing import Dict, Set, Optional
from telethon import TelegramClient, events
from telethon.sessions import StringSession

from logging_config import get_logger


from datetime import datetime, timezone
from db_helper import fetch_all, get_db, fetch_one, execute_sql
import base64

# Load environment variables
TELEGRAM_API_ID = os.getenv("TELEGRAM_API_ID")
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH")

from models.base import simple_decrypt, simple_encrypt

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
            cls._instance.locks = {} # license_id -> asyncio.Lock
            cls._instance.running = False
            cls._instance.monitor_task = None
            cls._instance.background_tasks = set() # Track fire-and-forget tasks
            cls._instance.distributed_lock = None # Distributed Lock instance
        return cls._instance

    def __init__(self):
        # Already initialized in __new__
        pass

    async def start(self):
        """Start the listener service"""
        if self.running:
            return

        # Check for manual disable (useful for dev/prod split)
        if os.getenv("DISABLE_TELEGRAM_LISTENER", "").lower() == "true":
            logger.info("Telegram Listener is disabled via environment variable.")
            return
            
        # --- Distributed Lock Mechanism ---
        # Replaces local PID file to support multiple deployments sharing a DB (e.g. Railway Rolling Updates)
        # Use a fixed key for Telegram Service (e.g. 884848)
        from services.distributed_lock import DistributedLock
        
        self.distributed_lock = DistributedLock(lock_id=884848, lock_name="telegram_listener")
        acquired = await self.distributed_lock.acquire()
        
        if not acquired:
            logger.warning("Telegram Listener Lock is held by another process/deployment. Entering Standby Mode.")
            # We do NOT return or stop completely - we just don't start the monitor task.
            # But wait - if we don't start, ensure_client_active calls might fail if they expect self.running?
            # Actually, ensure_client_active should check the lock too.
            # For now, simplest: Return. We are not the leader.
            return
        
        # --------------------------

        logger.info(f"Starting Telegram Listener Service (PID: {os.getpid()})...")
        self.running = True
        self.monitor_task = asyncio.create_task(self._monitor_sessions())

    async def stop(self):
        """Stop the listener service and all clients"""
        logger.info("Stopping Telegram Listener Service...")
        self.running = False
        
        # Release distributed lock if we own it
        if self.distributed_lock:
            await self.distributed_lock.release()

        if self.monitor_task:
            self.monitor_task.cancel()
            try:
                await self.monitor_task
            except asyncio.CancelledError:
                pass
            self.monitor_task = None
        
        # Disconnect all clients
        keys = list(self.clients.keys())
        for license_id in keys:
            await self._stop_client(license_id)

    async def _monitor_sessions(self):
        """Periodically check for active sessions and start listeners"""
        while self.running:
            try:
                # Heartbeat log - indicates leader is active and healthy
                logger.info(f"[Heartbeat] Telegram Leader (PID: {os.getpid()}) syncing sessions...")
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
                # Ensure lock exists
                if license_id not in self.locks:
                    self.locks[license_id] = asyncio.Lock()
                    
                async with self.locks[license_id]:
                    # Double check inside lock
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
        client = None
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

            # Store client immediately
            self.clients[license_id] = client
            logger.info(f"Telegram client started for license {license_id}")
            
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
                    # Filter: Only private chats or small groups? 
                    # For now process everything, filtering happens inside analysis
                    
                    sender = await event.get_sender()
                    # sender_id = sender.id
                    body = event.raw_text or ""
                    
                    if not body and not event.message.media:
                        return
                        
                    # Extract basic info
                    sender_name = "Unknown"
                    if hasattr(sender, 'first_name') and sender.first_name:
                        sender_name = f"{sender.first_name or ''} {sender.last_name or ''}".strip()
                    elif hasattr(sender, 'title'): # Group/Channel
                        sender_name = sender.title
                    elif hasattr(sender, 'username') and sender.username:
                        sender_name = sender.username
                        
                    sender_contact = None
                    # Normalize contact extraction to match telegram_phone_service.py
                    if hasattr(sender, 'phone') and sender.phone:
                        # Normalize phone: always add + prefix
                        phone = sender.phone
                        sender_contact = "+" + phone if phone.isdigit() else phone
                    elif hasattr(sender, 'username') and sender.username:
                        sender_contact = f"@{sender.username}"
                    elif hasattr(sender, 'id'):
                        sender_contact = str(sender.id)
                        
                    channel_message_id = str(event.message.id)
                    
                    # 3. Check for Duplicates (Basic check)
                    # Ideally we use Redis, but here we query DB via `models`
                    from models import get_inbox_messages
                    # A better way is to rely on `save_inbox_message` ignoring duplicates or returning existing ID
                    
                    # Filter own messages (should be covered by incoming=True but just in case)
                    if event.out:
                        return

                    # Filter specific unwanted updates (e.g. pinned message service msg)
                    if hasattr(event.message, 'action') and event.message.action:
                         # e.g. MessageActionPinMessage
                         return

                    # CRITICAL: Block Telegram Bots from entering inbox
                    # This prevents promotional bots, gaming bots, etc. from being processed
                    if sender:
                        sender_is_bot = getattr(sender, 'bot', False) or getattr(sender, 'is_bot', False)
                        sender_username = getattr(sender, 'username', '') or ''
                        username_is_bot = sender_username.lower().endswith('bot') if sender_username else False
                        
                        if sender_is_bot or username_is_bot:
                            logger.debug(f"Blocking bot message from {sender_username or sender.id}: is_bot={sender_is_bot}")
                            return

                    # -- Apply global filters (blocklist/whitelist) --
                    # Avoid overhead if possible, but safe to check
                    from message_filters import apply_filters
                    # Mock filter msg structure
                    filter_msg = {
                        "body": body,
                        "sender_contact": sender_contact,
                        "sender_name": sender_name,
                        "sender_id": str(event.sender_id) if hasattr(event, 'sender_id') else None,
                        "is_group": event.is_group,
                        "is_channel": event.is_channel,
                        "channel": "telegram"
                    }
                    
                    should_process, reason = await apply_filters(filter_msg, license_id, recent_messages=None)
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
                                        "data": b64_data,
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

                        task = asyncio.create_task(
                            analyze_inbox_message(
                                message_id=msg_id,
                                body=body,
                                license_id=license_id,
                                auto_reply=auto_reply,
                                telegram_chat_id=str(sender.id),
                                attachments=attachments
                            )
                        )
                        task.add_done_callback(self.background_tasks.discard)

                        # 7. Persist Updated Session (CRITICAL for access_hash management)
                        # StringSession stores new hashes as they are encountered. 
                        # Saving it back ensures we don't get "entity not found" on replies.
                        try:
                            updated_session = client.session.save()
                            encrypted_session = simple_encrypt(updated_session)
                            async with get_db() as db:
                                await execute_sql(
                                    db,
                                    "UPDATE telegram_phone_sessions SET session_data_encrypted = ?, updated_at = ? WHERE license_key_id = ?",
                                    [encrypted_session, datetime.now(timezone.utc), license_id]
                                )
                                await commit_db(db)
                            # logger.debug(f"Persisted updated Telegram session for license {license_id}")
                        except Exception as session_e:
                            logger.error(f"Failed to persist updated Telegram session: {session_e}")
                        
                except Exception as e:
                    logger.error(f"Error in Telegram real-time message handler: {e}")

        except Exception as e:
            if license_id in self.clients:
                del self.clients[license_id]
            
            # Ensure cleanup of local client
            if client:
                try:
                    await client.disconnect()
                except:
                    pass
            
            error_str = str(e)
            logger.error(f"Failed to start Telegram client for license {license_id}: {error_str}")

            # Critical: Check for Session Conflict / Revocation
            if "used under two different IP addresses" in error_str or "auth key" in error_str.lower():
                logger.critical(f"Telegram Session Revoked for license {license_id}. Disabling session in DB.")
                try:
                    from models import deactivate_telegram_phone_session
                    # We might need a direct DB call if models isn't fully async-compatible in this context,
                    # but assumes models functions work. 
                    # Actually, better to use direct DB here to be sure.
                    async with get_db() as db:
                        await execute_sql(
                            db, 
                            "UPDATE telegram_phone_sessions SET is_active = FALSE, updated_at = ? WHERE license_key_id = ?",
                            [datetime.now(timezone.utc), license_id]
                        )
                except Exception as db_e:
                    logger.error(f"Failed to disable revoked session {license_id}: {db_e}")

    async def ensure_client_active(self, license_id: int) -> Optional[TelegramClient]:
        """
        Ensure a client is active for the given license_id.
        If it's running, return it.
        If not, try to start it from the DB session.
        Uses a lock to prevent multiple simultaneous connection attempts for the same license.
        """
        # 1. if already active, return it
        if license_id in self.clients:
            client = self.clients[license_id]
            if client.is_connected():
                return client
            else:
                # Cleanup disconnected client
                del self.clients[license_id]

        # --- Distributed Lock Check ---
        # If we don't hold the distributed lock, we cannot start clients.
        # This handles multi-container deployments (Railway) correctly.
        if not self.distributed_lock or not self.distributed_lock.locked:
             logger.info(f"Process {os.getpid()} in standby mode (Not the Leader). Skipping client initialization.")
             return None
        # ----------------------
        
        # Initialize lock if needed
        if license_id not in self.locks:
             self.locks[license_id] = asyncio.Lock()
             
        # 2. Acquire lock to safely start client
        async with self.locks[license_id]:
             # Double-check after acquiring lock in case another task beat us to it
             if license_id in self.clients:
                 client = self.clients[license_id]
                 if client.is_connected():
                     return client
            
             # Fetch session from DB and start
             try:
                async with get_db() as db:
                    row = await fetch_one(
                        db,
                        "SELECT session_data_encrypted, phone_number FROM telegram_phone_sessions WHERE license_key_id = ? AND is_active = TRUE",
                        [license_id]
                    )
                    
                    if not row:
                        logger.warning(f"No active session found for license {license_id}")
                        return None
                        
                    session_data = row.get("session_data_encrypted") or row[0]
                    phone_number = row.get("phone_number") or row[1]
                    
                    try:
                        session_string = simple_decrypt(session_data)
                    except Exception as e:
                        logger.error(f"Failed to decrypt session: {e}")
                        return None
                        
                    await self._start_client(license_id, session_string, phone_number)
                    return self.clients.get(license_id)

             except Exception as e:
                logger.error(f"Error ensuring active client for {license_id}: {e}")
                return None

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
