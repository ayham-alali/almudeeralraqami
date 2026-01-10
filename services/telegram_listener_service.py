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
from services.websocket_manager import broadcast_typing_indicator, broadcast_recording_indicator
from db_helper import fetch_all, get_db

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
                    WHERE is_active = 1 OR is_active = TRUE
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
                    # We need the sender's phone or ID to broadcast
                    sender = await event.get_sender()
                    sender_contact = getattr(sender, 'phone', str(sender.id))
                    
                    if event.typing:
                        await broadcast_typing_indicator(
                            license_id=license_id,
                            sender_contact=sender_contact,
                            is_typing=True
                        )
                    elif event.recording:
                        await broadcast_recording_indicator(
                            license_id=license_id,
                            sender_contact=sender_contact,
                            is_recording=True
                        )
                    elif event.cancel:
                        # Cancel could mean stop typing OR stop recording
                        # We send false for both to be safe
                        await broadcast_typing_indicator(license_id, sender_contact, False)
                        await broadcast_recording_indicator(license_id, sender_contact, False)
                        
                except Exception as e:
                    logger.debug(f"Error handling Telegram UserUpdate: {e}")

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
