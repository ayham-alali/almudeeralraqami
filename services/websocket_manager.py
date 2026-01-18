"""
Al-Mudeer - WebSocket Service
Real-time updates for inbox, notifications, and analytics
Replaces polling with efficient push notifications

Enhanced with Redis pub/sub for horizontal scaling across multiple workers.
"""

import asyncio
import json
import os
from typing import Dict, Set, Optional, Any
from datetime import datetime
from dataclasses import dataclass, asdict

from fastapi import WebSocket, WebSocketDisconnect
from logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class WebSocketMessage:
    """Structure for WebSocket messages"""
    event: str  # "new_message", "notification", "analytics_update", etc.
    data: Dict[str, Any]
    timestamp: str = ""
    
    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.utcnow().isoformat()
    
    def to_json(self) -> str:
        return json.dumps(asdict(self))
    
    @classmethod
    def from_json(cls, json_str: str) -> "WebSocketMessage":
        """Parse from JSON string"""
        data = json.loads(json_str)
        return cls(
            event=data.get("event", ""),
            data=data.get("data", {}),
            timestamp=data.get("timestamp", "")
        )


class RedisPubSubManager:
    """
    Manages Redis pub/sub for cross-process WebSocket message delivery.
    Enables horizontal scaling by broadcasting messages through Redis.
    """
    
    CHANNEL_PREFIX = "almudeer:ws:"
    
    def __init__(self):
        self._redis_client = None
        self._pubsub = None
        self._listener_task: Optional[asyncio.Task] = None
        self._message_handlers: Dict[int, Any] = {}  # license_id -> callback
        self._initialized = False
    
    async def initialize(self) -> bool:
        """Initialize Redis connection for pub/sub"""
        if self._initialized:
            return True
            
        redis_url = os.getenv("REDIS_URL")
        if not redis_url:
            logger.info("Redis URL not configured, pub/sub disabled")
            return False
        
        try:
            import redis.asyncio as aioredis
            self._redis_client = await aioredis.from_url(
                redis_url,
                decode_responses=True
            )
            # Test connection
            await self._redis_client.ping()
            self._pubsub = self._redis_client.pubsub()
            self._initialized = True
            logger.info("Redis pub/sub initialized successfully")
            return True
        except ImportError:
            logger.warning("redis.asyncio not available, pub/sub disabled")
            return False
        except Exception as e:
            logger.warning(f"Failed to initialize Redis pub/sub: {e}")
            return False
    
    async def subscribe(self, license_id: int, handler):
        """Subscribe to messages for a specific license"""
        if not self._initialized:
            return
        
        channel = f"{self.CHANNEL_PREFIX}{license_id}"
        self._message_handlers[license_id] = handler
        
        try:
            await self._pubsub.subscribe(channel)
            logger.debug(f"Subscribed to Redis channel: {channel}")
            
            # Start listener if not already running
            if self._listener_task is None or self._listener_task.done():
                self._listener_task = asyncio.create_task(self._listen())
        except Exception as e:
            logger.error(f"Failed to subscribe to Redis channel: {e}")
    
    async def unsubscribe(self, license_id: int):
        """Unsubscribe from messages for a specific license"""
        if not self._initialized:
            return
        
        channel = f"{self.CHANNEL_PREFIX}{license_id}"
        self._message_handlers.pop(license_id, None)
        
        try:
            await self._pubsub.unsubscribe(channel)
            logger.debug(f"Unsubscribed from Redis channel: {channel}")
            
            # Stop listener if no more subscriptions
            if not self._message_handlers and self._listener_task:
                self._listener_task.cancel()
                self._listener_task = None
                logger.debug("Redis listener stopped (no subscriptions)")
                
        except Exception as e:
            logger.error(f"Failed to unsubscribe from Redis channel: {e}")
    
    async def publish(self, license_id: int, message: WebSocketMessage):
        """Publish a message to a license channel"""
        if not self._initialized:
            return False
        
        channel = f"{self.CHANNEL_PREFIX}{license_id}"
        try:
            await self._redis_client.publish(channel, message.to_json())
            logger.debug(f"Published to Redis channel: {channel}")
            return True
        except Exception as e:
            logger.error(f"Failed to publish to Redis: {e}")
            return False
    
    async def _listen(self):
        """Background task to listen for Redis messages"""
        try:
            while True:
                message = await self._pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=1.0
                )
                if message and message.get("type") == "message":
                    channel = message.get("channel", "")
                    data = message.get("data", "")
                    
                    # Extract license_id from channel
                    if channel.startswith(self.CHANNEL_PREFIX):
                        try:
                            license_id = int(channel[len(self.CHANNEL_PREFIX):])
                            handler = self._message_handlers.get(license_id)
                            if handler:
                                ws_message = WebSocketMessage.from_json(data)
                                await handler(ws_message)
                        except (ValueError, json.JSONDecodeError) as e:
                            logger.debug(f"Failed to parse Redis message: {e}")
                
                await asyncio.sleep(0.01)  # Small sleep to prevent busy loop
        except asyncio.CancelledError:
            pass
        except Exception as e:
            # Ignore "pubsub connection not set" error which can happen during shutdown/unsubscribe
            if "pubsub connection not set" in str(e):
                logger.debug(f"Redis listener stopped (connection closed): {e}")
                return
            logger.error(f"Redis listener error: {e}")
            # Wait a bit before restarting loop if it was a transient error
            await asyncio.sleep(1.0)
    
    async def close(self):
        """Close Redis connections"""
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
        
        if self._pubsub:
            await self._pubsub.close()
        
        if self._redis_client:
            await self._redis_client.close()
        
        self._initialized = False
    
    @property
    def is_available(self) -> bool:
        """Check if Redis pub/sub is available"""
        return self._initialized


class ConnectionManager:
    """
    Manages WebSocket connections for real-time updates.
    Organizes connections by license_id for targeted messaging.
    
    With Redis pub/sub enabled, messages are broadcast across all workers.
    """
    
    def __init__(self):
        # Active connections organized by license_id
        self._connections: Dict[int, Set[WebSocket]] = {}
        self._lock = asyncio.Lock()
        self._pubsub = RedisPubSubManager()
        self._pubsub_initialized = False
    
    async def _ensure_pubsub(self):
        """Initialize pub/sub lazily"""
        if not self._pubsub_initialized:
            await self._pubsub.initialize()
            self._pubsub_initialized = True
    
    async def connect(self, websocket: WebSocket, license_id: int):
        """Accept and register a new WebSocket connection"""
        await websocket.accept()
        await self._ensure_pubsub()
        
        async with self._lock:
            if license_id not in self._connections:
                self._connections[license_id] = set()
                # Subscribe to Redis channel for this license
                if self._pubsub.is_available:
                    await self._pubsub.subscribe(
                        license_id,
                        lambda msg: self._handle_redis_message(license_id, msg)
                    )
            self._connections[license_id].add(websocket)
        logger.info(f"WebSocket connected: license {license_id} (total: {self.connection_count})")
    
    async def disconnect(self, websocket: WebSocket, license_id: int):
        """Remove a WebSocket connection"""
        async with self._lock:
            if license_id in self._connections:
                self._connections[license_id].discard(websocket)
                if not self._connections[license_id]:
                    del self._connections[license_id]
                    # Unsubscribe from Redis when no more local connections
                    if self._pubsub.is_available:
                        await self._pubsub.unsubscribe(license_id)
        logger.info(f"WebSocket disconnected: license {license_id}")
    
    async def _handle_redis_message(self, license_id: int, message: WebSocketMessage):
        """Handle incoming message from Redis pub/sub"""
        # Send to local connections only (Redis already broadcast to other workers)
        await self._send_to_local_connections(license_id, message)
    
    async def _send_to_local_connections(self, license_id: int, message: WebSocketMessage):
        """Send message to local WebSocket connections only"""
        if license_id not in self._connections:
            return
        
        dead_connections = []
        json_message = message.to_json()
        
        for connection in list(self._connections.get(license_id, [])):
            try:
                await connection.send_text(json_message)
            except Exception as e:
                logger.debug(f"Failed to send to WebSocket: {e}")
                dead_connections.append(connection)
        
        # Clean up dead connections
        for conn in dead_connections:
            await self.disconnect(conn, license_id)
    
    async def send_to_license(self, license_id: int, message: WebSocketMessage):
        """
        Send a message to all connections for a specific license.
        If Redis is available, publishes to Redis for cross-worker delivery.
        Otherwise, sends directly to local connections.
        """
        if self._pubsub.is_available:
            # Publish to Redis - all workers will receive and forward to their local connections
            published = await self._pubsub.publish(license_id, message)
            if published:
                return
        
        # Fallback: Direct send to local connections
        await self._send_to_local_connections(license_id, message)
    
    async def broadcast(self, message: WebSocketMessage):
        """Send a message to all connected clients"""
        # For broadcast, we send to all local connections
        # Each worker handles its own local connections
        json_message = message.to_json()
        
        for license_id in list(self._connections.keys()):
            for connection in list(self._connections.get(license_id, [])):
                try:
                    await connection.send_text(json_message)
                except Exception:
                    pass
    
    @property
    def connection_count(self) -> int:
        """Get total number of active connections"""
        return sum(len(conns) for conns in self._connections.values())
    
    def get_connected_licenses(self) -> Set[int]:
        """Get set of license IDs with active connections"""
        return set(self._connections.keys())
    
    @property
    def redis_enabled(self) -> bool:
        """Check if Redis pub/sub is enabled"""
        return self._pubsub.is_available


# Global connection manager
_manager: Optional[ConnectionManager] = None


def get_websocket_manager() -> ConnectionManager:
    """Get or create the global WebSocket manager"""
    global _manager
    if _manager is None:
        _manager = ConnectionManager()
    return _manager


# ============ Event Broadcasting Helpers ============

async def broadcast_new_message(license_id: int, message_data: Dict[str, Any]):
    """Broadcast when a new inbox message arrives"""
    manager = get_websocket_manager()
    await manager.send_to_license(license_id, WebSocketMessage(
        event="new_message",
        data=message_data
    ))


async def broadcast_notification(license_id: int, notification: Dict[str, Any]):
    """Broadcast a new notification"""
    manager = get_websocket_manager()
    await manager.send_to_license(license_id, WebSocketMessage(
        event="notification",
        data=notification
    ))


async def broadcast_analytics_update(license_id: int, analytics: Dict[str, Any]):
    """Broadcast analytics data update"""
    manager = get_websocket_manager()
    await manager.send_to_license(license_id, WebSocketMessage(
        event="analytics_update",
        data=analytics
    ))


async def broadcast_task_complete(license_id: int, task_id: str, result: Dict[str, Any]):
    """Broadcast when an async task completes"""
    manager = get_websocket_manager()
    await manager.send_to_license(license_id, WebSocketMessage(
        event="task_complete",
        data={"task_id": task_id, "result": result}
    ))


# ============ Presence Broadcasting ============

async def broadcast_presence_update(license_id: int, is_online: bool, last_seen: str = None):
    """Broadcast presence status change to connected clients"""
    manager = get_websocket_manager()
    await manager.send_to_license(license_id, WebSocketMessage(
        event="presence_update",
        data={
            "is_online": is_online,
            "last_seen": last_seen
        }
    ))





# ============ Reaction Broadcasting ============

async def broadcast_reaction_added(license_id: int, message_id: int, emoji: str, user_type: str):
    """Broadcast when a reaction is added to a message"""
    manager = get_websocket_manager()
    await manager.send_to_license(license_id, WebSocketMessage(
        event="reaction_added",
        data={
            "message_id": message_id,
            "emoji": emoji,
            "user_type": user_type
        }
    ))


async def broadcast_reaction_removed(license_id: int, message_id: int, emoji: str, user_type: str):
    """Broadcast when a reaction is removed from a message"""
    manager = get_websocket_manager()
    await manager.send_to_license(license_id, WebSocketMessage(
        event="reaction_removed",
        data={
            "message_id": message_id,
            "emoji": emoji,
            "user_type": user_type
        }
    ))


# ============ Message Edit/Delete Broadcasting ============

async def broadcast_message_edited(license_id: int, message_id: int, new_body: str, edited_at: str):
    """Broadcast when a message is edited"""
    manager = get_websocket_manager()
    await manager.send_to_license(license_id, WebSocketMessage(
        event="message_edited",
        data={
            "message_id": message_id,
            "new_body": new_body,
            "edited_at": edited_at
        }
    ))


async def broadcast_message_deleted(license_id: int, message_id: int):
    """Broadcast when a message is deleted"""
    manager = get_websocket_manager()
    await manager.send_to_license(license_id, WebSocketMessage(
        event="message_deleted",
        data={
            "message_id": message_id
        }
    ))
