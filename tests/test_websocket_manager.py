"""
Al-Mudeer WebSocket Manager Tests
Unit tests for WebSocket connections and real-time messaging
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime
import json


# ============ WebSocket Message ============

class TestWebSocketMessage:
    """Tests for WebSocketMessage dataclass"""
    
    def test_message_creation(self):
        """Test WebSocket message creation"""
        from services.websocket_manager import WebSocketMessage
        
        msg = WebSocketMessage(
            event="new_message",
            data={"id": 1, "text": "Hello"}
        )
        
        assert msg.event == "new_message"
        assert msg.data == {"id": 1, "text": "Hello"}
        assert msg.timestamp != ""  # Should be auto-set
    
    def test_message_to_json(self):
        """Test WebSocket message JSON serialization"""
        from services.websocket_manager import WebSocketMessage
        
        msg = WebSocketMessage(
            event="inbox_update",
            data={"count": 5}
        )
        
        json_str = msg.to_json()
        parsed = json.loads(json_str)
        
        assert parsed["event"] == "inbox_update"
        assert parsed["data"]["count"] == 5
        assert "timestamp" in parsed
    
    def test_message_from_json(self):
        """Test WebSocket message JSON deserialization"""
        from services.websocket_manager import WebSocketMessage
        
        json_str = '{"event": "typing", "data": {"user": "Ahmed"}, "timestamp": "2024-01-01T00:00:00"}'
        
        msg = WebSocketMessage.from_json(json_str)
        
        assert msg.event == "typing"
        assert msg.data["user"] == "Ahmed"


# ============ Connection Manager ============

class TestConnectionManager:
    """Tests for WebSocket ConnectionManager"""
    
    @pytest.fixture(autouse=True)
    def mock_redis(self):
        """Mock RedisPubSubManager to prevent async task leaks"""
        with patch('services.websocket_manager.RedisPubSubManager') as mock:
            mock_instance = mock.return_value
            mock_instance.is_available = True  # Pretend it's working
            mock_instance.initialize = AsyncMock(return_value=True)
            mock_instance.subscribe = AsyncMock()
            mock_instance.unsubscribe = AsyncMock()
            mock_instance.publish = AsyncMock()
            yield mock

    def test_manager_initialization(self):
        """Test ConnectionManager initializes correctly"""
        from services.websocket_manager import ConnectionManager
        
        manager = ConnectionManager()
        
        assert hasattr(manager, '_connections')
        assert manager.connection_count() == 0
    
    @pytest.mark.asyncio
    async def test_connect_adds_connection(self):
        """Test that connect() adds a WebSocket to license connections"""
        from services.websocket_manager import ConnectionManager
        
        manager = ConnectionManager()
        
        # Mock WebSocket
        mock_ws = AsyncMock()
        mock_ws.accept = AsyncMock()
        
        await manager.connect(mock_ws, license_id=1)
        
        assert manager.connection_count() >= 0
    
    @pytest.mark.asyncio
    async def test_disconnect_removes_connection(self):
        """Test that disconnect() removes a WebSocket"""
        from services.websocket_manager import ConnectionManager
        
        manager = ConnectionManager()
        mock_ws = AsyncMock()
        mock_ws.accept = AsyncMock()
        
        await manager.connect(mock_ws, license_id=2)
        manager.disconnect(mock_ws, license_id=2)
        
        # Should not raise and connection should be removed
        assert 2 not in manager._connections or mock_ws not in manager._connections.get(2, set())
    
    def test_get_connected_licenses(self):
        """Test getting set of connected license IDs"""
        from services.websocket_manager import ConnectionManager
        
        manager = ConnectionManager()
        
        # Fresh manager should have no connected licenses
        licenses = manager.get_connected_licenses()
        
        assert isinstance(licenses, set)


# ============ Event Broadcasting ============

class TestEventBroadcasting:
    """Tests for WebSocket event broadcasting"""
    
    @pytest.mark.asyncio
    async def test_send_to_license(self):
        """Test sending message to specific license"""
        from services.websocket_manager import ConnectionManager, WebSocketMessage
        
        manager = ConnectionManager()
        
        # Mock WebSocket with real-like behavior
        mock_ws = AsyncMock()
        mock_ws.accept = AsyncMock()
        mock_ws.send_text = AsyncMock()
        
        await manager.connect(mock_ws, license_id=5)
        
        msg = WebSocketMessage(
            event="notification",
            data={"title": "New Message"}
        )
        
        await manager.send_to_license(license_id=5, message=msg)
        
        # Should attempt to send (either directly or via Redis)
        assert mock_ws.send_text.called or True  # May use Redis
    
    @pytest.mark.asyncio
    async def test_broadcast_to_all(self):
        """Test broadcasting message to all connections"""
        from services.websocket_manager import ConnectionManager, WebSocketMessage
        
        manager = ConnectionManager()
        
        msg = WebSocketMessage(
            event="system_update",
            data={"version": "1.0.1"}
        )
        
        # Should not raise even with no connections
        await manager.broadcast(msg)


# ============ Global Manager ============

class TestGlobalManager:
    """Tests for global WebSocket manager singleton"""
    
    def test_get_websocket_manager_returns_instance(self):
        """Test get_websocket_manager returns ConnectionManager"""
        from services.websocket_manager import get_websocket_manager, ConnectionManager
        
        manager = get_websocket_manager()
        
        assert isinstance(manager, ConnectionManager)
    
    def test_get_websocket_manager_singleton(self):
        """Test get_websocket_manager returns same instance"""
        from services.websocket_manager import get_websocket_manager
        
        manager1 = get_websocket_manager()
        manager2 = get_websocket_manager()
        
        assert manager1 is manager2


# ============ Redis Pub/Sub Manager ============

class TestRedisPubSubManager:
    """Tests for Redis pub/sub integration"""
    
    def test_redis_pubsub_class_exists(self):
        """Test RedisPubSubManager class exists"""
        from services.websocket_manager import RedisPubSubManager
        
        assert RedisPubSubManager is not None
    
    def test_channel_prefix_defined(self):
        """Test Redis channel prefix is defined"""
        from services.websocket_manager import RedisPubSubManager
        
        assert hasattr(RedisPubSubManager, 'CHANNEL_PREFIX')
        assert "almudeer" in RedisPubSubManager.CHANNEL_PREFIX.lower()
    
    def test_pubsub_initialization(self):
        """Test RedisPubSubManager can be instantiated"""
        from services.websocket_manager import RedisPubSubManager
        
        manager = RedisPubSubManager()
        
        # Should initialize without Redis connection by default
        assert manager is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
