"""
Al-Mudeer Chat Routes Tests
API tests for inbox, conversation, and messaging endpoints
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from httpx import AsyncClient, ASGITransport

# ============ Fixtures ============

@pytest.fixture
def mock_license_dependency():
    """Mock the get_license_from_header dependency"""
    with patch("routes.chat_routes.get_license_from_header") as mock:
        mock.return_value = {"license_id": 1, "key": "TEST-KEY"}
        yield mock

# ============ Inbox & Conversation List Config ============

@pytest.fixture
def app():
    """Create a minimal app with the chat router"""
    from fastapi import FastAPI
    from routes.chat_routes import router
    app = FastAPI()
    app.include_router(router)
    return app

class TestInboxRoutes:
    """Tests for /api/integrations/inbox and /conversations endpoints"""

    @pytest.mark.asyncio
    async def test_get_inbox_success(self, mock_license_dependency, app):
        """Test getting inbox messages"""
        # mock model functions
        with patch("routes.chat_routes.get_inbox_messages", new_callable=AsyncMock) as mock_get_msgs, \
             patch("routes.chat_routes.get_inbox_messages_count", new_callable=AsyncMock) as mock_get_count:
            
            mock_get_msgs.return_value = [{"id": 1, "body": "Hello"}]
            mock_get_count.return_value = 10
            
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                response = await client.get("/api/integrations/inbox?limit=5&offset=0")
                
                assert response.status_code == 200
                data = response.json()
                assert data["total"] == 10
                assert len(data["messages"]) == 1
                assert data["has_more"] is True

    @pytest.mark.asyncio
    async def test_get_conversations_success(self, mock_license_dependency, app):
        """Test getting conversations list"""
        
        with patch("routes.chat_routes.get_inbox_conversations", new_callable=AsyncMock) as mock_get_convs, \
             patch("routes.chat_routes.get_inbox_conversations_count", new_callable=AsyncMock) as mock_get_count, \
             patch("routes.chat_routes.get_inbox_status_counts", new_callable=AsyncMock) as mock_get_stats:
            
            mock_get_convs.return_value = [{"contact": "123", "last_message": "Hi"}]
            mock_get_count.return_value = 5
            mock_get_stats.return_value = {"open": 2, "closed": 3}
            
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                response = await client.get("/api/integrations/conversations")
                
                assert response.status_code == 200
                data = response.json()
                assert len(data["conversations"]) == 1
                assert data["status_counts"]["open"] == 2

    @pytest.mark.asyncio
    async def test_get_inbox_message_detail(self, mock_license_dependency, app):
        """Test getting single message detail"""
        
        with patch("models.inbox.get_inbox_message_by_id", new_callable=AsyncMock) as mock_get_msg:
            mock_get_msg.return_value = {"id": 99, "body": "Detail"}
            
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                response = await client.get("/api/integrations/inbox/99")
                
                assert response.status_code == 200
                assert response.json()["message"]["id"] == 99

    @pytest.mark.asyncio
    async def test_get_inbox_message_not_found(self, mock_license_dependency, app):
        """Test getting non-existent message returns 404"""
        
        with patch("models.inbox.get_inbox_message_by_id", new_callable=AsyncMock) as mock_get_msg:
            mock_get_msg.return_value = None
            
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                response = await client.get("/api/integrations/inbox/999")
                
                assert response.status_code == 404

# ============ Chat History & Search Config ============

class TestChatHistoryRoutes:
    """Tests for chat history and search endpoints"""

    @pytest.mark.asyncio
    async def test_search_messages(self, mock_license_dependency, app):
        """Test searching messages"""
        
        with patch("routes.chat_routes.search_messages", new_callable=AsyncMock) as mock_search:
            mock_search.return_value = [{"id": 1, "body": "Found it"}]
            
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                response = await client.get("/api/integrations/conversations/search?query=Found")
                
                assert response.status_code == 200
                assert len(response.json()) == 1

    @pytest.mark.asyncio
    async def test_get_conversation_detail(self, mock_license_dependency, app):
        """Test getting conversation detail with messages"""
        
        with patch("routes.chat_routes.get_full_chat_history", new_callable=AsyncMock) as mock_history, \
             patch("models.customers.get_customer_for_message", new_callable=AsyncMock) as mock_customer:
            
            mock_history.return_value = [
                {"id": 1, "direction": "incoming", "sender_name": "Alice", "body": "Hi"},
                {"id": 2, "direction": "outgoing", "body": "Hello"}
            ]
            mock_customer.return_value = {"lead_score": 85}
            
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                # URL encode the contact if needed, but simple string is fine for mock
                response = await client.get("/api/integrations/conversations/Alice123")
                
                assert response.status_code == 200
                data = response.json()
                assert data["sender_name"] == "Alice"
                assert data["lead_score"] == 85
                assert len(data["messages"]) == 2

    @pytest.mark.asyncio
    async def test_get_conversation_not_found(self, mock_license_dependency, app):
        """Test getting non-existent conversation returns 404"""
        
        with patch("routes.chat_routes.get_full_chat_history", new_callable=AsyncMock) as mock_history:
            mock_history.return_value = []
            
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                response = await client.get("/api/integrations/conversations/Nobody")
                
                assert response.status_code == 404

# ============ Actions Config ============

class TestChatActions:
    """Tests for sending and approving messages"""

    @pytest.mark.asyncio
    async def test_send_message_success(self, mock_license_dependency, app):
        """Test sending a new message"""
        
        with patch("routes.chat_routes.get_full_chat_history", new_callable=AsyncMock) as mock_history, \
             patch("routes.chat_routes.create_outbox_message", new_callable=AsyncMock) as mock_create, \
             patch("routes.chat_routes.approve_outbox_message", new_callable=AsyncMock) as mock_approve, \
             patch("routes.chat_routes.send_approved_message") as mock_send_task: # Background task
            
            mock_history.return_value = [{"channel": "whatsapp", "sender_id": "123"}]
            mock_create.return_value = 101
            
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                response = await client.post(
                    "/api/integrations/conversations/user123/send",
                    json={"message": "Reply text"}
                )
                
                assert response.status_code == 200
                assert response.json()["outbox_id"] == 101
                mock_create.assert_called_once()
                mock_approve.assert_called_once()

    @pytest.mark.asyncio
    async def test_approve_message_success(self, mock_license_dependency, app):
        """Test approving an AI draft"""
        
        with patch("models.inbox.get_inbox_message_by_id", new_callable=AsyncMock) as mock_get, \
             patch("routes.chat_routes.create_outbox_message", new_callable=AsyncMock) as mock_create, \
             patch("routes.chat_routes.approve_outbox_message", new_callable=AsyncMock) as mock_approve, \
             patch("routes.chat_routes.update_inbox_status", new_callable=AsyncMock) as mock_update, \
             patch("routes.chat_routes.send_approved_message") as mock_send_task: # Background task
            
            mock_get.return_value = {
                "id": 50, 
                "ai_draft_response": "Draft", 
                "channel": "whatsapp",
                "sender_id": "sender1"
            }
            mock_create.return_value = 202
            
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                response = await client.post(
                    "/api/integrations/inbox/50/approve",
                    json={"action": "approve"}
                )
                
                assert response.status_code == 200
                mock_create.assert_called_once()
                mock_update.assert_called_with(50, "approved")

    @pytest.mark.asyncio
    async def test_ignore_message(self, mock_license_dependency, app):
        """Test ignoring a message"""
        
        with patch("models.inbox.get_inbox_message_by_id", new_callable=AsyncMock) as mock_get, \
             patch("models.inbox.ignore_chat", new_callable=AsyncMock) as mock_ignore:
            
            mock_get.return_value = {"id": 51, "sender_contact": "spammer"}
            mock_ignore.return_value = 5  # count of ignored messages
            
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                response = await client.post(
                    "/api/integrations/inbox/51/approve",
                    json={"action": "ignore"}
                )
                
                assert response.status_code == 200
                assert "5" in response.json()["message"]
                mock_ignore.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_message(self, mock_license_dependency, app):
        """Test deleting a message"""
        
        with patch("models.inbox.soft_delete_message", new_callable=AsyncMock) as mock_delete, \
             patch("services.websocket_manager.broadcast_message_deleted", new_callable=AsyncMock):
            
            mock_delete.return_value = {"success": True}
            
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                response = await client.delete("/api/integrations/messages/100")
                
                assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_delete_conversation(self, mock_license_dependency, app):
        """Test deleting a full conversation"""
        
        with patch("models.inbox.soft_delete_conversation", new_callable=AsyncMock) as mock_delete, \
             patch("services.websocket_manager.broadcast_conversation_deleted", new_callable=AsyncMock):
            
            mock_delete.return_value = {"success": True}
            
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                response = await client.delete("/api/integrations/conversations/old_chat_123")
                
                assert response.status_code == 200
