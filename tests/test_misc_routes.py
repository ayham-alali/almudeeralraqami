"""
Al-Mudeer Miscellaneous Routes Tests
Tests for Subscription, System/Integrations, and Version routes
"""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock

# Mock authentication
@pytest.fixture
def mock_admin_auth():
    with patch("routes.subscription.verify_admin", return_value=True), \
         patch("routes.version.verify_admin", return_value=True):
        yield

@pytest.fixture
def mock_license_dependency():
    with patch("dependencies.get_license_from_header", return_value={"license_id": 1}):
        yield

class TestSubscriptionRoutes:
    
    @pytest.mark.asyncio
    async def test_create_subscription(self, mock_admin_auth):
        from routes.subscription import create_subscription, SubscriptionCreate
        
        with patch("routes.subscription.generate_license_key", new_callable=AsyncMock) as mock_gen:
            mock_gen.return_value = "KEY-123"
            
            payload = SubscriptionCreate(
                company_name="Test Company",
                days_valid=30,
                max_requests_per_day=100
            )
            
            response = await create_subscription(payload)
            
            assert response.success is True
            assert response.subscription_key == "KEY-123"
            assert response.company_name == "Test Company"

    @pytest.mark.asyncio
    async def test_list_subscriptions(self, mock_admin_auth):
        from routes.subscription import list_subscriptions
        
        # Mock DB for SQLite path (default)
        with patch("aiosqlite.connect") as mock_connect:
            mock_db = AsyncMock()
            mock_cursor = AsyncMock()
            mock_connect.return_value.__aenter__.return_value = mock_db
            mock_db.execute.return_value.__aenter__.return_value = mock_cursor
            
            # Mock fetchall rows
            mock_row = MagicMock()
            mock_row.keys.return_value = ["id", "company_name", "created_at"]
            # To make dict(row) work, we need __iter__ or similar behavior if it's not a real Row
            # Simulating dict behavior:
            row_data = {"id": 1, "company_name": "Test Co", "created_at": "2023-01-01"}
            
            # If the code does dict(row), it expects an iterable of (key, value) or similar
            # aiosqlite.Row is dict-like.
            # Let's just return a dict directly since the code converts it: `row_dict = dict(row)`
            # Wait, `dict({})` is fine.
            mock_cursor.fetchall.return_value = [{"id": 1, "company_name": "Test Co", "is_active": 1}]
            
            response = await list_subscriptions(limit=10)
            
            assert response.total == 1
            assert response.subscriptions[0]["company_name"] == "Test Co"


class TestSystemRoutes:
    
    @pytest.mark.asyncio
    async def test_llm_health_check(self, mock_license_dependency):
        from routes.system_routes import check_llm_health
        
        with patch("os.getenv") as mock_env, \
             patch("httpx.AsyncClient") as mock_client:
             
            # Configure Env
            def env_side_effect(key, default=None):
                if key == "OPENAI_API_KEY": return "test-key"
                return default
            mock_env.side_effect = env_side_effect
            
            # Mock OpenAI Response
            mock_post = AsyncMock()
            mock_post.status_code = 200
            mock_client.return_value.__aenter__.return_value.post = mock_post
            
            result = await check_llm_health()
            
            assert "openai" in result["providers"]
            assert result["providers"]["openai"]["status"] == "healthy"

    @pytest.mark.asyncio
    async def test_list_integration_accounts(self, mock_license_dependency):
        from routes.system_routes import list_integration_accounts
        
        with patch("routes.system_routes.get_email_config", new_callable=AsyncMock) as mock_email, \
             patch("routes.system_routes.get_telegram_config", new_callable=AsyncMock) as mock_tg, \
             patch("routes.system_routes.get_telegram_phone_session", new_callable=AsyncMock), \
             patch("routes.system_routes.get_whatsapp_config", new_callable=AsyncMock):
             
            mock_email.return_value = {"email_address": "test@gmail.com", "is_active": True}
            mock_tg.return_value = None
            
            response = await list_integration_accounts({"license_id": 1})
            
            accounts = response["accounts"]
            assert len(accounts) >= 1
            assert accounts[0].id == "email"
            assert accounts[0].display_name == "test@gmail.com"

class TestVersionRoutes:
    
    @pytest.mark.asyncio
    async def test_update_check(self):
        from routes.version import check_update, UpdateCheckResponse
        
        # Test basic flow without mocks (assuming no external calls in simple path)
        # Actually version.py uses 'CURRENT_VERSION' constant.
        
        response = await check_update(
            platform="android",
            current_version="1.0.0"
        )
        # Depending on CURRENT_VERSION in version.py, it might force update or not.
        # Just checking structure
        assert isinstance(response, UpdateCheckResponse)
