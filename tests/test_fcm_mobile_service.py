"""
Al-Mudeer FCM Service Tests
Tests for Firebase Cloud Messaging logic
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from services.fcm_mobile_service import (
    send_fcm_notification,
    _send_fcm_v1,
    _send_fcm_legacy,
    save_fcm_token
)

class TestFCMService:
    
    @pytest.mark.asyncio
    async def test_send_fcm_v1_success(self):
        """Test sending via FCM V1 API"""
        with patch("services.fcm_mobile_service.FCM_V1_AVAILABLE", True), \
             patch("services.fcm_mobile_service._get_access_token", return_value="fake-token"), \
             patch("services.fcm_mobile_service.FCM_PROJECT_ID", "test-project"), \
             patch("httpx.AsyncClient") as mock_client:
            
            mock_post = AsyncMock()
            mock_post.return_value.status_code = 200
            mock_client.return_value.__aenter__.return_value.post = mock_post
            
            result = await send_fcm_notification(
                token="device-token",
                title="Test",
                body="Message",
                data={"key": "value"}
            )
            
            assert result is True
            mock_post.assert_called_once()
            args, kwargs = mock_post.call_args
            assert "fcm.googleapis.com/v1" in args[0]
            assert kwargs["json"]["message"]["token"] == "device-token"
            assert kwargs["json"]["message"]["data"]["key"] == "value"

    @pytest.mark.asyncio
    async def test_send_fcm_v1_auth_failure_fallback(self):
        """Test fallback to return None if V1 auth fails"""
        with patch("services.fcm_mobile_service.FCM_V1_AVAILABLE", True), \
             patch("services.fcm_mobile_service._get_access_token", return_value="fake-token"), \
             patch("services.fcm_mobile_service.FCM_PROJECT_ID", "test-project"), \
             patch("httpx.AsyncClient") as mock_client:
            
            # Simulate 401 Unauthorized
            mock_post = AsyncMock()
            mock_post.return_value.status_code = 401
            mock_client.return_value.__aenter__.return_value.post = mock_post
            
            # This calls internal _send_fcm_v1 directly to verify it returns None (triggering fallback logic in main wrapper)
            result = await _send_fcm_v1("token", "title", "body")
            assert result is None

    @pytest.mark.asyncio
    async def test_send_fcm_legacy_success(self):
        """Test sending via Legacy API"""
        with patch("services.fcm_mobile_service.FCM_SERVER_KEY", "server-key"), \
             patch("httpx.AsyncClient") as mock_client:
            
            mock_post = AsyncMock()
            mock_post.return_value.status_code = 200
            mock_post.return_value.json.return_value = {"success": 1}
            mock_client.return_value.__aenter__.return_value.post = mock_post
            
            result = await _send_fcm_legacy("token", "title", "body")
            
            assert result is True
            mock_post.assert_called_once()
            assert "fcm.googleapis.com/fcm/send" in mock_post.call_args[0][0]

    @pytest.mark.asyncio
    async def test_save_fcm_token_update(self):
        """Test saving/updating FCM token in DB"""
        from services.fcm_mobile_service import save_fcm_token
        
        # Mock DB helpers
        with patch("db_helper.get_db") as mock_db, \
             patch("db_helper.fetch_one", new_callable=AsyncMock) as mock_fetch, \
             patch("db_helper.execute_sql", new_callable=AsyncMock) as mock_exec, \
             patch("db_helper.commit_db", new_callable=AsyncMock):
            
            # Simulate existing token
            mock_fetch.side_effect = [{"id": 10}] # First fetch finds ID 10
            
            license_id = 99
            new_id = await save_fcm_token(license_id, "new-token", "android", "device-123")
            
            assert new_id == 10
            mock_exec.assert_called() # Should call update
            # Verify update query logic roughly (hard to matching exact SQL string without strict equality)
            
    @pytest.mark.asyncio
    async def test_save_fcm_token_insert(self):
        """Test inserting new FCM token"""
        with patch("db_helper.get_db") as mock_db, \
             patch("db_helper.fetch_one", new_callable=AsyncMock) as mock_fetch, \
             patch("db_helper.execute_sql", new_callable=AsyncMock) as mock_exec, \
             patch("db_helper.commit_db", new_callable=AsyncMock):
            
            # First fetch returns None (not found), Insert happens, Second fetch returns ID
            mock_fetch.side_effect = [None, None, {"id": 11}] 
            
            new_id = await save_fcm_token(99, "new-token", "android")
            
            assert new_id == 11
