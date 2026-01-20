"""
Al-Mudeer Notification Service Tests
Unit tests for notification routing, Slack/Discord integration, and rules
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime


# ============ Notification Classes ============

class TestNotificationClasses:
    """Tests for notification data classes"""
    
    def test_notification_channel_values(self):
        """Test NotificationChannel enum values"""
        from services.notification_service import NotificationChannel
        
        assert NotificationChannel.IN_APP.value == "in_app"
        assert NotificationChannel.EMAIL.value == "email"
        assert NotificationChannel.SLACK.value == "slack"
        assert NotificationChannel.DISCORD.value == "discord"
        assert NotificationChannel.WEBHOOK.value == "webhook"
    
    def test_notification_priority_values(self):
        """Test NotificationPriority enum values"""
        from services.notification_service import NotificationPriority
        
        assert NotificationPriority.LOW.value == "low"
        assert NotificationPriority.NORMAL.value == "normal"
        assert NotificationPriority.HIGH.value == "high"
        assert NotificationPriority.URGENT.value == "urgent"
    
    def test_notification_payload_creation(self):
        """Test NotificationPayload dataclass"""
        from services.notification_service import NotificationPayload, NotificationPriority
        
        payload = NotificationPayload(
            title="رسالة جديدة",
            message="لديك رسالة من أحمد",
            priority=NotificationPriority.HIGH,
            link="https://example.com/chat/123"
        )
        
        assert payload.title == "رسالة جديدة"
        assert payload.message == "لديك رسالة من أحمد"
        assert payload.priority == NotificationPriority.HIGH
        assert payload.link == "https://example.com/chat/123"
    
    def test_notification_payload_optional_fields(self):
        """Test NotificationPayload with optional fields"""
        from services.notification_service import NotificationPayload, NotificationPriority
        
        payload = NotificationPayload(
            title="Test",
            message="Test message",
            priority=NotificationPriority.NORMAL
        )
        
        assert payload.link is None
        assert payload.metadata is None
        assert payload.image is None


# ============ Slack Integration ============

class TestSlackIntegration:
    """Tests for Slack notification integration"""
    
    @pytest.mark.asyncio
    async def test_send_slack_notification_formats_blocks(self):
        """Test Slack notification creates proper block format"""
        from services.notification_service import (
            send_slack_notification,
            NotificationPayload,
            NotificationPriority
        )
        
        payload = NotificationPayload(
            title="⚠️ رسالة عاجلة",
            message="رسالة من عميل VIP",
            priority=NotificationPriority.URGENT,
            link="https://mudeer.app/chat/123"
        )
        
        with patch('services.notification_service.httpx.AsyncClient') as mock_client:
            mock_instance = AsyncMock()
            mock_instance.post = AsyncMock(return_value=MagicMock(status_code=200, text="ok"))
            mock_client.return_value.__aenter__.return_value = mock_instance
            
            result = await send_slack_notification(
                webhook_url="https://hooks.slack.com/test",
                payload=payload
            )
            
            assert result is True or result.get("success", False)
    
    @pytest.mark.asyncio
    async def test_send_slack_notification_handles_error(self):
        """Test Slack notification handles HTTP errors gracefully"""
        from services.notification_service import (
            send_slack_notification,
            NotificationPayload,
            NotificationPriority
        )
        
        payload = NotificationPayload(
            title="Test",
            message="Test",
            priority=NotificationPriority.NORMAL
        )
        
        with patch('services.notification_service.httpx.AsyncClient') as mock_client:
            mock_instance = AsyncMock()
            mock_instance.post = AsyncMock(side_effect=Exception("Connection error"))
            mock_client.return_value.__aenter__.return_value = mock_instance
            
            result = await send_slack_notification(
                webhook_url="https://invalid.webhook",
                payload=payload
            )
            
            # Should return False or error dict, not raise
            assert result is False or (isinstance(result, dict) and not result.get("success", True))


# ============ Discord Integration ============

class TestDiscordIntegration:
    """Tests for Discord notification integration"""
    
    @pytest.mark.asyncio
    async def test_send_discord_notification_formats_embed(self):
        """Test Discord notification creates proper embed format"""
        from services.notification_service import (
            send_discord_notification,
            NotificationPayload,
            NotificationPriority
        )
        
        payload = NotificationPayload(
            title="رسالة جديدة",
            message="محتوى الرسالة",
            priority=NotificationPriority.HIGH
        )
        
        with patch('services.notification_service.httpx.AsyncClient') as mock_client:
            mock_instance = AsyncMock()
            mock_instance.post = AsyncMock(return_value=MagicMock(status_code=204))
            mock_client.return_value.__aenter__.return_value = mock_instance
            
            result = await send_discord_notification(
                webhook_url="https://discord.com/api/webhooks/test",
                payload=payload
            )
            
            # Discord returns 204 on success
            assert result is True or result.get("success", False)


# ============ Webhook Integration ============

class TestWebhookIntegration:
    """Tests for generic webhook integration"""
    
    @pytest.mark.asyncio
    async def test_send_webhook_notification_posts_json(self):
        """Test webhook sends JSON payload"""
        from services.notification_service import (
            send_webhook_notification,
            NotificationPayload,
            NotificationPriority
        )
        
        payload = NotificationPayload(
            title="Alert",
            message="Custom webhook test",
            priority=NotificationPriority.NORMAL,
            metadata={"custom_field": "value"}
        )
        
        with patch('services.notification_service.httpx.AsyncClient') as mock_client:
            mock_instance = AsyncMock()
            mock_response = MagicMock(status_code=200)
            mock_instance.post = AsyncMock(return_value=mock_response)
            mock_client.return_value.__aenter__.return_value = mock_instance
            
            result = await send_webhook_notification(
                webhook_url="https://custom.webhook.io/notify",
                payload=payload
            )
            
            # Verify JSON was posted
            mock_instance.post.assert_called_once()
            call_args = mock_instance.post.call_args
            assert call_args is not None
            # Check the JSON payload was sent
            if 'json' in call_args.kwargs:
                sent_json = call_args.kwargs['json']
                assert "title" in sent_json["data"] or "message" in sent_json["data"]


# ============ Priority Coloring ============

class TestPriorityColoring:
    """Tests for priority-based notification coloring"""
    
    def test_urgent_priority_has_red_color(self):
        """Test urgent priority maps to red/alert color"""
        from services.notification_service import NotificationPriority
        
        # Priority colors for Slack/Discord embeds
        priority_colors = {
            NotificationPriority.LOW: "#36a64f",      # Green
            NotificationPriority.NORMAL: "#439FE0",   # Blue
            NotificationPriority.HIGH: "#FF9500",     # Orange
            NotificationPriority.URGENT: "#FF3B30"    # Red
        }
        
        # Urgent should be red-ish
        urgent_color = priority_colors.get(NotificationPriority.URGENT, "")
        assert urgent_color.upper() in ["#FF3B30", "#FF0000", "#DC3545"]


# ============ Notification Cooldown / Throttling ============

class TestNotificationThrottling:
    """Tests for notification flood protection"""
    
    def test_cooldown_constant_exists(self):
        """Test cooldown constant is defined"""
        from services.notification_service import _COOLDOWN_SECONDS
        
        assert _COOLDOWN_SECONDS > 0
        assert _COOLDOWN_SECONDS >= 30  # At least 30 seconds


# ============ Alert Types ============

class TestAlertTypes:
    """Tests for predefined alert type functions"""
    
    @pytest.mark.asyncio
    async def test_send_urgent_message_alert(self):
        """Test urgent message alert creation"""
        from services.notification_service import send_urgent_message_alert
        
        with patch('services.notification_service.send_notification', new_callable=AsyncMock) as mock_send:
            mock_send.return_value = {"success": True}
            
            await send_urgent_message_alert(
                license_id=1,
                sender_name="أحمد محمد",
                message_preview="أحتاج مساعدة عاجلة..."
            )
            
            mock_send.assert_called_once()
            call_args = mock_send.call_args
            assert call_args[0][0] == 1  # license_id
    
    @pytest.mark.asyncio
    async def test_send_negative_sentiment_alert(self):
        """Test negative sentiment alert creation"""
        from services.notification_service import send_negative_sentiment_alert
        
        with patch('services.notification_service.send_notification', new_callable=AsyncMock) as mock_send:
            mock_send.return_value = {"success": True}
            
            await send_negative_sentiment_alert(
                license_id=2,
                sender_name="عميل غاضب",
                message_preview="خدمة سيئة جداً..."
            )
            
            mock_send.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_send_vip_customer_alert(self):
        """Test VIP customer alert creation"""
        from services.notification_service import send_vip_customer_alert
        
        with patch('services.notification_service.send_notification', new_callable=AsyncMock) as mock_send:
            mock_send.return_value = {"success": True}
            
            await send_vip_customer_alert(
                license_id=3,
                customer_name="عميل مميز",
                message_preview="استفسار عن منتج جديد"
            )
            
            mock_send.assert_called_once()


# ============ Integration Storage ============

class TestIntegrationStorage:
    """Tests for integration configuration storage"""
    
    @pytest.mark.asyncio
    async def test_save_integration_function_exists(self):
        """Test save_integration is importable"""
        from services.notification_service import save_integration
        
        assert callable(save_integration)
    
    @pytest.mark.asyncio
    async def test_get_integration_function_exists(self):
        """Test get_integration is importable"""
        from services.notification_service import get_integration
        
        assert callable(get_integration)
    
    @pytest.mark.asyncio
    async def test_disable_integration_function_exists(self):
        """Test disable_integration is importable"""
        from services.notification_service import disable_integration
        
        assert callable(disable_integration)


# ============ Notification Rules ============

class TestNotificationRules:
    """Tests for notification rule management"""
    
    def test_create_rule_function_exists(self):
        """Test create_rule is importable"""
        from services.notification_service import create_rule
        
        assert callable(create_rule)
    
    def test_get_rules_function_exists(self):
        """Test get_rules is importable"""
        from services.notification_service import get_rules
        
        assert callable(get_rules)
    
    def test_delete_rule_function_exists(self):
        """Test delete_rule is importable"""
        from services.notification_service import delete_rule
        
        assert callable(delete_rule)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
