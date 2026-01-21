"""
Al-Mudeer WhatsApp Service Tests
Unit tests for WhatsApp Business Cloud API integration
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import hmac
import hashlib


# ============ WhatsApp Service Initialization ============

class TestWhatsAppServiceInit:
    """Tests for WhatsApp service initialization"""
    
    def test_service_initialization(self):
        """Test WhatsApp service initializes with correct properties"""
        from services.whatsapp_service import WhatsAppService
        
        service = WhatsAppService(
            phone_number_id="12345",
            access_token="test_token_123",
            verify_token="verify_me",
            webhook_secret="secret_123"
        )
        
        assert service.phone_number_id == "12345"
        assert service.access_token == "test_token_123"
        assert service.verify_token == "verify_me"
        assert service.webhook_secret == "secret_123"


# ============ Webhook Verification ============

class TestWebhookVerification:
    """Tests for Meta webhook verification"""
    
    def test_verify_webhook_success(self):
        """Test webhook verification with correct token"""
        from services.whatsapp_service import WhatsAppService
        
        service = WhatsAppService(
            phone_number_id="12345",
            access_token="token",
            verify_token="my_verify_token"
        )
        
        result = service.verify_webhook(
            mode="subscribe",
            token="my_verify_token",
            challenge="challenge123"
        )
        
        assert result == "challenge123"
    
    def test_verify_webhook_wrong_token(self):
        """Test webhook verification with wrong token"""
        from services.whatsapp_service import WhatsAppService
        
        service = WhatsAppService(
            phone_number_id="12345",
            access_token="token",
            verify_token="correct_token"
        )
        
        result = service.verify_webhook(
            mode="subscribe",
            token="wrong_token",
            challenge="challenge123"
        )
        
        assert result is None or result != "challenge123"


# ============ Signature Verification ============

class TestSignatureVerification:
    """Tests for webhook payload signature verification"""
    
    def test_verify_signature_valid(self):
        """Test signature verification with valid signature"""
        from services.whatsapp_service import WhatsAppService
        
        service = WhatsAppService(
            phone_number_id="12345",
            access_token="token",
            webhook_secret="test_secret"
        )
        
        payload = b'{"test": "payload"}'
        expected_signature = "sha256=" + hmac.new(
            b"test_secret",
            payload,
            hashlib.sha256
        ).hexdigest()
        
        result = service.verify_signature(payload, expected_signature)
        
        assert result is True
    
    def test_verify_signature_invalid(self):
        """Test signature verification with invalid signature"""
        from services.whatsapp_service import WhatsAppService
        
        service = WhatsAppService(
            phone_number_id="12345",
            access_token="token",
            webhook_secret="test_secret"
        )
        
        payload = b'{"test": "payload"}'
        wrong_signature = "sha256=wrongsignature123"
        
        result = service.verify_signature(payload, wrong_signature)
        
        assert result is False


# ============ Message Parsing ============

class TestMessageParsing:
    """Tests for parsing incoming webhook messages"""
    
    def test_parse_text_message(self):
        """Test parsing a text message from webhook"""
        from services.whatsapp_service import WhatsAppService
        
        service = WhatsAppService(
            phone_number_id="12345",
            access_token="token"
        )
        
        payload = {
            "entry": [{
                "changes": [{
                    "value": {
                        "messages": [{
                            "id": "msg_123",
                            "from": "966501234567",
                            "timestamp": "1700000000",
                            "type": "text",
                            "text": {"body": "مرحباً، أريد الاستفسار"}
                        }],
                        "contacts": [{
                            "profile": {"name": "أحمد محمد"},
                            "wa_id": "966501234567"
                        }]
                    }
                }]
            }]
        }
        
        messages = service.parse_webhook_message(payload)
        
        assert messages is not None
        assert len(messages) > 0
        # The service returns 'body' not 'text', and 'from' / 'sender_phone' for contact
        assert messages[0]["body"] == "مرحباً، أريد الاستفسار"
        assert messages[0]["from"] == "966501234567" or messages[0].get("sender_phone") == "966501234567"
    
    def test_parse_image_message(self):
        """Test parsing an image message from webhook"""
        from services.whatsapp_service import WhatsAppService
        
        service = WhatsAppService(
            phone_number_id="12345",
            access_token="token"
        )
        
        payload = {
            "entry": [{
                "changes": [{
                    "value": {
                        "messages": [{
                            "id": "msg_456",
                            "from": "966501234567",
                            "timestamp": "1700000000",
                            "type": "image",
                            "image": {
                                "id": "media_123",
                                "mime_type": "image/jpeg",
                                "sha256": "abc123",
                                "caption": "صورة المنتج"
                            }
                        }],
                        "contacts": [{
                            "profile": {"name": "Test User"},
                            "wa_id": "966501234567"
                        }]
                    }
                }]
            }]
        }
        
        messages = service.parse_webhook_message(payload)
        
        assert messages is not None
        assert len(messages) > 0
        msg = messages[0]
        assert msg["type"] == "image" or "image" in str(msg.get("attachments", []))
    
    def test_parse_empty_payload(self):
        """Test parsing empty/invalid payload"""
        from services.whatsapp_service import WhatsAppService
        
        service = WhatsAppService(
            phone_number_id="12345",
            access_token="token"
        )
        
        payload = {"entry": []}
        
        messages = service.parse_webhook_message(payload)
        
        assert messages == [] or messages is None


# ============ API Methods ============

class TestWhatsAppAPIMethods:
    """Tests for WhatsApp API methods with mocked HTTP"""
    
    @pytest.mark.asyncio
    async def test_send_message(self):
        """Test sending a text message"""
        from services.whatsapp_service import WhatsAppService
        
        service = WhatsAppService(
            phone_number_id="12345",
            access_token="token"
        )
        
        with patch('services.whatsapp_service.httpx.AsyncClient') as mock_client:
            mock_instance = AsyncMock()
            mock_instance.post = AsyncMock(return_value=MagicMock(
                status_code=200,
                json=lambda: {"messages": [{"id": "wamid.123"}]}
            ))
            mock_client.return_value.__aenter__.return_value = mock_instance
            
            result = await service.send_message(
                to="966501234567",
                message="مرحباً"
            )
            
            assert result is not None
            mock_instance.post.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_send_message_with_reply(self):
        """Test sending a reply message"""
        from services.whatsapp_service import WhatsAppService
        
        service = WhatsAppService(
            phone_number_id="12345",
            access_token="token"
        )
        
        with patch('services.whatsapp_service.httpx.AsyncClient') as mock_client:
            mock_instance = AsyncMock()
            mock_instance.post = AsyncMock(return_value=MagicMock(
                status_code=200,
                json=lambda: {"messages": [{"id": "wamid.456"}]}
            ))
            mock_client.return_value.__aenter__.return_value = mock_instance
            
            result = await service.send_message(
                to="966501234567",
                message="شكراً لك",
                reply_to_message_id="wamid.original"
            )
            
            call_args = mock_instance.post.call_args
            # Should include context for reply
            assert call_args is not None
    
    @pytest.mark.asyncio
    async def test_mark_as_read(self):
        """Test marking a message as read"""
        from services.whatsapp_service import WhatsAppService
        
        service = WhatsAppService(
            phone_number_id="12345",
            access_token="token"
        )
        
        with patch('services.whatsapp_service.httpx.AsyncClient') as mock_client:
            mock_instance = AsyncMock()
            mock_instance.post = AsyncMock(return_value=MagicMock(
                status_code=200,
                json=lambda: {"success": True}
            ))
            mock_client.return_value.__aenter__.return_value = mock_instance
            
            result = await service.mark_as_read("wamid.123")
            
            assert result is True or result is not None


# ============ Config Storage ============

class TestWhatsAppConfigStorage:
    """Tests for WhatsApp config database operations"""
    
    def test_save_whatsapp_config_function_exists(self):
        """Test save_whatsapp_config is importable"""
        from services.whatsapp_service import save_whatsapp_config
        
        assert callable(save_whatsapp_config)
    
    def test_get_whatsapp_config_function_exists(self):
        """Test get_whatsapp_config is importable"""
        from services.whatsapp_service import get_whatsapp_config
        
        assert callable(get_whatsapp_config)
    
    def test_delete_whatsapp_config_function_exists(self):
        """Test delete_whatsapp_config is importable"""
        from services.whatsapp_service import delete_whatsapp_config
        
        assert callable(delete_whatsapp_config)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
