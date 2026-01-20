"""
Al-Mudeer Telegram Service Tests
Unit tests for Telegram Bot and Phone services
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime


# ============ Telegram Service - Parse Update ============

class TestTelegramServiceParseUpdate:
    """Tests for TelegramService.parse_update static method"""
    
    def test_parse_basic_text_message(self):
        """Test parsing a basic text message"""
        from services.telegram_service import TelegramService
        
        update = {
            "update_id": 123456,
            "message": {
                "message_id": 1,
                "chat": {
                    "id": 123,
                    "type": "private"
                },
                "from": {
                    "id": 456,
                    "first_name": "أحمد",
                    "last_name": "محمد",
                    "username": "ahmed_test",
                    "is_bot": False
                },
                "text": "مرحباً، كيف حالك؟",
                "date": 1700000000
            }
        }
        
        result = TelegramService.parse_update(update)
        
        assert result is not None
        assert result["update_id"] == 123456
        assert result["message_id"] == 1
        assert result["chat_id"] == "123"
        assert result["chat_type"] == "private"
        assert result["user_id"] == "456"
        assert result["username"] == "ahmed_test"
        assert result["first_name"] == "أحمد"
        assert result["last_name"] == "محمد"
        assert result["text"] == "مرحباً، كيف حالك؟"
        assert result["is_bot"] is False
        assert result["attachments"] == []
    
    def test_parse_message_with_photo(self):
        """Test parsing a message with photo attachment"""
        from services.telegram_service import TelegramService
        
        update = {
            "update_id": 123,
            "message": {
                "message_id": 2,
                "chat": {"id": 100, "type": "private"},
                "from": {"id": 200, "first_name": "Test", "is_bot": False},
                "caption": "صورة المنتج",
                "date": 1700000000,
                "photo": [
                    {"file_id": "small_id", "file_size": 1000, "width": 100, "height": 100},
                    {"file_id": "medium_id", "file_size": 5000, "width": 300, "height": 300},
                    {"file_id": "large_id", "file_size": 20000, "width": 800, "height": 800}
                ]
            }
        }
        
        result = TelegramService.parse_update(update)
        
        assert result["text"] == "صورة المنتج"
        assert len(result["attachments"]) == 1
        assert result["attachments"][0]["type"] == "photo"
        assert result["attachments"][0]["file_id"] == "large_id"  # Should get largest
        assert result["attachments"][0]["file_size"] == 20000
    
    def test_parse_message_with_voice(self):
        """Test parsing a voice message"""
        from services.telegram_service import TelegramService
        
        update = {
            "update_id": 124,
            "message": {
                "message_id": 3,
                "chat": {"id": 100, "type": "private"},
                "from": {"id": 200, "first_name": "Test", "is_bot": False},
                "date": 1700000000,
                "voice": {
                    "file_id": "voice_123",
                    "mime_type": "audio/ogg",
                    "file_size": 15000,
                    "duration": 10
                }
            }
        }
        
        result = TelegramService.parse_update(update)
        
        assert len(result["attachments"]) == 1
        assert result["attachments"][0]["type"] == "voice"
        assert result["attachments"][0]["file_id"] == "voice_123"
        assert result["attachments"][0]["mime_type"] == "audio/ogg"
    
    def test_parse_message_with_document(self):
        """Test parsing a document message"""
        from services.telegram_service import TelegramService
        
        update = {
            "update_id": 125,
            "message": {
                "message_id": 4,
                "chat": {"id": 100, "type": "private"},
                "from": {"id": 200, "first_name": "Test", "is_bot": False},
                "date": 1700000000,
                "document": {
                    "file_id": "doc_456",
                    "mime_type": "application/pdf",
                    "file_name": "invoice.pdf",
                    "file_size": 50000
                }
            }
        }
        
        result = TelegramService.parse_update(update)
        
        assert len(result["attachments"]) == 1
        assert result["attachments"][0]["type"] == "document"
        assert result["attachments"][0]["file_id"] == "doc_456"
        assert result["attachments"][0]["file_name"] == "invoice.pdf"
    
    def test_parse_message_with_audio(self):
        """Test parsing an audio file message"""
        from services.telegram_service import TelegramService
        
        update = {
            "update_id": 126,
            "message": {
                "message_id": 5,
                "chat": {"id": 100, "type": "private"},
                "from": {"id": 200, "first_name": "Test", "is_bot": False},
                "date": 1700000000,
                "audio": {
                    "file_id": "audio_789",
                    "mime_type": "audio/mpeg",
                    "file_size": 4000000,
                    "duration": 180
                }
            }
        }
        
        result = TelegramService.parse_update(update)
        
        assert len(result["attachments"]) == 1
        assert result["attachments"][0]["type"] == "audio"
        assert result["attachments"][0]["mime_type"] == "audio/mpeg"
    
    def test_detect_bot_user(self):
        """Test that bot users are detected correctly"""
        from services.telegram_service import TelegramService
        
        update = {
            "update_id": 127,
            "message": {
                "message_id": 6,
                "chat": {"id": 100, "type": "private"},
                "from": {
                    "id": 999,
                    "first_name": "TestBot",
                    "username": "test_bot",
                    "is_bot": True
                },
                "text": "Bot message",
                "date": 1700000000
            }
        }
        
        result = TelegramService.parse_update(update)
        
        assert result["is_bot"] is True
    
    def test_parse_group_message(self):
        """Test parsing a message from a group"""
        from services.telegram_service import TelegramService
        
        update = {
            "update_id": 128,
            "message": {
                "message_id": 7,
                "chat": {"id": -100123, "type": "supergroup", "title": "العمل"},
                "from": {"id": 200, "first_name": "محمد", "is_bot": False},
                "text": "رسالة في المجموعة",
                "date": 1700000000
            }
        }
        
        result = TelegramService.parse_update(update)
        
        assert result["chat_type"] == "supergroup"
        assert result["chat_id"] == "-100123"
    
    def test_parse_channel_post(self):
        """Test parsing a channel post (no from user)"""
        from services.telegram_service import TelegramService
        
        update = {
            "update_id": 129,
            "channel_post": {
                "message_id": 8,
                "chat": {"id": -10012345, "type": "channel", "title": "أخبار", "username": "news_channel"},
                "text": "إعلان جديد",
                "date": 1700000000
            }
        }
        
        result = TelegramService.parse_update(update)
        
        assert result is not None
        assert result["chat_type"] == "channel"
        assert result["first_name"] == "أخبار"
    
    def test_parse_edited_message(self):
        """Test parsing an edited message"""
        from services.telegram_service import TelegramService
        
        update = {
            "update_id": 130,
            "edited_message": {
                "message_id": 9,
                "chat": {"id": 100, "type": "private"},
                "from": {"id": 200, "first_name": "Test", "is_bot": False},
                "text": "رسالة معدّلة",
                "date": 1700000000,
                "edit_date": 1700000100
            }
        }
        
        result = TelegramService.parse_update(update)
        
        assert result is not None
        assert result["text"] == "رسالة معدّلة"
    
    def test_parse_empty_update_returns_none(self):
        """Test that empty update returns None"""
        from services.telegram_service import TelegramService
        
        update = {"update_id": 131}
        
        result = TelegramService.parse_update(update)
        
        assert result is None
    
    def test_parse_my_chat_member_returns_none(self):
        """Test that my_chat_member updates are ignored"""
        from services.telegram_service import TelegramService
        
        update = {
            "update_id": 132,
            "my_chat_member": {
                "chat": {"id": 100, "type": "group"},
                "from": {"id": 200, "first_name": "Admin"},
                "new_chat_member": {"status": "member"}
            }
        }
        
        result = TelegramService.parse_update(update)
        
        assert result is None


# ============ Telegram Bot Manager ============

class TestTelegramBotManager:
    """Tests for TelegramBotManager class"""
    
    def test_get_bot_creates_new_instance(self):
        """Test that get_bot creates a new instance for new license"""
        from services.telegram_service import TelegramBotManager, TelegramService
        
        # Clear any existing instances
        TelegramBotManager._instances.clear()
        
        bot = TelegramBotManager.get_bot(license_id=1, bot_token="test_token_123")
        
        assert isinstance(bot, TelegramService)
        assert bot.bot_token == "test_token_123"
        assert 1 in TelegramBotManager._instances
    
    def test_get_bot_returns_existing_instance(self):
        """Test that get_bot returns existing instance for same license"""
        from services.telegram_service import TelegramBotManager
        
        TelegramBotManager._instances.clear()
        
        bot1 = TelegramBotManager.get_bot(license_id=2, bot_token="token_abc")
        bot2 = TelegramBotManager.get_bot(license_id=2, bot_token="token_abc")
        
        assert bot1 is bot2
    
    def test_remove_bot_clears_instance(self):
        """Test that remove_bot clears the instance"""
        from services.telegram_service import TelegramBotManager
        
        TelegramBotManager._instances.clear()
        
        TelegramBotManager.get_bot(license_id=3, bot_token="token_xyz")
        assert 3 in TelegramBotManager._instances
        
        TelegramBotManager.remove_bot(license_id=3)
        assert 3 not in TelegramBotManager._instances
    
    def test_remove_nonexistent_bot_no_error(self):
        """Test that removing non-existent bot doesn't raise error"""
        from services.telegram_service import TelegramBotManager
        
        TelegramBotManager._instances.clear()
        
        # Should not raise
        TelegramBotManager.remove_bot(license_id=999)


# ============ Telegram Service API Methods ============

class TestTelegramServiceAPI:
    """Tests for TelegramService API methods with mocked HTTP"""
    
    @pytest.mark.asyncio
    async def test_get_me_success(self):
        """Test get_me returns bot info"""
        from services.telegram_service import TelegramService
        
        service = TelegramService("fake_token")
        
        with patch.object(service, '_request', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {
                "id": 123456789,
                "is_bot": True,
                "first_name": "Al-Mudeer Bot",
                "username": "almudeer_bot"
            }
            
            result = await service.get_me()
            
            assert result["id"] == 123456789
            assert result["is_bot"] is True
            mock_request.assert_called_once_with("getMe")
    
    @pytest.mark.asyncio
    async def test_send_message_with_reply(self):
        """Test send_message with reply_to_message_id"""
        from services.telegram_service import TelegramService
        
        service = TelegramService("fake_token")
        
        with patch.object(service, '_request', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {"message_id": 100}
            
            await service.send_message(
                chat_id="123",
                text="مرحباً",
                reply_to_message_id=99
            )
            
            mock_request.assert_called_once()
            call_args = mock_request.call_args
            assert call_args[0][0] == "sendMessage"
            assert call_args[0][1]["chat_id"] == "123"
            assert call_args[0][1]["text"] == "مرحباً"
            assert call_args[0][1]["reply_to_message_id"] == 99
    
    @pytest.mark.asyncio
    async def test_set_webhook_with_secret(self):
        """Test set_webhook includes secret token"""
        from services.telegram_service import TelegramService
        
        service = TelegramService("fake_token")
        
        with patch.object(service, '_request', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = True
            
            result = await service.set_webhook(
                webhook_url="https://example.com/webhook",
                secret_token="secret123"
            )
            
            assert result is True
            call_args = mock_request.call_args
            assert call_args[0][1]["url"] == "https://example.com/webhook"
            assert call_args[0][1]["secret_token"] == "secret123"
    
    @pytest.mark.asyncio
    async def test_test_connection_success(self):
        """Test test_connection returns success tuple"""
        from services.telegram_service import TelegramService
        
        service = TelegramService("fake_token")
        
        with patch.object(service, 'get_me', new_callable=AsyncMock) as mock_get_me:
            mock_get_me.return_value = {"id": 123, "username": "test_bot"}
            
            success, message, info = await service.test_connection()
            
            assert success is True
            assert "نجاح" in message
            assert info["id"] == 123
    
    @pytest.mark.asyncio
    async def test_test_connection_failure(self):
        """Test test_connection returns failure tuple on error"""
        from services.telegram_service import TelegramService
        
        service = TelegramService("invalid_token")
        
        with patch.object(service, 'get_me', new_callable=AsyncMock) as mock_get_me:
            mock_get_me.side_effect = Exception("Unauthorized")
            
            success, message, info = await service.test_connection()
            
            assert success is False
            assert "خطأ" in message
            assert info == {}


# ============ MIME Type Helper ============

class TestGetMimeType:
    """Tests for get_mime_type helper function"""
    
    def test_common_mime_types(self):
        """Test common file extension MIME types"""
        from services.telegram_phone_service import get_mime_type
        
        assert get_mime_type("jpg") == "image/jpeg"
        assert get_mime_type("png") == "image/png"
        assert get_mime_type("pdf") == "application/pdf"
        assert get_mime_type("mp3") == "audio/mpeg"
        assert get_mime_type("mp4") == "video/mp4"
    
    def test_unknown_extension_returns_default(self):
        """Test unknown extension returns octet-stream"""
        from services.telegram_phone_service import get_mime_type
        
        result = get_mime_type("xyz")
        assert result == "application/octet-stream"


# ============ Session State Encoding ============

class TestSessionStateEncoding:
    """Tests for session state encoding/decoding"""
    
    def test_encode_decode_roundtrip(self):
        """Test that encoding and decoding preserves data"""
        from services.telegram_phone_service import TelegramPhoneService
        
        # Use static methods directly to avoid init checks
        original_state = {
            "phone": "+963912345678",
            "session": "abc123session",
            "hash": "xyz789hash",
            "ts": 1700000000
        }
        encoded = TelegramPhoneService._encode_session_state(original_state)
        decoded = TelegramPhoneService._decode_session_state(encoded)
        
        assert decoded == original_state
    
    def test_encoded_is_url_safe(self):
        """Test that encoded string is URL-safe"""
        from services.telegram_phone_service import TelegramPhoneService
        
        state = {"phone": "+963912345678", "session": "test", "hash": "hash", "ts": 123}
        encoded = TelegramPhoneService._encode_session_state(state)
        
        # Should not contain characters that need URL encoding
        assert "+" not in encoded
        assert "/" not in encoded


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
