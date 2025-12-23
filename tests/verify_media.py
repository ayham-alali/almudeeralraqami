
import asyncio
import sys
import os
import unittest
from unittest.mock import MagicMock, AsyncMock, patch
from datetime import datetime
import base64

# Adjust path to import backend modules
sys.path.append("c:/roya/products/almudeer/backend")

from services.telegram_phone_service import TelegramPhoneService

class TestTelegramMedia(unittest.IsolatedAsyncioTestCase):
    
    async def test_media_download(self):
        # Mock dependencies
        with patch('services.telegram_phone_service.TelegramClient') as MockClient, \
             patch('services.telegram_phone_service.StringSession'), \
             patch.dict(os.environ, {"TELEGRAM_API_ID": "123", "TELEGRAM_API_HASH": "abc"}):
            
            client_instance = MockClient.return_value
            client_instance.connect = AsyncMock()
            client_instance.is_user_authorized = AsyncMock(return_value=True)
            client_instance.disconnect = AsyncMock()
            client_instance.get_me = AsyncMock()
            client_instance.get_dialogs = AsyncMock(return_value=[MagicMock(is_channel=False, is_group=False, entity=MagicMock())])
            
            # Mock Message with Media
            mock_msg = MagicMock()
            mock_msg.id = 123
            mock_msg.text = "Caption"
            mock_msg.out = False
            mock_msg.date = datetime.utcnow()
            mock_msg.media = MagicMock()
            mock_msg.media.document.size = 1000 # Small size
            
            # Mock download_media
            mock_msg.download_media = AsyncMock(return_value=b"fake_image_data")
            mock_msg.get_sender = AsyncMock(return_value=MagicMock(id=999, first_name="Sender", last_name="One", phone="+123", username="sender"))
            
            # Mock get_me for self-check
            client_instance.get_me = AsyncMock(return_value=MagicMock(id=888))
            
            # Mock iter_messages
            # Need to mock __aiter__
            async def async_iter(*args, **kwargs):
                yield mock_msg
            
            client_instance.iter_messages.return_value = async_iter()
            
            service = TelegramPhoneService()
            service.api_id = "123"
            service.api_hash = "abc"
            
            # Run get_recent_messages
            messages = await service.get_recent_messages("fake_session", limit=1)
            
            self.assertEqual(len(messages), 1)
            self.assertEqual(messages[0]["body"], "Caption")
            
            # Verify attachments
            attachments = messages[0].get("attachments")
            self.assertIsNotNone(attachments)
            self.assertEqual(len(attachments), 1)
            self.assertEqual(attachments[0]["base64"], base64.b64encode(b"fake_image_data").decode('utf-8'))
            
    async def test_exclude_ids(self):
        # Mock dependencies
        with patch('services.telegram_phone_service.TelegramClient') as MockClient, \
             patch('services.telegram_phone_service.StringSession'), \
             patch.dict(os.environ, {"TELEGRAM_API_ID": "123", "TELEGRAM_API_HASH": "abc"}):
            
            client_instance = MockClient.return_value
            client_instance.connect = AsyncMock()
            client_instance.is_user_authorized = AsyncMock(return_value=True)
            client_instance.disconnect = AsyncMock()
            client_instance.get_me = AsyncMock()
            client_instance.get_dialogs = AsyncMock(return_value=[MagicMock(is_channel=False, is_group=False, entity=MagicMock())])
            
            # Mock 2 messages
            msg1 = MagicMock(id=1, text="One", out=False, date=datetime.utcnow(), media=None)
            msg1.get_sender = AsyncMock(return_value=MagicMock(id=999, first_name="Sender", last_name="One", phone="+123", username="sender"))
            
            msg2 = MagicMock(id=2, text="Two", out=False, date=datetime.utcnow(), media=None)
            msg2.get_sender = AsyncMock(return_value=MagicMock(id=999, first_name="Sender", last_name="One", phone="+123", username="sender"))
            
            # Mock get_me
            client_instance.get_me = AsyncMock(return_value=MagicMock(id=888))
            
            async def async_iter(*args, **kwargs):
                yield msg2
                yield msg1
            
            client_instance.iter_messages.return_value = async_iter()
            
            service = TelegramPhoneService()
            service.api_id = "123"
            service.api_hash = "abc"
            
            # Run with exclude_ids=[ "2" ]
            messages = await service.get_recent_messages("fake_session", limit=10, exclude_ids=["2"])
            
            # Should only return msg1 (ID 1)
            self.assertEqual(len(messages), 1)
            self.assertEqual(messages[0]["channel_message_id"], "1")

if __name__ == "__main__":
    unittest.main()
