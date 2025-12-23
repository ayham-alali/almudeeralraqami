
import asyncio
import sys
import unittest
from unittest.mock import MagicMock, AsyncMock, patch
from datetime import datetime

# Adjust path to import backend modules
sys.path.append("c:/roya/products/almudeer/backend")

from workers import MessagePoller

class TestBurstHandling(unittest.IsolatedAsyncioTestCase):
    
    async def test_telegram_burst_grouping(self):
        # Mock dependencies
        with patch('workers.get_telegram_phone_session_data', new_callable=AsyncMock) as mock_get_session, \
             patch('workers.get_telegram_phone_session', new_callable=AsyncMock) as mock_get_session_info, \
             patch('workers.TelegramPhoneService') as MockService, \
             patch('workers.get_inbox_messages', new_callable=AsyncMock) as mock_get_inbox, \
             patch('workers.save_inbox_message', new_callable=AsyncMock) as mock_save, \
             patch('workers.update_inbox_analysis', new_callable=AsyncMock) as mock_update, \
             patch('workers.update_telegram_phone_session_sync_time', new_callable=AsyncMock):
            
            # Setup mocks
            mock_get_session.return_value = "fake_session"
            mock_get_session_info.return_value = {"created_at": datetime.utcnow()} # Mock session info
            
            mock_service_instance = MockService.return_value
            # return 3 messages from same sender
            mock_service_instance.get_recent_messages = AsyncMock(return_value=[
                {
                    "channel_message_id": "1",
                    "sender_contact": "+123456789",
                    "sender_name": "Test User", 
                    "body": "Hello",
                    "received_at": datetime(2023, 1, 1, 10, 0, 0)
                },
                {
                    "channel_message_id": "2", 
                    "sender_contact": "+123456789",
                    "sender_name": "Test User",
                    "body": "This is message 2",
                    "received_at": datetime(2023, 1, 1, 10, 0, 1)
                },
                {
                    "channel_message_id": "3",
                    "sender_contact": "+123456789", 
                    "sender_name": "Test User",
                    "body": "And verify this burst",
                    "received_at": datetime(2023, 1, 1, 10, 0, 2)
                }
            ])
            
            # Instantiate poller
            poller = MessagePoller()
            poller._check_existing_message = AsyncMock(return_value=False)
            poller._analyze_and_process_message = AsyncMock()
            poller._check_user_rate_limit = MagicMock(return_value=(True, ""))
            
            # Mock save_inbox_message to return dummy IDs
            mock_save.side_effect = [101, 102, 103]
            
            # Run poll logic
            await poller._poll_telegram(license_id=1)
            
            # Assertions
            # 1. Verify all 3 saved to DB
            self.assertEqual(mock_save.call_count, 3)
            
            # 2. Verify analyze called ONCE (for the group)
            self.assertEqual(poller._analyze_and_process_message.call_count, 1)
            
            # 3. Verify arguments correctly passed combined text
            call_args = poller._analyze_and_process_message.call_args
            # Args: message_id, body, ...
            # Message ID should be the latest (103)
            self.assertEqual(call_args[0][0], 103)
            # Body should contain all 3 texts
            combined_body = call_args[0][1]
            print(f"Combined Body: {combined_body}")
            self.assertIn("Hello", combined_body)
            self.assertIn("message 2", combined_body)
            self.assertIn("verify this burst", combined_body)
            
            # 4. Verify previous messages marked as merged
            # IDs 101 and 102 should be updated
            self.assertEqual(mock_update.call_count, 2)
            # Check ID of first update call
            self.assertEqual(mock_update.call_args_list[0].kwargs['message_id'], 101)
            self.assertEqual(mock_update.call_args_list[0].kwargs['intent'], "merged")

if __name__ == "__main__":
    unittest.main()
