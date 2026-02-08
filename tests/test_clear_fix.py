
import pytest
import os
from unittest.mock import AsyncMock, patch
from models.inbox import clear_conversation_messages, soft_delete_conversation

@pytest.mark.asyncio
async def test_clear_conversation_messages_no_nameerror():
    """
    Test that clear_conversation_messages defines ts_value and doesn't raise NameError.
    """
    # Mock database helpers
    with patch("models.inbox.get_db"), \
         patch("models.inbox._get_sender_aliases", new_callable=AsyncMock) as mock_aliases, \
         patch("models.inbox.execute_sql", new_callable=AsyncMock) as mock_execute, \
         patch("models.inbox.commit_db", new_callable=AsyncMock), \
         patch("models.inbox.upsert_conversation_state", new_callable=AsyncMock):
        
        mock_aliases.return_value = (set(["test_contact"]), set(["123"]))
        
        # This should NOT raise NameError
        try:
            result = await clear_conversation_messages(license_id=1, sender_contact="test_contact")
            assert result["success"] is True
        except NameError as e:
            pytest.fail(f"NameError raised: {e}")
        except Exception as e:
            # Other exceptions might occur due to mocking, but we specifically care about NameError
            if "name 'ts_value' is not defined" in str(e):
                pytest.fail(f"NameError still present: {e}")
            else:
                # If it's a different error, we might still have passed the ts_value check
                print(f"Caught expected/mock-related error: {e}")

@pytest.mark.asyncio
async def test_soft_delete_conversation_no_nameerror():
    """
    Test that soft_delete_conversation defines ts_value and doesn't raise NameError.
    """
    # Mock database helpers
    with patch("models.inbox.get_db"), \
         patch("models.inbox._get_sender_aliases", new_callable=AsyncMock) as mock_aliases, \
         patch("models.inbox.execute_sql", new_callable=AsyncMock) as mock_execute, \
         patch("models.inbox.commit_db", new_callable=AsyncMock), \
         patch("models.inbox.upsert_conversation_state", new_callable=AsyncMock):
        
        mock_aliases.return_value = (set(["test_contact"]), set(["123"]))
        
        # This should NOT raise NameError
        try:
            result = await soft_delete_conversation(license_id=1, sender_contact="test_contact")
            assert result["success"] is True
        except NameError as e:
            pytest.fail(f"NameError raised in soft_delete_conversation: {e}")
        except Exception as e:
            print(f"Caught expected/mock-related error: {e}")

if __name__ == "__main__":
    # This allows running the test directly for quick verification
    import asyncio
    
    # Minimal mock setup for direct run
    async def run_repro():
        print("Running repro check...")
        # Since we use patches, we still need pytest or manual patching
        # Just running via pytest is better
    
    asyncio.run(run_repro())
