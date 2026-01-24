import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
import sys
import os

# Add the backend directory to sys.path to ensure imports work
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Mock environment variables before importing the service
with patch.dict(os.environ, {"TELEGRAM_API_ID": "12345", "TELEGRAM_API_HASH": "abcde"}):
    from services.telegram_phone_service import TelegramPhoneService

@pytest.fixture
def mock_service():
    with patch.dict(os.environ, {"TELEGRAM_API_ID": "12345", "TELEGRAM_API_HASH": "abcde"}):
        return TelegramPhoneService()

@pytest.mark.asyncio
async def test_resolve_telegram_entity_direct_success(mock_service):
    client = AsyncMock()
    logger = MagicMock()
    
    # Mock successful direct resolution
    mock_entity = MagicMock()
    client.get_entity.return_value = mock_entity
    
    result = await mock_service._resolve_telegram_entity(client, "123456", logger)
    
    assert result == mock_entity
    client.get_entity.assert_called_with(123456)

@pytest.mark.asyncio
async def test_resolve_telegram_entity_fallback_dialogs(mock_service):
    client = AsyncMock()
    logger = MagicMock()
    
    # Direct resolution fails
    client.get_entity.side_effect = Exception("Not found")
    
    # Mock iter_dialogs
    mock_dialog = MagicMock()
    mock_dialog.id = 123456
    mock_dialog.entity = MagicMock()
    
    async def mock_iter_dialogs(limit):
        yield mock_dialog
        
    client.iter_dialogs = mock_iter_dialogs
    
    result = await mock_service._resolve_telegram_entity(client, "123456", logger)
    
    assert result == mock_dialog.entity

@pytest.mark.asyncio
async def test_resolve_telegram_entity_fallback_messages(mock_service):
    client = AsyncMock()
    logger = MagicMock()
    
    # Direct and dialog resolution fail
    client.get_entity.side_effect = Exception("Not found")
    
    async def mock_iter_dialogs(limit):
        if False: yield # Empty generator
        
    client.iter_dialogs = mock_iter_dialogs
    
    # Mock iter_messages
    mock_msg = MagicMock()
    mock_msg.peer_id = MagicMock()
    mock_msg.peer_id.user_id = 123456
    mock_msg.get_sender = AsyncMock(return_value=MagicMock())
    
    async def mock_iter_messages(entity, limit, search):
        yield mock_msg
        
    client.iter_messages = mock_iter_messages
    
    result = await mock_service._resolve_telegram_entity(client, "123456", logger)
    
    assert result == await mock_msg.get_sender()

@pytest.mark.asyncio
async def test_send_message_uses_resolver(mock_service):
    client = AsyncMock()
    
    with patch.object(mock_service, '_resolve_telegram_entity', new_callable=AsyncMock) as mock_resolve:
        mock_entity = MagicMock()
        mock_resolve.return_value = mock_entity
        
        # Mock other dependencies
        with patch.object(mock_service, 'create_client_from_session', return_value=client):
            with patch('logging_config.get_logger', return_value=MagicMock()):
                
                # Setup sent message return
                sent_msg = MagicMock()
                sent_msg.id = 999
                sent_msg.peer_id.user_id = 123456
                sent_msg.text = "hello"
                sent_msg.date = None
                
                # Mock _execute_with_retry to handle multiple calls
                async def side_effect(func, *args, **kwargs):
                    if func == client.get_dialogs:
                        return None
                    if func == client.send_message:
                        return sent_msg
                    return None
                
                mock_service._execute_with_retry = AsyncMock(side_effect=side_effect)
                
                result = await mock_service.send_message("session", "123456", "hello")
                
                assert result["id"] == 999
                mock_resolve.assert_called_once()
