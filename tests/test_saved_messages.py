import pytest
from unittest.mock import AsyncMock, patch
from routes.chat_routes import send_approved_message

@pytest.mark.asyncio
async def test_saved_messages_bypass_external_delivery():
    """Verify that messages with 'saved' channel bypass external delivery and are marked sent."""
    outbox_id = 123
    license_id = 1
    
    # Mock data
    mock_message = {
        "id": outbox_id,
        "status": "approved",
        "channel": "saved",
        "body": "Test saved message",
        "recipient_id": "__saved_messages__"
    }

    with patch("routes.chat_routes.get_pending_outbox", AsyncMock(return_value=[mock_message])), \
         patch("routes.chat_routes.mark_outbox_sent", AsyncMock()) as mock_sent, \
         patch("services.delivery_status.save_platform_message_id", AsyncMock()) as mock_save_id, \
         patch("services.whatsapp_service.WhatsAppService", return_value=AsyncMock()) as mock_whatsapp:
        
        await send_approved_message(outbox_id, license_id)
        
        # Verify it was marked sent
        mock_sent.assert_called_once_with(outbox_id)
        
        # Verify platform ID was saved with 'saved_' prefix
        mock_save_id.assert_called_once_with(outbox_id, f"saved_{outbox_id}")
        
        # Verify NO external service was called (WhatsApp in this case)
        mock_whatsapp.assert_not_called()
