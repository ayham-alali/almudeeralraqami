"""
Al-Mudeer - Presence and Real-time Indicators Routes
API endpoints for broadcasting typing, recording, and presence status
"""

from fastapi import APIRouter, Depends, Body
from typing import Dict, Any
from pydantic import BaseModel

from dependencies import get_license_from_header
from services.websocket_manager import broadcast_typing_indicator, broadcast_recording_indicator

router = APIRouter(prefix="/api/presence", tags=["Chat Features"])


class IndicatorRequest(BaseModel):
    sender_contact: str
    is_active: bool
    channel: str = "whatsapp" # default to whatsapp if missing, or maybe generic


@router.post("/typing")
async def send_typing_indicator(
    data: IndicatorRequest,
    license: dict = Depends(get_license_from_header)
):
    """
    Broadcast that an agent is typing in a conversation.
    This informs other connected clients/UIs.
    """
    license_id = license.get("license_id")
    
    # Broadcast to other internal UIs
    await broadcast_typing_indicator(
        license_id=license_id,
        sender_contact=data.sender_contact,
        is_typing=data.is_active
    )
    
    # Send to external platform
    try:
        if data.channel == "telegram":
            import models
            from services.telegram_phone_service import TelegramPhoneService
            
            # Find the session used for this contact
            session = await models.get_telegram_phone_session(license_id, data.sender_contact)
            if session and session.session_string:
                service = TelegramPhoneService()
                await service.set_typing(
                    session.session_string,
                    data.sender_contact,
                    action="typing" if data.is_active else "cancel"
                )
                
        elif data.channel == "whatsapp":
            from services.whatsapp_service import WhatsAppService, get_whatsapp_config
            
            if data.is_active: # Only send "ON", WhatsApp handles timeout automatically
                config = await get_whatsapp_config(license_id)
                if config and config.get("is_active"):
                    service = WhatsAppService(
                        phone_number_id=config["phone_number_id"],
                        access_token=config["access_token"]
                    )
                    await service.send_typing_indicator(data.sender_contact)

    except Exception as e:
        # Don't fail the request if external indicator fails
        print(f"Failed to send external typing indicator: {e}")

    return {"success": True}


@router.post("/recording")
async def send_recording_indicator(
    data: IndicatorRequest,
    license: dict = Depends(get_license_from_header)
):
    """
    Broadcast that an agent is recording audio in a conversation.
    """
    license_id = license.get("license_id")
    
    # Broadcast to other internal UIs
    await broadcast_recording_indicator(
        license_id=license_id,
        sender_contact=data.sender_contact,
        is_recording=data.is_active
    )
    
    # Send to external platform
    try:
        if data.channel == "telegram":
            import models
            from services.telegram_phone_service import TelegramPhoneService
            
            session = await models.get_telegram_phone_session(license_id, data.sender_contact)
            if session and session.session_string:
                service = TelegramPhoneService()
                await service.set_typing(
                    session.session_string,
                    data.sender_contact,
                    action="recording" if data.is_active else "cancel"
                )
        # WhatsApp doesn't support "recording" distinct from "typing" well in Cloud API stub
        # We can map it to typing if we want, or ignore.
        
    except Exception as e:
        print(f"Failed to send external recording indicator: {e}")

    return {"success": True}
