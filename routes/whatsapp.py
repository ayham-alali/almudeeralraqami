"""
Al-Mudeer - WhatsApp Business Integration Routes
"""

import base64
import os
from fastapi import APIRouter, HTTPException, Depends, Request, Response, BackgroundTasks
from pydantic import BaseModel, Field
from typing import Optional

from services.whatsapp_service import (
    WhatsAppService,
    save_whatsapp_config,
    get_whatsapp_config,
    delete_whatsapp_config,
)
from models import save_inbox_message, create_smart_notification
from security import sanitize_phone, sanitize_string, sanitize_message
from dependencies import get_license_from_header

router = APIRouter(prefix="/api/integrations/whatsapp", tags=["WhatsApp"])


# ============ Schemas ============

class WhatsAppConfigCreate(BaseModel):
    phone_number_id: str = Field(..., description="WhatsApp Business Phone Number ID")
    access_token: str = Field(..., description="Access Token from Meta")
    business_account_id: Optional[str] = Field(None, description="Business Account ID")
    auto_reply_enabled: bool = False


class WhatsAppSendMessage(BaseModel):
    to: str = Field(..., description="Recipient phone number with country code")
    message: str = Field(..., description="Message text")
    reply_to_message_id: Optional[str] = None


# ============ Routes ============

@router.get("/guide")
async def get_whatsapp_guide():
    """Get WhatsApp Business API setup guide"""
    return {
        "guide": """
# دليل إعداد واتساب للأعمال

## المتطلبات:
1. حساب Meta Business (Facebook Business)
2. تطبيق في Meta Developers
3. رقم هاتف للأعمال (غير مرتبط بواتساب عادي)

## الخطوات:

### 1. إنشاء تطبيق Meta
- اذهب إلى: https://developers.facebook.com/apps
- أنشئ تطبيق جديد → Business → WhatsApp

### 2. إعداد WhatsApp Business
- في لوحة التحكم، اذهب إلى WhatsApp → Getting Started
- اضغط "Add phone number" لإضافة رقمك

### 3. الحصول على المعلومات المطلوبة:
- **Phone Number ID**: من WhatsApp → Configuration
- **Access Token**: من App Dashboard → WhatsApp → API Setup
- **Business Account ID**: من WhatsApp → Configuration

### 4. إعداد Webhook (للرسائل الواردة):
- URL: https://almudeer.up.railway.app/api/integrations/whatsapp/webhook
- Verify Token: سيُعطى لك بعد حفظ الإعدادات
- اشترك في: messages, messaging_postbacks

### ملاحظات:
- الرسائل الأولى للعملاء تتطلب استخدام Template Messages
- يجب الموافقة على القوالب من Meta قبل استخدامها
""",
        "requirements": [
            "Meta Business Account",
            "Meta Developer App",
            "WhatsApp Business Phone Number"
        ],
        "webhook_url": "https://almudeer.up.railway.app/api/integrations/whatsapp/webhook"
    }


@router.get("/config")
async def get_config(license: dict = Depends(get_license_from_header)):
    """Get current WhatsApp configuration"""
    config = await get_whatsapp_config(license["license_id"])
    
    if config:
        # Mask sensitive data
        config["access_token"] = config["access_token"][:10] + "..." if config.get("access_token") else None
        
    return {"config": config}


@router.post("/config")
async def save_config(
    config: WhatsAppConfigCreate,
    license: dict = Depends(get_license_from_header)
):
    """Save WhatsApp configuration"""
    import os

    # Basic hygiene on IDs/tokens without altering response shape
    phone_number_id = sanitize_string(config.phone_number_id, max_length=128)
    business_account_id = (
        sanitize_string(config.business_account_id, max_length=128)
        if config.business_account_id
        else None
    )

    verify_token = os.urandom(16).hex()
    
    config_id = await save_whatsapp_config(
        license_id=license["license_id"],
        phone_number_id=phone_number_id,
        access_token=config.access_token,
        business_account_id=business_account_id,
        verify_token=verify_token,
        auto_reply_enabled=config.auto_reply_enabled
    )
    
    return {
        "success": True,
        "message": "تم حفظ إعدادات واتساب بنجاح",
        "config_id": config_id,
        "verify_token": verify_token,
        "webhook_url": "https://almudeer.up.railway.app/api/integrations/whatsapp/webhook"
    }


@router.delete("/config")
async def remove_config(license: dict = Depends(get_license_from_header)):
    """Delete WhatsApp configuration"""
    await delete_whatsapp_config(license["license_id"])
    return {"success": True, "message": "تم حذف إعدادات واتساب"}


@router.post("/test")
async def test_connection(license: dict = Depends(get_license_from_header)):
    """Test WhatsApp API connection"""
    config = await get_whatsapp_config(license["license_id"])
    
    if not config:
        raise HTTPException(status_code=404, detail="لم يتم إعداد واتساب بعد")
    
    service = WhatsAppService(
        phone_number_id=config["phone_number_id"],
        access_token=config["access_token"]
    )
    
    # Test by getting phone number info
    import httpx
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"https://graph.facebook.com/v18.0/{config['phone_number_id']}",
            headers={"Authorization": f"Bearer {config['access_token']}"}
        )
        
        if response.status_code == 200:
            data = response.json()
            return {
                "success": True,
                "message": "الاتصال ناجح!",
                "phone_number": data.get("display_phone_number"),
                "quality_rating": data.get("quality_rating")
            }
        else:
            return {
                "success": False,
                "message": "فشل الاتصال",
                "error": response.text
            }


@router.post("/send")
async def send_message(
    msg: WhatsAppSendMessage,
    license: dict = Depends(get_license_from_header)
):
    """Send a WhatsApp message"""
    # Sanitize phone and message input while preserving response shape
    sanitized_to = sanitize_phone(msg.to)
    if not sanitized_to:
        raise HTTPException(status_code=400, detail="رقم الهاتف غير صالح")

    sanitized_body = sanitize_message(msg.message, max_length=2000)

    config = await get_whatsapp_config(license["license_id"])
    
    if not config:
        raise HTTPException(status_code=404, detail="لم يتم إعداد واتساب بعد")
    
    service = WhatsAppService(
        phone_number_id=config["phone_number_id"],
        access_token=config["access_token"]
    )
    
    result = await service.send_message(
        to=sanitized_to,
        message=sanitized_body,
        reply_to_message_id=msg.reply_to_message_id
    )
    
    if not result["success"]:
        raise HTTPException(status_code=500, detail=result.get("error", "فشل الإرسال"))
    
    return {
        "success": True,
        "message_id": result.get("message_id"),
        "message": "تم إرسال الرسالة بنجاح"
    }


# ============ Webhook Routes ============

@router.get("/webhook")
async def verify_webhook(request: Request):
    """Webhook verification endpoint for Meta"""
    mode = request.query_params.get("hub.mode")
    token = request.query_params.get("hub.verify_token")
    challenge = request.query_params.get("hub.challenge")
    
    if mode != "subscribe" or not token or not challenge:
        raise HTTPException(status_code=403, detail="Verification failed: missing parameters")
    
    # SECURITY: Properly validate the verify_token against stored configurations
    from db_helper import get_db, fetch_one
    
    async with get_db() as db:
        # Look for a config with this verify token
        config = await fetch_one(
            db,
            "SELECT id, license_key_id FROM whatsapp_configs WHERE verify_token = ?",
            [token]
        )
    
    if not config:
        # Token not found - reject the verification
        raise HTTPException(status_code=403, detail="Verification failed: invalid token")
    
    # Token is valid, return the challenge
    return Response(content=challenge, media_type="text/plain")


@router.post("/webhook")
async def receive_webhook(request: Request):
    """Receive incoming WhatsApp messages"""
    try:
        payload = await request.json()
        
        # Parse the webhook payload
        # We need to find the right config based on phone_number_id
        # For now, process all incoming messages
        
        entry = payload.get("entry", [])
        for e in entry:
            changes = e.get("changes", [])
            for change in changes:
                value = change.get("value", {})
                phone_number_id = value.get("metadata", {}).get("phone_number_id")
                
                if not phone_number_id:
                    continue
                
                # Find license by phone_number_id using unified db_helper layer
                from db_helper import get_db, fetch_one

                async with get_db() as db:
                    config = await fetch_one(
                        db,
                        "SELECT * FROM whatsapp_configs WHERE phone_number_id = ?",
                        [phone_number_id],
                    )

                if not config:
                    continue
                
                license_id = config["license_key_id"]
                
                service = WhatsAppService(
                    phone_number_id=phone_number_id,
                    access_token=config["access_token"]
                )
                
                messages = service.parse_webhook_message(payload)
                
                # Import filters
                from message_filters import apply_filters

                for msg in messages:
                    # Apply Filters (Spam, Groups, etc.)
                    # We pass empty list for recent_messages for now (optional optimization)
                    filter_msg = {
                        "body": msg.get("body", ""),
                        "sender_contact": msg.get("sender_phone"),
                        "sender_name": msg.get("sender_name"),
                        "channel": "whatsapp",
                        "is_group": msg.get("is_group"),
                    }
                    
                    should_process, reason = await apply_filters(filter_msg, license_id, [])
                    if not should_process:
                        print(f"WhatsApp message filtered: {reason}")
                        continue

                    # ============ PROCESS DELIVERY STATUS UPDATES ============
                    if msg.get("type") == "status":
                        # WhatsApp sends: sent, delivered, read, failed
                        try:
                            from services.delivery_status import update_delivery_status
                            from datetime import datetime
                            
                            status = msg.get("status")  # sent, delivered, read, failed
                            wa_message_id = msg.get("message_id")
                            timestamp_str = msg.get("timestamp")
                            
                            if status and wa_message_id:
                                # Parse timestamp
                                timestamp = None
                                if timestamp_str:
                                    try:
                                        timestamp = datetime.fromtimestamp(int(timestamp_str))
                                    except:
                                        pass
                                
                                await update_delivery_status(
                                    platform_message_id=wa_message_id,
                                    status=status,
                                    timestamp=timestamp
                                )
                                print(f"WhatsApp delivery status update: {wa_message_id} -> {status}")

                        except Exception as status_error:
                            print(f"Failed to process WhatsApp status: {status_error}")
                        
                        continue  # Status processed, skip to next message
                    
                    # Media Handling
                    attachments = []
                    if msg.get("media_id"):
                        try:
                            content = await service.download_media(msg["media_id"])
                            if content:
                                attachments.append({
                                    "type": msg.get("type", "image"),
                                    "base64": base64.b64encode(content).decode('utf-8'),
                                    "file_id": msg["media_id"]
                                })
                        except Exception as e:
                            print(f"Error downloading WhatsApp media: {e}")

                    # Save to inbox
                    inbox_id = await save_inbox_message(
                        license_id=license_id,
                        channel="whatsapp",
                        channel_message_id=msg.get("message_id"),
                        sender_id=msg.get("from"),
                        sender_name=msg.get("sender_name"),
                        sender_contact=msg.get("sender_phone"),
                        body=msg.get("body", ""),
                        received_at=msg.get("timestamp"),
                        attachments=attachments
                    )


                    # Analyze with AI (WhatsApp auto-analysis)
                    try:
                        from routes.chat_routes import analyze_inbox_message  # local import to avoid cycles
                        import asyncio
                        
                        # Determine auto_reply from config if available
                        # config is already available above as 'config' variable
                        auto_reply_enabled = bool(config and config.get("auto_reply_enabled"))

                        # Use asyncio.create_task for proper background execution
                        asyncio.create_task(
                            analyze_inbox_message(
                                inbox_id,
                                msg.get("body", ""),
                                license_id,
                                auto_reply_enabled,
                                None,  # telegram_chat_id
                                attachments  # Pass attachments
                            )
                        )
                    except Exception as e:
                        print(f"WhatsApp auto-analysis scheduling failed: {e}")
                    
                    # Create notification
                    await create_smart_notification(
                        license_id=license_id,
                        event_type="new_message",
                        data={"sender": msg.get("sender_name", msg.get("from"))}
                    )
        
        return {"status": "ok"}
        
    except Exception as e:
        print(f"WhatsApp webhook error: {e}")
        return {"status": "error", "message": str(e)}

