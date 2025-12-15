"""
Al-Mudeer - WhatsApp Business Integration Routes
"""

from fastapi import APIRouter, HTTPException, Depends, Request, Response
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
    
    # Find config with this verify token
    # For simplicity, we'll accept any valid token format
    # In production, you'd look up the config by token
    
    if mode == "subscribe" and token and challenge:
        return Response(content=challenge, media_type="text/plain")
    
    raise HTTPException(status_code=403, detail="Verification failed")


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
                
                # Find license by phone_number_id
                import aiosqlite
                from models import DATABASE_PATH
                
                async with aiosqlite.connect(DATABASE_PATH) as db:
                    db.row_factory = aiosqlite.Row
                    async with db.execute(
                        "SELECT * FROM whatsapp_configs WHERE phone_number_id = ?",
                        (phone_number_id,)
                    ) as cursor:
                        config_row = await cursor.fetchone()
                
                if not config_row:
                    continue
                
                config = dict(config_row)
                license_id = config["license_key_id"]
                
                service = WhatsAppService(
                    phone_number_id=phone_number_id,
                    access_token=config["access_token"]
                )
                
                messages = service.parse_webhook_message(payload)
                
                for msg in messages:
                    if msg.get("type") == "status":
                        continue  # Skip status updates for now
                    
                    # Save to inbox
                    await save_inbox_message(
                        license_id=license_id,
                        channel="whatsapp",
                        channel_message_id=msg.get("message_id"),
                        sender_id=msg.get("from"),
                        sender_name=msg.get("sender_name"),
                        sender_contact=msg.get("sender_phone"),
                        body=msg.get("body", ""),
                        received_at=msg.get("timestamp")
                    )
                    
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

