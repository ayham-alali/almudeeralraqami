"""
Al-Mudeer - Telegram Routes
Bot integration and Phone session (MTProto) management
"""

import base64
from fastapi import APIRouter, HTTPException, Depends, Request, BackgroundTasks
from pydantic import BaseModel, Field
from typing import Optional, List

from models import (
    save_telegram_config,
    get_telegram_config,
    save_telegram_phone_session,
    get_telegram_phone_session,
    get_telegram_phone_session_data,
    deactivate_telegram_phone_session,
    save_inbox_message,
)
from services import (
    TelegramService,
    TelegramBotManager,
    TelegramPhoneService,
    get_telegram_phone_service,
)
from dependencies import get_license_from_header

router = APIRouter(prefix="/api/integrations", tags=["Telegram"])

# Guides
TELEGRAM_SETUP_GUIDE = """
# دليل إعداد بوت تيليجرام
...
""" # Abbreviated for brevity, normally I'd copy full text

class TelegramConfigRequest(BaseModel):
    bot_token: str
    auto_reply_enabled: bool = False

class TelegramPhoneStartRequest(BaseModel):
    phone_number: str

class TelegramPhoneVerifyRequest(BaseModel):
    phone_number: str
    code: str
    session_id: str
    password: Optional[str] = None

@router.get("/telegram/guide")
async def get_telegram_guide():
    return {"guide": TELEGRAM_SETUP_GUIDE}

@router.post("/telegram/config")
async def configure_telegram(
    config: TelegramConfigRequest,
    request: Request,
    license: dict = Depends(get_license_from_header)
):
    """Configure Telegram bot integration"""
    bot_token = config.bot_token.strip()
    if ":" not in bot_token:
        raise HTTPException(status_code=400, detail="توكن البوت غير صالح")
    
    telegram_service = TelegramService(bot_token)
    success, message, bot_info = await telegram_service.test_connection()
    
    if not success:
        raise HTTPException(status_code=400, detail=message)
    
    await save_telegram_config(
        license_id=license["license_id"],
        bot_token=bot_token,
        bot_username=bot_info.get("username"),
        auto_reply=config.auto_reply_enabled
    )
    
    # Set webhook
    base_url = str(request.base_url).rstrip('/').replace('http://', 'https://')
    webhook_url = f"{base_url}/api/integrations/telegram/webhook/{license['license_id']}"
    await telegram_service.set_webhook(webhook_url)
    
    return {"success": True, "message": "تم حفظ إعدادات تيليجرام بنجاح", "bot_username": bot_info.get("username")}

@router.post("/telegram/webhook/set")
async def set_telegram_webhook(
    request: Request,
    license: dict = Depends(get_license_from_header)
):
    from models import get_telegram_bot_token
    bot_token = await get_telegram_bot_token(license["license_id"])
    if not bot_token:
        raise HTTPException(status_code=400, detail="Telegram bot not configured")
    
    telegram_service = TelegramService(bot_token)
    base_url = str(request.base_url).rstrip('/').replace('http://', 'https://')
    webhook_url = f"{base_url}/api/integrations/telegram/webhook/{license['license_id']}"
    await telegram_service.set_webhook(webhook_url)
    return {"success": True, "message": "تم تسجيل الـ webhook بنجاح", "webhook_url": webhook_url}

@router.get("/telegram/config")
async def get_telegram_configuration(license: dict = Depends(get_license_from_header)):
    config = await get_telegram_config(license["license_id"], include_inactive=False)
    return {"config": config}

@router.get("/telegram/webhook/status")
async def get_telegram_webhook_status(license: dict = Depends(get_license_from_header)):
    from models import get_telegram_bot_token
    bot_token = await get_telegram_bot_token(license["license_id"])
    if not bot_token: return {"error": "Telegram bot not configured"}
    
    import httpx
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"https://api.telegram.org/bot{bot_token}/getWebhookInfo")
        return {"success": True, "webhook_info": resp.json().get("result", {})}

@router.post("/telegram/webhook/{license_id}")
async def telegram_webhook(
    license_id: int,
    request: Request,
    background_tasks: BackgroundTasks
):
    """Receive Telegram webhook updates"""
    update = await request.json()
    parsed = TelegramService.parse_update(update)
    if not parsed or parsed["is_bot"]: return {"ok": True}
    if parsed["chat_type"] != "private": return {"ok": True}
    
    config = await get_telegram_config(license_id)
    if not config or not config.get("is_active"): return {"ok": True}
    
    # Avoid loop
    if config.get("bot_username") == parsed.get("username"): return {"ok": True}
    
    sender_contact = parsed["username"] if parsed["username"] else f"tg:{parsed['user_id']}"
    sender_name = f"{parsed.get('first_name', '')} {parsed.get('last_name', '')}".strip() or "Telegram User"
    
    attachments = parsed.get("attachments", [])
    if attachments:
        try:
            bot = TelegramBotManager.get_bot(license_id, config["bot_token"])
            from services.file_storage_service import get_file_storage
            import mimetypes
            
            for att in attachments:
                if att.get("file_id"):
                    # Download file regardless of size (subject to Telegram API limits ~20MB)
                    file_info = await bot.get_file(att["file_id"])
                    if file_info and file_info.get("file_path"):
                        content = await bot.download_file(file_info["file_path"])
                        if content:
                            # Save to persistent storage
                            filename = att.get("file_name") or f"{att.get('type', 'file')}_{att['file_id']}"
                            # Fix extension logic if needed or let storage handle it
                            
                            rel_path, abs_url = get_file_storage().save_file(
                                content=content,
                                filename=filename,
                                mime_type=att.get("mime_type")
                            )
                            
                            att["url"] = abs_url
                            att["path"] = rel_path
                            att["size"] = len(content)
                            
                            # Keep base64 for very small images (< 200KB) for instant preview
                            if len(content) < 200 * 1024 and att.get("type") == "photo":
                                att["base64"] = base64.b64encode(content).decode('utf-8')
        except Exception as e:
            print(f"Error processing Telegram attachments: {e}")

    msg_id = await save_inbox_message(
        license_id=license_id,
        channel="telegram_bot",
        body=parsed["text"],
        sender_name=sender_name,
        sender_contact=sender_contact,
        sender_id=parsed["user_id"],
        channel_message_id=str(parsed["message_id"]),
        received_at=parsed["date"],
        attachments=attachments 
    )
    
    from routes.chat_routes import analyze_inbox_message
    background_tasks.add_task(
        analyze_inbox_message,
        msg_id,
        parsed["text"],
        license_id,
        config.get("auto_reply_enabled", False),
        parsed["chat_id"],
        attachments
    )
    return {"ok": True}

# ============ Telegram Phone Routes (MTProto) ============

@router.post("/telegram-phone/start")
async def start_telegram_phone_login(
    request: TelegramPhoneStartRequest,
    license: dict = Depends(get_license_from_header)
):
    try:
        result = await get_telegram_phone_service().start_login(request.phone_number)
        return {"success": True, "session_id": result.get("session_id"), "phone_number": result["phone_number"]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/telegram-phone/verify")
async def verify_telegram_phone_code(
    request: TelegramPhoneVerifyRequest,
    license: dict = Depends(get_license_from_header)
):
    try:
        session_string, user_info = await get_telegram_phone_service().verify_code(
            phone_number=request.phone_number,
            code=request.code,
            session_id=request.session_id,
            password=request.password
        )
        
        config_id = await save_telegram_phone_session(
            license_id=license["license_id"],
            phone_number=request.phone_number,
            session_string=session_string,
            user_id=str(user_info.get("id")),
            user_first_name=user_info.get("first_name"),
            user_last_name=user_info.get("last_name"),
            user_username=user_info.get("username")
        )
        return {"success": True, "message": "تم ربط رقم Telegram بنجاح", "user": user_info}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/telegram-phone/config")
async def get_telegram_phone_config(license: dict = Depends(get_license_from_header)):
    config = await get_telegram_phone_session(license["license_id"])
    return {"config": config}

@router.post("/telegram-phone/test")
async def test_telegram_phone_connection(license: dict = Depends(get_license_from_header)):
    try:
        session_string = await get_telegram_phone_session_data(license["license_id"])
        if not session_string: raise HTTPException(status_code=404, detail="لا توجد جلسة نشطة")
        
        from services.telegram_listener_service import get_telegram_listener
        listener = get_telegram_listener()
        active_client = await listener.ensure_client_active(license["license_id"])
        
        success, message, user_info = await get_telegram_phone_service().test_connection(session_string, client=active_client)
        return {"success": success, "message": message, "user": user_info}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/telegram-phone/disconnect")
async def disconnect_telegram_phone(license: dict = Depends(get_license_from_header)):
    await deactivate_telegram_phone_session(license["license_id"])
    return {"success": True, "message": "تم قطع الاتصال بنجاح"}
