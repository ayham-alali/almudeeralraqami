"""
Al-Mudeer - Integration Routes
Email & Telegram configuration and inbox management
"""

from fastapi import APIRouter, HTTPException, Depends, Request, BackgroundTasks
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime

from models import (
    save_email_config,
    get_email_config,
    get_email_password,
    save_telegram_config,
    get_telegram_config,
    save_inbox_message,
    update_inbox_analysis,
    get_inbox_messages,
    update_inbox_status,
    create_outbox_message,
    approve_outbox_message,
    mark_outbox_sent,
    get_pending_outbox,
    init_enhanced_tables,
)
from services import EmailService, EMAIL_PROVIDERS, TelegramService, TELEGRAM_SETUP_GUIDE
from agent import process_message
from workers import get_worker_status
from security import sanitize_email, sanitize_string
from dependencies import get_license_from_header

router = APIRouter(prefix="/api/integrations", tags=["Integrations"])


# ============ Schemas ============

class EmailConfigRequest(BaseModel):
    provider: str = Field(..., description="gmail, outlook, yahoo, custom")
    email_address: str
    password: str
    imap_server: Optional[str] = None
    smtp_server: Optional[str] = None
    imap_port: Optional[int] = 993
    smtp_port: Optional[int] = 587
    auto_reply_enabled: bool = False
    check_interval_minutes: int = 5


class TelegramConfigRequest(BaseModel):
    bot_token: str
    auto_reply_enabled: bool = False


class ApprovalRequest(BaseModel):
    action: str = Field(..., description="approve, reject, edit")
    edited_body: Optional[str] = None


class InboxMessageResponse(BaseModel):
    id: int
    channel: str
    sender_name: Optional[str]
    sender_contact: Optional[str]
    subject: Optional[str]
    body: str
    received_at: Optional[str]
    intent: Optional[str]
    urgency: Optional[str]
    sentiment: Optional[str]
    ai_summary: Optional[str]
    ai_draft_response: Optional[str]
    status: str
    created_at: str


# ============ Email Routes ============

@router.get("/email/providers")
async def get_email_providers():
    """Get list of supported email providers"""
    return {"providers": EMAIL_PROVIDERS}


@router.post("/email/config")
async def configure_email(
    config: EmailConfigRequest,
    license: dict = Depends(get_license_from_header)
):
    """Configure email integration"""
    # Sanitize email input early (keeps response shape identical)
    sanitized_email = sanitize_email(config.email_address)
    if not sanitized_email:
        raise HTTPException(status_code=400, detail="البريد الإلكتروني غير صالح")

    # Get server settings from provider or use custom
    if config.provider not in EMAIL_PROVIDERS:
        raise HTTPException(status_code=400, detail="مزود البريد غير مدعوم")
    provider = EMAIL_PROVIDERS[config.provider]
    
    imap_server = config.imap_server or provider["imap_server"]
    smtp_server = config.smtp_server or provider["smtp_server"]
    imap_port = config.imap_port or provider["imap_port"]
    smtp_port = config.smtp_port or provider["smtp_port"]
    
    if not imap_server or not smtp_server:
        raise HTTPException(status_code=400, detail="يجب تحديد خادم IMAP و SMTP")
    
    # Test connection first
    email_service = EmailService(
        email_address=sanitized_email,
        password=config.password,
        imap_server=imap_server,
        smtp_server=smtp_server,
        imap_port=imap_port,
        smtp_port=smtp_port
    )
    
    success, message = await email_service.test_connection()
    if not success:
        raise HTTPException(status_code=400, detail=f"فشل الاتصال: {message}")
    
    # Save configuration
    config_id = await save_email_config(
        license_id=license["license_id"],
        email_address=sanitized_email,
        imap_server=imap_server,
        smtp_server=smtp_server,
        password=config.password,
        imap_port=imap_port,
        smtp_port=smtp_port,
        auto_reply=config.auto_reply_enabled,
        check_interval=config.check_interval_minutes
    )
    
    return {
        "success": True,
        "message": "تم حفظ إعدادات البريد الإلكتروني بنجاح",
        "config_id": config_id
    }


@router.get("/email/config")
async def get_email_configuration(license: dict = Depends(get_license_from_header)):
    """Get current email configuration"""
    config = await get_email_config(license["license_id"])
    return {"config": config}


@router.post("/email/test")
async def test_email_connection(
    config: EmailConfigRequest,
    license: dict = Depends(get_license_from_header)
):
    """Test email connection without saving"""
    provider = EMAIL_PROVIDERS.get(config.provider, EMAIL_PROVIDERS["custom"])

    sanitized_email = sanitize_email(config.email_address)
    if not sanitized_email:
        raise HTTPException(status_code=400, detail="البريد الإلكتروني غير صالح")
    
    email_service = EmailService(
        email_address=sanitized_email,
        password=config.password,
        imap_server=config.imap_server or provider["imap_server"],
        smtp_server=config.smtp_server or provider["smtp_server"],
        imap_port=config.imap_port or provider["imap_port"],
        smtp_port=config.smtp_port or provider["smtp_port"]
    )
    
    success, message = await email_service.test_connection()
    return {"success": success, "message": message}


@router.post("/email/fetch")
async def fetch_emails(
    background_tasks: BackgroundTasks,
    license: dict = Depends(get_license_from_header)
):
    """Manually trigger email fetch"""
    config = await get_email_config(license["license_id"])
    if not config:
        raise HTTPException(status_code=400, detail="لم يتم تكوين البريد الإلكتروني")
    
    password = await get_email_password(license["license_id"])
    
    email_service = EmailService(
        email_address=config["email_address"],
        password=password,
        imap_server=config["imap_server"],
        smtp_server=config["smtp_server"],
        imap_port=config["imap_port"],
        smtp_port=config["smtp_port"]
    )
    
    # Fetch emails
    emails = await email_service.fetch_new_emails(since_hours=24)
    
    # Process each email
    processed = 0
    for email_data in emails:
        # Save to inbox
        msg_id = await save_inbox_message(
            license_id=license["license_id"],
            channel="email",
            body=email_data["body"],
            sender_name=email_data["sender_name"],
            sender_contact=email_data["sender_contact"],
            subject=email_data["subject"],
            channel_message_id=email_data["channel_message_id"],
            received_at=email_data["received_at"]
        )
        
        # Analyze with AI in background
        background_tasks.add_task(
            analyze_inbox_message,
            msg_id,
            email_data["body"],
            license["license_id"],
            config.get("auto_reply_enabled", False)
        )
        
        processed += 1
    
    return {
        "success": True,
        "message": f"تم جلب {processed} رسالة جديدة",
        "count": processed
    }


# ============ Telegram Routes ============

@router.get("/telegram/guide")
async def get_telegram_guide():
    """Get Telegram bot setup guide"""
    return {"guide": TELEGRAM_SETUP_GUIDE}


@router.post("/telegram/config")
async def configure_telegram(
    config: TelegramConfigRequest,
    request: Request,
    license: dict = Depends(get_license_from_header)
):
    """Configure Telegram bot integration"""
    # Test bot token
    telegram_service = TelegramService(config.bot_token.strip())
    success, message, bot_info = await telegram_service.test_connection()
    
    if not success:
        raise HTTPException(status_code=400, detail=message)
    
    # Save configuration
    config_id = await save_telegram_config(
        license_id=license["license_id"],
        bot_token=config.bot_token,
        bot_username=bot_info.get("username"),
        auto_reply=config.auto_reply_enabled
    )
    
    # Set webhook
    base_url = str(request.base_url).rstrip('/')
    webhook_url = f"{base_url}/api/integrations/telegram/webhook/{license['license_id']}"
    
    try:
        await telegram_service.set_webhook(webhook_url)
    except Exception as e:
        print(f"Webhook setup error: {e}")
        # Continue anyway - webhook can be set up later
    
    return {
        "success": True,
        "message": "تم حفظ إعدادات تيليجرام بنجاح",
        "bot_username": bot_info.get("username"),
        "webhook_url": webhook_url
    }


@router.get("/telegram/config")
async def get_telegram_configuration(license: dict = Depends(get_license_from_header)):
    """Get current Telegram configuration"""
    config = await get_telegram_config(license["license_id"])
    return {"config": config}


@router.post("/telegram/webhook/{license_id}")
async def telegram_webhook(
    license_id: int,
    request: Request,
    background_tasks: BackgroundTasks
):
    """Receive Telegram webhook updates"""
    try:
        update = await request.json()
    except:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    
    # Parse the update
    parsed = TelegramService.parse_update(update)
    if not parsed or parsed["is_bot"]:
        return {"ok": True}  # Ignore bot messages
    
    # Only process private messages for now
    if parsed["chat_type"] != "private":
        return {"ok": True}
    
    # Get config to check if auto-reply is enabled
    config = await get_telegram_config(license_id)
    if not config or not config.get("is_active"):
        return {"ok": True}
    
    # Save to inbox
    msg_id = await save_inbox_message(
        license_id=license_id,
        channel="telegram",
        body=parsed["text"],
        sender_name=f"{parsed['first_name']} {parsed['last_name']}".strip(),
        sender_contact=parsed["username"],
        sender_id=parsed["user_id"],
        channel_message_id=str(parsed["message_id"]),
        received_at=parsed["date"]
    )
    
    # Analyze in background
    background_tasks.add_task(
        analyze_inbox_message,
        msg_id,
        parsed["text"],
        license_id,
        config.get("auto_reply_enabled", False),
        parsed["chat_id"]
    )
    
    return {"ok": True}


# ============ Inbox Routes ============

@router.get("/inbox")
async def get_inbox(
    status: Optional[str] = None,
    channel: Optional[str] = None,
    limit: int = 50,
    license: dict = Depends(get_license_from_header)
):
    """Get inbox messages"""
    messages = await get_inbox_messages(
        license_id=license["license_id"],
        status=status,
        channel=channel,
        limit=limit
    )
    return {"messages": messages, "total": len(messages)}


@router.get("/inbox/{message_id}")
async def get_inbox_message(
    message_id: int,
    license: dict = Depends(get_license_from_header)
):
    """Get single inbox message"""
    messages = await get_inbox_messages(license["license_id"])
    message = next((m for m in messages if m["id"] == message_id), None)
    if not message:
        raise HTTPException(status_code=404, detail="الرسالة غير موجودة")
    return {"message": message}


@router.post("/inbox/{message_id}/analyze")
async def analyze_message(
    message_id: int,
    background_tasks: BackgroundTasks,
    license: dict = Depends(get_license_from_header)
):
    """Re-analyze a message with AI"""
    messages = await get_inbox_messages(license["license_id"])
    message = next((m for m in messages if m["id"] == message_id), None)
    if not message:
        raise HTTPException(status_code=404, detail="الرسالة غير موجودة")
    
    background_tasks.add_task(
        analyze_inbox_message,
        message_id,
        message["body"],
        license["license_id"],
        False
    )
    
    return {"success": True, "message": "جاري تحليل الرسالة"}


@router.post("/inbox/{message_id}/approve")
async def approve_message(
    message_id: int,
    approval: ApprovalRequest,
    background_tasks: BackgroundTasks,
    license: dict = Depends(get_license_from_header)
):
    """Approve, reject, or edit a draft response"""
    messages = await get_inbox_messages(license["license_id"])
    message = next((m for m in messages if m["id"] == message_id), None)
    if not message:
        raise HTTPException(status_code=404, detail="الرسالة غير موجودة")
    
    if approval.action == "reject":
        await update_inbox_status(message_id, "rejected")
        return {"success": True, "message": "تم رفض الرد"}
    
    elif approval.action in ["approve", "edit"]:
        response_body = approval.edited_body or message.get("ai_draft_response", "")
        
        if not response_body:
            raise HTTPException(status_code=400, detail="لا يوجد رد للإرسال")
        
        # Create outbox entry
        outbox_id = await create_outbox_message(
            inbox_message_id=message_id,
            license_id=license["license_id"],
            channel=message["channel"],
            body=response_body,
            recipient_id=message.get("sender_id"),
            recipient_email=message.get("sender_contact"),
            subject=f"Re: {message.get('subject', '')}" if message.get("subject") else None
        )
        
        # Approve it
        await approve_outbox_message(outbox_id, response_body)
        await update_inbox_status(message_id, "approved")
        
        # Send in background
        background_tasks.add_task(
            send_approved_message,
            outbox_id,
            license["license_id"]
        )
        
        return {"success": True, "message": "تم إرسال الرد"}
    
    raise HTTPException(status_code=400, detail="إجراء غير صالح")


@router.get("/outbox")
async def get_outbox(license: dict = Depends(get_license_from_header)):
    """Get outbox messages"""
    messages = await get_pending_outbox(license["license_id"])
    return {"messages": messages}


# ============ Workers Status ============

@router.get("/workers/status")
async def workers_status(
    license: dict = Depends(get_license_from_header),
):
    """
    Lightweight status endpoint for background workers.

    Returns a structure compatible with frontend WorkerStatus:
    - email_polling: { last_check, status, next_check }
    - telegram_polling: { last_check, status }
    """
    return {"workers": get_worker_status()}


# ============ Background Tasks ============

async def analyze_inbox_message(
    message_id: int,
    body: str,
    license_id: int,
    auto_reply: bool = False,
    telegram_chat_id: str = None
):
    """Analyze message with AI and optionally auto-reply"""
    try:
        # Process with AI
        result = await process_message(body)
        
        if result["success"]:
            data = result["data"]
            await update_inbox_analysis(
                message_id=message_id,
                intent=data["intent"],
                urgency=data["urgency"],
                sentiment=data["sentiment"],
                summary=data["summary"],
                draft_response=data["draft_response"]
            )
            
            # Auto-reply if enabled
            if auto_reply and data["draft_response"]:
                # Get message details for sending
                messages = await get_inbox_messages(license_id)
                message = next((m for m in messages if m["id"] == message_id), None)
                
                if message:
                    outbox_id = await create_outbox_message(
                        inbox_message_id=message_id,
                        license_id=license_id,
                        channel=message["channel"],
                        body=data["draft_response"],
                        recipient_id=message.get("sender_id"),
                        recipient_email=message.get("sender_contact")
                    )
                    
                    await approve_outbox_message(outbox_id)
                    await update_inbox_status(message_id, "auto_replied")
                    await send_approved_message(outbox_id, license_id)
        
    except Exception as e:
        print(f"Error analyzing message {message_id}: {e}")


async def send_approved_message(outbox_id: int, license_id: int):
    """Send an approved message"""
    try:
        outbox = await get_pending_outbox(license_id)
        message = next((m for m in outbox if m["id"] == outbox_id), None)
        
        if not message or message["status"] != "approved":
            return
        
        if message["channel"] == "email":
            # Send via email
            config = await get_email_config(license_id)
            password = await get_email_password(license_id)
            
            if config and password:
                email_service = EmailService(
                    email_address=config["email_address"],
                    password=password,
                    imap_server=config["imap_server"],
                    smtp_server=config["smtp_server"],
                    imap_port=config["imap_port"],
                    smtp_port=config["smtp_port"]
                )
                
                await email_service.send_email(
                    to_email=message["recipient_email"],
                    subject=message.get("subject", "رد على رسالتك"),
                    body=message["body"]
                )
                
                await mark_outbox_sent(outbox_id)
        
        elif message["channel"] == "telegram":
            # Send via Telegram
            from aiosqlite import connect
            async with connect("almudeer.db") as db:
                async with db.execute(
                    "SELECT bot_token FROM telegram_configs WHERE license_key_id = ?",
                    (license_id,)
                ) as cursor:
                    row = await cursor.fetchone()
                    if row:
                        telegram_service = TelegramService(row[0])
                        await telegram_service.send_message(
                            chat_id=message["recipient_id"],
                            text=message["body"]
                        )
                        await mark_outbox_sent(outbox_id)
    
    except Exception as e:
        print(f"Error sending message {outbox_id}: {e}")

