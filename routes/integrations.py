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
    get_email_oauth_tokens,
    update_email_config_settings,
    save_telegram_config,
    get_telegram_config,
    save_telegram_phone_session,
    get_telegram_phone_session,
    get_telegram_phone_session_data,
    deactivate_telegram_phone_session,
    update_telegram_phone_session_sync_time,
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
from services import EmailService, EMAIL_PROVIDERS, TelegramService, TELEGRAM_SETUP_GUIDE, GmailOAuthService, GmailAPIService, TelegramPhoneService
from models import get_email_oauth_tokens
from agent import process_message
from workers import get_worker_status
from security import sanitize_email, sanitize_string
from dependencies import get_license_from_header

router = APIRouter(prefix="/api/integrations", tags=["Integrations"])


# ============ Schemas ============

class EmailConfigRequest(BaseModel):
    provider: str = Field(..., description="gmail (OAuth 2.0 only)")
    email_address: str  # Will be set from OAuth token
    auto_reply_enabled: bool = False
    check_interval_minutes: int = 5


class TelegramConfigRequest(BaseModel):
    bot_token: str
    auto_reply_enabled: bool = False


class TelegramPhoneStartRequest(BaseModel):
    phone_number: str


class TelegramPhoneVerifyRequest(BaseModel):
    phone_number: str
    code: str
    session_id: Optional[str] = None


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


# ============ Email Routes (Gmail OAuth 2.0) ============

@router.get("/email/providers")
async def get_email_providers():
    """Get list of supported email providers (Gmail only)"""
    return {"providers": EMAIL_PROVIDERS}


@router.get("/email/oauth/authorize")
async def authorize_gmail(license: dict = Depends(get_license_from_header)):
    """Get OAuth 2.0 authorization URL for Gmail"""
    try:
        oauth_service = GmailOAuthService()
        state = GmailOAuthService.encode_state(license["license_id"])
        auth_url = oauth_service.get_authorization_url(state)
        
        return {
            "authorization_url": auth_url,
            "state": state,
            "message": "يرجى فتح هذا الرابط وتسجيل الدخول بحساب Google الخاص بك"
        }
    except ValueError as e:
        raise HTTPException(status_code=500, detail=f"خطأ في إعداد OAuth: {str(e)}")


@router.get("/email/oauth/callback")
async def gmail_oauth_callback(
    code: str,
    state: str,
    request: Request
):
    """Handle OAuth 2.0 callback from Google"""
    try:
        # Decode state to get license_id
        state_data = GmailOAuthService.decode_state(state)
        license_id = state_data.get("license_id")
        
        if not license_id:
            raise HTTPException(status_code=400, detail="حالة غير صالحة")
        
        # Exchange code for tokens
        oauth_service = GmailOAuthService()
        tokens = await oauth_service.exchange_code_for_tokens(code)
        
        access_token = tokens["access_token"]
        refresh_token = tokens.get("refresh_token")
        expires_in = tokens.get("expires_in", 3600)
        
        # Calculate expiration time
        from datetime import datetime, timedelta
        token_expires_at = datetime.now() + timedelta(seconds=expires_in)
        
        # Get user email from token
        token_info = await oauth_service.get_token_info(access_token)
        email_address = token_info.get("email")
        
        if not email_address:
            raise HTTPException(status_code=400, detail="تعذر الحصول على عنوان البريد الإلكتروني")
        
        # Test Gmail API connection
        gmail_service = GmailAPIService(access_token, refresh_token, oauth_service)
        profile = await gmail_service.get_profile()
        verified_email = profile.get("emailAddress")
        
        if verified_email != email_address:
            email_address = verified_email  # Use verified email from profile
        
        # Save configuration
        config_id = await save_email_config(
            license_id=license_id,
            email_address=email_address,
            access_token=access_token,
            refresh_token=refresh_token,
            token_expires_at=token_expires_at,
            auto_reply=False,  # Default
            check_interval=5  # Default
        )
        
        # Return success page (frontend will handle redirect)
        return {
            "success": True,
            "message": "تم ربط حساب Gmail بنجاح",
            "email": email_address,
            "config_id": config_id
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"فشل ربط Gmail: {str(e)}")


@router.post("/email/config")
async def configure_email(
    config: EmailConfigRequest,
    license: dict = Depends(get_license_from_header)
):
    """Update email configuration settings (after OAuth)"""
    if config.provider != "gmail":
        raise HTTPException(status_code=400, detail="يتم دعم Gmail فقط عبر OAuth 2.0")
    
    # Get existing config
    existing_config = await get_email_config(license["license_id"])
    if not existing_config:
        raise HTTPException(status_code=404, detail="لم يتم ربط حساب Gmail بعد. يرجى استخدام OAuth أولاً.")
    
    # Update only settings (not tokens) using the helper function
    await update_email_config_settings(
        license_id=license["license_id"],
        auto_reply=config.auto_reply_enabled,
        check_interval=config.check_interval_minutes
    )
    
    return {
        "success": True,
        "message": "تم تحديث إعدادات البريد الإلكتروني بنجاح"
    }


@router.get("/email/config")
async def get_email_configuration(license: dict = Depends(get_license_from_header)):
    """Get current email configuration"""
    config = await get_email_config(license["license_id"])
    return {"config": config}


@router.post("/email/test")
async def test_email_connection(license: dict = Depends(get_license_from_header)):
    """Test Gmail connection using OAuth tokens"""
    tokens = await get_email_oauth_tokens(license["license_id"])
    
    if not tokens or not tokens.get("access_token"):
        raise HTTPException(status_code=404, detail="لم يتم ربط حساب Gmail بعد. يرجى استخدام OAuth أولاً.")
    
    try:
        oauth_service = GmailOAuthService()
        gmail_service = GmailAPIService(
            tokens["access_token"],
            tokens.get("refresh_token"),
            oauth_service
        )
        
        # Test by getting profile
        profile = await gmail_service.get_profile()
        email = profile.get("emailAddress")
        
        return {
            "success": True,
            "message": f"الاتصال ناجح مع {email}",
            "email": email
        }
    except Exception as e:
        return {
            "success": False,
            "message": f"فشل الاتصال: {str(e)}"
        }


@router.post("/email/fetch")
async def fetch_emails(
    background_tasks: BackgroundTasks,
    license: dict = Depends(get_license_from_header)
):
    """Manually trigger email fetch using Gmail API"""
    config = await get_email_config(license["license_id"])
    if not config:
        raise HTTPException(status_code=400, detail="لم يتم تكوين البريد الإلكتروني")
    
    tokens = await get_email_oauth_tokens(license["license_id"])
    if not tokens or not tokens.get("access_token"):
        raise HTTPException(status_code=400, detail="لم يتم ربط حساب Gmail بعد. يرجى استخدام OAuth أولاً.")
    
    try:
        oauth_service = GmailOAuthService()
        gmail_service = GmailAPIService(
            tokens["access_token"],
            tokens.get("refresh_token"),
            oauth_service
        )
        
        # Fetch emails using Gmail API
        emails = await gmail_service.fetch_new_emails(
            since_hours=24,
            limit=50
        )
        
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
                subject=email_data.get("subject", ""),
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
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"خطأ في جلب الرسائل: {str(e)}")


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


# ============ Telegram Phone Routes (MTProto) ============

@router.post("/telegram-phone/start")
async def start_telegram_phone_login(
    request: TelegramPhoneStartRequest,
    license: dict = Depends(get_license_from_header)
):
    """Start Telegram phone number login - sends verification code"""
    try:
        phone_service = TelegramPhoneService()
        result = await phone_service.start_login(request.phone_number)
        
        return {
            "success": True,
            "message": result["message"],
            "session_id": result.get("session_id"),
            "phone_number": result["phone_number"]
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"خطأ في طلب الكود: {str(e)}")


@router.post("/telegram-phone/verify")
async def verify_telegram_phone_code(
    request: TelegramPhoneVerifyRequest,
    license: dict = Depends(get_license_from_header)
):
    """Verify Telegram code and complete login"""
    try:
        phone_service = TelegramPhoneService()
        session_string, user_info = await phone_service.verify_code(
            phone_number=request.phone_number,
            code=request.code,
            session_id=request.session_id
        )
        
        # Save session to database
        config_id = await save_telegram_phone_session(
            license_id=license["license_id"],
            phone_number=request.phone_number,
            session_string=session_string,
            user_id=str(user_info.get("id")),
            user_first_name=user_info.get("first_name"),
            user_last_name=user_info.get("last_name"),
            user_username=user_info.get("username")
        )
        
        return {
            "success": True,
            "message": "تم ربط رقم Telegram بنجاح",
            "user": {
                "id": user_info.get("id"),
                "phone": user_info.get("phone"),
                "first_name": user_info.get("first_name"),
                "last_name": user_info.get("last_name"),
                "username": user_info.get("username")
            },
            "config_id": config_id
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"خطأ في التحقق: {str(e)}")


@router.get("/telegram-phone/config")
async def get_telegram_phone_config(license: dict = Depends(get_license_from_header)):
    """Get current Telegram phone session configuration"""
    config = await get_telegram_phone_session(license["license_id"])
    return {"config": config}


@router.post("/telegram-phone/test")
async def test_telegram_phone_connection(license: dict = Depends(get_license_from_header)):
    """Test Telegram phone session connection"""
    try:
        session_string = await get_telegram_phone_session_data(license["license_id"])
        if not session_string:
            raise HTTPException(status_code=404, detail="لا توجد جلسة Telegram نشطة")
        
        phone_service = TelegramPhoneService()
        success, message, user_info = await phone_service.test_connection(session_string)
        
        return {
            "success": success,
            "message": message,
            "user": user_info if success else None
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"خطأ في الاختبار: {str(e)}")


@router.post("/telegram-phone/disconnect")
async def disconnect_telegram_phone(license: dict = Depends(get_license_from_header)):
    """Disconnect Telegram phone session"""
    await deactivate_telegram_phone_session(license["license_id"])
    return {
        "success": True,
        "message": "تم قطع الاتصال بنجاح"
    }


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
            # Send via Gmail API using OAuth
            tokens = await get_email_oauth_tokens(license_id)
            
            if tokens and tokens.get("access_token"):
                oauth_service = GmailOAuthService()
                gmail_service = GmailAPIService(
                    tokens["access_token"],
                    tokens.get("refresh_token"),
                    oauth_service
                )
                
                await gmail_service.send_message(
                    to_email=message["recipient_email"],
                    subject=message.get("subject", "رد على رسالتك"),
                    body=message["body"],
                    reply_to_message_id=message.get("inbox_message_id")  # For threading
                )
                
                await mark_outbox_sent(outbox_id)
        
        elif message["channel"] == "telegram":
            # Send via Telegram - need to query bot_token directly
            from db_helper import get_db, fetch_one
            async with get_db() as db:
                row = await fetch_one(
                    db,
                    "SELECT bot_token FROM telegram_configs WHERE license_key_id = ?",
                    [license_id],
                )
                if row and row.get("bot_token"):
                    telegram_service = TelegramService(row["bot_token"])
                    await telegram_service.send_message(
                        chat_id=message["recipient_id"],
                        text=message["body"]
                    )
                    await mark_outbox_sent(outbox_id)
    
    except Exception as e:
        print(f"Error sending message {outbox_id}: {e}")

