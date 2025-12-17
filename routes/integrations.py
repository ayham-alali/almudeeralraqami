"""
Al-Mudeer - Integration Routes
Email & Telegram configuration and inbox management
"""

import os
import html
from fastapi import APIRouter, HTTPException, Depends, Request, BackgroundTasks
from fastapi.responses import HTMLResponse
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
    get_whatsapp_config,
    get_customer_for_message,
)
from services import EmailService, EMAIL_PROVIDERS, TelegramService, TELEGRAM_SETUP_GUIDE, GmailOAuthService, GmailAPIService, TelegramPhoneService
from models import get_email_oauth_tokens
from agent import process_message
from workers import get_worker_status
from security import sanitize_email, sanitize_string
from dependencies import get_license_from_header

router = APIRouter(prefix="/api/integrations", tags=["Integrations"])

# Shared Telegram phone service instance (per process) so that the same
# Telethon client can handle both send_code_request and sign_in for a phone.
# Using lazy initialization to allow app to start without Telegram credentials
_telegram_phone_service = None

def get_telegram_phone_service():
    """Get or create TelegramPhoneService instance (lazy initialization)"""
    global _telegram_phone_service
    if _telegram_phone_service is None:
        _telegram_phone_service = TelegramPhoneService()
    return _telegram_phone_service


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
    password: Optional[str] = None  # 2FA password


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


class WorkerStatusResponse(BaseModel):
    email_polling: dict
    telegram_polling: dict


class IntegrationAccount(BaseModel):
    id: str
    channel_type: str
    display_name: str
    is_active: bool
    details: Optional[str] = None


class InboxCustomerResponse(BaseModel):
    customer: Optional[dict]


# ============ Email Routes (Gmail OAuth 2.0) ============

@router.get("/email/providers")
async def get_email_providers():
    """Get list of supported email providers (Gmail only)"""
    return {"providers": EMAIL_PROVIDERS}


@router.get("/workers/status", response_model=WorkerStatusResponse)
async def workers_status():
    """
    Lightweight status endpoint used by the dashboard to display 24/7 system activity.
    Does not require a license key; it's purely operational.
    """
    return get_worker_status()


@router.get("/accounts")
async def list_integration_accounts(license: dict = Depends(get_license_from_header)):
    """
    Unified view of all connected channels/accounts for a given workspace.

    This currently aggregates:
    - Email (Gmail OAuth config)
    - Telegram bot
    - Telegram phone session
    - WhatsApp Business
    """
    license_id = license["license_id"]
    accounts: List[IntegrationAccount] = []

    # Email
    email_cfg = await get_email_config(license_id)
    if email_cfg:
        accounts.append(
            IntegrationAccount(
                id="email",
                channel_type="email",
                display_name=email_cfg.get("email_address") or "Gmail",
                is_active=bool(email_cfg.get("is_active")),
                details="Gmail OAuth",
            )
        )

    # Telegram bot
    telegram_cfg = await get_telegram_config(license_id)
    if telegram_cfg:
        display = telegram_cfg.get("bot_username") or "Telegram Bot"
        accounts.append(
            IntegrationAccount(
                id="telegram_bot",
                channel_type="telegram_bot",
                display_name=display,
                is_active=bool(telegram_cfg.get("is_active")),
                details=telegram_cfg.get("bot_token_masked"),
            )
        )

    # Telegram phone
    phone_cfg = await get_telegram_phone_session(license_id)
    if phone_cfg:
        display = phone_cfg.get("phone_number_masked") or phone_cfg.get("phone_number") or "Telegram Phone"
        accounts.append(
            IntegrationAccount(
                id="telegram_phone",
                channel_type="telegram_phone",
                display_name=display,
                is_active=bool(phone_cfg.get("is_active", True)),
                details=phone_cfg.get("user_username"),
            )
        )

    # WhatsApp
    whatsapp_cfg = await get_whatsapp_config(license_id)
    if whatsapp_cfg:
        display = whatsapp_cfg.get("phone_number_id") or "WhatsApp Business"
        accounts.append(
            IntegrationAccount(
                id="whatsapp",
                channel_type="whatsapp",
                display_name=str(display),
                is_active=bool(whatsapp_cfg.get("is_active")),
                details=whatsapp_cfg.get("business_account_id"),
            )
        )

    return {"accounts": accounts}


@router.post("/accounts")
async def create_integration_account(
    request: dict,
    license: dict = Depends(get_license_from_header)
):
    """
    Create/link a new integration account.
    
    Request body should contain:
    - channel_type: "email" | "telegram_bot" | "telegram_phone" | "whatsapp"
    - Additional fields depending on channel_type
    
    For email: redirects to OAuth flow
    For others: requires config data
    """
    license_id = license["license_id"]
    channel_type = request.get("channel_type")
    
    if not channel_type:
        raise HTTPException(status_code=400, detail="channel_type مطلوب")
    
    if channel_type == "email":
        # For email, return OAuth URL
        oauth_service = GmailOAuthService()
        state = GmailOAuthService.encode_state(license_id)
        auth_url = oauth_service.get_authorization_url(state)
        return {
            "success": True,
            "action": "oauth_redirect",
            "authorization_url": auth_url,
            "message": "يرجى فتح هذا الرابط وتسجيل الدخول بحساب Google الخاص بك"
        }
    elif channel_type == "telegram_bot":
        bot_token = request.get("bot_token")
        if not bot_token:
            raise HTTPException(status_code=400, detail="bot_token مطلوب لربط Telegram Bot")
        
        # Save telegram config
        await save_telegram_config(
            license_id=license_id,
            bot_token=bot_token,
            auto_reply_enabled=request.get("auto_reply_enabled", False)
        )
        return {
            "success": True,
            "message": "تم ربط Telegram Bot بنجاح",
            "account_id": "telegram_bot"
        }
    elif channel_type == "whatsapp":
        phone_number_id = request.get("phone_number_id")
        access_token = request.get("access_token")
        if not phone_number_id or not access_token:
            raise HTTPException(status_code=400, detail="phone_number_id و access_token مطلوبان لربط WhatsApp")
        
        # Use WhatsApp service to save config
        from services.whatsapp_service import save_whatsapp_config
        import os
        verify_token = os.urandom(16).hex()
        
        await save_whatsapp_config(
            license_id=license_id,
            phone_number_id=phone_number_id,
            access_token=access_token,
            business_account_id=request.get("business_account_id"),
            verify_token=verify_token,
            auto_reply_enabled=request.get("auto_reply_enabled", False)
        )
        return {
            "success": True,
            "message": "تم ربط WhatsApp Business بنجاح",
            "account_id": "whatsapp",
            "verify_token": verify_token
        }
    elif channel_type == "telegram_phone":
        # Telegram phone requires a multi-step flow, return instructions
        return {
            "success": True,
            "action": "multi_step",
            "message": "استخدم /telegram-phone/start لبدء عملية ربط Telegram Phone",
            "steps": [
                "استدعي POST /api/integrations/telegram-phone/start مع رقم الهاتف",
                "استلم رمز التحقق من Telegram",
                "استدعي POST /api/integrations/telegram-phone/verify مع الرمز"
            ]
        }
    else:
        raise HTTPException(status_code=400, detail=f"نوع القناة غير مدعوم: {channel_type}")


@router.delete("/accounts/{account_id}")
async def delete_integration_account(
    account_id: str,
    license: dict = Depends(get_license_from_header)
):
    """
    Delete/disconnect an integration account.
    
    account_id can be: "email", "telegram_bot", "telegram_phone", "whatsapp"
    """
    license_id = license["license_id"]
    
    if account_id == "email":
        # Deactivate email config
        email_cfg = await get_email_config(license_id)
        if email_cfg:
            await update_email_config_settings(
                license_id=license_id,
                is_active=False
            )
            return {"success": True, "message": "تم إلغاء تفعيل حساب البريد الإلكتروني"}
        else:
            raise HTTPException(status_code=404, detail="لا يوجد حساب بريد إلكتروني مرتبط")
    
    elif account_id == "telegram_bot":
        # Delete telegram config (we'd need a delete function, or just deactivate)
        telegram_cfg = await get_telegram_config(license_id)
        if telegram_cfg:
            # For now, we'll deactivate it (full deletion would require a delete function)
            from db_helper import get_db, execute_sql, commit_db
            async with get_db() as db:
                await execute_sql(
                    db,
                    "UPDATE telegram_configs SET is_active = 0 WHERE license_key_id = ?",
                    [license_id]
                )
                await commit_db(db)
            return {"success": True, "message": "تم إلغاء تفعيل Telegram Bot"}
        else:
            raise HTTPException(status_code=404, detail="لا يوجد Telegram Bot مرتبط")
    
    elif account_id == "telegram_phone":
        # Disconnect telegram phone session
        await deactivate_telegram_phone_session(license_id)
        return {"success": True, "message": "تم قطع الاتصال بـ Telegram Phone"}
    
    elif account_id == "whatsapp":
        # Deactivate WhatsApp config
        whatsapp_cfg = await get_whatsapp_config(license_id)
        if whatsapp_cfg:
            from db_helper import get_db, execute_sql, commit_db
            async with get_db() as db:
                await execute_sql(
                    db,
                    "UPDATE whatsapp_configs SET is_active = 0 WHERE license_key_id = ?",
                    [license_id]
                )
                await commit_db(db)
            return {"success": True, "message": "تم إلغاء تفعيل WhatsApp Business"}
        else:
            raise HTTPException(status_code=404, detail="لا يوجد WhatsApp Business مرتبط")
    
    else:
        raise HTTPException(status_code=400, detail=f"معرف الحساب غير صالح: {account_id}")


@router.get("/inbox/{message_id}/customer", response_model=InboxCustomerResponse)
async def get_inbox_customer(
    message_id: int,
    license: dict = Depends(get_license_from_header),
):
    """
    Get the customer linked to a specific inbox message (if any).
    """
    license_id = license["license_id"]
    customer = await get_customer_for_message(license_id, message_id)
    return {"customer": customer}


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
    # Get frontend URL for postMessage target origin (used in all responses)
    frontend_url = os.getenv("FRONTEND_URL", "https://almudeer.royaraqamia.com")
    frontend_origin = frontend_url.rstrip('/')
    
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
        
        # Return HTML page that sends postMessage to parent window
        html_content = f"""
        <!DOCTYPE html>
        <html dir="rtl" lang="ar">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>تم ربط Gmail بنجاح</title>
            <style>
                body {{
                    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    height: 100vh;
                    margin: 0;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white;
                }}
                .container {{
                    text-align: center;
                    padding: 2rem;
                }}
                .success-icon {{
                    font-size: 4rem;
                    margin-bottom: 1rem;
                }}
                h1 {{
                    margin: 0.5rem 0;
                }}
                p {{
                    opacity: 0.9;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="success-icon">✅</div>
                <h1>تم ربط حساب Gmail بنجاح!</h1>
                <p>{email_address}</p>
                <p>يمكنك إغلاق هذه النافذة الآن.</p>
            </div>
            <script>
                // Send success message to parent window
                if (window.opener) {{
                    window.opener.postMessage({{
                        type: 'GMAIL_OAUTH_SUCCESS',
                        message: 'تم ربط حساب Gmail بنجاح',
                        email: '{email_address}',
                        config_id: {config_id}
                    }}, '{frontend_origin}');
                    
                    // Close popup after a short delay
                    setTimeout(() => {{
                        window.close();
                    }}, 1500);
                }} else {{
                    // If no opener, show message and redirect
                    setTimeout(() => {{
                        window.location.href = '{frontend_origin}/dashboard/integrations';
                    }}, 2000);
                }}
            </script>
        </body>
        </html>
        """
        return HTMLResponse(content=html_content)
        
    except HTTPException as e:
        # Return error HTML page
        error_html = f"""
        <!DOCTYPE html>
        <html dir="rtl" lang="ar">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>خطأ في ربط Gmail</title>
            <style>
                body {{
                    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    height: 100vh;
                    margin: 0;
                    background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
                    color: white;
                }}
                .container {{
                    text-align: center;
                    padding: 2rem;
                }}
                .error-icon {{
                    font-size: 4rem;
                    margin-bottom: 1rem;
                }}
                h1 {{
                    margin: 0.5rem 0;
                }}
                p {{
                    opacity: 0.9;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="error-icon">❌</div>
                <h1>فشل ربط حساب Gmail</h1>
                <p>{html.escape(str(e.detail))}</p>
                <p>يرجى المحاولة مرة أخرى.</p>
            </div>
            <script>
                // Send error message to parent window
                if (window.opener) {{
                    window.opener.postMessage({{
                        type: 'GMAIL_OAUTH_ERROR',
                        message: {repr(str(e.detail))}
                    }}, '{frontend_origin}');
                    
                    // Close popup after a short delay
                    setTimeout(() => {{
                        window.close();
                    }}, 2000);
                }} else {{
                    // If no opener, redirect
                    setTimeout(() => {{
                        window.location.href = '{frontend_origin}/dashboard/integrations';
                    }}, 2000);
                }}
            </script>
        </body>
        </html>
        """
        return HTMLResponse(content=error_html, status_code=e.status_code)
    except Exception as e:
        # Return error HTML page for unexpected errors
        error_html = f"""
        <!DOCTYPE html>
        <html dir="rtl" lang="ar">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>خطأ في ربط Gmail</title>
            <style>
                body {{
                    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    height: 100vh;
                    margin: 0;
                    background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
                    color: white;
                }}
                .container {{
                    text-align: center;
                    padding: 2rem;
                }}
                .error-icon {{
                    font-size: 4rem;
                    margin-bottom: 1rem;
                }}
                h1 {{
                    margin: 0.5rem 0;
                }}
                p {{
                    opacity: 0.9;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="error-icon">❌</div>
                <h1>فشل ربط حساب Gmail</h1>
                <p>فشل ربط Gmail: {html.escape(str(e))}</p>
                <p>يرجى المحاولة مرة أخرى.</p>
            </div>
            <script>
                // Send error message to parent window
                if (window.opener) {{
                    window.opener.postMessage({{
                        type: 'GMAIL_OAUTH_ERROR',
                        message: {repr(f'فشل ربط Gmail: {str(e)}')}
                    }}, '{frontend_origin}');
                    
                    // Close popup after a short delay
                    setTimeout(() => {{
                        window.close();
                    }}, 2000);
                }} else {{
                    // If no opener, redirect
                    setTimeout(() => {{
                        window.location.href = '{frontend_origin}/dashboard/integrations';
                    }}, 2000);
                }}
            </script>
        </body>
        </html>
        """
        return HTMLResponse(content=error_html, status_code=400)


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
        
        # Fetch emails using Gmail API (last 72 hours only)
        emails = await gmail_service.fetch_new_emails(
            since_hours=72,
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
        result = await get_telegram_phone_service().start_login(request.phone_number)
        
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
    """Verify Telegram code and complete login (supports 2FA)"""
    try:
        session_string, user_info = await get_telegram_phone_service().verify_code(
            phone_number=request.phone_number,
            code=request.code,
            session_id=request.session_id,
            password=request.password  # 2FA password if needed
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
        
        success, message, user_info = await get_telegram_phone_service().test_connection(session_string)
        
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
                language=data.get("language"),
                dialect=data.get("dialect"),
                summary=data["summary"],
                draft_response=data["draft_response"],
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

