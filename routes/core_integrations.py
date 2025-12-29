"""
Al-Mudeer - Integration Routes
Email & Telegram configuration and inbox management
"""

import os
import html
import base64
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
    get_inbox_messages_count,
    get_inbox_conversations,
    get_inbox_conversations_count,
    get_inbox_status_counts,
    get_conversation_messages,
    update_inbox_status,
    create_outbox_message,
    approve_outbox_message,
    mark_outbox_sent,
    get_pending_outbox,
    init_enhanced_tables,
    get_whatsapp_config,
    get_customer_for_message,
)
from services import EmailService, EMAIL_PROVIDERS, TelegramService, TelegramBotManager, TELEGRAM_SETUP_GUIDE, GmailOAuthService, GmailAPIService, TelegramPhoneService
from models import get_email_oauth_tokens
from agent import process_message
from workers import get_worker_status
from security import sanitize_email, sanitize_string
from dependencies import get_license_from_header

router = APIRouter(prefix="/api/integrations", tags=["Integrations"])

@router.get("/debug")
def debug_integrations():
    return {"status": "ok", "message": "Integrations router is loaded"}


@router.get("/llm-health")
async def check_llm_health(license: dict = Depends(get_license_from_header)):
    """
    Diagnostic endpoint to check LLM API key health.
    Tests each provider with a minimal request to verify:
    - API key is valid
    - Account has quota/credits
    - Model access is available
    """
    import httpx
    import os
    
    results = {
        "openai": {"status": "unknown", "error": None, "model": None},
        "gemini": {"status": "unknown", "error": None, "model": None},
    }
    
    # Test OpenAI
    openai_key = os.getenv("OPENAI_API_KEY", "")
    openai_model = os.getenv("OPENAI_MODEL", "gpt-4o")
    if openai_key:
        results["openai"]["model"] = openai_model
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    f"{os.getenv('OPENAI_BASE_URL', 'https://api.openai.com/v1')}/chat/completions",
                    headers={
                        "Authorization": f"Bearer {openai_key}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": openai_model,
                        "messages": [{"role": "user", "content": "Hi"}],
                        "max_tokens": 5,
                    },
                )
                if response.status_code == 200:
                    results["openai"]["status"] = "healthy"
                elif response.status_code == 429:
                    results["openai"]["status"] = "rate_limited"
                    results["openai"]["error"] = "Quota exceeded or rate limited - check billing"
                elif response.status_code == 401:
                    results["openai"]["status"] = "invalid_key"
                    results["openai"]["error"] = "Invalid API key"
                elif response.status_code == 404:
                    results["openai"]["status"] = "model_not_found"
                    results["openai"]["error"] = f"Model {openai_model} not available on this account"
                else:
                    results["openai"]["status"] = "error"
                    results["openai"]["error"] = f"HTTP {response.status_code}: {response.text[:200]}"
        except Exception as e:
            results["openai"]["status"] = "error"
            results["openai"]["error"] = str(e)
    else:
        results["openai"]["status"] = "not_configured"
        results["openai"]["error"] = "OPENAI_API_KEY not set"
    
    # Test Gemini
    google_key = os.getenv("GOOGLE_API_KEY", "")
    google_model = os.getenv("GOOGLE_MODEL", "gemini-2.0-flash")
    if google_key:
        results["gemini"]["model"] = google_model
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    f"https://generativelanguage.googleapis.com/v1beta/models/{google_model}:generateContent?key={google_key}",
                    headers={"Content-Type": "application/json"},
                    json={
                        "contents": [{"parts": [{"text": "Hi"}]}],
                        "generationConfig": {"maxOutputTokens": 5}
                    },
                )
                if response.status_code == 200:
                    results["gemini"]["status"] = "healthy"
                elif response.status_code == 429:
                    results["gemini"]["status"] = "rate_limited"
                    results["gemini"]["error"] = "Quota exceeded - check Google Cloud quotas"
                elif response.status_code == 400:
                    error_detail = response.json().get("error", {}).get("message", "")
                    results["gemini"]["status"] = "error"
                    results["gemini"]["error"] = error_detail[:200]
                elif response.status_code == 403:
                    results["gemini"]["status"] = "permission_denied"
                    results["gemini"]["error"] = "API key doesn't have access to this model"
                else:
                    results["gemini"]["status"] = "error"
                    results["gemini"]["error"] = f"HTTP {response.status_code}: {response.text[:200]}"
        except Exception as e:
            results["gemini"]["status"] = "error"
            results["gemini"]["error"] = str(e)
    else:
        results["gemini"]["status"] = "not_configured"
        results["gemini"]["error"] = "GOOGLE_API_KEY not set"
    
    # Overall health
    healthy_count = sum(1 for r in results.values() if r["status"] == "healthy")
    overall = "healthy" if healthy_count > 0 else "unhealthy"
    
    return {
        "overall": overall,
        "providers": results,
        "recommendation": (
            "All providers are working" if healthy_count == 2 else
            "Check billing/quota for rate-limited providers" if any(r["status"] == "rate_limited" for r in results.values()) else
            "Configure at least one LLM provider"
        )
    }

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
    email_cfg = await get_email_config(license_id, include_inactive=False)
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
    telegram_cfg = await get_telegram_config(license_id, include_inactive=False)
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
            from db_helper import get_db, execute_sql, commit_db
            async with get_db() as db:
                await execute_sql(
                    db,
                    "DELETE FROM email_configs WHERE license_key_id = ?",
                    [license_id]
                )
                await commit_db(db)
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
                    "DELETE FROM telegram_configs WHERE license_key_id = ?",
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
                    "DELETE FROM whatsapp_configs WHERE license_key_id = ?",
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
    config = await get_email_config(license["license_id"], include_inactive=False)
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
        
        # Calculate since_hours based on when the channel was connected
        # This ensures we ONLY fetch messages received after the channel was connected
        from datetime import datetime, timedelta
        config_created_at = config.get("created_at")
        
        if config_created_at:
            if isinstance(config_created_at, str):
                try:
                    created_dt = datetime.fromisoformat(config_created_at.replace("Z", "+00:00"))
                    if created_dt.tzinfo:
                        created_dt = created_dt.replace(tzinfo=None)
                except ValueError:
                    created_dt = None
            elif hasattr(config_created_at, "isoformat"):
                created_dt = config_created_at
                if hasattr(created_dt, 'tzinfo') and created_dt.tzinfo:
                    created_dt = created_dt.replace(tzinfo=None)
            else:
                created_dt = None
            
            if created_dt:
                hours_since_connected = (datetime.utcnow() - created_dt).total_seconds() / 3600
                # Add 1 hour buffer to catch any edge cases
                since_hours = int(hours_since_connected) + 1
            else:
                # Fallback: if no created_at, only fetch last 1 hour
                since_hours = 1
        else:
            # No created_at means new config, only fetch last 1 hour
            since_hours = 1
        
        # Fetch emails using Gmail API
        emails = await gmail_service.fetch_new_emails(
            since_hours=since_hours,
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
    from logging_config import get_logger
    logger = get_logger("telegram_config")
    
    # Validate token format (should be like 123456789:ABCdefGHI...)
    bot_token = config.bot_token.strip()
    if ":" not in bot_token:
        raise HTTPException(
            status_code=400, 
            detail="توكن البوت غير صالح. يجب أن يكون بالصيغة: 123456789:ABCdefGHI..."
        )
    
    parts = bot_token.split(":")
    if len(parts) != 2 or not parts[0].isdigit():
        raise HTTPException(
            status_code=400, 
            detail="توكن البوت غير صالح. تأكد من نسخ التوكن كاملاً من BotFather"
        )
    
    logger.info(f"Configuring Telegram bot for license {license['license_id']}, token prefix: {parts[0]}")
    
    # Test bot token
    telegram_service = TelegramService(bot_token)
    success, message, bot_info = await telegram_service.test_connection()
    
    if not success:
        logger.error(f"Telegram bot test failed: {message}")
        raise HTTPException(status_code=400, detail=message)
    
    # Save configuration
    config_id = await save_telegram_config(
        license_id=license["license_id"],
        bot_token=bot_token,  # Use the stripped token
        bot_username=bot_info.get("username"),
        auto_reply=config.auto_reply_enabled
    )
    
    # Set webhook - force HTTPS since Railway terminates SSL at load balancer
    base_url = str(request.base_url).rstrip('/').replace('http://', 'https://')
    webhook_url = f"{base_url}/api/integrations/telegram/webhook/{license['license_id']}"
    
    webhook_success = False
    webhook_error = None
    try:
        await telegram_service.set_webhook(webhook_url)
        webhook_success = True
        logger.info(f"Webhook set successfully: {webhook_url}")
    except Exception as e:
        webhook_error = str(e)
        logger.error(f"Webhook setup error: {e}")
    
    return {
        "success": True,
        "message": "تم حفظ إعدادات تيليجرام بنجاح",
        "bot_username": bot_info.get("username"),
        "webhook_url": webhook_url,
        "webhook_registered": webhook_success,
        "webhook_error": webhook_error
    }


@router.post("/telegram/set-webhook")
async def set_telegram_webhook(
    request: Request,
    license: dict = Depends(get_license_from_header)
):
    """Manually set the Telegram webhook (useful if initial setup failed)"""
    from models import get_telegram_bot_token
    from logging_config import get_logger
    logger = get_logger("telegram_webhook")
    
    bot_token = await get_telegram_bot_token(license["license_id"])
    if not bot_token:
        raise HTTPException(status_code=400, detail="Telegram bot not configured")
    
    telegram_service = TelegramService(bot_token)
    
    # Force HTTPS since Railway terminates SSL at load balancer
    base_url = str(request.base_url).rstrip('/').replace('http://', 'https://')
    webhook_url = f"{base_url}/api/integrations/telegram/webhook/{license['license_id']}"
    
    try:
        await telegram_service.set_webhook(webhook_url)
        logger.info(f"Webhook manually set: {webhook_url}")
        return {
            "success": True,
            "message": "تم تسجيل الـ webhook بنجاح",
            "webhook_url": webhook_url
        }
    except Exception as e:
        logger.error(f"Manual webhook setup failed: {e}")
        raise HTTPException(status_code=500, detail=f"فشل تسجيل الـ webhook: {str(e)}")


@router.get("/telegram/config")
async def get_telegram_configuration(license: dict = Depends(get_license_from_header)):
    """Get current Telegram configuration"""
    config = await get_telegram_config(license["license_id"], include_inactive=False)
    return {"config": config}


@router.get("/telegram/webhook-status")
async def get_telegram_webhook_status(license: dict = Depends(get_license_from_header)):
    """Debug endpoint: Check Telegram webhook status from Telegram's API"""
    from models import get_telegram_bot_token
    
    bot_token = await get_telegram_bot_token(license["license_id"])
    if not bot_token:
        return {"error": "Telegram bot not configured or inactive"}
    
    import httpx
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"https://api.telegram.org/bot{bot_token}/getWebhookInfo"
            )
            data = resp.json()
            return {
                "success": True,
                "webhook_info": data.get("result", {}),
                "configured_correctly": bool(data.get("result", {}).get("url"))
            }
    except Exception as e:
        return {"error": str(e)}


@router.post("/telegram/webhook/{license_id}")
async def telegram_webhook(
    license_id: int,
    request: Request,
    background_tasks: BackgroundTasks
):
    """Receive Telegram webhook updates"""
    from logging_config import get_logger
    logger = get_logger("telegram_webhook")
    
    try:
        update = await request.json()
        logger.info(f"Telegram webhook received for license {license_id}: {update.get('update_id', 'unknown')}")
    except:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    
    # Parse the update
    parsed = TelegramService.parse_update(update)
    if not parsed:
        logger.warning(f"Could not parse Telegram update: {update}")
        return {"ok": True}
    
    if parsed["is_bot"]:
        logger.debug("Ignoring bot message (sender is a bot)")
        return {"ok": True}  # Ignore bot messages
    
    # Only process private messages for now
    if parsed["chat_type"] != "private":
        logger.debug(f"Ignoring non-private message, chat_type: {parsed['chat_type']}")
        return {"ok": True}
    
    # Get config to check if auto-reply is enabled
    config = await get_telegram_config(license_id)
    if not config or not config.get("is_active"):
        logger.warning(f"Telegram config not active for license {license_id}")
        return {"ok": True}
    
    # CRITICAL: Check if this message was sent BY our bot (to prevent AI loop)
    # When the bot sends a message, Telegram sends us a webhook update for it too
    bot_username = config.get("bot_username")
    if bot_username and parsed.get("username") == bot_username:
        logger.debug(f"Skipping self-message from bot @{bot_username}")
        return {"ok": True}
    
    # Save to inbox
    logger.info(f"Saving Telegram message to inbox: {parsed['text'][:50]}...")
    
    # Use username if available, otherwise use telegram user_id as contact identifier
    # This ensures customers are always created even if user has no username
    sender_contact = parsed["username"] if parsed["username"] else f"tg:{parsed['user_id']}"
    sender_name = f"{parsed['first_name']} {parsed['last_name']}".strip() or f"Telegram User"
    
    # Prepare attachments if any
    attachments = parsed.get("attachments", [])
    
    # Download media if present
    if attachments:
        try:
            bot = TelegramBotManager.get_bot(license_id, config["bot_token"])
            for att in attachments:
                if att.get("file_id"):
                    # Get file path
                    file_info = await bot.get_file(att["file_id"])
                    if file_info and file_info.get("file_path"):
                        # Download content
                        content = await bot.download_file(file_info["file_path"])
                        if content:
                            if len(content) > 5 * 1024 * 1024: # 5MB limit
                                logger.warning(f"File too large: {len(content)} bytes")
                                continue
                            
                            # Convert to base64
                            att["base64"] = base64.b64encode(content).decode('utf-8')
                            logger.info(f"Downloaded media {att['type']} ({len(content)} bytes)")
        except Exception as e:
            logger.error(f"Failed to download telegram media: {e}")

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
    logger.info(f"Telegram message saved with id {msg_id}")
    
    # Analyze in background
    background_tasks.add_task(
        analyze_inbox_message,
        msg_id,
        parsed["text"],
        license_id,
        config.get("auto_reply_enabled", False),
        parsed["chat_id"],
        attachments # Pass attachments to analysis
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
    limit: int = 25,
    offset: int = 0,
    license: dict = Depends(get_license_from_header)
):
    """Get inbox messages with pagination support for infinite scroll"""
    messages = await get_inbox_messages(
        license_id=license["license_id"],
        status=status,
        channel=channel,
        limit=limit,
        offset=offset
    )
    
    # Get total count for pagination
    total = await get_inbox_messages_count(
        license_id=license["license_id"],
        status=status,
        channel=channel
    )
    
    return {
        "messages": messages,
        "total": total,
        "limit": limit,
        "offset": offset,
        "has_more": offset + len(messages) < total
    }


@router.get("/inbox/{message_id}")
async def get_inbox_message(
    message_id: int,
    license: dict = Depends(get_license_from_header)
):
    """Get single inbox message"""
    from models.inbox import get_inbox_message_by_id
    message = await get_inbox_message_by_id(message_id, license["license_id"])
    if not message:
        raise HTTPException(status_code=404, detail="الرسالة غير موجودة")
    return {"message": message}


# ============ Conversations (Chat-Style Inbox) ============

@router.get("/conversations")
async def get_conversations(
    status: Optional[str] = None,
    channel: Optional[str] = None,
    limit: int = 25,
    offset: int = 0,
    license: dict = Depends(get_license_from_header)
):
    """
    Get inbox grouped by sender (chat-style view).
    Each item represents a unique contact with their latest message and message count.
    """
    conversations = await get_inbox_conversations(
        license_id=license["license_id"],
        status=status,
        channel=channel,
        limit=limit,
        offset=offset
    )
    
    # Get total count for pagination
    total = await get_inbox_conversations_count(
        license_id=license["license_id"],
        status=status,
        channel=channel
    )
    
    # Get status counts across ALL channels (not filtered) for badge display
    status_counts = await get_inbox_status_counts(
        license_id=license["license_id"]
    )
    
    return {
        "conversations": conversations,
        "total": total,
        "limit": limit,
        "offset": offset,
        "has_more": offset + len(conversations) < total,
        "status_counts": status_counts
    }


@router.get("/conversations/{sender_contact:path}")
async def get_conversation_detail(
    sender_contact: str,
    limit: int = 100,
    license: dict = Depends(get_license_from_header)
):
    """
    Get complete chat history for a conversation (both incoming and outgoing messages).
    Returns messages in chronological order with direction markers.
    """
    from models import get_full_chat_history
    
    messages = await get_full_chat_history(
        license_id=license["license_id"],
        sender_contact=sender_contact,
        limit=limit
    )
    
    if not messages:
        raise HTTPException(status_code=404, detail="المحادثة غير موجودة")
    
    # Get sender info from first incoming message
    incoming_msgs = [m for m in messages if m.get("direction") == "incoming"]
    sender_name = incoming_msgs[0].get("sender_name", "عميل") if incoming_msgs else "عميل"
    
    return {
        "sender_name": sender_name,
        "sender_contact": sender_contact,
        "messages": messages,
        "total": len(messages)
    }


@router.post("/inbox/{message_id}/analyze")
async def analyze_message(
    message_id: int,
    background_tasks: BackgroundTasks,
    license: dict = Depends(get_license_from_header)
):
    """Re-analyze a message with AI"""
    from models.inbox import get_inbox_message_by_id
    message = await get_inbox_message_by_id(message_id, license["license_id"])
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
    """Approve or ignore a message/chat"""
    from models import ignore_chat, approve_chat_messages
    from models.inbox import get_inbox_message_by_id
    
    message = await get_inbox_message_by_id(message_id, license["license_id"])
    if not message:
        raise HTTPException(status_code=404, detail="الرسالة غير موجودة")
    
    if approval.action == "ignore":
        # Ignore entire chat - mark all messages from this sender as ignored
        sender_contact = message.get("sender_contact") or message.get("sender_id") or ""
        if sender_contact:
            count = await ignore_chat(license["license_id"], sender_contact)
            return {"success": True, "message": f"تم تجاهل المحادثة ({count} رسائل)"}
        else:
            # Fallback: just ignore this single message
            await update_inbox_status(message_id, "ignored")
            return {"success": True, "message": "تم تجاهل الرسالة"}
    
    elif approval.action == "approve":
        # Approve and send - use edited_body if provided, otherwise use AI draft
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
        
        # FIX: Also mark other "analyzed" messages in this conversation as approved
        # This ensures the conversation status moves to "approved" even if there are other pending messages
        sender_contact = message.get("sender_contact") or message.get("sender_id") or ""
        if sender_contact:
            await approve_chat_messages(license["license_id"], sender_contact)
        
        # Send in background
        background_tasks.add_task(
            send_approved_message,
            outbox_id,
            license["license_id"]
        )
        
        return {"success": True, "message": "تم إرسال الرد"}
    
    raise HTTPException(status_code=400, detail="إجراء غير صالح")


@router.post("/conversations/{sender_contact:path}/read")
async def mark_conversation_read(
    sender_contact: str,
    license: dict = Depends(get_license_from_header)
):
    """
    Mark all messages in a conversation as read.
    Clears the unread badge.
    """
    from models.inbox import mark_chat_read
    
    await mark_chat_read(license["license_id"], sender_contact)
    return {"success": True, "message": "تم تحديد المحادثة كمقروءة"}


@router.post("/inbox/cleanup")
async def cleanup_inbox_status(
    license: dict = Depends(get_license_from_header)
):
    """
    Run a cleanup task to fix stuck 'Waiting for Approval' chats.
    Mark "analyzed" messages as "approved" if there is a later "sent" message in the same conversation.
    """
    from models.inbox import fix_stale_inbox_status
    
    try:
        count = await fix_stale_inbox_status(license["license_id"])
        return {
            "success": True, 
            "message": "تم تنظيف المحادثات العالقة بنجاح", 
            "details": f"Ran cleanup task"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cleanup failed: {e}")


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
    telegram_chat_id: str = None,
    attachments: Optional[List[dict]] = None
):
    """Analyze message with AI and optionally auto-reply"""
    try:
        # Fetch chat history for context (last 10 messages)
        # We need to find the sender_contact first. 
        # Since we likely just saved the message, we can get it from the DB or pass it.
        # Ideally, we should have passed sender_contact to this function, but for now let's query the message.
        from models.inbox import get_inbox_message_by_id, get_chat_history_for_llm
        
        # Get the message details to find the sender
        message_data = await get_inbox_message_by_id(message_id, license_id)
        chat_history = ""
        
        if message_data:
            sender_contact = message_data.get("sender_contact") or message_data.get("sender_id")
            if sender_contact:
                try:
                    chat_history = await get_chat_history_for_llm(license_id, sender_contact, limit=10)
                except Exception as ex:
                    print(f"Failed to fetch history: {ex}")

        # Process with AI
        result = await process_message(
            message=body,
            attachments=attachments,
            history=chat_history  # Pass history to agent
        )

        if result["success"]:
            data = result["data"]
            
            # AUDIO RESPONSE GENERATION
            # If input has audio attachments, generate spoken response
            has_audio_input = False
            if attachments:
                for att in attachments:
                    att_type = att.get("type", "")
                    if att_type.startswith("audio") or att_type.startswith("voice"):
                        has_audio_input = True
                        break
            
            if has_audio_input and data.get("draft_response"):
                try:
                    from services.tts_service import generate_speech_to_file
                    # Generate speech from the response text (saves to file for WhatsApp upload)
                    audio_path = await generate_speech_to_file(data["draft_response"])
                    
                    # Append audio tag to draft response for processing in send_approved_message
                    data["draft_response"] += f"\n[AUDIO: {audio_path}]"
                except Exception as tts_e:
                    print(f"Failed to generate TTS audio: {tts_e}")

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
            
            # Link message to customer and update lead score (same as workers.py)
            try:
                from db_helper import get_db, fetch_one, execute_sql, commit_db, DB_TYPE
                from models.customers import get_or_create_customer, increment_customer_messages, update_customer_lead_score
                from models.inbox import get_inbox_message_by_id as get_msg_by_id
                
                message = await get_msg_by_id(message_id, license_id)
                
                if message:
                    sender_contact = message.get("sender_contact") or ""
                    sender_name = message.get("sender_name") or ""
                    
                    if sender_contact:
                        # Extract email or phone from contact
                        email = None
                        phone = None
                        if "@" in sender_contact:
                            email = sender_contact
                        elif sender_contact.replace("+", "").replace("-", "").replace(" ", "").isdigit():
                            phone = sender_contact
                        
                        # Get or create customer
                        customer = await get_or_create_customer(
                            license_id=license_id,
                            phone=phone,
                            email=email,
                            name=sender_name
                        )
                        
                        if customer and customer.get("id"):
                            customer_id = customer["id"]
                            
                            # Increment message count
                            await increment_customer_messages(customer_id)
                            
                            # Link message to customer
                            async with get_db() as db:
                                existing = await fetch_one(
                                    db,
                                    "SELECT 1 FROM customer_messages WHERE customer_id = ? AND inbox_message_id = ?",
                                    [customer_id, message_id]
                                )
                                if not existing:
                                    if DB_TYPE == "postgresql":
                                        await execute_sql(
                                            db,
                                            """
                                            INSERT INTO customer_messages (customer_id, inbox_message_id)
                                            VALUES (?, ?)
                                            ON CONFLICT (customer_id, inbox_message_id) DO NOTHING
                                            """,
                                            [customer_id, message_id]
                                        )
                                    else:
                                        await execute_sql(
                                            db,
                                            """
                                            INSERT OR IGNORE INTO customer_messages (customer_id, inbox_message_id)
                                            VALUES (?, ?)
                                            """,
                                            [customer_id, message_id]
                                        )
                                    await commit_db(db)
                            
                            # Update lead score
                            await update_customer_lead_score(
                                license_id=license_id,
                                customer_id=customer_id,
                                intent=data.get("intent"),
                                sentiment=data.get("sentiment"),
                                sentiment_score=0.0
                            )
            except Exception as crm_error:
                print(f"Error updating CRM for webhook message {message_id}: {crm_error}")
            
            # Auto-reply if enabled
            if auto_reply and data["draft_response"]:
                # Get message details for sending
                from models.inbox import get_inbox_message_by_id
                message = await get_inbox_message_by_id(message_id, license_id)
                
                if message:
                    # ============ 1. MARK AS READ (Blue Ticks) because we are replying ============
                    try:
                        channel = message.get("channel")
                        if channel == "whatsapp":
                            from services.whatsapp_service import get_whatsapp_config, WhatsAppService
                            wa_config = await get_whatsapp_config(license_id)
                            if wa_config and message.get("channel_message_id"):
                                wa_svc = WhatsAppService(wa_config["phone_number_id"], wa_config["access_token"])
                                await wa_svc.mark_as_read(message["channel_message_id"])
                        
                        elif channel == "telegram" or channel == "telegram_bot":
                            # Try Telegram Phone first
                            from models.telegram_config import get_telegram_phone_session_data
                            session_string = await get_telegram_phone_session_data(license_id)
                            
                            if session_string and message.get("sender_contact"):
                                from services.telegram_phone_service import TelegramPhoneService
                                ph_svc = TelegramPhoneService()
                                mid = message.get("channel_message_id")
                                max_id = int(mid) if mid and mid.isdigit() else 0
                                await ph_svc.mark_as_read(session_string, message["sender_contact"], max_id)
                    except Exception as read_err:
                        print(f"Failed to auto-mark as read during auto-reply: {read_err}")

                    # ============ 2. SEND AUTO-REPLY ============
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
        
        # Extract Audio Tag
        import re
        body = message["body"]
        audio_path = None
        
        audio_match = re.search(r'\[AUDIO: (.*?)\]', body)
        if audio_match:
            audio_path = audio_match.group(1).strip()
            # Remove tag from body for text sending
            body = body.replace(audio_match.group(0), "").strip()
        
        sent_anything = False

        # SEND TEXT PART (only if NO audio - audio-only for natural human-like response)
        if body and not audio_path:  # Skip text when audio is present
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
                        body=body, # Use stripped body
                        reply_to_message_id=message.get("inbox_message_id")  # For threading
                    )
                    
                    await mark_outbox_sent(outbox_id)
                    sent_anything = True
            
            elif message["channel"] == "telegram":
                # Send via Telegram Phone (MTProto) ONLY
                session_string = await get_telegram_phone_session_data(license_id)
                
                if session_string:
                    try:
                        phone_service = TelegramPhoneService()
                        # Use recipient_id or sender_id as the chat we're replying to
                        recipient = message.get("recipient_id") or message.get("sender_id")
                        if recipient:
                            await phone_service.send_message(
                                session_string=session_string,
                                recipient_id=str(recipient),
                                text=body # Use stripped body
                            )
                            await mark_outbox_sent(outbox_id)
                            sent_anything = True
                    except Exception as e:
                        print(f"Failed to send via Telegram phone for outbox {outbox_id}: {e}")
                else:
                     print(f"No active Telegram phone session for license {license_id}")
    
            elif message["channel"] == "telegram_bot":
                # Send via Telegram Bot API ONLY
                try:
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
                                text=body # Use stripped body
                            )
                            await mark_outbox_sent(outbox_id)
                            sent_anything = True
                            print(f"Sent Telegram bot reply for outbox {outbox_id}")
                        else:
                            print(f"No bot token found for license {license_id}")
                except Exception as e:
                    print(f"Failed to send via Telegram bot for outbox {outbox_id}: {e}")

            elif message["channel"] == "whatsapp":
                try:
                    config = await get_whatsapp_config(license_id)
                    
                    if config:
                        from services.whatsapp_service import WhatsAppService
                        whatsapp_service = WhatsAppService(
                            phone_number_id=config["phone_number_id"],
                            access_token=config["access_token"]
                        )
                        
                        result = await whatsapp_service.send_message(
                            to=message["recipient_id"],
                            message=body # Use stripped body
                        )
                        
                        if result["success"]:
                            await mark_outbox_sent(outbox_id)
                            sent_anything = True
                            print(f"Sent WhatsApp reply for outbox {outbox_id}")
                            
                            # Save platform message ID for delivery receipt tracking
                            wa_message_id = result.get("message_id")
                            if wa_message_id:
                                try:
                                    from services.delivery_status import save_platform_message_id
                                    await save_platform_message_id(outbox_id, wa_message_id)
                                except Exception as e:
                                    print(f"Failed to save WA message ID: {e}")
                except Exception as e:
                    print(f"Failed to send WhatsApp message: {e}")

        # SEND AUDIO PART (All Channels)
        if audio_path:
            channel = message["channel"]
            try:
                if channel == "whatsapp":
                    config = await get_whatsapp_config(license_id)
                    if config:
                        from services.whatsapp_service import WhatsAppService
                        whatsapp_service = WhatsAppService(
                            phone_number_id=config["phone_number_id"],
                            access_token=config["access_token"]
                        )
                        
                        import asyncio
                        media_id = await whatsapp_service.upload_media(audio_path)
                        
                        if media_id:
                            await asyncio.sleep(1)
                            await whatsapp_service.send_audio_message(
                                to=message["recipient_id"],
                                media_id=media_id
                            )
                            if not sent_anything:
                                await mark_outbox_sent(outbox_id)
                                sent_anything = True
                            print(f"Sent WhatsApp audio reply for outbox {outbox_id}")
                
                elif channel == "telegram_bot":
                    from db_helper import get_db, fetch_one
                    async with get_db() as db:
                        row = await fetch_one(
                            db,
                            "SELECT bot_token FROM telegram_configs WHERE license_key_id = ?",
                            [license_id],
                        )
                        if row and row.get("bot_token"):
                            from services.telegram_service import TelegramService
                            telegram_service = TelegramService(row["bot_token"])
                            import asyncio
                            await asyncio.sleep(1)
                            await telegram_service.send_voice(
                                chat_id=message["recipient_id"],
                                audio_path=audio_path
                            )
                            if not sent_anything:
                                await mark_outbox_sent(outbox_id)
                                sent_anything = True
                            print(f"Sent Telegram Bot audio reply for outbox {outbox_id}")
                
                elif channel == "telegram":
                    session_string = await get_telegram_phone_session_data(license_id)
                    if session_string:
                        phone_service = TelegramPhoneService()
                        recipient = message.get("recipient_id") or message.get("sender_id")
                        if recipient:
                            import asyncio
                            await asyncio.sleep(1)
                            await phone_service.send_voice(
                                session_string=session_string,
                                recipient_id=str(recipient),
                                audio_path=audio_path
                            )
                            if not sent_anything:
                                await mark_outbox_sent(outbox_id)
                                sent_anything = True
                            print(f"Sent Telegram Phone audio reply for outbox {outbox_id}")
                
                elif channel == "email":
                    # TODO: Add audio attachment support to GmailAPIService
                    print(f"Email audio attachments not yet implemented for outbox {outbox_id}")
                    
            except Exception as e:
                print(f"Failed to send audio for channel {channel}: {e}")

        # ============ SMART AI REACTION ============
        # After successfully sending response, add a human-like reaction to the original message
        # Only reacts when appropriate (gratitude, celebration, etc.) - not robotic
        if sent_anything and message.get("inbox_message_id"):
            try:
                from services.smart_reactions import add_smart_reaction
                from models.inbox import get_inbox_message_by_id
                
                # Get the original customer message
                original_msg = await get_inbox_message_by_id(
                    message["inbox_message_id"], 
                    license_id
                )
                
                if original_msg:
                    await add_smart_reaction(
                        message_id=original_msg["id"],
                        license_id=license_id,
                        message_body=original_msg.get("body", ""),
                        sentiment=original_msg.get("sentiment"),
                        intent=original_msg.get("intent")
                    )
            except Exception as react_error:
                print(f"Smart reaction failed (non-critical): {react_error}")

    except Exception as e:
        print(f"Error sending message {outbox_id}: {e}")


# ============ Message Reactions ============

class ReactionRequest(BaseModel):
    emoji: str = Field(..., description="Emoji to react with (e.g., '❤️', '👍')")


@router.post("/inbox/messages/{message_id}/reactions")
async def add_message_reaction(
    message_id: int,
    reaction: ReactionRequest,
    license: dict = Depends(get_license_from_header)
):
    """
    Add a reaction to a message.
    Reactions are visible to both agent and customer.
    """
    from models.reactions import add_reaction
    from services.websocket_manager import broadcast_reaction_added
    
    result = await add_reaction(
        message_id=message_id,
        license_id=license["license_id"],
        emoji=reaction.emoji,
        user_type="agent"
    )
    
    if result["success"]:
        # Broadcast to connected clients
        await broadcast_reaction_added(
            license["license_id"],
            message_id,
            reaction.emoji,
            "agent"
        )
        return {"success": True, "reaction_id": result["reaction_id"]}
    else:
        raise HTTPException(status_code=500, detail=result.get("error", "Failed to add reaction"))


@router.delete("/inbox/messages/{message_id}/reactions/{emoji}")
async def remove_message_reaction(
    message_id: int,
    emoji: str,
    license: dict = Depends(get_license_from_header)
):
    """
    Remove a reaction from a message.
    """
    from models.reactions import remove_reaction
    from services.websocket_manager import broadcast_reaction_removed
    
    # URL decode the emoji
    import urllib.parse
    emoji = urllib.parse.unquote(emoji)
    
    result = await remove_reaction(
        message_id=message_id,
        license_id=license["license_id"],
        emoji=emoji,
        user_type="agent"
    )
    
    if result["success"]:
        # Broadcast to connected clients
        await broadcast_reaction_removed(
            license["license_id"],
            message_id,
            emoji,
            "agent"
        )
        return {"success": True, "removed": result["removed"]}
    else:
        raise HTTPException(status_code=500, detail=result.get("error", "Failed to remove reaction"))


@router.get("/inbox/messages/{message_id}/reactions")
async def get_message_reactions(
    message_id: int,
    license: dict = Depends(get_license_from_header)
):
    """
    Get all reactions for a message.
    """
    from models.reactions import get_message_reactions
    
    reactions = await get_message_reactions(message_id)
    return {"reactions": reactions}


# ============ Presence / Online Status ============

@router.get("/presence/{contact_id:path}")
async def get_contact_presence(
    contact_id: str,
    license: dict = Depends(get_license_from_header)
):
    """
    Get online status for a customer contact.
    Returns real last seen from WhatsApp/Telegram based on their last message.
    """
    from services.customer_presence import get_customer_presence
    
    # Get customer's real presence (based on their last message/activity)
    presence = await get_customer_presence(
        license_id=license["license_id"],
        sender_contact=contact_id
    )
    
    return {
        "contact_id": contact_id,
        "is_online": presence["is_online"],
        "last_seen": presence.get("last_seen"),
        "last_activity": presence.get("last_activity"),
        "status_text": presence["status_text"],
        "channel": presence.get("channel")
    }


@router.post("/presence/heartbeat")
async def presence_heartbeat(
    license: dict = Depends(get_license_from_header)
):
    """
    Update presence heartbeat - call periodically to stay "online".
    """
    from models.presence import heartbeat
    
    await heartbeat(license["license_id"])
    return {"success": True}


# ============ Message Forwarding ============

class ForwardRequest(BaseModel):
    target_channel: str = Field(..., description="Channel to forward to (whatsapp, telegram, email)")
    target_contact: str = Field(..., description="Contact ID/email/phone to forward to")


@router.post("/inbox/messages/{message_id}/forward")
async def forward_message(
    message_id: int,
    forward: ForwardRequest,
    background_tasks: BackgroundTasks,
    license: dict = Depends(get_license_from_header)
):
    """
    Forward a message to another channel/contact.
    Creates a new outbox message marked as forwarded.
    """
    from models.inbox import get_inbox_message_by_id
    
    # Get original message
    message = await get_inbox_message_by_id(message_id, license["license_id"])
    if not message:
        raise HTTPException(status_code=404, detail="الرسالة غير موجودة")
    
    # Prepare forwarded message body
    original_sender = message.get("sender_name") or message.get("sender_contact") or "مجهول"
    forwarded_body = f"📤 تم التحويل من: {original_sender}\n\n{message['body']}"
    
    # Create outbox message with forwarding info
    outbox_id = await create_outbox_message(
        inbox_message_id=message_id,
        license_id=license["license_id"],
        channel=forward.target_channel,
        body=forwarded_body,
        recipient_id=forward.target_contact,
        recipient_email=forward.target_contact if "@" in forward.target_contact else None
    )
    
    # Auto-approve and send
    await approve_outbox_message(outbox_id, forwarded_body)
    
    # Send in background
    background_tasks.add_task(
        send_approved_message,
        outbox_id,
        license["license_id"]
    )
    
    return {
        "success": True,
        "message": "تم تحويل الرسالة",
        "outbox_id": outbox_id
    }


@router.post("/inbox/messages/{message_id}/read")
async def mark_message_read(
    message_id: int,
    license: dict = Depends(get_license_from_header)
):
    """
    Mark a message as read (send receipt to sender).
    This turns the ticks BLUE on WhatsApp/Telegram.
    """
    from models.inbox import get_inbox_message_by_id
    from services.whatsapp_service import get_whatsapp_config, WhatsAppService
    
    # Get message details
    message = await get_inbox_message_by_id(message_id, license["license_id"])
    if not message:
        raise HTTPException(status_code=404, detail="Message not found")
    
    channel = message.get("channel")
    channel_msg_id = message.get("channel_message_id")
    contact_id = message.get("sender_contact") or message.get("sender_id")
    
    success = False
    
    # WhatsApp Read Receipt
    if channel == "whatsapp" and channel_msg_id:
        try:
            config = await get_whatsapp_config(license["license_id"])
            if config:
                wa_service = WhatsAppService(
                    phone_number_id=config["phone_number_id"],
                    access_token=config["access_token"]
                )
                await wa_service.mark_as_read(channel_msg_id)
                success = True
                print(f"Marked WhatsApp message {message_id} as read")
        except Exception as e:
            print(f"Failed to send WhatsApp read receipt: {e}")
            
    # Telegram Read Receipt
    elif (channel == "telegram" or channel == "telegram_bot") and contact_id:
        try:
            # Check for Telegram Phone first (priority for true read receipts)
            from db_helper import get_db, fetch_one
            async with get_db() as db:
                phone_config = await fetch_one(
                    db,
                    "SELECT session_string FROM telegram_phone_sessions WHERE license_key_id = ?",
                    [license["license_id"]]
                )
            
            if phone_config:
                # Use Telegram Phone (Telethon) - Supports true read receipts
                # We need to import inside to avoid circular deps if any
                from services.telegram_phone_service import TelegramPhoneService
                phone_service = TelegramPhoneService()
                
                # Convert channel_msg_id to int for Max ID
                max_id = int(channel_msg_id) if channel_msg_id and channel_msg_id.isdigit() else 0
                
                await phone_service.mark_as_read(
                    session_string=phone_config["session_string"],
                    chat_id=contact_id,
                    max_id=max_id
                )
                success = True
                print(f"Marked Telegram message {message_id} as read via Phone Service")
            
            else:
                # Telegram Bot Fallback
                # Bots cannot natively mark messages as "read" for the user (blue ticks)
                # But we can at least ensure our system knows it's read
                success = True 
                pass
                
        except Exception as e:
            print(f"Failed to send Telegram read receipt: {e}")

    # Also mark in our database as read (if not already)
    # This is internal status, distinct from the receipt sent to platform
    # (Assuming we have a mechanism for internal read status, if not we skip)

    return {"success": success}
