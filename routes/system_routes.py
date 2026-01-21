"""
Al-Mudeer - System & Account Routes
Health checks, worker status, AI usage, and unified account management
"""

import os
from fastapi import APIRouter, HTTPException, Depends, Request
from typing import List, Optional
from pydantic import BaseModel, Field

from models import (
    get_email_config,
    get_telegram_config,
    get_telegram_phone_session,
    get_whatsapp_config,
    update_email_config_settings,
    update_telegram_config_settings,
    update_telegram_phone_session_settings,
    update_whatsapp_config_settings,
)
from services import GmailOAuthService
from workers import get_worker_status
from dependencies import get_license_from_header
from db_helper import get_db, execute_sql, commit_db

router = APIRouter(prefix="/api/integrations", tags=["System & Accounts"])

class IntegrationAccount(BaseModel):
    id: str
    channel_type: str
    display_name: str
    is_active: bool
    details: Optional[str] = None
    auto_reply_enabled: bool = False

class WorkerStatusResponse(BaseModel):
    email_polling: dict
    telegram_polling: dict

@router.get("/debug")
def debug_integrations():
    return {"status": "ok", "message": "System & Accounts router is loaded"}

@router.get("/llm-health")
async def check_llm_health(license: dict = Depends(get_license_from_header)):
    """
    Diagnostic endpoint to check LLM API key health.
    Tests each provider with a minimal request to verify.
    """
    import httpx
    
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

@router.get("/ai-usage")
async def get_ai_usage(license: dict = Depends(get_license_from_header)):
    """Get today's AI usage stats"""
    from models import get_ai_usage_today
    return await get_ai_usage_today(license["license_id"])

@router.get("/workers/status", response_model=WorkerStatusResponse)
async def worker_status_v1():
    """Operational status of background workers"""
    return get_worker_status()

# Specialized endpoint with license dependency (v2)
@router.get("/workers/status/detail")
async def worker_status_v2(license: dict = Depends(get_license_from_header)):
    """Detailed worker status for a specific license"""
    return {"workers": get_worker_status()}

@router.get("/accounts")
async def list_integration_accounts(license: dict = Depends(get_license_from_header)):
    """Unified view of all connected channels/accounts"""
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
                auto_reply_enabled=bool(email_cfg.get("auto_reply_enabled")),
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
                auto_reply_enabled=bool(telegram_cfg.get("auto_reply_enabled")),
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
                auto_reply_enabled=bool(phone_cfg.get("auto_reply_enabled")),
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
                auto_reply_enabled=bool(whatsapp_cfg.get("auto_reply_enabled")),
            )
        )

    return {"accounts": accounts}

@router.post("/accounts")
async def create_integration_account(
    request: dict,
    license: dict = Depends(get_license_from_header)
):
    """Create/link a new integration account"""
    license_id = license["license_id"]
    channel_type = request.get("channel_type")
    
    if not channel_type:
        raise HTTPException(status_code=400, detail="channel_type مطلوب")
    
    if channel_type == "email":
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
        
        from models import save_telegram_config
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
        
        from services.whatsapp_service import save_whatsapp_config as save_wa_config
        verify_token = os.urandom(16).hex()
        
        await save_wa_config(
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
    """Delete/disconnect an integration account"""
    license_id = license["license_id"]
    
    if account_id == "email":
        email_cfg = await get_email_config(license_id)
        if email_cfg:
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
        telegram_cfg = await get_telegram_config(license_id)
        if telegram_cfg:
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
        from models import deactivate_telegram_phone_session
        await deactivate_telegram_phone_session(license_id)
        return {"success": True, "message": "تم قطع الاتصال بـ Telegram Phone"}
    
    elif account_id == "whatsapp":
        whatsapp_cfg = await get_whatsapp_config(license_id)
        if whatsapp_cfg:
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

@router.patch("/accounts/{account_id}")
async def update_integration_account(
    account_id: str,
    request: dict,
    license: dict = Depends(get_license_from_header)
):
    """Update integration account settings"""
    license_id = license["license_id"]
    auto_reply = request.get("auto_reply_enabled")
    
    if auto_reply is None:
        raise HTTPException(status_code=400, detail="لا توجد إعدادات للتحديث")
    
    if account_id == "email":
        await update_email_config_settings(license_id, auto_reply=auto_reply)
        return {"success": True, "message": "تم تحديث إعدادات البريد الإلكتروني"}
    
    elif account_id in ("telegram", "telegram_bot"):
        await update_telegram_config_settings(license_id, auto_reply=auto_reply)
        return {"success": True, "message": "تم تحديث إعدادات Telegram Bot"}
    
    elif account_id == "telegram_phone":
        await update_telegram_phone_session_settings(license_id, auto_reply=auto_reply)
        return {"success": True, "message": "تم تحديث إعدادات Telegram Phone"}
    
    elif account_id == "whatsapp":
        await update_whatsapp_config_settings(license_id, auto_reply=auto_reply)
        return {"success": True, "message": "تم تحديث إعدادات WhatsApp"}
    
    else:
        raise HTTPException(status_code=400, detail=f"نوع الحساب غير مدعوم: {account_id}")
