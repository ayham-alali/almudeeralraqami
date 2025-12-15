"""
Al-Mudeer - Feature Routes
Templates, Customers, Analytics, Preferences, Voice Transcription
"""

from fastapi import APIRouter, HTTPException, Depends, UploadFile, File
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime, timedelta

from models import (
    get_templates,
    save_template,
    delete_template,
    increment_template_usage,
    get_customers,
    get_customer,
    update_customer,
    get_or_create_customer,
    get_analytics_summary,
    update_daily_analytics,
    get_preferences,
    update_preferences,
    get_notifications,
    get_unread_count,
    mark_notification_read,
    mark_all_notifications_read,
    create_notification,
)
from services.voice_service import (
    transcribe_voice_message,
    transcribe_from_base64,
    transcribe_from_url,
    estimate_time_saved
)
from services.auto_categorization import categorize_message_dict, categorize_messages_batch
from security import sanitize_email, sanitize_phone, sanitize_string
from dependencies import get_license_from_header, get_optional_license_from_header
from db_helper import get_db, fetch_all

router = APIRouter(prefix="/api", tags=["Features"])


# ============ Templates Schemas ============

class TemplateCreate(BaseModel):
    shortcut: str = Field(..., min_length=1, max_length=20)
    title: str = Field(..., min_length=1, max_length=100)
    body: str = Field(..., min_length=1)
    category: str = Field(default="عام")


class TemplateResponse(BaseModel):
    id: int
    shortcut: str
    title: str
    body: str
    category: str
    use_count: int


# ============ Templates Routes ============

@router.get("/templates")
async def list_templates(license: dict = Depends(get_license_from_header)):
    """Get all quick reply templates"""
    templates = await get_templates(license["license_id"])
    return {"templates": templates}


@router.post("/templates")
async def create_template(
    template: TemplateCreate,
    license: dict = Depends(get_license_from_header)
):
    """Create a new quick reply template"""
    template_id = await save_template(
        license_id=license["license_id"],
        shortcut=template.shortcut,
        title=template.title,
        body=template.body,
        category=template.category
    )
    return {"success": True, "template_id": template_id, "message": "تم إنشاء القالب بنجاح"}


@router.delete("/templates/{template_id}")
async def remove_template(
    template_id: int,
    license: dict = Depends(get_license_from_header)
):
    """Delete a template"""
    await delete_template(license["license_id"], template_id)
    return {"success": True, "message": "تم حذف القالب"}


@router.post("/templates/{template_id}/use")
async def use_template(
    template_id: int,
    license: dict = Depends(get_license_from_header)
):
    """Mark template as used (increment counter)"""
    await increment_template_usage(template_id)
    return {"success": True}


# ============ Customers Schemas ============

class CustomerUpdate(BaseModel):
    name: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    company: Optional[str] = None
    notes: Optional[str] = None
    tags: Optional[str] = None
    is_vip: Optional[bool] = None


# ============ Customers Routes ============

@router.get("/customers")
async def list_customers(
    limit: int = 100,
    license: dict = Depends(get_license_from_header)
):
    """Get all customers"""
    customers = await get_customers(license["license_id"], limit)
    return {"customers": customers, "total": len(customers)}


@router.get("/customers/{customer_id}")
async def get_customer_detail(
    customer_id: int,
    license: dict = Depends(get_license_from_header)
):
    """Get customer details with message history"""
    customer = await get_customer(license["license_id"], customer_id)
    if not customer:
        raise HTTPException(status_code=404, detail="العميل غير موجود")
    
    # Get customer's messages
    from models import get_inbox_messages
    # Note: We'd need to join with customer_messages table
    # For now, return customer data
    
    return {"customer": customer}


@router.patch("/customers/{customer_id}")
async def update_customer_detail(
    customer_id: int,
    data: CustomerUpdate,
    license: dict = Depends(get_license_from_header)
):
    """Update customer details"""
    # Sanitize and normalize incoming data without changing the response shape
    raw_data = data.dict(exclude_none=True)

    if "email" in raw_data:
        sanitized_email = sanitize_email(raw_data["email"])
        if not sanitized_email and raw_data["email"]:
            raise HTTPException(status_code=400, detail="البريد الإلكتروني غير صالح")
        raw_data["email"] = sanitized_email

    if "phone" in raw_data:
        sanitized_phone = sanitize_phone(raw_data["phone"])
        if not sanitized_phone and raw_data["phone"]:
            raise HTTPException(status_code=400, detail="رقم الهاتف غير صالح")
        raw_data["phone"] = sanitized_phone

    # Light sanitization for free-text fields (notes/tags/company/name)
    for field_name in ("name", "company", "notes", "tags"):
        if field_name in raw_data and raw_data[field_name] is not None:
            raw_data[field_name] = sanitize_string(str(raw_data[field_name]), max_length=1000)

    success = await update_customer(
        license["license_id"],
        customer_id,
        **raw_data
    )
    if not success:
        raise HTTPException(status_code=400, detail="فشل التحديث")
    
    return {"success": True, "message": "تم تحديث بيانات العميل"}


# ============ Analytics Routes ============

@router.get("/analytics/summary")
async def get_dashboard_analytics(
    days: int = 30,
    license: dict = Depends(get_license_from_header)
):
    """Get analytics summary for dashboard"""
    summary = await get_analytics_summary(license["license_id"], days)
    return {"analytics": summary, "period_days": days}


@router.get("/analytics/chart")
async def get_chart_data(
    days: int = 7,
    license: dict = Depends(get_license_from_header)
):
    """Get daily data for charts (works with SQLite and PostgreSQL)."""
    cutoff_date = (datetime.utcnow().date() - timedelta(days=days)).isoformat()

    async with get_db() as db:
        rows = await fetch_all(
            db,
            """
            SELECT date, messages_received, messages_replied, auto_replies
            FROM analytics 
            WHERE license_key_id = ? 
              AND date >= ?
            ORDER BY date ASC
            """,
            [license["license_id"], cutoff_date],
        )

    return {"data": rows}


# ============ Preferences Schemas ============

class PreferencesUpdate(BaseModel):
    dark_mode: Optional[bool] = None
    notifications_enabled: Optional[bool] = None
    notification_sound: Optional[bool] = None
    auto_reply_delay_seconds: Optional[int] = None
    onboarding_completed: Optional[bool] = None


# ============ Preferences Routes ============

@router.get("/preferences")
async def get_user_preferences(license: dict = Depends(get_license_from_header)):
    """Get user preferences"""
    prefs = await get_preferences(license["license_id"])
    return {"preferences": prefs}


@router.patch("/preferences")
async def update_user_preferences(
    data: PreferencesUpdate,
    license: dict = Depends(get_license_from_header)
):
    """Update user preferences"""
    await update_preferences(
        license["license_id"],
        **data.dict(exclude_none=True)
    )
    return {"success": True, "message": "تم حفظ التفضيلات"}


# ============ Default Templates ============

DEFAULT_TEMPLATES = [
    {
        "shortcut": "سعر",
        "title": "استفسار عن الأسعار",
        "body": "شكراً لتواصلكم معنا!\n\nأسعارنا تبدأ من [أدخل السعر] وتختلف حسب نوع الخدمة المطلوبة.\n\nهل تود معرفة تفاصيل أكثر عن خدمة معينة؟",
        "category": "أسعار"
    },
    {
        "shortcut": "شكرا",
        "title": "شكر وتقدير",
        "body": "شكراً جزيلاً لتواصلكم معنا! 🙏\n\nنسعد دائماً بخدمتكم.\n\nمع أطيب التحيات،\nفريق خدمة العملاء",
        "category": "عام"
    },
    {
        "shortcut": "موقع",
        "title": "الموقع والعنوان",
        "body": "موقعنا:\n📍 [العنوان]\n\nساعات العمل:\n🕐 [من - إلى]\n\nللتواصل:\n📱 [رقم الهاتف]",
        "category": "معلومات"
    },
    {
        "shortcut": "انتظر",
        "title": "طلب الانتظار",
        "body": "شكراً لتواصلكم!\n\nتم استلام رسالتكم وسيتم الرد عليكم في أقرب وقت ممكن.\n\nنقدر صبركم! 🙏",
        "category": "عام"
    },
    {
        "shortcut": "حجز",
        "title": "تأكيد الحجز",
        "body": "تم تأكيد حجزكم بنجاح! ✅\n\n📅 التاريخ: [التاريخ]\n🕐 الوقت: [الوقت]\n\nنتطلع لرؤيتكم!",
        "category": "حجوزات"
    }
]


@router.post("/templates/defaults")
async def create_default_templates(license: dict = Depends(get_license_from_header)):
    """Create default templates for new users"""
    created = 0
    for template in DEFAULT_TEMPLATES:
        try:
            await save_template(
                license_id=license["license_id"],
                **template
            )
            created += 1
        except:
            pass  # Template might already exist
    
    return {"success": True, "created": created, "message": f"تم إنشاء {created} قوالب افتراضية"}


# ============ Voice Transcription Schemas ============

class VoiceBase64Request(BaseModel):
    audio_base64: str = Field(..., description="Base64 encoded audio data")


class VoiceURLRequest(BaseModel):
    audio_url: str = Field(..., description="URL to audio file")


# ============ Voice Transcription Routes ============

@router.post("/voice/transcribe")
async def transcribe_voice_upload(
    file: UploadFile = File(...),
    license: dict = Depends(get_license_from_header)
):
    """
    Transcribe uploaded voice message (Arabic)
    Supported formats: mp3, mp4, mpeg, mpga, m4a, wav, webm, ogg
    """
    # Validate file type
    allowed_types = ["audio/ogg", "audio/mpeg", "audio/mp3", "audio/wav", 
                     "audio/webm", "audio/m4a", "video/mp4", "audio/mp4"]
    
    if file.content_type and file.content_type not in allowed_types:
        raise HTTPException(
            status_code=400, 
            detail=f"نوع الملف غير مدعوم. الأنواع المدعومة: {', '.join(allowed_types)}"
        )
    
    # Read audio data
    audio_data = await file.read()
    
    # Max 25MB
    if len(audio_data) > 25 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="حجم الملف كبير جداً (الحد الأقصى 25MB)")
    
    # Transcribe
    result = await transcribe_voice_message(audio_data, file.filename or "audio.ogg")
    
    if not result["success"]:
        raise HTTPException(status_code=500, detail=result["error"])
    
    # Track time saved
    duration = result.get("duration", 60)  # Default 60 seconds if unknown
    time_saved = estimate_time_saved(duration)
    
    await update_daily_analytics(
        license["license_id"],
        time_saved_seconds=time_saved
    )
    
    return {
        "success": True,
        "transcription": result["text"],
        "language": result.get("language", "ar"),
        "duration_seconds": duration,
        "time_saved_seconds": time_saved,
        "message": "تم تحويل الرسالة الصوتية إلى نص بنجاح"
    }


@router.post("/voice/transcribe-base64")
async def transcribe_voice_base64(
    request: VoiceBase64Request,
    license: dict = Depends(get_license_from_header)
):
    """Transcribe voice message from base64 encoded audio"""
    result = await transcribe_from_base64(request.audio_base64)
    
    if not result["success"]:
        raise HTTPException(status_code=500, detail=result["error"])
    
    # Track time saved
    duration = result.get("duration", 60)
    time_saved = estimate_time_saved(duration)
    
    await update_daily_analytics(
        license["license_id"],
        time_saved_seconds=time_saved
    )
    
    return {
        "success": True,
        "transcription": result["text"],
        "duration_seconds": duration,
        "time_saved_seconds": time_saved,
        "message": "تم تحويل الرسالة الصوتية إلى نص بنجاح"
    }


@router.post("/voice/transcribe-url")
async def transcribe_voice_url(
    request: VoiceURLRequest,
    license: dict = Depends(get_license_from_header)
):
    """Transcribe voice message from URL"""
    result = await transcribe_from_url(request.audio_url)
    
    if not result["success"]:
        raise HTTPException(status_code=500, detail=result["error"])
    
    # Track time saved
    duration = result.get("duration", 60)
    time_saved = estimate_time_saved(duration)
    
    await update_daily_analytics(
        license["license_id"],
        time_saved_seconds=time_saved
    )
    
    return {
        "success": True,
        "transcription": result["text"],
        "duration_seconds": duration,
        "time_saved_seconds": time_saved,
        "message": "تم تحويل الرسالة الصوتية إلى نص بنجاح"
    }


# ============ Notifications Routes ============

@router.get("/notifications")
async def list_notifications(
    unread_only: bool = False,
    limit: int = 50,
    license: dict = Depends(get_optional_license_from_header)
):
    """Get user notifications"""
    if not license:
        # When there is no valid license key, return empty notifications instead of 401
        return {
            "notifications": [],
            "unread_count": 0,
            "total": 0
        }

    notifications = await get_notifications(license["license_id"], unread_only, limit)
    unread = await get_unread_count(license["license_id"])
    
    return {
        "notifications": notifications,
        "unread_count": unread,
        "total": len(notifications)
    }


@router.get("/notifications/count")
async def get_notification_count(license: dict = Depends(get_optional_license_from_header)):
    """Get unread notification count.

    If the license key is missing or invalid we return 0 instead of an error
    so the dashboard badge doesn't break for unauthenticated or pre-rendered views.
    """
    if not license:
        return {"unread_count": 0}

    count = await get_unread_count(license["license_id"])
    return {"unread_count": count}


@router.post("/notifications/{notification_id}/read")
async def read_notification(
    notification_id: int,
    license: dict = Depends(get_license_from_header)
):
    """Mark notification as read"""
    await mark_notification_read(license["license_id"], notification_id)
    return {"success": True, "message": "تم تحديث الإشعار"}


@router.post("/notifications/read-all")
async def read_all_notifications(license: dict = Depends(get_license_from_header)):
    """Mark all notifications as read"""
    await mark_all_notifications_read(license["license_id"])
    return {"success": True, "message": "تم تحديث جميع الإشعارات"}


# ============ Auto-Categorization Routes ============

class CategorizeRequest(BaseModel):
    message: str = Field(..., min_length=1)


class CategorizeMultipleRequest(BaseModel):
    messages: List[str] = Field(..., min_items=1)


@router.post("/categorize")
async def categorize_single_message(
    data: CategorizeRequest,
    license: dict = Depends(get_license_from_header)
):
    """
    Auto-categorize a single message
    Returns: category, tags, priority, sentiment, suggested folder, and auto-actions
    """
    result = categorize_message_dict(data.message)
    return {
        "success": True,
        "categorization": result,
        "message": "تم تصنيف الرسالة بنجاح"
    }


@router.post("/categorize/batch")
async def categorize_multiple_messages(
    data: CategorizeMultipleRequest,
    license: dict = Depends(get_license_from_header)
):
    """
    Auto-categorize multiple messages at once
    """
    results = categorize_messages_batch(data.messages)
    return {
        "success": True,
        "categorizations": results,
        "count": len(results),
        "message": f"تم تصنيف {len(results)} رسالة"
    }


# ============ Template Suggestions ============

@router.get("/templates/suggest")
async def suggest_templates(
    intent: Optional[str] = None,
    message: Optional[str] = None,
    limit: int = 5,
    license: dict = Depends(get_license_from_header)
):
    """
    Get AI-suggested templates based on message intent or content
    """
    templates = await get_templates(license["license_id"])
    
    if not templates:
        return {"templates": [], "message": "لا توجد قوالب. قم بإنشاء قوالب أولاً."}
    
    # Category mapping for intents
    intent_categories = {
        'استفسار': ['أسعار', 'معلومات', 'عام'],
        'طلب خدمة': ['حجوزات', 'طلبات', 'عام'],
        'شكوى': ['شكاوى', 'دعم', 'عام'],
        'متابعة': ['متابعة', 'عام'],
        'عرض': ['عروض', 'عام'],
    }
    
    suggested = []
    
    # Filter by intent
    if intent and intent in intent_categories:
        categories = intent_categories[intent]
        suggested = [t for t in templates if t.get('category') in categories]
    
    # Filter by message content keywords
    if message and not suggested:
        message_lower = message.lower()
        for template in templates:
            template_text = f"{template['title']} {template['body']} {template['shortcut']}".lower()
            if any(word in template_text for word in message_lower.split() if len(word) > 2):
                suggested.append(template)
    
    # Fall back to most used templates
    if not suggested:
        suggested = sorted(templates, key=lambda t: t.get('use_count', 0), reverse=True)
    
    return {
        "templates": suggested[:limit],
        "total": len(suggested),
        "intent": intent
    }

