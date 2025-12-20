"""
Al-Mudeer - Notification Routes
Smart notifications, Slack/Discord integration, notification rules
Admin broadcast for subscription reminders, team updates, and promotions
"""

import os
from fastapi import APIRouter, HTTPException, Depends, Header
from pydantic import BaseModel, Field
from typing import Optional, List
from dotenv import load_dotenv

from services.notification_service import (
    init_notification_tables,
    save_integration,
    get_integration,
    get_all_integrations,
    disable_integration,
    create_rule,
    get_rules,
    delete_rule,
    send_notification,
    test_slack_webhook,
    test_discord_webhook,
    NotificationPayload,
    NotificationPriority,
    NotificationChannel,
)
from dependencies import get_license_from_header
from models import create_notification

load_dotenv()

router = APIRouter(prefix="/api/notifications", tags=["Notifications"])

# Admin authentication (same pattern as subscription.py)
ADMIN_KEY = os.getenv("ADMIN_KEY")
if not ADMIN_KEY:
    raise ValueError("ADMIN_KEY environment variable is required")


async def verify_admin(x_admin_key: str = Header(None, alias="X-Admin-Key")):
    """Verify admin key"""
    if not x_admin_key or x_admin_key != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="غير مصرح - Admin key required")


# ============ Admin Broadcast Schemas ============

class AdminBroadcast(BaseModel):
    """Admin broadcast notification to users"""
    license_ids: Optional[List[int]] = Field(None, description="List of license IDs to notify, or null for all")
    title: str = Field(..., min_length=1, max_length=200)
    message: str = Field(..., min_length=1, max_length=1000)
    notification_type: str = Field(
        default="team_update",
        description="subscription_expiring, subscription_expired, team_update, promotion"
    )
    link: Optional[str] = Field(None, description="Optional link to navigate to")
    priority: str = Field(default="normal", description="low, normal, high, urgent")


# ============ Admin Broadcast Route ============

@router.post("/admin/broadcast")
async def broadcast_notification(
    data: AdminBroadcast,
    _: None = Depends(verify_admin)
):
    """
    Send notification to all users or specific users.
    Admin-only endpoint for subscription reminders, team updates, and promotions.
    """
    from db_helper import get_db, fetch_all
    from database import DB_TYPE
    from logging_config import get_logger
    
    logger = get_logger(__name__)
    
    try:
        # Get target license IDs
        if data.license_ids:
            license_ids = data.license_ids
        else:
            # Get all active license IDs
            async with get_db() as db:
                if DB_TYPE == "postgresql":
                    rows = await fetch_all(db, "SELECT id FROM license_keys WHERE is_active = TRUE", [])
                else:
                    rows = await fetch_all(db, "SELECT id FROM license_keys WHERE is_active = 1", [])
                license_ids = [row["id"] for row in rows]
        
        # Validate notification type
        valid_types = ["subscription_expiring", "subscription_expired", "team_update", "promotion"]
        if data.notification_type not in valid_types:
            data.notification_type = "team_update"
        
        # Send notification to each license
        sent_count = 0
        for license_id in license_ids:
            try:
                await create_notification(
                    license_id=license_id,
                    notification_type=data.notification_type,
                    title=data.title,
                    message=data.message,
                    priority=data.priority,
                    link=data.link
                )
                sent_count += 1
            except Exception as e:
                logger.warning(f"Failed to send notification to license {license_id}: {e}")
                continue
        
        logger.info(f"Admin broadcast sent to {sent_count} users: {data.title}")
        
        return {
            "success": True,
            "sent_count": sent_count,
            "total_targets": len(license_ids),
            "message": f"تم إرسال الإشعار إلى {sent_count} مستخدم"
        }
    
    except Exception as e:
        logger.error(f"Error broadcasting notification: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="حدث خطأ أثناء إرسال الإشعارات")


# ============ Integration Schemas ============

class IntegrationCreate(BaseModel):
    channel_type: str = Field(..., description="slack, discord, or webhook")
    webhook_url: str = Field(..., min_length=10)
    channel_name: Optional[str] = None


class IntegrationTest(BaseModel):
    channel_type: str
    webhook_url: str


class RuleCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    condition_type: str = Field(..., description="sentiment, urgency, keyword, vip_customer")
    condition_value: str = Field(..., min_length=1)
    channels: List[str] = Field(..., description="in_app, slack, discord, webhook")


class NotificationSend(BaseModel):
    title: str = Field(..., min_length=1)
    message: str = Field(..., min_length=1)
    priority: str = Field(default="normal")
    link: Optional[str] = None
    channels: List[str] = Field(default=["in_app"])


# ============ Integration Routes ============

@router.get("/integrations")
async def list_integrations(license: dict = Depends(get_license_from_header)):
    """Get all notification integrations"""
    integrations = await get_all_integrations(license["license_id"])
    
    # Mask webhook URLs for security
    for integration in integrations:
        if integration.get("webhook_url"):
            url = integration["webhook_url"]
            integration["webhook_url_masked"] = url[:30] + "..." if len(url) > 30 else url
    
    return {
        "integrations": integrations,
        "available_channels": ["slack", "discord", "webhook"]
    }


@router.post("/integrations")
async def create_integration(
    data: IntegrationCreate,
    license: dict = Depends(get_license_from_header)
):
    """Create or update notification integration"""
    if data.channel_type not in ["slack", "discord", "webhook"]:
        raise HTTPException(status_code=400, detail="نوع القناة غير صالح")
    
    integration_id = await save_integration(
        license["license_id"],
        data.channel_type,
        data.webhook_url,
        data.channel_name
    )
    
    return {
        "success": True,
        "integration_id": integration_id,
        "message": f"تم ربط {data.channel_type} بنجاح"
    }


@router.post("/integrations/test")
async def test_integration(
    data: IntegrationTest,
    license: dict = Depends(get_license_from_header)
):
    """Test webhook integration"""
    if data.channel_type == "slack":
        result = await test_slack_webhook(data.webhook_url)
    elif data.channel_type == "discord":
        result = await test_discord_webhook(data.webhook_url)
    else:
        raise HTTPException(status_code=400, detail="نوع القناة غير مدعوم للاختبار")
    
    if result.get("success"):
        return {"success": True, "message": "تم إرسال رسالة الاختبار بنجاح"}
    else:
        raise HTTPException(
            status_code=400, 
            detail=f"فشل الاتصال: {result.get('error', 'خطأ غير معروف')}"
        )


@router.delete("/integrations/{channel_type}")
async def remove_integration(
    channel_type: str,
    license: dict = Depends(get_license_from_header)
):
    """Disable notification integration"""
    await disable_integration(license["license_id"], channel_type)
    return {"success": True, "message": "تم إلغاء الربط"}


# ============ Rules Routes ============

@router.get("/rules")
async def list_rules(license: dict = Depends(get_license_from_header)):
    """Get all notification rules"""
    rules = await get_rules(license["license_id"])
    
    return {
        "rules": rules,
        "condition_types": [
            {"value": "sentiment", "label": "المشاعر", "description": "إشعار عند رسالة سلبية"},
            {"value": "urgency", "label": "الأهمية", "description": "إشعار عند رسالة عاجلة"},
            {"value": "keyword", "label": "كلمة مفتاحية", "description": "إشعار عند وجود كلمة معينة"},
            {"value": "vip_customer", "label": "عميل VIP", "description": "إشعار عند رسالة من عميل مهم"}
        ]
    }


@router.post("/rules")
async def add_rule(
    data: RuleCreate,
    license: dict = Depends(get_license_from_header)
):
    """Create notification rule"""
    # Validate condition type
    valid_conditions = ["sentiment", "urgency", "keyword", "vip_customer"]
    if data.condition_type not in valid_conditions:
        raise HTTPException(status_code=400, detail="نوع الشرط غير صالح")
    
    # Validate channels
    valid_channels = ["in_app", "slack", "discord", "webhook"]
    for channel in data.channels:
        if channel not in valid_channels:
            raise HTTPException(status_code=400, detail=f"قناة غير صالحة: {channel}")
    
    rule_id = await create_rule(
        license["license_id"],
        data.name,
        data.condition_type,
        data.condition_value,
        data.channels
    )
    
    return {
        "success": True,
        "rule_id": rule_id,
        "message": "تم إنشاء القاعدة بنجاح"
    }


@router.delete("/rules/{rule_id}")
async def remove_rule(
    rule_id: int,
    license: dict = Depends(get_license_from_header)
):
    """Delete notification rule"""
    await delete_rule(license["license_id"], rule_id)
    return {"success": True, "message": "تم حذف القاعدة"}


# ============ Send Notification Route ============

@router.post("/send")
async def send_custom_notification(
    data: NotificationSend,
    license: dict = Depends(get_license_from_header)
):
    """Send custom notification"""
    # Map priority string to enum
    priority_map = {
        "low": NotificationPriority.LOW,
        "normal": NotificationPriority.NORMAL,
        "high": NotificationPriority.HIGH,
        "urgent": NotificationPriority.URGENT
    }
    priority = priority_map.get(data.priority, NotificationPriority.NORMAL)
    
    # Map channel strings to enums
    channels = []
    for ch in data.channels:
        try:
            channels.append(NotificationChannel(ch))
        except ValueError:
            pass
    
    if not channels:
        channels = [NotificationChannel.IN_APP]
    
    payload = NotificationPayload(
        title=data.title,
        message=data.message,
        priority=priority,
        link=data.link
    )
    
    result = await send_notification(
        license["license_id"],
        payload,
        channels
    )
    
    return result


# ============ Slack Setup Guide ============

@router.get("/guides/slack")
async def get_slack_guide():
    """Get Slack webhook setup guide"""
    return {
        "guide": """
## كيفية ربط Slack بالمدير

### الخطوة 1: إنشاء تطبيق Slack
1. اذهب إلى https://api.slack.com/apps
2. اضغط على "Create New App"
3. اختر "From scratch"
4. أدخل اسم التطبيق (مثلاً: "المدير")
5. اختر مساحة العمل (Workspace)

### الخطوة 2: إضافة Webhook
1. من القائمة الجانبية، اختر "Incoming Webhooks"
2. فعّل "Activate Incoming Webhooks"
3. اضغط "Add New Webhook to Workspace"
4. اختر القناة التي تريد إرسال الإشعارات إليها
5. اضغط "Allow"

### الخطوة 3: نسخ رابط Webhook
1. ستظهر لك قائمة بالـ Webhooks
2. انسخ الرابط الذي يبدأ بـ "https://hooks.slack.com/services/"
3. الصق الرابط في إعدادات المدير

### ملاحظات
- الرابط سري، لا تشاركه مع أحد
- يمكنك إنشاء webhooks متعددة لقنوات مختلفة
- الإشعارات ستظهر في القناة التي اخترتها
        """,
        "webhook_example": "https://hooks.slack.com/services/YOUR_WORKSPACE_ID/YOUR_CHANNEL_ID/YOUR_WEBHOOK_TOKEN"
    }


# ============ Discord Setup Guide ============

@router.get("/guides/discord")
async def get_discord_guide():
    """Get Discord webhook setup guide"""
    return {
        "guide": """
## كيفية ربط Discord بالمدير

### الخطوة 1: فتح إعدادات السيرفر
1. افتح سيرفر Discord الخاص بك
2. اضغط على اسم السيرفر في الأعلى
3. اختر "Server Settings" (إعدادات السيرفر)

### الخطوة 2: إنشاء Webhook
1. من القائمة الجانبية، اختر "Integrations"
2. اضغط على "Webhooks"
3. اضغط "New Webhook"
4. اختر اسماً للـ Webhook (مثلاً: "المدير")
5. اختر القناة التي تريد إرسال الإشعارات إليها

### الخطوة 3: نسخ رابط Webhook
1. اضغط على الـ Webhook الذي أنشأته
2. اضغط "Copy Webhook URL"
3. الصق الرابط في إعدادات المدير

### ملاحظات
- الرابط سري، لا تشاركه مع أحد
- يمكنك تخصيص صورة الـ Webhook واسمه
- الإشعارات ستظهر في القناة التي اخترتها
        """,
        "webhook_example": "https://discord.com/api/webhooks/YOUR_WEBHOOK_ID/YOUR_WEBHOOK_TOKEN"
    }


# Tables will be initialized in main.py lifespan function
# No automatic initialization on import to avoid database connection issues


# ============ Web Push Notification Endpoints ============

class PushSubscription(BaseModel):
    """Browser push subscription info"""
    endpoint: str = Field(..., description="Push service endpoint URL")
    keys: dict = Field(..., description="Keys object with p256dh and auth")


@router.get("/push/vapid-key")
async def get_vapid_public_key():
    """Get VAPID public key for frontend push subscription."""
    from services.push_service import get_vapid_public_key
    
    public_key = get_vapid_public_key()
    if not public_key:
        raise HTTPException(
            status_code=503,
            detail="Push notifications not configured. VAPID keys missing."
        )
    
    return {"publicKey": public_key}


@router.post("/push/subscribe")
async def subscribe_push(
    data: PushSubscription,
    license: dict = Depends(get_license_from_header)
):
    """Subscribe to Web Push notifications."""
    from services.push_service import save_push_subscription, WEBPUSH_AVAILABLE
    
    if not WEBPUSH_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="Push notifications not available. pywebpush not installed."
        )
    
    subscription_info = {
        "endpoint": data.endpoint,
        "keys": data.keys
    }
    
    subscription_id = await save_push_subscription(
        license_id=license["license_id"],
        subscription_info=subscription_info,
        user_agent=None  # Could extract from request headers
    )
    
    return {
        "success": True,
        "subscription_id": subscription_id,
        "message": "تم تفعيل إشعارات المتصفح بنجاح"
    }


@router.delete("/push/unsubscribe")
async def unsubscribe_push(
    data: PushSubscription,
    license: dict = Depends(get_license_from_header)
):
    """Unsubscribe from Web Push notifications."""
    from services.push_service import remove_push_subscription
    
    await remove_push_subscription(data.endpoint)
    
    return {
        "success": True,
        "message": "تم إلغاء تفعيل إشعارات المتصفح"
    }


@router.post("/push/test")
async def test_push_notification(
    license: dict = Depends(get_license_from_header)
):
    """Send a test push notification to all subscribed devices."""
    from services.push_service import send_push_to_license
    
    sent_count = await send_push_to_license(
        license_id=license["license_id"],
        title="🔔 إشعار تجريبي",
        message="تم تفعيل إشعارات المتصفح بنجاح!",
        link="/dashboard",
        tag="test-notification"
    )
    
    if sent_count == 0:
        return {
            "success": False,
            "message": "لا توجد أجهزة مشتركة في الإشعارات"
        }
    
    return {
        "success": True,
        "sent_count": sent_count,
        "message": f"تم إرسال الإشعار إلى {sent_count} جهاز"
    }
