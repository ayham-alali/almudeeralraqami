"""
Al-Mudeer Smart Notification Service
Handles priority alerts, Slack/Discord integration, and notification rules
"""

import os
import json
import asyncio
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
import httpx

# Unified DB helper (works for both SQLite and PostgreSQL)
from db_helper import (
    get_db,
    execute_sql,
    fetch_one,
    fetch_all,
    commit_db,
    DB_TYPE,
)

# Webhook URLs from environment
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")


class NotificationChannel(Enum):
    IN_APP = "in_app"
    EMAIL = "email"
    SLACK = "slack"
    DISCORD = "discord"
    WEBHOOK = "webhook"


class NotificationPriority(Enum):
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    URGENT = "urgent"


@dataclass
class NotificationRule:
    id: int
    license_id: int
    name: str
    condition_type: str  # sentiment, urgency, keyword, vip_customer
    condition_value: str
    channels: List[NotificationChannel]
    is_active: bool


@dataclass
class NotificationPayload:
    title: str
    message: str
    priority: NotificationPriority
    link: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    image: Optional[str] = None


async def init_notification_tables():
    """Initialize notification-related tables (DB agnostic via db_helper)."""
    async with get_db() as db:
        # Notification rules table
        await execute_sql(
            db,
            """
            CREATE TABLE IF NOT EXISTS notification_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                license_key_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                condition_type TEXT NOT NULL,
                condition_value TEXT NOT NULL,
                channels TEXT NOT NULL,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (license_key_id) REFERENCES license_keys(id)
            )
            """,
        )

        # External integrations (Slack, Discord, generic webhooks)
        await execute_sql(
            db,
            """
            CREATE TABLE IF NOT EXISTS notification_integrations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                license_key_id INTEGER NOT NULL,
                channel_type TEXT NOT NULL,
                webhook_url TEXT NOT NULL,
                channel_name TEXT,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (license_key_id) REFERENCES license_keys(id),
                UNIQUE(license_key_id, channel_type)
            )
            """,
        )

        # Notification log
        await execute_sql(
            db,
            """
            CREATE TABLE IF NOT EXISTS notification_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                license_key_id INTEGER NOT NULL,
                channel TEXT NOT NULL,
                priority TEXT NOT NULL,
                title TEXT NOT NULL,
                message TEXT NOT NULL,
                status TEXT DEFAULT 'sent',
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (license_key_id) REFERENCES license_keys(id)
            )
            """,
        )

        # Notification analytics table for delivery/open tracking
        await execute_sql(
            db,
            """
            CREATE TABLE IF NOT EXISTS notification_analytics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                notification_id INTEGER,
                license_key_id INTEGER NOT NULL,
                platform TEXT DEFAULT 'unknown',
                delivered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                opened_at TIMESTAMP,
                notification_type TEXT,
                FOREIGN KEY (license_key_id) REFERENCES license_keys(id)
            )
            """,
        )
        
        # Index for efficient analytics queries
        await execute_sql(
            db,
            """
            CREATE INDEX IF NOT EXISTS idx_analytics_license
            ON notification_analytics(license_key_id)
            """,
        )
        
        await execute_sql(
            db,
            """
            CREATE INDEX IF NOT EXISTS idx_analytics_dates
            ON notification_analytics(delivered_at, opened_at)
            """,
        )

        await commit_db(db)
    print("OK Notification tables initialized")


# ============ Integration Management ============

async def save_integration(
    license_id: int,
    channel_type: str,
    webhook_url: str,
    channel_name: str = None
) -> int:
    """Save or update notification integration (DB agnostic)."""
    async with get_db() as db:
        # Use INSERT OR REPLACE-style logic via ON CONFLICT emulation
        await execute_sql(
            db,
            """
            INSERT INTO notification_integrations 
                (license_key_id, channel_type, webhook_url, channel_name, is_active)
            VALUES (?, ?, ?, ?, TRUE)
            ON CONFLICT(license_key_id, channel_type) 
            DO UPDATE SET webhook_url = ?, channel_name = ?, is_active = TRUE
            """,
            [
                license_id,
                channel_type,
                webhook_url,
                channel_name,
                webhook_url,
                channel_name,
            ],
        )

        row = await fetch_one(
            db,
            """
            SELECT id FROM notification_integrations
            WHERE license_key_id = ? AND channel_type = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            [license_id, channel_type],
        )
        await commit_db(db)
        return row["id"] if row else 0


async def get_integration(license_id: int, channel_type: str) -> Optional[dict]:
    """Get integration config (DB agnostic)."""
    async with get_db() as db:
        row = await fetch_one(
            db,
            """
            SELECT * FROM notification_integrations 
            WHERE license_key_id = ? AND channel_type = ? AND is_active = TRUE
            """,
            [license_id, channel_type],
        )
        return row


async def get_all_integrations(license_id: int) -> List[dict]:
    """Get all integrations for a license (DB agnostic)."""
    async with get_db() as db:
        rows = await fetch_all(
            db,
            """
            SELECT * FROM notification_integrations 
            WHERE license_key_id = ?
            """,
            [license_id],
        )
        return rows


async def disable_integration(license_id: int, channel_type: str) -> bool:
    """Disable an integration (DB agnostic)."""
    async with get_db() as db:
        await execute_sql(
            db,
            """
            UPDATE notification_integrations 
            SET is_active = FALSE 
            WHERE license_key_id = ? AND channel_type = ?
            """,
            [license_id, channel_type],
        )
        await commit_db(db)
        return True


# ============ Notification Rules ============

async def create_rule(
    license_id: int,
    name: str,
    condition_type: str,
    condition_value: str,
    channels: List[str]
) -> int:
    """Create a notification rule (DB agnostic)."""
    async with get_db() as db:
        await execute_sql(
            db,
            """
            INSERT INTO notification_rules 
                (license_key_id, name, condition_type, condition_value, channels)
            VALUES (?, ?, ?, ?, ?)
            """,
            [license_id, name, condition_type, condition_value, json.dumps(channels)],
        )

        row = await fetch_one(
            db,
            """
            SELECT id FROM notification_rules
            WHERE license_key_id = ? AND name = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            [license_id, name],
        )
        await commit_db(db)
        return row["id"] if row else 0


async def get_rules(license_id: int) -> List[dict]:
    """Get all notification rules (DB agnostic), auto-creating defaults if missing."""
    async with get_db() as db:
        # Fetch ALL rules (active and inactive) to check existence
        all_rows = await fetch_all(
            db,
            "SELECT * FROM notification_rules WHERE license_key_id = ?",
            [license_id],
        )

        # Check for default "waiting_for_reply" rule
        has_waiting_rule = any(row["condition_type"] == "waiting_for_reply" for row in all_rows)
        
        if not has_waiting_rule:
            # Auto-create default rule
            try:
                await execute_sql(
                    db,
                    """
                    INSERT INTO notification_rules (license_key_id, name, condition_type, condition_value, channels, is_active)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    [
                        license_id, 
                        "تنبيه بانتظار الرد", 
                        "waiting_for_reply", 
                        "true", 
                        json.dumps(["in_app"]), 
                        True
                    ]
                )
                await commit_db(db)
                
                # Re-fetch to get the new rule with ID
                all_rows = await fetch_all(
                    db,
                    "SELECT * FROM notification_rules WHERE license_key_id = ?",
                    [license_id],
                )
            except Exception as e:
                # Log error but don't fail the request
                print(f"Error creating default rule: {e}")

        # Return only ACTIVE rules
        return [
            {**row, "channels": json.loads(row["channels"])}
            for row in all_rows
            if row["is_active"]
        ]


async def delete_rule(license_id: int, rule_id: int) -> bool:
    """Delete a notification rule (DB agnostic)."""
    async with get_db() as db:
        await execute_sql(
            db,
            """
            DELETE FROM notification_rules 
            WHERE id = ? AND license_key_id = ?
            """,
            [rule_id, license_id],
        )
        await commit_db(db)
        return True


# ============ Slack Integration ============

async def send_slack_notification(
    webhook_url: str,
    payload: NotificationPayload
) -> dict:
    """Send notification to Slack"""
    
    # Emoji based on priority
    priority_emoji = {
        NotificationPriority.LOW: "ℹ️",
        NotificationPriority.NORMAL: "📬",
        NotificationPriority.HIGH: "⚠️",
        NotificationPriority.URGENT: "🚨"
    }
    
    # Color based on priority
    priority_color = {
        NotificationPriority.LOW: "#6b7280",
        NotificationPriority.NORMAL: "#3b82f6",
        NotificationPriority.HIGH: "#f59e0b",
        NotificationPriority.URGENT: "#ef4444"
    }
    
    emoji = priority_emoji.get(payload.priority, "📬")
    color = priority_color.get(payload.priority, "#3b82f6")
    
    # Build Slack message
    slack_message = {
        "attachments": [
            {
                "color": color,
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": f"{emoji} {payload.title}",
                            "emoji": True
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": payload.message
                        }
                    }
                ]
            }
        ]
    }
    
    # Add link button if provided
    if payload.link:
        slack_message["attachments"][0]["blocks"].append({
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "فتح في المدير",
                        "emoji": True
                    },
                    "url": payload.link,
                    "style": "primary"
                }
            ]
        })
    
    # Add metadata if provided
    if payload.metadata:
        fields = []
        for key, value in payload.metadata.items():
            fields.append({
                "type": "mrkdwn",
                "text": f"*{key}:* {value}"
            })
        slack_message["attachments"][0]["blocks"].append({
            "type": "section",
            "fields": fields[:10]  # Slack limit
        })
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                webhook_url,
                json=slack_message,
                timeout=10.0
            )
            
            return {
                "success": response.status_code == 200,
                "status_code": response.status_code,
                "error": None if response.status_code == 200 else response.text
            }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


# ============ Discord Integration ============

async def send_discord_notification(
    webhook_url: str,
    payload: NotificationPayload
) -> dict:
    """Send notification to Discord"""
    
    # Color based on priority (Discord uses decimal)
    priority_color = {
        NotificationPriority.LOW: 6908265,  # Gray
        NotificationPriority.NORMAL: 3447003,  # Blue
        NotificationPriority.HIGH: 16098851,  # Orange
        NotificationPriority.URGENT: 15548997  # Red
    }
    
    color = priority_color.get(payload.priority, 3447003)
    
    # Build Discord embed
    embed = {
        "title": payload.title,
        "description": payload.message,
        "color": color,
        "timestamp": datetime.utcnow().isoformat(),
        "footer": {
            "text": "المدير - Al-Mudeer"
        }
    }
    
    # Add fields from metadata
    if payload.metadata:
        embed["fields"] = [
            {"name": key, "value": str(value), "inline": True}
            for key, value in list(payload.metadata.items())[:25]  # Discord limit
        ]
    
    # Add link
    if payload.link:
        embed["url"] = payload.link
    
    discord_message = {
        "embeds": [embed]
    }
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                webhook_url,
                json=discord_message,
                timeout=10.0
            )
            
            return {
                "success": response.status_code in [200, 204],
                "status_code": response.status_code,
                "error": None if response.status_code in [200, 204] else response.text
            }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


# ============ Generic Webhook ============

async def send_webhook_notification(
    webhook_url: str,
    payload: NotificationPayload
) -> dict:
    """Send notification to generic webhook"""
    
    webhook_payload = {
        "event": "almudeer_notification",
        "timestamp": datetime.utcnow().isoformat(),
        "data": {
            "title": payload.title,
            "message": payload.message,
            "priority": payload.priority.value,
            "link": payload.link,
            "metadata": payload.metadata or {}
        }
    }
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                webhook_url,
                json=webhook_payload,
                timeout=10.0
            )
            
            return {
                "success": response.status_code < 400,
                "status_code": response.status_code,
                "error": None if response.status_code < 400 else response.text
            }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


# ============ Main Notification Handler ============

async def send_notification(
    license_id: int,
    payload: NotificationPayload,
    channels: List[NotificationChannel] = None
) -> dict:
    """
    Main function to send notifications through multiple channels
    """
    results = {}
    
    # Default to in-app only
    if channels is None:
        channels = [NotificationChannel.IN_APP]
    
    # In-app notification (Database entry)
    if NotificationChannel.IN_APP in channels:
        from models import create_notification
        try:
            # 1. Save to database (Inbox)
            await create_notification(
                license_id=license_id,
                notification_type=payload.priority.value,
                title=payload.title,
                message=payload.message,
                priority=payload.priority.value,
                link=payload.link
            )
            results["in_app"] = {"success": True}

            # 2. Trigger Mobile Push (FCM)
            # We assume IN_APP implies a desire to reach the user's device
            try:
                from services.fcm_mobile_service import send_fcm_to_license
                fcm_count = await send_fcm_to_license(
                    license_id=license_id,
                    title=payload.title,
                    body=payload.message,
                    link=payload.link,
                    data=payload.metadata,
                    image=payload.image
                )
                results["mobile_push"] = {"success": True, "count": fcm_count}
            except Exception as e:
                # Log but don't fail the whole request
                results["mobile_push"] = {"success": False, "error": str(e)}

            # 3. Trigger Web Push
            try:
                from services.push_service import send_push_to_license, WEBPUSH_AVAILABLE
                if WEBPUSH_AVAILABLE:
                    web_count = await send_push_to_license(
                        license_id=license_id,
                        title=payload.title,
                        message=payload.message,
                        link=payload.link or "/dashboard/notifications"
                    )
                    results["web_push"] = {"success": True, "count": web_count}
            except Exception as e:
                results["web_push"] = {"success": False, "error": str(e)}

        except Exception as e:
            results["in_app"] = {"success": False, "error": str(e)}
    
    # Slack
    if NotificationChannel.SLACK in channels:
        integration = await get_integration(license_id, "slack")
        if integration:
            result = await send_slack_notification(
                integration["webhook_url"],
                payload
            )
            results["slack"] = result
        else:
            results["slack"] = {"success": False, "error": "Integration not configured"}
    
    # Discord
    if NotificationChannel.DISCORD in channels:
        integration = await get_integration(license_id, "discord")
        if integration:
            result = await send_discord_notification(
                integration["webhook_url"],
                payload
            )
            results["discord"] = result
        else:
            results["discord"] = {"success": False, "error": "Integration not configured"}
    
    # Generic webhook
    if NotificationChannel.WEBHOOK in channels:
        integration = await get_integration(license_id, "webhook")
        if integration:
            result = await send_webhook_notification(
                integration["webhook_url"],
                payload
            )
            results["webhook"] = result
        else:
            results["webhook"] = {"success": False, "error": "Integration not configured"}
    
    # Log notification
    await log_notification(license_id, payload, results)
    
    return {
        "success": any(r.get("success") for r in results.values()),
        "channels": results
    }


async def log_notification(
    license_id: int,
    payload: NotificationPayload,
    results: dict
):
    """Log notification to database (DB agnostic)."""
    async with get_db() as db:
        for channel, result in results.items():
            await execute_sql(
                db,
                """
                INSERT INTO notification_log 
                    (license_key_id, channel, priority, title, message, status, error_message)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    license_id,
                    channel,
                    payload.priority.value,
                    payload.title,
                    payload.message,
                    "sent" if result.get("success") else "failed",
                    result.get("error"),
                ],
            )
        await commit_db(db)


# ============ Smart Notification Triggers ============

async def process_message_notifications(
    license_id: int,
    message_data: dict
) -> List[dict]:
    """
    Process incoming message and trigger appropriate notifications
    based on configured rules
    """
    notifications_sent = []
    
    # Get all active rules
    rules = await get_rules(license_id)
    
    # Collect all matching rules first
    matched_rules = []

    for rule in rules:
        should_trigger = False
        
        # Check rule condition
        if rule["condition_type"] == "sentiment":
            if rule["condition_value"] == "negative" and message_data.get("sentiment") == "سلبي":
                should_trigger = True
        
        elif rule["condition_type"] == "urgency":
            if rule["condition_value"] == "urgent" and message_data.get("urgency") == "عاجل":
                should_trigger = True
        
        elif rule["condition_type"] == "keyword":
            keywords = rule["condition_value"].split(",")
            message_text = (message_data.get("body", "") + " " + message_data.get("subject", "")).lower()
            if any(kw.strip().lower() in message_text for kw in keywords):
                should_trigger = True
        
        elif rule["condition_type"] == "vip_customer":
            if message_data.get("is_vip"):
                should_trigger = True

        elif rule["condition_type"] == "waiting_for_reply":
             # Trigger for any valid incoming message
             should_trigger = True
        
        if should_trigger:
            matched_rules.append(rule)
            
    if not matched_rules:
        return []

    # === Deduplication Logic ===
    # 1. Separate generic vs specific rules
    specific_rules = [r for r in matched_rules if r["condition_type"] != "waiting_for_reply"]
    generic_rules = [r for r in matched_rules if r["condition_type"] == "waiting_for_reply"]
    
    final_rule = None
    
    # 2. If we have ANY specific rule, ignore the generic "waiting_for_reply"
    if specific_rules:
        # 3. If multiple specific rules, prioritize by type (Urgency > Sentiment > VIP > Keyword)
        # We can implement a simple priority map
        priority_map = {
            "urgency": 100,
            "sentiment": 90,
            "vip_customer": 80,
            "keyword": 70
        }
        # Sort by priority desc
        specific_rules.sort(key=lambda x: priority_map.get(x["condition_type"], 0), reverse=True)
        final_rule = specific_rules[0]
    elif generic_rules:
        # Only generic matched
        final_rule = generic_rules[0]
        
    if final_rule:
        # Standard WhatsApp-style notification
        # Title: Sender Name
        # Body: Message Preview
        sender_name = message_data.get('sender_name', 'New Message')
        body_preview = message_data.get('body', '')[:100]
        
        # Try to get profile picture
        profile_pic = None
        sender_contact = message_data.get("sender_contact") or message_data.get("sender_id")
        
        if sender_contact:
            try:
                from models.customers import get_or_create_customer
                # We use get_or_create just to be safe and get the record, 
                # but typically it should exist if they messaged us
                customer = await get_or_create_customer(
                    license_id=license_id,
                    phone=sender_contact if "@" not in str(sender_contact) else None,
                    email=sender_contact if "@" in str(sender_contact) else None,
                    name=sender_name
                )
                if customer:
                    profile_pic = customer.get("profile_pic_url")
            except Exception as e:
                # Don't fail notification if customer fetch fails
                pass

        payload = NotificationPayload(
            title=sender_name,
            message=body_preview,
            priority=NotificationPriority.NORMAL, # Always normal priority as requested
            link=f"/dashboard/inbox",
            metadata={
                "sender": sender_name,
                "channel": message_data.get("channel", "-"),
                "type": "message"
            },
            image=profile_pic
        )
        
        channels = [
            NotificationChannel(ch) for ch in final_rule["channels"]
            if ch in [e.value for e in NotificationChannel]
        ]
        
        result = await send_notification(license_id, payload, channels)
        notifications_sent.append({
            "rule": final_rule["name"],
            "result": result
        })
    
    return notifications_sent


# ============ Predefined Alert Types ============

async def send_urgent_message_alert(
    license_id: int,
    sender_name: str,
    message_preview: str
):
    """Send alert for urgent messages"""
    payload = NotificationPayload(
        title="🚨 رسالة عاجلة",
        message=f"رسالة عاجلة من {sender_name}:\n{message_preview[:200]}",
        priority=NotificationPriority.URGENT,
        link="/dashboard/inbox"
    )
    return await send_notification(
        license_id,
        payload,
        [NotificationChannel.IN_APP, NotificationChannel.SLACK, NotificationChannel.DISCORD]
    )


async def send_negative_sentiment_alert(
    license_id: int,
    sender_name: str,
    message_preview: str
):
    """Send alert for negative sentiment messages"""
    payload = NotificationPayload(
        title="⚠️ عميل غاضب",
        message=f"تم اكتشاف رسالة سلبية من {sender_name}:\n{message_preview[:200]}",
        priority=NotificationPriority.HIGH,
        link="/dashboard/inbox"
    )
    return await send_notification(
        license_id,
        payload,
        [NotificationChannel.IN_APP, NotificationChannel.SLACK]
    )


async def send_vip_customer_alert(
    license_id: int,
    customer_name: str,
    message_preview: str
):
    """Send alert for VIP customer messages"""
    payload = NotificationPayload(
        title="⭐ رسالة من عميل VIP",
        message=f"رسالة جديدة من العميل المميز {customer_name}:\n{message_preview[:200]}",
        priority=NotificationPriority.HIGH,
        link="/dashboard/inbox"
    )
    return await send_notification(
        license_id,
        payload,
        [NotificationChannel.IN_APP, NotificationChannel.SLACK]
    )



async def send_daily_summary(license_id: int, stats: dict):
    """Send daily summary notification"""
    payload = NotificationPayload(
        title="📊 ملخص اليوم",
        message=f"تمت معالجة {stats.get('messages', 0)} رسالة ووفرت {stats.get('time_saved', 0)} دقيقة",
        priority=NotificationPriority.LOW,
        link="/dashboard/overview",
        metadata={
            "الرسائل": str(stats.get('messages', 0)),
            "الردود": str(stats.get('replies', 0)),
            "الوقت الموفر": f"{stats.get('time_saved', 0)} دقيقة"
        }
    )
    return await send_notification(
        license_id,
        payload,
        [NotificationChannel.IN_APP, NotificationChannel.SLACK]
    )


async def send_tool_action_alert(
    license_id: int,
    action_name: str,
    details: str
):
    """Send alert for sensitive agent actions (Tools)"""
    payload = NotificationPayload(
        title=f"🤖 إجراء تلقائي: {action_name}",
        message=f"قام الوكيل الذكي بتنفيذ إجراء: {action_name}\nالتفاصيل: {details}",
        priority=NotificationPriority.NORMAL,
        link="/dashboard/crm", # Link to CRM or relevant page
        metadata={
            "الإجراء": action_name,
            "الوقت": datetime.now().strftime("%H:%M")
        }
    )
    # Default to In-App and Slack for visibility
    return await send_notification(
        license_id,
        payload,
        [NotificationChannel.IN_APP, NotificationChannel.SLACK]
    )


# ============ Test Functions ============

async def test_slack_webhook(webhook_url: str) -> dict:
    """Test Slack webhook connection"""
    payload = NotificationPayload(
        title="اختبار الاتصال",
        message="تم ربط المدير بـ Slack بنجاح! 🎉",
        priority=NotificationPriority.NORMAL
    )
    return await send_slack_notification(webhook_url, payload)


async def test_discord_webhook(webhook_url: str) -> dict:
    """Test Discord webhook connection"""
    payload = NotificationPayload(
        title="اختبار الاتصال",
        message="تم ربط المدير بـ Discord بنجاح! 🎉",
        priority=NotificationPriority.NORMAL
    )
    return await send_discord_notification(webhook_url, payload)


# ============ Notification Analytics ============

async def track_notification_delivery(
    license_id: int,
    notification_id: Optional[int] = None,
    platform: str = "unknown",
    notification_type: str = "general"
) -> int:
    """
    Track when a notification is delivered to a device.
    
    Args:
        license_id: License key ID
        notification_id: Optional notification ID from notifications table
        platform: Device platform (android, ios, web)
        notification_type: Type of notification
    
    Returns:
        Analytics record ID
    """
    async with get_db() as db:
        await execute_sql(
            db,
            """
            INSERT INTO notification_analytics 
                (license_key_id, notification_id, platform, notification_type)
            VALUES (?, ?, ?, ?)
            """,
            [license_id, notification_id, platform, notification_type]
        )
        
        row = await fetch_one(
            db,
            "SELECT MAX(id) as id FROM notification_analytics WHERE license_key_id = ?",
            [license_id]
        )
        await commit_db(db)
        return row["id"] if row else 0


async def track_notification_open(
    license_id: int,
    analytics_id: Optional[int] = None,
    notification_id: Optional[int] = None
) -> bool:
    """
    Track when a user opens/taps a notification.
    
    Can match by analytics_id (from track_notification_delivery) or notification_id.
    
    Args:
        license_id: License key ID
        analytics_id: Analytics record ID from track_notification_delivery
        notification_id: Original notification ID
    
    Returns:
        True if updated successfully
    """
    async with get_db() as db:
        if analytics_id:
            await execute_sql(
                db,
                """
                UPDATE notification_analytics 
                SET opened_at = CURRENT_TIMESTAMP
                WHERE id = ? AND license_key_id = ?
                """,
                [analytics_id, license_id]
            )
        elif notification_id:
            # Update the most recent analytics record for this notification
            await execute_sql(
                db,
                """
                UPDATE notification_analytics 
                SET opened_at = CURRENT_TIMESTAMP
                WHERE notification_id = ? AND license_key_id = ? AND opened_at IS NULL
                """,
                [notification_id, license_id]
            )
        else:
            return False
        
        await commit_db(db)
        return True


async def get_notification_stats(
    license_id: Optional[int] = None,
    days: int = 30
) -> dict:
    """
    Get notification delivery and open rate statistics.
    
    Args:
        license_id: Optional license ID to filter by (None = all licenses, admin only)
        days: Number of days to include in stats (default: 30)
    
    Returns:
        Dict with total_delivered, total_opened, open_rate, by_platform stats
    """
    async with get_db() as db:
        # Build query based on whether license_id is provided
        if license_id:
            base_where = "WHERE license_key_id = ? AND delivered_at >= datetime('now', ?)"
            params = [license_id, f"-{days} days"]
        else:
            base_where = "WHERE delivered_at >= datetime('now', ?)"
            params = [f"-{days} days"]
        
        # Total delivered
        delivered_row = await fetch_one(
            db,
            f"SELECT COUNT(*) as count FROM notification_analytics {base_where}",
            params
        )
        total_delivered = delivered_row["count"] if delivered_row else 0
        
        # Total opened
        opened_row = await fetch_one(
            db,
            f"SELECT COUNT(*) as count FROM notification_analytics {base_where} AND opened_at IS NOT NULL",
            params
        )
        total_opened = opened_row["count"] if opened_row else 0
        
        # By platform breakdown
        platform_rows = await fetch_all(
            db,
            f"""
            SELECT 
                platform,
                COUNT(*) as delivered,
                SUM(CASE WHEN opened_at IS NOT NULL THEN 1 ELSE 0 END) as opened
            FROM notification_analytics 
            {base_where}
            GROUP BY platform
            """,
            params
        )
        
        by_platform = {
            row["platform"]: {
                "delivered": row["delivered"],
                "opened": row["opened"],
                "open_rate": round((row["opened"] / row["delivered"]) * 100, 1) if row["delivered"] > 0 else 0
            }
            for row in platform_rows
        }
        
        return {
            "period_days": days,
            "total_delivered": total_delivered,
            "total_opened": total_opened,
            "open_rate": round((total_opened / total_delivered) * 100, 1) if total_delivered > 0 else 0,
            "by_platform": by_platform
        }


