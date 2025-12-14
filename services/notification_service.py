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

# Database configuration
DB_TYPE = os.getenv("DB_TYPE", "sqlite").lower()
DATABASE_PATH = os.getenv("DATABASE_PATH", "almudeer.db")
DATABASE_URL = os.getenv("DATABASE_URL")

# Import appropriate database driver
if DB_TYPE == "postgresql":
    try:
        import asyncpg
        POSTGRES_AVAILABLE = True
        aiosqlite = None
    except ImportError:
        raise ImportError(
            "PostgreSQL selected but asyncpg not installed. "
            "Install with: pip install asyncpg"
        )
else:
    import aiosqlite
    POSTGRES_AVAILABLE = False

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


async def init_notification_tables():
    """Initialize notification-related tables"""
    if DB_TYPE == "postgresql" and POSTGRES_AVAILABLE:
        if not DATABASE_URL:
            raise ValueError("DATABASE_URL is required for PostgreSQL")
        conn = await asyncpg.connect(DATABASE_URL)
        try:
            # Notification rules table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS notification_rules (
                    id SERIAL PRIMARY KEY,
                    license_key_id INTEGER NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    condition_type VARCHAR(255) NOT NULL,
                    condition_value TEXT NOT NULL,
                    channels TEXT NOT NULL,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT NOW(),
                    FOREIGN KEY (license_key_id) REFERENCES license_keys(id)
                )
            """)
            
            # External integrations (Slack, Discord)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS notification_integrations (
                    id SERIAL PRIMARY KEY,
                    license_key_id INTEGER NOT NULL,
                    channel_type VARCHAR(255) NOT NULL,
                    webhook_url TEXT NOT NULL,
                    channel_name VARCHAR(255),
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT NOW(),
                    FOREIGN KEY (license_key_id) REFERENCES license_keys(id),
                    UNIQUE(license_key_id, channel_type)
                )
            """)
            
            # Notification log
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS notification_log (
                    id SERIAL PRIMARY KEY,
                    license_key_id INTEGER NOT NULL,
                    channel VARCHAR(255) NOT NULL,
                    priority VARCHAR(255) NOT NULL,
                    title TEXT NOT NULL,
                    message TEXT NOT NULL,
                    status VARCHAR(255) DEFAULT 'sent',
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT NOW(),
                    FOREIGN KEY (license_key_id) REFERENCES license_keys(id)
                )
            """)
        finally:
            await conn.close()
    else:
        async with aiosqlite.connect(DATABASE_PATH) as db:
            # Notification rules table
            await db.execute("""
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
            """)
            
            # External integrations (Slack, Discord)
            await db.execute("""
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
            """)
            
            # Notification log
            await db.execute("""
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
            """)
            
            await db.commit()
    print("OK Notification tables initialized")


# ============ Integration Management ============

async def save_integration(
    license_id: int,
    channel_type: str,
    webhook_url: str,
    channel_name: str = None
) -> int:
    """Save or update notification integration"""
    if DB_TYPE == "postgresql" and POSTGRES_AVAILABLE:
        if not DATABASE_URL:
            raise ValueError("DATABASE_URL is required for PostgreSQL")
        conn = await asyncpg.connect(DATABASE_URL)
        try:
            row = await conn.fetchrow("""
                INSERT INTO notification_integrations 
                (license_key_id, channel_type, webhook_url, channel_name)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT(license_key_id, channel_type) 
                DO UPDATE SET webhook_url = $3, channel_name = $4, is_active = TRUE
                RETURNING id
            """, license_id, channel_type, webhook_url, channel_name)
            return row['id']
        finally:
            await conn.close()
    else:
        async with aiosqlite.connect(DATABASE_PATH) as db:
            cursor = await db.execute("""
                INSERT INTO notification_integrations 
                (license_key_id, channel_type, webhook_url, channel_name)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(license_key_id, channel_type) 
                DO UPDATE SET webhook_url = ?, channel_name = ?, is_active = TRUE
            """, (license_id, channel_type, webhook_url, channel_name, webhook_url, channel_name))
            await db.commit()
            return cursor.lastrowid


async def get_integration(license_id: int, channel_type: str) -> Optional[dict]:
    """Get integration config"""
    if DB_TYPE == "postgresql" and POSTGRES_AVAILABLE:
        if not DATABASE_URL:
            raise ValueError("DATABASE_URL is required for PostgreSQL")
        conn = await asyncpg.connect(DATABASE_URL)
        try:
            row = await conn.fetchrow("""
                SELECT * FROM notification_integrations 
                WHERE license_key_id = $1 AND channel_type = $2 AND is_active = TRUE
            """, license_id, channel_type)
            return dict(row) if row else None
        finally:
            await conn.close()
    else:
        async with aiosqlite.connect(DATABASE_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("""
                SELECT * FROM notification_integrations 
                WHERE license_key_id = ? AND channel_type = ? AND is_active = TRUE
            """, (license_id, channel_type)) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None


async def get_all_integrations(license_id: int) -> List[dict]:
    """Get all integrations for a license"""
    if DB_TYPE == "postgresql" and POSTGRES_AVAILABLE:
        if not DATABASE_URL:
            raise ValueError("DATABASE_URL is required for PostgreSQL")
        conn = await asyncpg.connect(DATABASE_URL)
        try:
            rows = await conn.fetch("""
                SELECT * FROM notification_integrations 
                WHERE license_key_id = $1
            """, license_id)
            return [dict(row) for row in rows]
        finally:
            await conn.close()
    else:
        async with aiosqlite.connect(DATABASE_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("""
                SELECT * FROM notification_integrations 
                WHERE license_key_id = ?
            """, (license_id,)) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]


async def disable_integration(license_id: int, channel_type: str) -> bool:
    """Disable an integration"""
    if DB_TYPE == "postgresql" and POSTGRES_AVAILABLE:
        if not DATABASE_URL:
            raise ValueError("DATABASE_URL is required for PostgreSQL")
        conn = await asyncpg.connect(DATABASE_URL)
        try:
            await conn.execute("""
                UPDATE notification_integrations 
                SET is_active = FALSE 
                WHERE license_key_id = $1 AND channel_type = $2
            """, license_id, channel_type)
            return True
        finally:
            await conn.close()
    else:
        async with aiosqlite.connect(DATABASE_PATH) as db:
            await db.execute("""
                UPDATE notification_integrations 
                SET is_active = FALSE 
                WHERE license_key_id = ? AND channel_type = ?
            """, (license_id, channel_type))
            await db.commit()
            return True


# ============ Notification Rules ============

async def create_rule(
    license_id: int,
    name: str,
    condition_type: str,
    condition_value: str,
    channels: List[str]
) -> int:
    """Create a notification rule"""
    if DB_TYPE == "postgresql" and POSTGRES_AVAILABLE:
        if not DATABASE_URL:
            raise ValueError("DATABASE_URL is required for PostgreSQL")
        conn = await asyncpg.connect(DATABASE_URL)
        try:
            row = await conn.fetchrow("""
                INSERT INTO notification_rules 
                (license_key_id, name, condition_type, condition_value, channels)
                VALUES ($1, $2, $3, $4, $5)
                RETURNING id
            """, license_id, name, condition_type, condition_value, json.dumps(channels))
            return row['id']
        finally:
            await conn.close()
    else:
        async with aiosqlite.connect(DATABASE_PATH) as db:
            cursor = await db.execute("""
                INSERT INTO notification_rules 
                (license_key_id, name, condition_type, condition_value, channels)
                VALUES (?, ?, ?, ?, ?)
            """, (license_id, name, condition_type, condition_value, json.dumps(channels)))
            await db.commit()
            return cursor.lastrowid


async def get_rules(license_id: int) -> List[dict]:
    """Get all notification rules"""
    if DB_TYPE == "postgresql" and POSTGRES_AVAILABLE:
        if not DATABASE_URL:
            raise ValueError("DATABASE_URL is required for PostgreSQL")
        conn = await asyncpg.connect(DATABASE_URL)
        try:
            rows = await conn.fetch("""
                SELECT * FROM notification_rules 
                WHERE license_key_id = $1 AND is_active = TRUE
            """, license_id)
            return [
                {**dict(row), "channels": json.loads(row["channels"])}
                for row in rows
            ]
        finally:
            await conn.close()
    else:
        async with aiosqlite.connect(DATABASE_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("""
                SELECT * FROM notification_rules 
                WHERE license_key_id = ? AND is_active = TRUE
            """, (license_id,)) as cursor:
                rows = await cursor.fetchall()
                return [
                    {**dict(row), "channels": json.loads(row["channels"])}
                    for row in rows
                ]


async def delete_rule(license_id: int, rule_id: int) -> bool:
    """Delete a notification rule"""
    if DB_TYPE == "postgresql" and POSTGRES_AVAILABLE:
        if not DATABASE_URL:
            raise ValueError("DATABASE_URL is required for PostgreSQL")
        conn = await asyncpg.connect(DATABASE_URL)
        try:
            await conn.execute("""
                DELETE FROM notification_rules 
                WHERE id = $1 AND license_key_id = $2
            """, rule_id, license_id)
            return True
        finally:
            await conn.close()
    else:
        async with aiosqlite.connect(DATABASE_PATH) as db:
            await db.execute("""
                DELETE FROM notification_rules 
                WHERE id = ? AND license_key_id = ?
            """, (rule_id, license_id))
            await db.commit()
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
    
    # In-app notification
    if NotificationChannel.IN_APP in channels:
        from models import create_notification
        try:
            await create_notification(
                license_id=license_id,
                notification_type=payload.priority.value,
                title=payload.title,
                message=payload.message,
                priority=payload.priority.value,
                link=payload.link
            )
            results["in_app"] = {"success": True}
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
    """Log notification to database"""
    if DB_TYPE == "postgresql" and POSTGRES_AVAILABLE:
        if not DATABASE_URL:
            raise ValueError("DATABASE_URL is required for PostgreSQL")
        conn = await asyncpg.connect(DATABASE_URL)
        try:
            for channel, result in results.items():
                await conn.execute("""
                    INSERT INTO notification_log 
                    (license_key_id, channel, priority, title, message, status, error_message)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                """, (
                    license_id,
                    channel,
                    payload.priority.value,
                    payload.title,
                    payload.message,
                    "sent" if result.get("success") else "failed",
                    result.get("error")
                ))
        finally:
            await conn.close()
    else:
        async with aiosqlite.connect(DATABASE_PATH) as db:
            for channel, result in results.items():
                await db.execute("""
                    INSERT INTO notification_log 
                    (license_key_id, channel, priority, title, message, status, error_message)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    license_id,
                    channel,
                    payload.priority.value,
                    payload.title,
                    payload.message,
                    "sent" if result.get("success") else "failed",
                    result.get("error")
                ))
            await db.commit()


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
            message_text = message_data.get("body", "") + " " + message_data.get("subject", "")
            if any(kw.strip() in message_text for kw in keywords):
                should_trigger = True
        
        elif rule["condition_type"] == "vip_customer":
            if message_data.get("is_vip"):
                should_trigger = True
        
        # Trigger notification if condition met
        if should_trigger:
            payload = NotificationPayload(
                title=f"تنبيه: {rule['name']}",
                message=f"رسالة جديدة من {message_data.get('sender_name', 'مرسل غير معروف')}: {message_data.get('body', '')[:100]}...",
                priority=NotificationPriority.HIGH if rule["condition_type"] in ["urgency", "sentiment"] else NotificationPriority.NORMAL,
                link=f"/dashboard/inbox",
                metadata={
                    "المرسل": message_data.get("sender_name", "-"),
                    "القناة": message_data.get("channel", "-"),
                    "النوع": message_data.get("intent", "-")
                }
            )
            
            channels = [
                NotificationChannel(ch) for ch in rule["channels"]
                if ch in [e.value for e in NotificationChannel]
            ]
            
            result = await send_notification(license_id, payload, channels)
            notifications_sent.append({
                "rule": rule["name"],
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

