"""
Al-Mudeer - Customer and Analytics Models
Customer profiles, lead scoring, analytics, preferences, notifications, and team management
"""

import os
from datetime import datetime, timedelta
from typing import Optional, List

from db_helper import get_db, execute_sql, fetch_all, fetch_one, commit_db, DB_TYPE

# For functions that still use aiosqlite directly (to be migrated)
DATABASE_PATH = os.getenv("DATABASE_PATH", "almudeer.db")
if DB_TYPE != "postgresql":
    import aiosqlite


# ============ Customer Profiles ============

async def get_or_create_customer(
    license_id: int,
    phone: str = None,
    email: str = None,
    name: str = None
) -> dict:
    """Get existing customer or create new one (SQLite & PostgreSQL compatible)."""
    async with get_db() as db:
        # Try to find by phone or email
        if phone:
            row = await fetch_one(
                db,
                "SELECT * FROM customers WHERE license_key_id = ? AND phone = ?",
                [license_id, phone]
            )
            if row:
                return dict(row)
        
        if email:
            row = await fetch_one(
                db,
                "SELECT * FROM customers WHERE license_key_id = ? AND email = ?",
                [license_id, email]
            )
            if row:
                return dict(row)
        
        # Create new customer
        if DB_TYPE == "postgresql":
            # PostgreSQL: use RETURNING
            row = await fetch_one(
                db,
                """
                INSERT INTO customers (license_key_id, name, phone, email, lead_score, segment)
                VALUES (?, ?, ?, ?, 0, 'New')
                RETURNING *
                """,
                [license_id, name, phone, email]
            )
            await commit_db(db)
            return dict(row) if row else {}
        else:
            # SQLite: insert then fetch
            await execute_sql(
                db,
                """
                INSERT INTO customers (license_key_id, name, phone, email, lead_score, segment)
                VALUES (?, ?, ?, ?, 0, 'New')
                """,
                [license_id, name, phone, email]
            )
            await commit_db(db)
            
            # Get the inserted row
            row = await fetch_one(
                db,
                "SELECT * FROM customers WHERE license_key_id = ? AND (phone = ? OR email = ?) ORDER BY id DESC LIMIT 1",
                [license_id, phone or "", email or ""]
            )
            return dict(row) if row else {
                "id": None,
                "license_key_id": license_id,
                "name": name,
                "phone": phone,
                "email": email,
                "total_messages": 0,
                "is_vip": False,
                "lead_score": 0,
                "segment": "New"
            }


async def get_customers(license_id: int, limit: int = 100) -> List[dict]:
    """Get all customers for a license (SQLite & PostgreSQL compatible)."""
    async with get_db() as db:
        rows = await fetch_all(
            db,
            """
            SELECT * FROM customers 
            WHERE license_key_id = ? 
            ORDER BY last_contact_at DESC
            LIMIT ?
            """,
            [license_id, limit],
        )
        return rows


async def get_customer(license_id: int, customer_id: int) -> Optional[dict]:
    """Get a specific customer"""
    async with get_db() as db:
        row = await fetch_one(
            db,
            "SELECT * FROM customers WHERE id = ? AND license_key_id = ?",
            [customer_id, license_id]
        )
        return dict(row) if row else None


async def update_customer(
    license_id: int,
    customer_id: int,
    **kwargs
) -> bool:
    """Update customer details"""
    allowed_fields = ['name', 'phone', 'email', 'company', 'notes', 'tags', 'is_vip']
    updates = {k: v for k, v in kwargs.items() if k in allowed_fields}
    
    if not updates:
        return False
    
    set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
    values = list(updates.values()) + [customer_id, license_id]

    async with get_db() as db:
        await execute_sql(
            db,
            f"""
            UPDATE customers SET {set_clause}
            WHERE id = ? AND license_key_id = ?
            """,
            values
        )
        await commit_db(db)
        return True


async def get_recent_conversation(
    license_id: int,
    sender_contact: str,
    limit: int = 5,
) -> str:
    """
    Get recent messages for a given customer (by sender_contact) as conversation context.
    Returns a single concatenated string (most recent first).
    """
    if not sender_contact:
        return ""

    async with get_db() as db:
        rows = await fetch_all(
            db,
            """
            SELECT body, created_at, channel
            FROM inbox_messages
            WHERE license_key_id = ?
              AND sender_contact = ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            [license_id, sender_contact, limit],
        )

    if not rows:
        return ""

    parts = []
    for row in rows:
        channel = row.get("channel") or ""
        ts = row.get("created_at") or ""
        body = row.get("body") or ""
        parts.append(f"[{channel} @ {ts}] {body}".strip())

    return "\n".join(parts)


async def get_customer_for_message(
    license_id: int,
    inbox_message_id: int,
) -> Optional[dict]:
    """
    Get the customer associated with a specific inbox message via customer_messages.
    """
    async with get_db() as db:
        row = await fetch_one(
            db,
            """
            SELECT c.*
            FROM customers c
            JOIN customer_messages cm
              ON cm.customer_id = c.id
            WHERE cm.inbox_message_id = ?
              AND c.license_key_id = ?
            LIMIT 1
            """,
            [inbox_message_id, license_id],
        )

    return dict(row) if row else None


async def increment_customer_messages(customer_id: int):
    """Increment customer message count and update last contact (SQLite & PostgreSQL compatible)."""
    now = datetime.utcnow()
    
    if DB_TYPE == "postgresql":
        ts_value = now
    else:
        ts_value = now.isoformat()
    
    async with get_db() as db:
        await execute_sql(
            db,
            """
            UPDATE customers SET 
                total_messages = total_messages + 1,
                last_contact_at = ?
            WHERE id = ?
            """,
            [ts_value, customer_id]
        )
        await commit_db(db)


# ============ Lead Scoring ============

def calculate_lead_score(
    total_messages: int,
    intent: str = None,
    sentiment: str = None,
    sentiment_score: float = 0.0,
    days_since_last_contact: int = None
) -> int:
    """
    Calculate lead score based on engagement metrics.
    Returns a score from 0-100.
    """
    score = 0
    
    # Base score from message count (0-30 points)
    if total_messages == 0:
        score += 0
    elif total_messages == 1:
        score += 5
    elif total_messages <= 3:
        score += 10
    elif total_messages <= 10:
        score += 20
    else:
        score += 30
    
    # Intent-based scoring (0-30 points)
    if intent:
        intent_lower = intent.lower()
        if "عرض" in intent_lower or "طلب خدمة" in intent_lower or "شراء" in intent_lower:
            score += 30
        elif "استفسار" in intent_lower or "متابعة" in intent_lower:
            score += 20
        elif "شكوى" in intent_lower:
            score += 10
        else:
            score += 5
    
    # Sentiment-based scoring (0-25 points)
    if sentiment:
        sentiment_lower = sentiment.lower()
        if "إيجابي" in sentiment_lower or "positive" in sentiment_lower:
            score += 25
        elif "محايد" in sentiment_lower or "neutral" in sentiment_lower:
            score += 15
        else:
            score += 5
    
    # Sentiment score bonus (0-10 points)
    if sentiment_score > 0.7:
        score += 10
    elif sentiment_score > 0.4:
        score += 5
    elif sentiment_score < -0.3:
        score -= 5
    
    # Recency bonus (0-5 points) - recent contact is better
    if days_since_last_contact is not None:
        if days_since_last_contact <= 1:
            score += 5
        elif days_since_last_contact <= 7:
            score += 3
        elif days_since_last_contact <= 30:
            score += 1
    
    # Ensure score is between 0-100
    return max(0, min(100, score))


def determine_segment(lead_score: int, total_messages: int, is_vip: bool = False) -> str:
    """
    Determine customer segment based on lead score and other factors.
    """
    if is_vip:
        return "VIP"
    
    if lead_score >= 70:
        return "High-Value"
    elif lead_score >= 50:
        return "Warm Lead"
    elif lead_score >= 30:
        return "Cold Lead"
    elif total_messages == 0:
        return "New"
    else:
        return "Low-Engagement"


async def update_customer_lead_score(
    license_id: int,
    customer_id: int,
    intent: str = None,
    sentiment: str = None,
    sentiment_score: float = 0.0
) -> bool:
    """
    Update customer lead score and segment based on new message analysis.
    Uses get_db() for cross-database compatibility.
    """
    async with get_db() as db:
        # Get current customer data
        customer = await fetch_one(
            db,
            "SELECT total_messages, sentiment_score, last_contact_at FROM customers WHERE id = ? AND license_key_id = ?",
            [customer_id, license_id]
        )
        
        if not customer:
            return False
        
        total_messages = customer.get("total_messages", 0) or 0
        current_sentiment_score = customer.get("sentiment_score", 0.0) or 0.0
        
        # Calculate days since last contact
        last_contact = customer.get("last_contact_at")
        days_since = None
        if last_contact:
            if isinstance(last_contact, str):
                try:
                    last_contact_dt = datetime.fromisoformat(last_contact.replace('Z', '+00:00'))
                except:
                    last_contact_dt = datetime.utcnow()
            else:
                last_contact_dt = last_contact
            days_since = (datetime.utcnow() - last_contact_dt.replace(tzinfo=None)).days
        
        # Calculate new lead score
        new_score = calculate_lead_score(
            total_messages=total_messages,
            intent=intent,
            sentiment=sentiment,
            sentiment_score=current_sentiment_score,
            days_since_last_contact=days_since
        )
        
        # Determine segment
        new_segment = determine_segment(
            lead_score=new_score,
            total_messages=total_messages,
            is_vip=customer.get("is_vip", False)
        )
        
        # Update customer
        await execute_sql(
            db,
            "UPDATE customers SET lead_score = ?, segment = ? WHERE id = ? AND license_key_id = ?",
            [new_score, new_segment, customer_id, license_id]
        )
        await commit_db(db)
        
        return True


# ============ Analytics ============

async def update_daily_analytics(
    license_id: int,
    messages_received: int = 0,
    messages_replied: int = 0,
    auto_replies: int = 0,
    sentiment: str = None,
    time_saved_seconds: int = 0
):
    """Update daily analytics"""
    today = datetime.now().date().isoformat()
    
    async with get_db() as db:
        # Get or create today's record
        row = await fetch_one(
            db,
            "SELECT id FROM analytics WHERE license_key_id = ? AND date = ?",
            [license_id, today]
        )
        
        if row:
            # Update existing
            sentiment_field = ""
            if sentiment == "إيجابي":
                sentiment_field = ", positive_sentiment = positive_sentiment + 1"
            elif sentiment == "سلبي":
                sentiment_field = ", negative_sentiment = negative_sentiment + 1"
            elif sentiment == "محايد":
                sentiment_field = ", neutral_sentiment = neutral_sentiment + 1"
            
            await execute_sql(
                db,
                f"""
                UPDATE analytics SET
                    messages_received = messages_received + ?,
                    messages_replied = messages_replied + ?,
                    auto_replies = auto_replies + ?,
                    time_saved_seconds = time_saved_seconds + ?
                    {sentiment_field}
                WHERE license_key_id = ? AND date = ?
                """,
                [messages_received, messages_replied, auto_replies, time_saved_seconds, license_id, today]
            )
        else:
            # Create new
            pos = 1 if sentiment == "إيجابي" else 0
            neg = 1 if sentiment == "سلبي" else 0
            neu = 1 if sentiment == "محايد" else 0
            
            await execute_sql(
                db,
                """
                INSERT INTO analytics 
                (license_key_id, date, messages_received, messages_replied,
                 auto_replies, positive_sentiment, negative_sentiment,
                 neutral_sentiment, time_saved_seconds)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [license_id, today, messages_received, messages_replied,
                 auto_replies, pos, neg, neu, time_saved_seconds]
            )
        
        await commit_db(db)


async def get_analytics_summary(license_id: int, days: int = 30) -> dict:
    """
    Get analytics summary for dashboard.

    Uses the unified db_helper layer so it works with both SQLite and PostgreSQL.
    """
    # Calculate cutoff date as a real date object.
    cutoff_date = datetime.utcnow().date() - timedelta(days=days)

    async with get_db() as db:
        row = await fetch_one(
            db,
            """
            SELECT 
                SUM(messages_received) as total_received,
                SUM(messages_replied) as total_replied,
                SUM(auto_replies) as total_auto,
                SUM(positive_sentiment) as positive,
                SUM(negative_sentiment) as negative,
                SUM(neutral_sentiment) as neutral,
                SUM(time_saved_seconds) as time_saved
            FROM analytics 
            WHERE license_key_id = ?
              AND date >= ?
            """,
            [license_id, cutoff_date],
        )

    if row:
        data = row
        total_sentiment = (data.get("positive") or 0) + (data.get("negative") or 0) + (data.get("neutral") or 0)

        return {
            "total_messages": data.get("total_received") or 0,
            "total_replied": data.get("total_replied") or 0,
            "auto_replies": data.get("total_auto") or 0,
            "time_saved_hours": round((data.get("time_saved") or 0) / 3600, 1),
            "satisfaction_rate": round((data.get("positive") or 0) / max(total_sentiment, 1) * 100),
            "response_rate": round(
                (data.get("total_replied") or 0) / max(data.get("total_received") or 1, 1) * 100
            ),
        }

    return {
        "total_messages": 0,
        "total_replied": 0,
        "auto_replies": 0,
        "time_saved_hours": 0,
        "satisfaction_rate": 0,
        "response_rate": 0,
    }


# ============ User Preferences ============

async def get_preferences(license_id: int) -> dict:
    """Get user preferences"""
    async with get_db() as db:
        row = await fetch_one(
            db,
            "SELECT * FROM user_preferences WHERE license_key_id = ?",
            [license_id]
        )
        if row:
            return dict(row)

        # Create default preferences including AI tone defaults
        await execute_sql(
            db,
            """
            INSERT INTO user_preferences (
                license_key_id,
                tone,
                language,
                preferred_languages
            ) VALUES (?, 'formal', 'ar', 'ar')
            """,
            [license_id]
        )
        await commit_db(db)

        return {
            "license_key_id": license_id,
            "dark_mode": False,
            "notifications_enabled": True,
            "notification_sound": True,
            "auto_reply_delay_seconds": 30,
            "language": "ar",
            "onboarding_completed": False,
            "tone": "formal",
            "custom_tone_guidelines": None,
            "business_name": None,
            "industry": None,
            "products_services": None,
            "preferred_languages": "ar",
            "reply_length": None,
            "formality_level": None,
        }


async def update_preferences(license_id: int, **kwargs) -> bool:
    """Update user preferences"""
    allowed = [
        'dark_mode',
        'notifications_enabled',
        'notification_sound',
        'auto_reply_delay_seconds',
        'onboarding_completed',
        # AI / workspace tone & business profile
        'tone',
        'custom_tone_guidelines',
        'business_name',
        'industry',
        'products_services',
        'preferred_languages',
        'reply_length',
        'formality_level',
    ]
    updates = {k: v for k, v in kwargs.items() if k in allowed}
    
    if not updates:
        return False
    
    set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
    values = list(updates.values()) + [license_id]

    async with get_db() as db:
        # Use UPSERT pattern
        await execute_sql(
            db,
            f"""
            INSERT INTO user_preferences (license_key_id) VALUES (?)
            ON CONFLICT(license_key_id) DO UPDATE SET {set_clause}
            """.replace("DO UPDATE SET", f"DO UPDATE SET {set_clause}"),
            [license_id] + list(updates.values())
        )
        await commit_db(db)
        return True


# ============ Notifications ============

async def create_notification(
    license_id: int,
    notification_type: str,
    title: str,
    message: str,
    priority: str = "normal",
    link: str = None
) -> int:
    """Create a new notification"""
    async with get_db() as db:
        await execute_sql(
            db,
            """
            INSERT INTO notifications (license_key_id, type, priority, title, message, link)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [license_id, notification_type, priority, title, message, link],
        )

        row = await fetch_one(
            db,
            """
            SELECT id FROM notifications
            WHERE license_key_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            [license_id],
        )
        await commit_db(db)
        return row["id"] if row else 0


async def get_notifications(license_id: int, unread_only: bool = False, limit: int = 50) -> List[dict]:
    """Get notifications for a user"""
    async with get_db() as db:
        query = "SELECT * FROM notifications WHERE license_key_id = ?"
        params = [license_id]

        if unread_only:
            query += " AND is_read = FALSE"

        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)

        rows = await fetch_all(db, query, params)
        return rows


async def get_unread_count(license_id: int) -> int:
    """Get count of unread notifications"""
    async with get_db() as db:
        row = await fetch_one(
            db,
            "SELECT COUNT(*) AS cnt FROM notifications WHERE license_key_id = ? AND is_read = FALSE",
            [license_id],
        )
        return int(row.get("cnt", 0)) if row else 0


async def mark_notification_read(license_id: int, notification_id: int) -> bool:
    """Mark a notification as read"""
    async with get_db() as db:
        await execute_sql(
            db,
            "UPDATE notifications SET is_read = TRUE WHERE id = ? AND license_key_id = ?",
            [notification_id, license_id],
        )
        await commit_db(db)
        return True


async def mark_all_notifications_read(license_id: int) -> bool:
    """Mark all notifications as read"""
    async with get_db() as db:
        await execute_sql(
            db,
            "UPDATE notifications SET is_read = TRUE WHERE license_key_id = ?",
            [license_id],
        )
        await commit_db(db)
        return True


async def delete_old_notifications(days: int = 30):
    """Delete notifications older than specified days"""
    async with get_db() as db:
        if DB_TYPE == "postgresql":
            sql = f"DELETE FROM notifications WHERE created_at < CURRENT_TIMESTAMP - INTERVAL '%s days'"
            await execute_sql(db, sql, [days])
        else:
            await execute_sql(
                db,
                "DELETE FROM notifications WHERE created_at < datetime('now', ?)",
                [f"-{days} days"],
            )
        await commit_db(db)


# Smart notification triggers
async def create_smart_notification(
    license_id: int,
    event_type: str,
    data: dict = None
):
    """Create smart notifications based on events"""
    data = data or {}
    
    notifications_map = {
        "new_message": {
            "type": "message",
            "priority": "normal",
            "title": "📨 رسالة جديدة",
            "message": f"رسالة جديدة من {data.get('sender', 'مرسل مجهول')}",
            "link": "/dashboard/inbox"
        },
        "urgent_message": {
            "type": "urgent",
            "priority": "high",
            "title": "🔴 رسالة عاجلة",
            "message": f"رسالة عاجلة تحتاج انتباهك من {data.get('sender', 'مرسل')}",
            "link": "/dashboard/inbox"
        },
        "negative_sentiment": {
            "type": "alert",
            "priority": "high",
            "title": "⚠️ عميل غاضب",
            "message": f"تم اكتشاف شكوى من {data.get('customer', 'عميل')}",
            "link": "/dashboard/inbox"
        },
        "vip_message": {
            "type": "vip",
            "priority": "high",
            "title": "⭐ رسالة من عميل VIP",
            "message": f"رسالة من عميل VIP: {data.get('customer', 'عميل مهم')}",
            "link": "/dashboard/inbox"
        },
        "milestone": {
            "type": "achievement",
            "priority": "normal",
            "title": "🎉 إنجاز جديد!",
            "message": data.get('message', 'لقد حققت إنجازاً جديداً!'),
            "link": "/dashboard/overview"
        },
        "daily_summary": {
            "type": "summary",
            "priority": "low",
            "title": "📊 ملخص اليوم",
            "message": f"عالجت {data.get('count', 0)} رسالة ووفرت {data.get('time_saved', 0)} دقيقة",
            "link": "/dashboard/overview"
        }
    }
    
    if event_type not in notifications_map:
        return None
    
    notif = notifications_map[event_type]
    
    return await create_notification(
        license_id=license_id,
        notification_type=notif["type"],
        title=notif["title"],
        message=notif["message"],
        priority=notif["priority"],
        link=notif.get("link")
    )


# ============ Team Management ============

ROLES = {
    "owner": {
        "name": "المالك",
        "permissions": ["*"]  # All permissions
    },
    "admin": {
        "name": "مدير",
        "permissions": ["read", "write", "reply", "manage_integrations", "view_analytics"]
    },
    "agent": {
        "name": "موظف",
        "permissions": ["read", "write", "reply"]
    },
    "viewer": {
        "name": "مشاهد",
        "permissions": ["read", "view_analytics"]
    }
}


async def create_team_member(
    license_id: int,
    email: str,
    name: str,
    role: str = "agent",
    invited_by: int = None,
    password_hash: str = None
) -> int:
    """Create a new team member (SQLite & PostgreSQL compatible)."""
    if role not in ROLES:
        role = "agent"

    normalized_email = email.lower()
    permissions = ",".join(ROLES[role]["permissions"])

    async with get_db() as db:
        await execute_sql(
            db,
            """
            INSERT INTO team_members
                (license_key_id, email, name, role, invited_by, password_hash, permissions)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [license_id, normalized_email, name, role, invited_by, password_hash, permissions],
        )

        # Fetch created member id in a DB‑agnostic way
        row = await fetch_one(
            db,
            """
            SELECT id FROM team_members
            WHERE license_key_id = ? AND email = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            [license_id, normalized_email],
        )
        await commit_db(db)
        return row["id"] if row else 0


async def get_team_members(license_id: int) -> List[dict]:
    """Get all team members for a license"""
    async with get_db() as db:
        rows = await fetch_all(
            db,
            """
            SELECT id, email, name, role, is_active, last_login_at, created_at
            FROM team_members 
            WHERE license_key_id = ?
            ORDER BY created_at ASC
            """,
            [license_id],
        )
        return rows


async def get_team_member(license_id: int, member_id: int) -> Optional[dict]:
    """Get a specific team member"""
    async with get_db() as db:
        return await fetch_one(
            db,
            """
            SELECT * FROM team_members 
            WHERE id = ? AND license_key_id = ?
            """,
            [member_id, license_id],
        )


async def get_team_member_by_email(license_id: int, email: str) -> Optional[dict]:
    """Get team member by email"""
    async with get_db() as db:
        return await fetch_one(
            db,
            """
            SELECT * FROM team_members 
            WHERE email = ? AND license_key_id = ?
            """,
            [email.lower(), license_id],
        )


async def update_team_member(
    license_id: int,
    member_id: int,
    **kwargs
) -> bool:
    """Update team member details"""
    allowed = ['name', 'role', 'is_active', 'permissions']
    updates = {k: v for k, v in kwargs.items() if k in allowed}
    
    if not updates:
        return False
    
    # If role is being updated, also update permissions
    if 'role' in updates and updates['role'] in ROLES:
        updates['permissions'] = ",".join(ROLES[updates['role']]["permissions"])
    
    set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
    values = list(updates.values()) + [member_id, license_id]

    async with get_db() as db:
        await execute_sql(
            db,
            f"""
            UPDATE team_members SET {set_clause}
            WHERE id = ? AND license_key_id = ?
            """,
            values,
        )
        await commit_db(db)
        return True


async def delete_team_member(license_id: int, member_id: int) -> bool:
    """Delete a team member"""
    async with get_db() as db:
        await execute_sql(
            db,
            "DELETE FROM team_members WHERE id = ? AND license_key_id = ?",
            [member_id, license_id],
        )
        await commit_db(db)
        return True


async def check_permission(license_id: int, member_id: int, permission: str) -> bool:
    """Check if a team member has a specific permission"""
    member = await get_team_member(license_id, member_id)
    if not member:
        return False
    
    permissions = (member.get('permissions') or '').split(',')
    return '*' in permissions or permission in permissions


async def log_team_activity(
    license_id: int,
    member_id: int,
    action: str,
    details: str = None,
    ip_address: str = None
):
    """Log team member activity"""
    async with get_db() as db:
        await execute_sql(
            db,
            """
            INSERT INTO team_activity_log 
            (license_key_id, team_member_id, action, details, ip_address)
            VALUES (?, ?, ?, ?, ?)
            """,
            [license_id, member_id, action, details, ip_address],
        )
        await commit_db(db)


async def get_team_activity(license_id: int, limit: int = 100) -> List[dict]:
    """Get team activity log"""
    async with get_db() as db:
        rows = await fetch_all(
            db,
            """
            SELECT a.*, m.name as member_name
            FROM team_activity_log a
            LEFT JOIN team_members m ON a.team_member_id = m.id
            WHERE a.license_key_id = ?
            ORDER BY a.created_at DESC
            LIMIT ?
            """,
            [license_id, limit],
        )
        return rows
