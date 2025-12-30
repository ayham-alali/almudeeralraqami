"""
Al-Mudeer - Customer and Analytics Models
Customer profiles, lead scoring, analytics, preferences, notifications, and team management
"""

import os
from datetime import datetime, timedelta, date
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
            # PostgreSQL: insert then fetch the last inserted row
            # Note: Using separate INSERT + SELECT to work around RETURNING issues
            try:
                await execute_sql(
                    db,
                    """
                    INSERT INTO customers (license_key_id, name, phone, email, lead_score, segment)
                    VALUES (?, ?, ?, ?, 0, 'New')
                    """,
                    [license_id, name, phone, email]
                )
            except Exception as e:
                # Fallback check for missing auto-increment/serial on 'id' column
                if "null value in column \"id\"" in str(e):
                    import logging
                    logger = logging.getLogger("models.customers")
                    logger.warning(f"Auto-increment missing on customers table. Using manual ID generation fallback. Error: {e}")
                    
                    # Manual ID generation (not concurrency safe but prevents crash)
                    max_row = await fetch_one(db, "SELECT MAX(id) as max_id FROM customers")
                    next_id = (max_row["max_id"] or 0) + 1
                    
                    await execute_sql(
                        db,
                        """
                        INSERT INTO customers (id, license_key_id, name, phone, email, lead_score, segment)
                        VALUES (?, ?, ?, ?, ?, 0, 'New')
                        """,
                        [next_id, license_id, name, phone, email]
                    )
                else:
                    raise e
                    
            await commit_db(db)
            
            # Fetch the created customer
            row = await fetch_one(
                db,
                """
                SELECT * FROM customers 
                WHERE license_key_id = ? 
                AND (phone = ? OR email = ? OR (phone IS NULL AND email IS NULL))
                ORDER BY id DESC LIMIT 1
                """,
                [license_id, phone or '', email or '']
            )
            return dict(row) if row else {"id": None}
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


async def get_customer_sentiment_history(
    license_id: int,
    customer_id: int,
    limit: int = 10
) -> List[float]:
    """
    Get sentiment history for a customer based on analyzed messages.
    Returns a list of sentiment scores from oldest to newest.
    """
    async with get_db() as db:
        # Get customer contact info
        customer = await fetch_one(
            db,
            "SELECT phone, email FROM customers WHERE id = ? AND license_key_id = ?",
            [customer_id, license_id]
        )
        
        if not customer:
            return []
        
        phone = customer.get("phone")
        email = customer.get("email")
        
        # Build query to find messages from this customer
        conditions = []
        params = [license_id]
        
        if phone:
            conditions.append("sender_contact = ?")
            params.append(phone)
        if email:
            conditions.append("sender_contact = ?")
            params.append(email)
        
        if not conditions:
            return []
        
        where_clause = " OR ".join(conditions)
        params.append(limit)
        
        rows = await fetch_all(
            db,
            f"""
            SELECT sentiment, created_at
            FROM inbox_messages
            WHERE license_key_id = ? AND ({where_clause}) AND sentiment IS NOT NULL
            ORDER BY created_at ASC
            LIMIT ?
            """,
            params
        )
        
        # Map sentiment strings to scores
        sentiment_map = {
            "إيجابي": 0.7,
            "positive": 0.7,
            "محايد": 0.0,
            "neutral": 0.0,
            "سلبي": -0.7,
            "negative": -0.7
        }
        
        return [sentiment_map.get(row.get("sentiment", "").lower(), 0.0) for row in rows]


async def get_customer_avg_response_time(
    license_id: int,
    customer_id: int
) -> Optional[int]:
    """
    Calculate average response time for a customer in seconds.
    This measures how quickly we respond to their messages.
    """
    async with get_db() as db:
        # Get customer contact info
        customer = await fetch_one(
            db,
            "SELECT phone, email FROM customers WHERE id = ? AND license_key_id = ?",
            [customer_id, license_id]
        )
        
        if not customer:
            return None
        
        phone = customer.get("phone")
        email = customer.get("email")
        
        # For now, calculate based on message timestamps
        # A more accurate implementation would track outbound message times
        conditions = []
        params = [license_id]
        
        if phone:
            conditions.append("sender_contact = ?")
            params.append(phone)
        if email:
            conditions.append("sender_contact = ?")
            params.append(email)
        
        if not conditions:
            return None
        
        where_clause = " OR ".join(conditions)
        
        # Get messages that have been replied to
        rows = await fetch_all(
            db,
            f"""
            SELECT i.created_at, o.created_at as replied_at
            FROM inbox_messages i
            JOIN outbox_messages o ON o.inbox_message_id = i.id
            WHERE i.license_key_id = ? AND ({where_clause}) 
              AND o.created_at IS NOT NULL
            ORDER BY i.created_at DESC
            LIMIT 20
            """,
            params
        )
        
        if not rows:
            return None
        
        total_seconds = 0
        count = 0
        
        for row in rows:
            created = row.get("created_at")
            replied = row.get("replied_at")
            if created and replied:
                try:
                    if isinstance(created, str):
                        created = datetime.fromisoformat(created.replace('Z', '+00:00'))
                    if isinstance(replied, str):
                        replied = datetime.fromisoformat(replied.replace('Z', '+00:00'))
                    
                    diff = (replied - created).total_seconds()
                    if diff > 0:
                        total_seconds += diff
                        count += 1
                except Exception:
                    pass
        
        return int(total_seconds / count) if count > 0 else None


async def get_customer_with_analytics(
    license_id: int,
    customer_id: int
) -> Optional[dict]:
    """
    Get customer with computed analytics: sentiment history, response time, lifetime value.
    """
    customer = await get_customer(license_id, customer_id)
    if not customer:
        return None
    
    # Get sentiment history
    sentiment_history = await get_customer_sentiment_history(license_id, customer_id)
    
    # Get average response time
    avg_response_time = await get_customer_avg_response_time(license_id, customer_id)
    
    # Get purchases (import here to avoid circular import)
    try:
        from models.purchases import get_customer_purchases, get_customer_lifetime_value
        purchases = await get_customer_purchases(license_id, customer_id, limit=10)
        lifetime_value = await get_customer_lifetime_value(license_id, customer_id)
    except Exception:
        purchases = []
        lifetime_value = customer.get("lifetime_value", 0) or 0
    
    # Calculate interaction count (messages * 2 for rough estimate of back-and-forth)
    interaction_count = (customer.get("total_messages", 0) or 0) * 2
    
    # Format response time
    avg_response_time_str = None
    if avg_response_time:
        hours = avg_response_time // 3600
        if hours > 0:
            avg_response_time_str = f"{hours} ساعة"
        else:
            minutes = avg_response_time // 60
            avg_response_time_str = f"{minutes} دقيقة" if minutes > 0 else "أقل من دقيقة"
    
    return {
        **customer,
        "sentiment_history": sentiment_history,
        "purchase_history": purchases,
        "interaction_count": interaction_count,
        "avg_response_time": avg_response_time_str,
        "avg_response_time_seconds": avg_response_time,
        "lifetime_value": lifetime_value
    }


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
    days_since_last_contact: int = None,
    purchase_count: int = 0,
    total_purchase_value: float = 0.0,
    days_since_first_contact: int = None
) -> int:
    """
    Calculate lead score based on engagement metrics and business value.
    Returns a score from 0-100.
    
    Scoring weights (rebalanced for accuracy):
    - Purchase history: 0-35 points (most important)
    - Message engagement: 0-25 points
    - Engagement duration: 0-15 points
    - Intent signals: 0-15 points
    - Sentiment: 0-10 points
    """
    score = 0
    
    # === PURCHASE HISTORY (0-35 points) - Most important factor ===
    if purchase_count > 0:
        # Base purchase points
        if purchase_count >= 5:
            score += 20
        elif purchase_count >= 3:
            score += 15
        elif purchase_count >= 1:
            score += 10
        
        # Purchase value bonus
        if total_purchase_value >= 1000:
            score += 15
        elif total_purchase_value >= 500:
            score += 10
        elif total_purchase_value >= 100:
            score += 5
    
    # === MESSAGE ENGAGEMENT (0-25 points) ===
    if total_messages == 0:
        score += 0
    elif total_messages <= 2:
        score += 5
    elif total_messages <= 5:
        score += 10
    elif total_messages <= 15:
        score += 15
    elif total_messages <= 30:
        score += 20
    else:
        score += 25
    
    # === ENGAGEMENT DURATION (0-15 points) - Time-based loyalty ===
    if days_since_first_contact is not None:
        if days_since_first_contact >= 90:  # 3+ months
            score += 15
        elif days_since_first_contact >= 30:  # 1+ month
            score += 10
        elif days_since_first_contact >= 7:  # 1+ week
            score += 5
        # Less than a week: no bonus
    
    # === INTENT SIGNALS (0-15 points) - Reduced weight ===
    if intent:
        intent_lower = intent.lower()
        if "شراء" in intent_lower or "طلب" in intent_lower:
            score += 15
        elif "عرض" in intent_lower or "سعر" in intent_lower:
            score += 10
        elif "استفسار" in intent_lower:
            score += 5
        # Generic intents: no bonus
    
    # === SENTIMENT (0-10 points) - Reduced weight ===
    if sentiment:
        sentiment_lower = sentiment.lower()
        if "إيجابي" in sentiment_lower or "positive" in sentiment_lower:
            score += 10
        elif "محايد" in sentiment_lower or "neutral" in sentiment_lower:
            score += 5
        # Negative: no bonus
    
    # === RECENCY ADJUSTMENT (-5 to +5 points) ===
    if days_since_last_contact is not None:
        if days_since_last_contact <= 3:
            score += 5  # Very recent
        elif days_since_last_contact <= 14:
            score += 2  # Recent
        elif days_since_last_contact > 60:
            score -= 5  # Inactive penalty
    
    # Ensure score is between 0-100
    return max(0, min(100, score))


def determine_segment(
    lead_score: int, 
    total_messages: int, 
    is_vip: bool = False,
    purchase_count: int = 0
) -> str:
    """
    Determine customer segment based on lead score and business factors.
    
    Segments (with stricter requirements):
    - VIP: Manually marked
    - High-Value (عميل استراتيجي): Score 75+ AND has purchases
    - Warm Lead (عميل واعد): Score 50+ OR has purchases
    - Cold Lead (غير متفاعل): Score 25-49, some engagement
    - New (جديد): No messages yet
    - Low-Engagement (غير نشط): Low score, minimal engagement
    """
    if is_vip:
        return "VIP"
    
    # High-Value REQUIRES actual purchases - can't be strategic without transactions
    if lead_score >= 75 and purchase_count > 0:
        return "High-Value"
    
    # Warm Lead: Good engagement OR has made a purchase
    if lead_score >= 50 or purchase_count > 0:
        return "Warm Lead"
    
    # Cold Lead: Some engagement but not converting
    if lead_score >= 25:
        return "Cold Lead"
    
    # New: Never contacted
    if total_messages == 0:
        return "New"
    
    # Low engagement: Has contacted but very low score
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
    Now includes purchase history and engagement duration in scoring.
    """
    async with get_db() as db:
        # Get current customer data including is_vip and created_at
        customer = await fetch_one(
            db,
            """SELECT total_messages, sentiment_score, last_contact_at, is_vip, created_at 
               FROM customers WHERE id = ? AND license_key_id = ?""",
            [customer_id, license_id]
        )
        
        if not customer:
            return False
        
        total_messages = customer.get("total_messages", 0) or 0
        current_sentiment_score = customer.get("sentiment_score", 0.0) or 0.0
        is_vip = customer.get("is_vip", False) or False
        
        # Get purchase history stats
        purchase_stats = await fetch_one(
            db,
            """SELECT COUNT(*) as purchase_count, COALESCE(SUM(amount), 0) as total_value
               FROM purchases WHERE customer_id = ?""",
            [customer_id]
        )
        purchase_count = purchase_stats.get("purchase_count", 0) if purchase_stats else 0
        total_purchase_value = float(purchase_stats.get("total_value", 0) or 0) if purchase_stats else 0.0
        
        # Calculate days since last contact
        last_contact = customer.get("last_contact_at")
        days_since_last = None
        if last_contact:
            last_contact_dt = _parse_datetime(last_contact)
            if last_contact_dt:
                days_since_last = (datetime.utcnow() - last_contact_dt.replace(tzinfo=None)).days
        
        # Calculate days since first contact (customer creation = first contact)
        first_contact = customer.get("created_at")
        days_since_first = None
        if first_contact:
            first_contact_dt = _parse_datetime(first_contact)
            if first_contact_dt:
                days_since_first = (datetime.utcnow() - first_contact_dt.replace(tzinfo=None)).days
        
        # Calculate new lead score with all factors
        new_score = calculate_lead_score(
            total_messages=total_messages,
            intent=intent,
            sentiment=sentiment,
            sentiment_score=current_sentiment_score,
            days_since_last_contact=days_since_last,
            purchase_count=purchase_count,
            total_purchase_value=total_purchase_value,
            days_since_first_contact=days_since_first
        )
        
        # Determine segment with purchase requirement
        new_segment = determine_segment(
            lead_score=new_score,
            total_messages=total_messages,
            is_vip=is_vip,
            purchase_count=purchase_count
        )
        
        # Update customer
        await execute_sql(
            db,
            "UPDATE customers SET lead_score = ?, segment = ? WHERE id = ? AND license_key_id = ?",
            [new_score, new_segment, customer_id, license_id]
        )
        await commit_db(db)
        
        return True


def _parse_datetime(value) -> datetime:
    """Helper to parse datetime from various formats."""
    if isinstance(value, datetime):
        return value
    elif isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    elif isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace('Z', '+00:00'))
        except:
            return None
    return None


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
    today_date = datetime.now().date()
    # For PostgreSQL (asyncpg), pass actual date object
    # For SQLite, pass ISO format string
    today = today_date if DB_TYPE == "postgresql" else today_date.isoformat()
    
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
            
            try:
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
            except Exception as e:
                # Handle NotNullViolationError for missing SERIAL/AUTOINCREMENT on 'id'
                if "null value in column \"id\"" in str(e):
                    # Manual ID generation fallback
                    max_row = await fetch_one(db, "SELECT MAX(id) as max_id FROM analytics")
                    next_id = (max_row.get("max_id") or 0) + 1
                    
                    await execute_sql(
                        db,
                        """
                        INSERT INTO analytics 
                        (id, license_key_id, date, messages_received, messages_replied,
                         auto_replies, positive_sentiment, negative_sentiment,
                         neutral_sentiment, time_saved_seconds)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [next_id, license_id, today, messages_received, messages_replied,
                         auto_replies, pos, neg, neu, time_saved_seconds]
                    )
                else:
                    raise e
        
        await commit_db(db)


async def get_ai_usage_today(license_id: int) -> dict:
    """Get today's AI usage for quota display.
    
    Tracks AI-generated draft responses (messages_replied) as the quota metric.
    This counts every message that received an AI-generated response.
    """
    today_date = datetime.now().date()
    # Use date object for PostgreSQL, ISO string for SQLite
    today = today_date if DB_TYPE == "postgresql" else today_date.isoformat()
    
    # Daily limit per user (matching workers.py MAX_MESSAGES_PER_USER_PER_DAY)
    daily_limit = 50
    
    async with get_db() as db:
        row = await fetch_one(
            db,
            """
            SELECT messages_received, messages_replied, auto_replies
            FROM analytics 
            WHERE license_key_id = ? AND date = ?
            """,
            [license_id, today]
        )
        
        if row:
            # Use messages_replied as it counts AI-generated draft responses
            # (incremented in agent.py draft_node and agent_enhanced.py enhanced_draft_node)
            ai_used = row.get("messages_replied") or 0
            messages_received = row.get("messages_received") or 0
        else:
            ai_used = 0
            messages_received = 0
        
        return {
            "used": ai_used,
            "limit": daily_limit,
            "remaining": max(0, daily_limit - ai_used),
            "percentage": min(100, round((ai_used / daily_limit) * 100)) if daily_limit > 0 else 0,
            "date": today,
            "messages_received": messages_received,  # For reference/display
        }


async def get_analytics_summary(license_id: int, days: int = 30) -> dict:
    """
    Get analytics summary for dashboard with period-over-period trends.

    Uses the unified db_helper layer so it works with both SQLite and PostgreSQL.
    Returns current period metrics plus trend percentages compared to previous period.
    """
    # Calculate date ranges
    current_end = datetime.utcnow().date()
    current_start = current_end - timedelta(days=days)
    previous_start = current_start - timedelta(days=days)

    async with get_db() as db:
        # Current period data
        current_row = await fetch_one(
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
            [license_id, current_start],
        )
        
        # Previous period data (for trend calculation)
        previous_row = await fetch_one(
            db,
            """
            SELECT 
                SUM(messages_received) as total_received,
                SUM(messages_replied) as total_replied,
                SUM(auto_replies) as total_auto,
                SUM(time_saved_seconds) as time_saved
            FROM analytics 
            WHERE license_key_id = ?
              AND date >= ?
              AND date < ?
            """,
            [license_id, previous_start, current_start],
        )

    def calc_trend(current: float, previous: float) -> int:
        """Calculate percentage change between periods."""
        if previous == 0:
            return 0  # No previous data, no trend to show
        change = ((current - previous) / previous) * 100
        return round(change)

    if current_row:
        data = current_row
        total_sentiment = (data.get("positive") or 0) + (data.get("negative") or 0) + (data.get("neutral") or 0)
        
        # Current values
        total_messages = data.get("total_received") or 0
        total_replied = data.get("total_replied") or 0
        auto_replies = data.get("total_auto") or 0
        time_saved_hours = round((data.get("time_saved") or 0) / 3600, 1)
        
        # Calculate response rates
        current_response_rate = round(total_replied / max(total_messages, 1) * 100)
        
        # Previous values for trends
        prev = previous_row or {}
        prev_messages = prev.get("total_received") or 0
        prev_replied = prev.get("total_replied") or 0
        prev_auto = prev.get("total_auto") or 0
        prev_time_saved = round((prev.get("time_saved") or 0) / 3600, 1)
        prev_response_rate = round(prev_replied / max(prev_messages, 1) * 100) if prev_messages > 0 else 0

        return {
            # Current period values
            "total_messages": total_messages,
            "total_replied": total_replied,
            "auto_replies": auto_replies,
            "time_saved_hours": time_saved_hours,
            "satisfaction_rate": round((data.get("positive") or 0) / max(total_sentiment, 1) * 100),
            "response_rate": current_response_rate,
            # Raw sentiment counts for frontend
            "positive_count": data.get("positive") or 0,
            "negative_count": data.get("negative") or 0,
            "neutral_count": data.get("neutral") or 0,
            # Period-over-period trends
            "total_messages_trend": calc_trend(total_messages, prev_messages),
            "time_saved_trend": calc_trend(time_saved_hours, prev_time_saved),
            "response_rate_trend": calc_trend(current_response_rate, prev_response_rate),
            "auto_replies_trend": calc_trend(auto_replies, prev_auto),
        }

    return {
        "total_messages": 0,
        "total_replied": 0,
        "auto_replies": 0,
        "time_saved_hours": 0,
        "satisfaction_rate": 0,
        "response_rate": 0,
        "positive_count": 0,
        "negative_count": 0,
        "neutral_count": 0,
        "total_messages_trend": 0,
        "time_saved_trend": 0,
        "response_rate_trend": 0,
        "auto_replies_trend": 0,
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
            prefs = dict(row)
            # CRITICAL: Ensure notifications_enabled defaults to True if NULL in DB
            if prefs.get("notifications_enabled") is None:
                prefs["notifications_enabled"] = True
            return prefs

        # Create default preferences including AI tone defaults
        await execute_sql(
            db,
            """
            INSERT INTO user_preferences (
                license_key_id,
                tone,
                language,
                preferred_languages,
                notifications_enabled
            ) VALUES (?, 'formal', 'ar', 'ar', ?)
            """,
            [license_id, True]
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
    from logging_config import get_logger
    logger = get_logger(__name__)
    
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
    
    logger.info(f"update_preferences called: license_id={license_id}, updates={updates}")
    
    if not updates:
        logger.info("No updates to apply")
        return False
    
    set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
    update_values = list(updates.values())

    async with get_db() as db:
        # Use UPSERT pattern - INSERT with ON CONFLICT UPDATE
        # PostgreSQL and SQLite both support this syntax
        if DB_TYPE == "postgresql":
            # PostgreSQL: use EXCLUDED reference for cleaner syntax
            set_clause_pg = ", ".join(f"{k} = EXCLUDED.{k}" for k in updates.keys())
            cols = ", ".join(["license_key_id"] + list(updates.keys()))
            placeholders = ", ".join(["?"] * (1 + len(updates)))
            sql = f"""
                INSERT INTO user_preferences ({cols}) VALUES ({placeholders})
                ON CONFLICT(license_key_id) DO UPDATE SET {set_clause_pg}
                """
            logger.info(f"PostgreSQL UPSERT SQL: {sql.strip()}, params: {[license_id] + update_values}")
            await execute_sql(
                db,
                sql,
                [license_id] + update_values
            )
        else:
            # SQLite: use standard ON CONFLICT DO UPDATE SET with explicit values
            cols = ", ".join(["license_key_id"] + list(updates.keys()))
            placeholders = ", ".join(["?"] * (1 + len(updates)))
            await execute_sql(
                db,
                f"""
                INSERT INTO user_preferences ({cols}) VALUES ({placeholders})
                ON CONFLICT(license_key_id) DO UPDATE SET {set_clause}
                """,
                [license_id] + update_values + update_values  # Values for INSERT + UPDATE
            )
        await commit_db(db)
        logger.info("Preferences update completed successfully")
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
    """Create a new notification and send Web Push if subscribers exist."""
    notification_id = 0
    
    async with get_db() as db:
        try:
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
            notification_id = row["id"] if row else 0
        except Exception as e:
            # Handle NotNullViolationError for missing SERIAL/AUTOINCREMENT on 'id'
            # This can happen on PostgreSQL if table was created without proper SERIAL type
            if "null value in column \"id\"" in str(e):
                # Manual ID generation fallback
                max_row = await fetch_one(db, "SELECT MAX(id) as max_id FROM notifications")
                next_id = (max_row.get("max_id") or 0) + 1
                
                await execute_sql(
                    db,
                    """
                    INSERT INTO notifications (id, license_key_id, type, priority, title, message, link)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    [next_id, license_id, notification_type, priority, title, message, link],
                )
                await commit_db(db)
                notification_id = next_id
            else:
                raise e
    
    # Send Web Push notification in background (non-blocking)
    try:
        from services.push_service import send_push_to_license, WEBPUSH_AVAILABLE
        if WEBPUSH_AVAILABLE:
            import asyncio
            asyncio.create_task(
                send_push_to_license(
                    license_id=license_id,
                    title=title,
                    message=message,
                    link=link,
                    tag=f"notification-{notification_id}",
                    priority=priority
                )
            )
    except Exception:
        pass  # Web Push is optional, don't fail if it errors
    
    return notification_id


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

