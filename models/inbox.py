"""
Al-Mudeer - Inbox/Outbox Models
Unified message inbox and outbox management
"""

from datetime import datetime, timezone
from typing import Optional, List, Any

from db_helper import get_db, execute_sql, fetch_all, fetch_one, commit_db, DB_TYPE


async def save_inbox_message(
    license_id: int,
    channel: str,
    body: str,
    sender_name: str = None,
    sender_contact: str = None,
    sender_id: str = None,
    subject: str = None,
    channel_message_id: str = None,
    received_at: datetime = None
) -> int:
    """Save incoming message to inbox (SQLite & PostgreSQL compatible)."""

    # Normalize received_at to a UTC datetime; asyncpg prefers naive UTC
    if isinstance(received_at, str):
        try:
            received = datetime.fromisoformat(received_at)
        except ValueError:
            received = datetime.utcnow()
    elif isinstance(received_at, datetime):
        received = received_at
    else:
        received = datetime.utcnow()

    if received.tzinfo is not None:
        received = received.astimezone(timezone.utc).replace(tzinfo=None)

    # For PostgreSQL (asyncpg), pass a naive UTC datetime.
    # For SQLite, use ISO string.
    ts_value: Any
    if DB_TYPE == "postgresql":
        ts_value = received
    else:
        ts_value = received.isoformat()

    async with get_db() as db:
        await execute_sql(
            db,
            """
            INSERT INTO inbox_messages 
                (license_key_id, channel, channel_message_id, sender_id, sender_name,
                 sender_contact, subject, body, received_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                license_id,
                channel,
                channel_message_id,
                sender_id,
                sender_name,
                sender_contact,
                subject,
                body,
                ts_value,
            ],
        )

        # Fetch the last inserted id in a DB-agnostic way
        row = await fetch_one(
            db,
            """
            SELECT id FROM inbox_messages
            WHERE license_key_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            [license_id],
        )
        await commit_db(db)
        return row["id"] if row else 0


async def update_inbox_analysis(
    message_id: int,
    intent: str,
    urgency: str,
    sentiment: str,
    language: Optional[str],
    dialect: Optional[str],
    summary: str,
    draft_response: str
):
    """Update inbox message with AI analysis (DB agnostic)."""

    now = datetime.utcnow()
    ts_value = now if DB_TYPE == "postgresql" else now.isoformat()

    async with get_db() as db:
        try:
            # Try to update with all columns including language/dialect
            await execute_sql(
                db,
                """
                UPDATE inbox_messages SET
                    intent = ?, urgency = ?, sentiment = ?,
                    language = ?, dialect = ?,
                    ai_summary = ?, ai_draft_response = ?,
                    status = 'analyzed', processed_at = ?
                WHERE id = ?
                """,
                [intent, urgency, sentiment, language, dialect, summary, draft_response, ts_value, message_id],
            )
            await commit_db(db)
        except Exception as e:
            # If language/dialect columns don't exist, update without them
            if "language" in str(e).lower() or "dialect" in str(e).lower():
                from logging_config import get_logger
                logger = get_logger(__name__)
                logger.warning(f"Language/dialect columns not found, updating without them: {e}")
                await execute_sql(
                    db,
                    """
                    UPDATE inbox_messages SET
                        intent = ?, urgency = ?, sentiment = ?,
                        ai_summary = ?, ai_draft_response = ?,
                        status = 'analyzed', processed_at = ?
                    WHERE id = ?
                    """,
                    [intent, urgency, sentiment, summary, draft_response, ts_value, message_id],
                )
                await commit_db(db)
            else:
                raise


async def get_inbox_messages(
    license_id: int,
    status: str = None,
    channel: str = None,
    limit: int = 50,
    offset: int = 0
) -> List[dict]:
    """Get inbox messages for a license with pagination (SQLite & PostgreSQL compatible)."""

    query = "SELECT * FROM inbox_messages WHERE license_key_id = ?"
    params = [license_id]

    if status:
        query += " AND status = ?"
        params.append(status)

    if channel:
        query += " AND channel = ?"
        params.append(channel)

    query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
    params.append(limit)
    params.append(offset)

    async with get_db() as db:
        rows = await fetch_all(db, query, params)
        return rows


async def get_inbox_message_by_id(message_id: int, license_id: int) -> Optional[dict]:
    """Get a single inbox message by ID (efficient direct lookup)."""
    async with get_db() as db:
        row = await fetch_one(
            db,
            "SELECT * FROM inbox_messages WHERE id = ? AND license_key_id = ?",
            [message_id, license_id]
        )
        return row



async def get_inbox_messages_count(
    license_id: int,
    status: str = None,
    channel: str = None
) -> int:
    """Get total count of inbox messages for pagination."""
    
    query = "SELECT COUNT(*) as count FROM inbox_messages WHERE license_key_id = ?"
    params = [license_id]

    if status:
        query += " AND status = ?"
        params.append(status)

    if channel:
        query += " AND channel = ?"
        params.append(channel)

    async with get_db() as db:
        row = await fetch_one(db, query, params)
        return row["count"] if row else 0


async def update_inbox_status(message_id: int, status: str):
    """Update inbox message status (DB agnostic)."""
    async with get_db() as db:
        await execute_sql(
            db,
            "UPDATE inbox_messages SET status = ? WHERE id = ?",
            [status, message_id],
        )
        await commit_db(db)


# ============ Outbox Functions ============

async def create_outbox_message(
    inbox_message_id: int,
    license_id: int,
    channel: str,
    body: str,
    recipient_id: str = None,
    recipient_email: str = None,
    subject: str = None
) -> int:
    """Create outbox message for approval (DB agnostic)."""
    async with get_db() as db:
        await execute_sql(
            db,
            """
            INSERT INTO outbox_messages 
                (inbox_message_id, license_key_id, channel, recipient_id,
                 recipient_email, subject, body)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [inbox_message_id, license_id, channel, recipient_id, recipient_email, subject, body],
        )

        row = await fetch_one(
            db,
            """
            SELECT id FROM outbox_messages
            WHERE license_key_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            [license_id],
        )
        await commit_db(db)
        return row["id"] if row else 0


async def approve_outbox_message(message_id: int, edited_body: str = None):
    """Approve an outbox message for sending (DB agnostic)."""

    now = datetime.utcnow()
    ts_value = now if DB_TYPE == "postgresql" else now.isoformat()

    async with get_db() as db:
        if edited_body:
            await execute_sql(
                db,
                """
                UPDATE outbox_messages SET
                    body = ?, status = 'approved', approved_at = ?
                WHERE id = ?
                """,
                [edited_body, ts_value, message_id],
            )
        else:
            await execute_sql(
                db,
                """
                UPDATE outbox_messages SET
                    status = 'approved', approved_at = ?
                WHERE id = ?
                """,
                [ts_value, message_id],
            )
        await commit_db(db)


async def mark_outbox_sent(message_id: int):
    """Mark outbox message as sent (DB agnostic)."""

    now = datetime.utcnow()
    ts_value = now if DB_TYPE == "postgresql" else now.isoformat()

    async with get_db() as db:
        await execute_sql(
            db,
            """
            UPDATE outbox_messages SET
                status = 'sent', sent_at = ?
            WHERE id = ?
            """,
            [ts_value, message_id],
        )
        await commit_db(db)


async def get_pending_outbox(license_id: int) -> List[dict]:
    """Get pending outbox messages (DB agnostic)."""
    async with get_db() as db:
        rows = await fetch_all(
            db,
            """
            SELECT o.*, i.sender_name, i.body as original_message
            FROM outbox_messages o
            JOIN inbox_messages i ON o.inbox_message_id = i.id
            WHERE o.license_key_id = ? AND o.status IN ('pending', 'approved')
            ORDER BY o.created_at DESC
            """,
            [license_id],
        )
        return rows


async def get_inbox_conversations(
    license_id: int,
    status: str = None,
    channel: str = None,
    limit: int = 50,
    offset: int = 0
) -> List[dict]:
    """
    Get inbox messages grouped by sender (chat-style view).
    Returns one row per sender with their latest message and stats.
    
    IMPORTANT: Status filter is applied to the LATEST message of each conversation,
    not to all messages. This ensures conversations are grouped correctly before filtering.
    """
    from db_helper import DB_TYPE
    
    # Build base WHERE for license (always applied)
    base_where = "license_key_id = ?"
    base_params = [license_id]
    
    # Channel filter can be applied in base query
    if channel:
        base_where += " AND channel = ?"
        base_params.append(channel)
    
    # Build status filter (applied AFTER grouping)
    status_filter = ""
    status_params = []
    if status == 'sent':
        status_filter = "status IN ('approved', 'sent', 'auto_replied')"
    elif status:
        status_filter = "status = ?"
        status_params.append(status)
    
    # Use database-specific query for grouping
    if DB_TYPE == "postgresql":
        # PostgreSQL: First get latest message per sender, THEN filter by status
        query = f"""
            WITH latest_per_sender AS (
                SELECT DISTINCT ON (COALESCE(sender_contact, sender_id::text, 'unknown'))
                    *,
                    (SELECT COUNT(*) FROM inbox_messages m2 
                     WHERE m2.license_key_id = inbox_messages.license_key_id 
                     AND COALESCE(m2.sender_contact, m2.sender_id::text, 'unknown') = COALESCE(inbox_messages.sender_contact, inbox_messages.sender_id::text, 'unknown')
                    ) as message_count,
                    (SELECT COUNT(*) FROM inbox_messages m2 
                     WHERE m2.license_key_id = inbox_messages.license_key_id 
                     AND COALESCE(m2.sender_contact, m2.sender_id::text, 'unknown') = COALESCE(inbox_messages.sender_contact, inbox_messages.sender_id::text, 'unknown')
                     AND m2.status = 'pending'
                    ) as unread_count
                FROM inbox_messages
                WHERE {base_where}
                ORDER BY COALESCE(sender_contact, sender_id::text, 'unknown'), created_at DESC
            )
            SELECT 
                id, channel, sender_name, sender_contact, sender_id, subject, body,
                intent, urgency, sentiment, language, dialect, ai_summary, ai_draft_response,
                status, created_at, received_at,
                message_count, unread_count
            FROM latest_per_sender
            {"WHERE " + status_filter if status_filter else ""}
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?
        """
        params = base_params + status_params + [limit, offset]
    else:
        # SQLite version - get latest message per sender, then filter by status
        query = f"""
            SELECT 
                m.*,
                (SELECT COUNT(*) FROM inbox_messages m2 
                 WHERE m2.license_key_id = m.license_key_id 
                 AND COALESCE(m2.sender_contact, m2.sender_id, 'unknown') = COALESCE(m.sender_contact, m.sender_id, 'unknown')
                ) as message_count,
                (SELECT COUNT(*) FROM inbox_messages m2 
                 WHERE m2.license_key_id = m.license_key_id 
                 AND COALESCE(m2.sender_contact, m2.sender_id, 'unknown') = COALESCE(m.sender_contact, m.sender_id, 'unknown')
                 AND m2.status = 'pending'
                ) as unread_count
            FROM inbox_messages m
            WHERE {base_where}
            AND m.id = (
                SELECT m3.id FROM inbox_messages m3
                WHERE m3.license_key_id = m.license_key_id
                AND COALESCE(m3.sender_contact, m3.sender_id, 'unknown') = COALESCE(m.sender_contact, m.sender_id, 'unknown')
                ORDER BY m3.created_at DESC
                LIMIT 1
            )
            {"AND " + status_filter if status_filter else ""}
            ORDER BY m.created_at DESC
            LIMIT ? OFFSET ?
        """
        params = base_params + status_params + [limit, offset]
    
    async with get_db() as db:
        rows = await fetch_all(db, query, params)
        return rows


async def get_inbox_conversations_count(
    license_id: int,
    status: str = None,
    channel: str = None
) -> int:
    """Get total number of unique conversations (senders)."""
    where_clauses = ["license_key_id = ?"]
    params = [license_id]
    
    if status == 'sent':
        where_clauses.append("status IN ('approved', 'sent', 'auto_replied')")
    elif status:
        where_clauses.append("status = ?")
        params.append(status)
    
    if channel:
        where_clauses.append("channel = ?")
        params.append(channel)
    
    where_sql = " AND ".join(where_clauses)
    
    query = f"""
        SELECT COUNT(DISTINCT COALESCE(sender_contact, sender_id, 'unknown')) as count
        FROM inbox_messages
        WHERE {where_sql}
    """
    
    async with get_db() as db:
        row = await fetch_one(db, query, params)
        return row["count"] if row else 0


async def get_inbox_status_counts(license_id: int) -> dict:
    """
    Get status counts across all channels for badge display.
    Counts unique conversations (senders) by their latest message status.
    Returns: {analyzed: N, sent: N, ignored: N}
    """
    from db_helper import DB_TYPE
    
    if DB_TYPE == "postgresql":
        query = """
            WITH latest_per_sender AS (
                SELECT DISTINCT ON (COALESCE(sender_contact, sender_id::text, 'unknown'))
                    status
                FROM inbox_messages
                WHERE license_key_id = ?
                ORDER BY COALESCE(sender_contact, sender_id::text, 'unknown'), created_at DESC
            )
            SELECT 
                COALESCE(SUM(CASE WHEN status = 'analyzed' THEN 1 ELSE 0 END), 0) as analyzed,
                COALESCE(SUM(CASE WHEN status IN ('approved', 'sent', 'auto_replied') THEN 1 ELSE 0 END), 0) as sent,
                COALESCE(SUM(CASE WHEN status = 'ignored' THEN 1 ELSE 0 END), 0) as ignored
            FROM latest_per_sender
        """
    else:
        # SQLite version
        query = """
            SELECT 
                COALESCE(SUM(CASE WHEN status = 'analyzed' THEN 1 ELSE 0 END), 0) as analyzed,
                COALESCE(SUM(CASE WHEN status IN ('approved', 'sent', 'auto_replied') THEN 1 ELSE 0 END), 0) as sent,
                COALESCE(SUM(CASE WHEN status = 'ignored' THEN 1 ELSE 0 END), 0) as ignored
            FROM inbox_messages m
            WHERE m.license_key_id = ?
            AND m.id = (
                SELECT m2.id FROM inbox_messages m2
                WHERE m2.license_key_id = m.license_key_id
                AND COALESCE(m2.sender_contact, m2.sender_id, 'unknown') = COALESCE(m.sender_contact, m.sender_id, 'unknown')
                ORDER BY m2.created_at DESC
                LIMIT 1
            )
        """
    
    async with get_db() as db:
        row = await fetch_one(db, query, [license_id])
        if row:
            return {
                "analyzed": row["analyzed"] or 0,
                "sent": row["sent"] or 0,
                "ignored": row["ignored"] or 0
            }
        return {"analyzed": 0, "sent": 0, "ignored": 0}


async def get_conversation_messages(
    license_id: int,
    sender_contact: str,
    limit: int = 50
) -> List[dict]:
    """Get all messages from a specific sender (for conversation detail view)."""
    # Handle the tg: prefix for telegram user IDs
    async with get_db() as db:
        rows = await fetch_all(
            db,
            """
            SELECT * FROM inbox_messages
            WHERE license_key_id = ?
            AND (sender_contact = ? OR sender_id = ? OR sender_contact LIKE ?)
            ORDER BY created_at ASC
            LIMIT ?
            """,
            [license_id, sender_contact, sender_contact, f"%{sender_contact}%", limit]
        )
        return rows


async def ignore_chat(license_id: int, sender_contact: str) -> int:
    """
    Mark all messages from a sender as 'ignored' (entire chat).
    Returns the count of messages updated.
    """
    async with get_db() as db:
        # Update all messages from this sender
        await execute_sql(
            db,
            """
            UPDATE inbox_messages 
            SET status = 'ignored'
            WHERE license_key_id = ?
            AND (sender_contact = ? OR sender_id = ? OR sender_contact LIKE ?)
            """,
            [license_id, sender_contact, sender_contact, f"%{sender_contact}%"]
        )
        
        # Get count of affected rows
        row = await fetch_one(
            db,
            """
            SELECT COUNT(*) as count FROM inbox_messages
            WHERE license_key_id = ?
            AND (sender_contact = ? OR sender_id = ? OR sender_contact LIKE ?)
            AND status = 'ignored'
            """,
            [license_id, sender_contact, sender_contact, f"%{sender_contact}%"]
        )
        await commit_db(db)
        return row["count"] if row else 0


async def get_full_chat_history(
    license_id: int,
    sender_contact: str,
    limit: int = 100
) -> List[dict]:
    """
    Get complete chat history including both incoming (inbox) and outgoing (outbox) messages.
    Returns messages sorted by timestamp, each marked with 'direction' field.
    """
    async with get_db() as db:
        # Get incoming messages (from client to us)
        inbox_rows = await fetch_all(
            db,
            """
            SELECT 
                id, channel, sender_name, sender_contact, sender_id, 
                subject, body, 
                intent, urgency, sentiment, language, dialect,
                ai_summary, ai_draft_response, status,
                created_at, received_at
            FROM inbox_messages
            WHERE license_key_id = ?
            AND (sender_contact = ? OR sender_id = ? OR sender_contact LIKE ?)
            ORDER BY created_at ASC
            LIMIT ?
            """,
            [license_id, sender_contact, sender_contact, f"%{sender_contact}%", limit]
        )
        
        # Get outgoing messages (from us to client) - sent replies
        outbox_rows = await fetch_all(
            db,
            """
            SELECT 
                o.id, o.channel, o.recipient_email as sender_contact, o.recipient_id as sender_id,
                o.subject, o.body, o.status,
                o.created_at, o.sent_at,
                i.sender_name
            FROM outbox_messages o
            LEFT JOIN inbox_messages i ON o.inbox_message_id = i.id
            WHERE o.license_key_id = ?
            AND (o.recipient_email = ? OR o.recipient_id = ? OR o.recipient_email LIKE ?)
            AND o.status IN ('sent', 'approved')
            ORDER BY o.created_at ASC
            LIMIT ?
            """,
            [license_id, sender_contact, sender_contact, f"%{sender_contact}%", limit]
        )
        
        # Convert to list with direction marker
        messages = []
        
        for row in inbox_rows:
            msg = dict(row)
            msg["direction"] = "incoming"
            msg["timestamp"] = msg.get("received_at") or msg.get("created_at")
            messages.append(msg)
        
        for row in outbox_rows:
            msg = dict(row)
            msg["direction"] = "outgoing"
            msg["timestamp"] = msg.get("sent_at") or msg.get("created_at")
            # Mark outgoing status as descriptive
            if msg.get("status") == "sent":
                msg["status"] = "sent"
            elif msg.get("status") == "approved":
                msg["status"] = "sending"
            messages.append(msg)
        
        # Sort all messages by timestamp
        def get_timestamp(m):
            ts = m.get("timestamp")
            if ts is None:
                return ""
            if isinstance(ts, str):
                return ts
            return ts.isoformat() if hasattr(ts, 'isoformat') else str(ts)
        
        messages.sort(key=get_timestamp)
        
        return messages



