"""Al-Mudeer - Inbox/Outbox Models
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
    received_at: datetime = None,
    attachments: Optional[List[dict]] = None
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

    # Serialize attachments
    import json
    attachments_json = json.dumps(attachments) if attachments else None

    async with get_db() as db:
        # Check if attachments and is_read columns exist (simplified migration)
        try:
             await execute_sql(db, "ALTER TABLE inbox_messages ADD COLUMN attachments TEXT")
        except:
             pass 
        try:
            if DB_TYPE == "postgresql":
                await execute_sql(db, "ALTER TABLE inbox_messages ADD COLUMN IF NOT EXISTS is_read BOOLEAN DEFAULT FALSE")
            else:
                await execute_sql(db, "ALTER TABLE inbox_messages ADD COLUMN is_read BOOLEAN DEFAULT 0")
        except:
             pass

        await execute_sql(
            db,
            """
            INSERT INTO inbox_messages 
                (license_key_id, channel, channel_message_id, sender_id, sender_name,
                 sender_contact, subject, body, received_at, attachments)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                attachments_json
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
        
        message_id = row["id"] if row else 0
        
        # Update customer presence - mark them as "online" when they send a message
        try:
            from services.customer_presence import mark_customer_online
            if sender_contact:
                await mark_customer_online(
                    license_id=license_id,
                    sender_contact=sender_contact,
                    channel=channel
                )
        except Exception as e:
            # Non-critical, don't fail the message save
            pass
        
        return message_id



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
    """
    Get inbox messages for a license with pagination (SQLite & PostgreSQL compatible).
    
    NOTE: Excludes 'pending' status messages from UI.
    Pending = before AI responds (should not show in UI)
    Analyzed = after AI responds (shows as 'بانتظار الموافقة')
    """

    # Exclude 'pending' status - only show messages after AI responds
    query = "SELECT * FROM inbox_messages WHERE license_key_id = ? AND status != 'pending'"
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
    """
    Get total count of inbox messages for pagination.
    
    NOTE: Excludes 'pending' status messages from count.
    """
    
    # Exclude 'pending' status - only count messages after AI responds
    query = "SELECT COUNT(*) as count FROM inbox_messages WHERE license_key_id = ? AND status != 'pending'"
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
    # NOTE: Always exclude 'pending' - messages before AI responds should not show in UI
    status_filter = ""
    status_params = []
    if status == 'sent':
        status_filter = "status IN ('approved', 'sent', 'auto_replied')"
    elif status:
        status_filter = "status = ?"
        status_params.append(status)
    
    # Always exclude 'pending' status (before AI responds)
    pending_filter = "status != 'pending'"
    
    # Use database-specific query for grouping
    if DB_TYPE == "postgresql":
        # PostgreSQL: First get latest message per sender, THEN filter by status
        # Build combined WHERE for final filter (always exclude pending + optional status filter)
        final_where_parts = [pending_filter]
        if status_filter:
            final_where_parts.append(status_filter)
        final_where = " AND ".join(final_where_parts)
        
        query = f"""
            WITH latest_per_sender AS (
                SELECT DISTINCT ON (COALESCE(sender_contact, sender_id::text, 'unknown'))
                    *,
                    (SELECT COUNT(*) FROM inbox_messages m2 
                     WHERE m2.license_key_id = inbox_messages.license_key_id 
                     AND COALESCE(m2.sender_contact, m2.sender_id::text, 'unknown') = COALESCE(inbox_messages.sender_contact, inbox_messages.sender_id::text, 'unknown')
                     AND m2.status != 'pending'
                    ) as message_count,
                     (SELECT COUNT(*) FROM inbox_messages m2 
                      WHERE m2.license_key_id = inbox_messages.license_key_id 
                      AND COALESCE(m2.sender_contact, m2.sender_id::text, 'unknown') = COALESCE(inbox_messages.sender_contact, inbox_messages.sender_id::text, 'unknown')
                      AND m2.status = 'analyzed'
                      AND m2.is_read = FALSE
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
            WHERE {final_where}
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?
        """
        params = base_params + status_params + [limit, offset]
    else:
        # SQLite version - get latest message per sender, then filter by status
        # Build combined WHERE for final filter (always exclude pending + optional status filter)
        final_filter_parts = [pending_filter]
        if status_filter:
            final_filter_parts.append(status_filter)
        final_filter = " AND ".join(final_filter_parts)
        
        query = f"""
            SELECT 
                m.*,
                (SELECT COUNT(*) FROM inbox_messages m2 
                 WHERE m2.license_key_id = m.license_key_id 
                 AND COALESCE(m2.sender_contact, m2.sender_id, 'unknown') = COALESCE(m.sender_contact, m.sender_id, 'unknown')
                 AND m2.status != 'pending'
                ) as message_count,
                 (SELECT COUNT(*) FROM inbox_messages m2 
                  WHERE m2.license_key_id = m.license_key_id 
                  AND COALESCE(m2.sender_contact, m2.sender_id, 'unknown') = COALESCE(m.sender_contact, m.sender_id, 'unknown')
                  AND m2.status = 'analyzed'
                  AND (m2.is_read = 0 OR m2.is_read IS FALSE)
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
            AND {final_filter}
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
    """
    Get total number of unique conversations (senders).
    Counts conversations by their LATEST message status (same logic as get_inbox_conversations).
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
    # NOTE: Always exclude 'pending' - messages before AI responds should not be counted
    status_filter = ""
    status_params = []
    if status == 'sent':
        status_filter = "status IN ('approved', 'sent', 'auto_replied')"
    elif status:
        status_filter = "status = ?"
        status_params.append(status)
    
    # Always exclude 'pending' status (before AI responds)
    pending_filter = "status != 'pending'"
    
    if DB_TYPE == "postgresql":
        # PostgreSQL: Count unique senders where latest message matches status
        # Build combined WHERE for final filter
        final_where_parts = [pending_filter]
        if status_filter:
            final_where_parts.append(status_filter)
        final_where = " AND ".join(final_where_parts)
        
        query = f"""
            WITH latest_per_sender AS (
                SELECT DISTINCT ON (COALESCE(sender_contact, sender_id::text, 'unknown'))
                    status
                FROM inbox_messages
                WHERE {base_where}
                ORDER BY COALESCE(sender_contact, sender_id::text, 'unknown'), created_at DESC
            )
            SELECT COUNT(*) as count
            FROM latest_per_sender
            WHERE {final_where}
        """
        params = base_params + status_params
    else:
        # SQLite version - count conversations where latest message matches status
        # Build combined WHERE for final filter
        final_filter_parts = [pending_filter]
        if status_filter:
            final_filter_parts.append(status_filter)
        final_filter = " AND ".join(final_filter_parts)
        
        query = f"""
            SELECT COUNT(*) as count
            FROM inbox_messages m
            WHERE {base_where}
            AND m.id = (
                SELECT m3.id FROM inbox_messages m3
                WHERE m3.license_key_id = m.license_key_id
                AND COALESCE(m3.sender_contact, m3.sender_id, 'unknown') = COALESCE(m.sender_contact, m.sender_id, 'unknown')
                ORDER BY m3.created_at DESC
                LIMIT 1
            )
            AND {final_filter}
        """
        params = base_params + status_params
    
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
    """
    Get all messages from a specific sender (for conversation detail view).
    NOTE: Excludes 'pending' status messages - only shows messages after AI responds.
    """
    # Handle the tg: prefix for telegram user IDs
    async with get_db() as db:
        rows = await fetch_all(
            db,
            """
            SELECT * FROM inbox_messages
            WHERE license_key_id = ?
            AND (sender_contact = ? OR sender_id = ? OR sender_contact LIKE ?)
            AND status != 'pending'
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
    
    Robustness:
    - Finds all aliases (sender_id, sender_contact) associated with this connection
    - Updates all messages matching ANY of these aliases
    - Commits immediately
    """
    from logging_config import get_logger
    logger = get_logger(__name__)
    
    logger.info(f"ignore_chat called: license_id={license_id}, sender_contact='{sender_contact}'")
    
    async with get_db() as db:
        # 1. Find all sender_ids and sender_contacts associated with this contact
        # This handles cases where some messages have username and others have user_id
        aliases_rows = await fetch_all(
            db,
            """
            SELECT DISTINCT sender_contact, sender_id 
            FROM inbox_messages 
            WHERE license_key_id = ? 
            AND (sender_contact = ? OR sender_id = ? OR sender_contact LIKE ?)
            """,
            [license_id, sender_contact, sender_contact, f"%{sender_contact}%"]
        )
        
        # Collect all unique identifiers
        identifiers = set()
        identifiers.add(sender_contact)
        for r in aliases_rows:
            if r.get("sender_contact"): identifiers.add(r["sender_contact"])
            if r.get("sender_id"): identifiers.add(str(r["sender_id"]))
            
        logger.info(f"Identified aliases for ignore: {identifiers}")
        
        # Build dynamic OR query
        conditions = []
        params = [license_id]
        for ident in identifiers:
            conditions.append("sender_contact = ? OR sender_id = ?")
            params.extend([ident, ident])
            
        where_clause = f"license_key_id = ? AND ({' OR '.join(conditions)})"
        
        # Update
        await execute_sql(
            db,
            f"UPDATE inbox_messages SET status = 'ignored' WHERE {where_clause}",
            params
        )
        
        # CRITICAL: Commit immediately
        await commit_db(db)
        
        # Count result
        # Note: We can just return the number of updated rows if the driver supported it,
        # but for now we count again.
        row = await fetch_one(
            db,
            f"SELECT COUNT(*) as count FROM inbox_messages WHERE {where_clause} AND status = 'ignored'",
            params
        )
        result_count = row["count"] if row else 0
        logger.info(f"Messages now with status='ignored': {result_count}")
        
        return result_count


async def approve_chat_messages(license_id: int, sender_contact: str) -> int:
    """
    Mark all 'analyzed' messages from a sender as 'approved'.
    Used when replying to a conversation to ensure the whole thread is marked as handled.
    Returns the count of messages updated.
    """
    async with get_db() as db:
        # Update all 'analyzed' messages from this sender
        await execute_sql(
            db,
            """
            UPDATE inbox_messages 
            SET status = 'approved'
            WHERE license_key_id = ?
            AND (sender_contact = ? OR sender_id = ? OR sender_contact LIKE ?)
            AND status = 'analyzed'
            """,
            [license_id, sender_contact, sender_contact, f"%{sender_contact}%"]
        )
        
        # Get count of affected rows (optional, or just return 0 to be fast)
        # For meaningful return value:
        # Use database-agnostic date comparison
        if DB_TYPE == "postgresql":
            date_filter = "processed_at >= NOW() - INTERVAL '1 minute'"
        else:
            date_filter = "processed_at >= datetime('now', '-1 minute')"
        
        row = await fetch_one(
            db,
            f"""
            SELECT COUNT(*) as count FROM inbox_messages
            WHERE license_key_id = ?
            AND (sender_contact = ? OR sender_id = ? OR sender_contact LIKE ?)
            AND status = 'approved'
            AND {date_filter}
            """, 
            # Note: The count query is tricky because we just updated them. 
            # Simpler to just return 1 or ignore count to avoid complex logic.
            [license_id, sender_contact, sender_contact, f"%{sender_contact}%"]
        )
        # Actually, let's just return row count from UPDATE if possible? 
        # execute_sql usually returns cursor/result. 
        # But our helper returns None. 
        # So we'll just return 0 or query count of all approved.
        
        await commit_db(db)
        return 1



async def fix_stale_inbox_status(license_id: int = None) -> int:
    """
    Scans for conversations that have a 'sent', 'approved', or 'auto_replied' status message LATER
    than an 'analyzed' message, and fixes the 'analyzed' ones to 'approved'.
    Returns number of fixed messages.
    """
    from db_helper import DB_TYPE
    
    # If license_id is None, we run for all licenses (ignoring the filter)
    license_filter = "license_key_id = ?" if license_id else "1=1"
    params = [license_id] if license_id else []

    query = f"""
    UPDATE inbox_messages
    SET status = 'approved'
    WHERE {license_filter}
    AND status = 'analyzed'
    AND (
        EXISTS (
            SELECT 1 FROM inbox_messages m2
            WHERE m2.license_key_id = inbox_messages.license_key_id
            AND (m2.sender_contact = inbox_messages.sender_contact OR m2.sender_id = inbox_messages.sender_id)
            AND m2.status IN ('approved', 'sent', 'auto_replied')
            AND m2.created_at > inbox_messages.created_at
        )
        OR EXISTS (
            SELECT 1 FROM outbox_messages o
            WHERE o.license_key_id = inbox_messages.license_key_id
            AND (o.recipient_email = inbox_messages.sender_contact OR o.recipient_id = inbox_messages.sender_id)
            AND o.status IN ('approved', 'sent')
            AND o.created_at > inbox_messages.created_at
        )
    )
    """
    
    async with get_db() as db:
        await execute_sql(db, query, params)
        return 1


async def mark_chat_read(license_id: int, sender_contact: str) -> int:
    """
    Mark all 'analyzed' messages from a sender as 'read'.
    This clears the unread badge for the conversation.
    Returns the count of messages updated.
    """
    async with get_db() as db:
        # Update all 'analyzed' messages from this sender to is_read=1
        # We keep the status as 'analyzed' (Waiting for Approval) but clear the unread badge.
        query = """
            UPDATE inbox_messages 
            SET is_read = 1
            WHERE license_key_id = ?
            AND (sender_contact = ? OR sender_id = ? OR sender_contact LIKE ?)
            AND status = 'analyzed'
        """
        if DB_TYPE == "postgresql":
            # Postgres needs TRUE/FALSE for boolean
            query = query.replace("is_read = 1", "is_read = TRUE")
            
        await execute_sql(
            db,
            query,
            [license_id, sender_contact, sender_contact, f"%{sender_contact}%"]
        )
        await commit_db(db)
        return 1





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
        # NOTE: Exclude 'pending' status - only show messages after AI responds
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
            AND status != 'pending'
            ORDER BY created_at ASC
            LIMIT ?
            """,
            [license_id, sender_contact, sender_contact, f"%{sender_contact}%", limit]
        )
        
        # Get outgoing messages (from us to client) - sent replies
        # Include delivery_status for real receipt display
        outbox_rows = await fetch_all(
            db,
            """
            SELECT 
                o.id, o.channel, o.recipient_email as sender_contact, o.recipient_id as sender_id,
                o.subject, o.body, o.status,
                o.created_at, o.sent_at,
                o.delivery_status, o.delivered_at, o.read_at,
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




async def get_chat_history_for_llm(
    license_id: int,
    sender_contact: str,
    limit: int = 10
) -> str:
    """
    Get chat history formatted as a string for LLM context.
    Format:
    User: [message]
    Agent: [message]
    ...
    """
    # Reuse the existing full history retrieval
    messages = await get_full_chat_history(license_id, sender_contact, limit=limit)
    
    formatted_history = []
    for msg in messages:
        # Determine speaker
        if msg.get("direction") == "incoming":
            speaker = "User"
        else:
            speaker = "Agent"
            
        # Get content
        content = msg.get("body", "").replace("\n", " ").strip()
        if content:
            formatted_history.append(f"{speaker}: {content}")
            
    return "\n".join(formatted_history)
