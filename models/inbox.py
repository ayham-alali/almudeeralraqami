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

    # Centralized Bot & Spam Protection
    # Prevent saving messages from known bots and promotional senders
    # Added: Calendly, Submagic, IconScout per user request
    blocked_keywords = [
        "bot", "api", 
        "no-reply", "noreply", "donotreply",
        "newsletter", "bulletin", 
        "calendly", "submagic", "iconscout"
    ]
    
    def is_blocked(text: str) -> bool:
        if not text: return False
        text_lower = text.lower()
        return any(keyword in text_lower for keyword in blocked_keywords)

    if is_blocked(sender_name) or is_blocked(sender_contact):
        # Return 0 to indicate no message was saved
        return 0

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
        
        
        # Call upsert to update conversation state
        # We do this asynchronously/fire-and-forget or await it? 
        # Await it to ensure UI is consistent on next fetch.
        await upsert_conversation_state(license_id, sender_contact, sender_name, channel)

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
        # First, get the message details to pass to upsert_conversation_state
        message_row = await fetch_one(db, "SELECT license_key_id, sender_contact, sender_name, channel FROM inbox_messages WHERE id = ?", [message_id])
        
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
                WHERE id = ? AND (status IS NULL OR status = 'pending')
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
                    WHERE id = ? AND (status IS NULL OR status = 'pending')
                    """,
                    [intent, urgency, sentiment, summary, draft_response, ts_value, message_id],
                )
                await commit_db(db)
            else:
                raise
        
        if message_row:
            await upsert_conversation_state(
                message_row["license_key_id"],
                message_row["sender_contact"],
                message_row["sender_name"],
                message_row["channel"]
            )


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
    # Also exclude soft-deleted messages
    query = "SELECT * FROM inbox_messages WHERE license_key_id = ? AND status != 'pending' AND deleted_at IS NULL"
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
    # Also exclude soft-deleted messages
    query = "SELECT COUNT(*) as count FROM inbox_messages WHERE license_key_id = ? AND status != 'pending' AND deleted_at IS NULL"
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
    subject: str = None,
    attachments: Optional[List[dict]] = None
) -> int:
    """Create outbox message for approval (DB agnostic)."""
    
    # Serialize attachments
    import json
    attachments_json = json.dumps(attachments) if attachments else None
    
    async with get_db() as db:

        await execute_sql(
            db,
            """
            INSERT INTO outbox_messages 
                (inbox_message_id, license_key_id, channel, recipient_id,
                 recipient_email, subject, body, attachments)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [inbox_message_id, license_id, channel, recipient_id, recipient_email, subject, body, attachments_json],
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
        # Get message details before update for upsert_conversation_state
        message_row = await fetch_one(db, "SELECT license_key_id, inbox_message_id FROM outbox_messages WHERE id = ?", [message_id])
        
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

        if message_row and message_row["inbox_message_id"]:
            # Fetch sender_contact from the original inbox message
            inbox_msg = await fetch_one(db, "SELECT sender_contact FROM inbox_messages WHERE id = ?", [message_row["inbox_message_id"]])
            if inbox_msg and inbox_msg["sender_contact"]:
                await upsert_conversation_state(message_row["license_key_id"], inbox_msg["sender_contact"])


async def mark_outbox_sent(message_id: int):
    """Mark outbox message as sent (DB agnostic)."""

    now = datetime.utcnow()
    ts_value = now if DB_TYPE == "postgresql" else now.isoformat()

    async with get_db() as db:
        # Get message details before update for upsert_conversation_state
        message_row = await fetch_one(db, "SELECT license_key_id, inbox_message_id FROM outbox_messages WHERE id = ?", [message_id])

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

        if message_row and message_row["inbox_message_id"]:
            # Fetch sender_contact from the original inbox message
            inbox_msg = await fetch_one(db, "SELECT sender_contact FROM inbox_messages WHERE id = ?", [message_row["inbox_message_id"]])
            if inbox_msg and inbox_msg["sender_contact"]:
                await upsert_conversation_state(message_row["license_key_id"], inbox_msg["sender_contact"])


async def get_pending_outbox(license_id: int) -> List[dict]:
    """Get pending outbox messages (DB agnostic)."""
    async with get_db() as db:
        rows = await fetch_all(
            db,
            """
            SELECT o.*, i.sender_name, i.body as original_message
            FROM outbox_messages o
            LEFT JOIN inbox_messages i ON o.inbox_message_id = i.id
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
    Get inbox conversations using the optimized `inbox_conversations` table.
    This is O(1) per page instead of O(N) full scan.
    """
    params = [license_id]
    where_clauses = ["license_key_id = ?"]
    
    if status and status != "all":
        where_clauses.append("status = ?")
        params.append(status)
        
    if channel and channel != "all":
        where_clauses.append("channel = ?")
        params.append(channel)
        
    where_sql = " AND ".join(where_clauses)
    
    query = f"""
        SELECT 
            COALESCE(ic.last_message_id, 0) as id,
            ic.sender_contact, ic.sender_name, ic.channel,
            last_message_body as body,
            last_message_at as created_at,
            ic.status,
            unread_count
        FROM inbox_conversations ic
        WHERE {where_sql}
        ORDER BY ic.updated_at DESC
        LIMIT ? OFFSET ?
    """
    params.extend([limit, offset])
    
    async with get_db() as db:
        rows = await fetch_all(db, query, params)
        return [dict(row) for row in rows]


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
    """Get counts using the optimized inbox_conversations table."""
    async with get_db() as db:
        # We count CONVERSATIONS (rows in inbox_conversations), not individual messages
        # This aligns with the "Inbox" view
        
        analyzed_row = await fetch_one(db, """
            SELECT COUNT(*) as count FROM inbox_conversations 
            WHERE license_key_id = ? AND status = 'analyzed'
        """, [license_id])
        
        sent_row = await fetch_one(db, """
            SELECT COUNT(*) as count FROM inbox_conversations 
            WHERE license_key_id = ? AND status = 'sent'
        """, [license_id])
        
        ignored_row = await fetch_one(db, """
            SELECT COUNT(*) as count FROM inbox_conversations 
            WHERE license_key_id = ? AND status = 'ignored'
        """, [license_id])
        
        return {
            "analyzed": analyzed_row["count"] if analyzed_row else 0,
            "sent": sent_row["count"] if sent_row else 0,
            "ignored": ignored_row["count"] if ignored_row else 0
        }


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
    # Handle tg: prefix for telegram user IDs
    check_ids = [sender_contact]
    if sender_contact.startswith("tg:"):
        check_ids.append(sender_contact[3:])  # Add ID without tg: prefix

    # Create placeholders for OR condition
    # (sender_contact IN (?, ?) OR sender_id IN (?, ?) OR sender_contact LIKE ?)
    placeholders = ", ".join(["?" for _ in check_ids])
    
    params = [license_id]
    params.extend(check_ids) # For sender_contact IN
    params.extend(check_ids) # For sender_id IN
    params.append(f"%{sender_contact}%") # For LIKE
    params.append(limit)

    async with get_db() as db:
        rows = await fetch_all(
            db,
            f"""
            SELECT * FROM inbox_messages
            WHERE license_key_id = ?
            AND (sender_contact IN ({placeholders}) OR sender_id IN ({placeholders}) OR sender_contact LIKE ?)
            AND status != 'pending'
            AND deleted_at IS NULL
            ORDER BY created_at ASC
            LIMIT ?
            """,
            params
        )
        return rows


async def get_conversation_messages_cursor(
    license_id: int,
    sender_contact: str,
    limit: int = 25,
    cursor: Optional[str] = None,
    direction: str = "older"  # "older" (scroll up) or "newer" (new messages)
) -> dict:
    """
    Get messages from a specific sender with cursor-based pagination.
    
    Cursor format: "{created_at_iso}_{message_id}"
    
    Args:
        license_id: The license ID
        sender_contact: The sender's contact identifier
        limit: Number of messages to fetch
        cursor: Pagination cursor (created_at_iso_message_id)
        direction: "older" to get older messages, "newer" for new messages
        
    Returns:
        {
            "messages": [...],
            "next_cursor": "2024-01-01T12:00:00_123" or None,
            "has_more": True/False
        }
    """
    import base64
    
    # Parse cursor if provided
    cursor_created_at = None
    cursor_id = None
    if cursor:
        try:
            # Decode base64 cursor
            decoded = base64.b64decode(cursor).decode('utf-8')
            parts = decoded.rsplit('_', 1)
            if len(parts) == 2:
                cursor_created_at = parts[0]
                cursor_id = int(parts[1])
        except Exception:
            pass  # Invalid cursor, start from beginning
    
    # Handle tg: prefix
    check_ids = [sender_contact]
    if sender_contact.startswith("tg:"):
        check_ids.append(sender_contact[3:])
        
    placeholders = ", ".join(["?" for _ in check_ids])
    
    async with get_db() as db:
        # Build query based on direction
        if direction == "older":
            # For scrolling up (loading older messages)
            if cursor_created_at and cursor_id:
                query = f"""
                    SELECT * FROM inbox_messages
                    WHERE license_key_id = ?
                    AND (sender_contact IN ({placeholders}) OR sender_id IN ({placeholders}) OR sender_contact LIKE ?)
                    AND status != 'pending'
                    AND (created_at < ? OR (created_at = ? AND id < ?))
                    ORDER BY created_at DESC, id DESC
                    LIMIT ?
                """
                params = [license_id]
                params.extend(check_ids)
                params.extend(check_ids)
                params.append(f"%{sender_contact}%")
                params.extend([cursor_created_at, cursor_created_at, cursor_id, limit + 1])
            else:
                # No cursor - get newest messages first (bottom of chat)
                query = f"""
                    SELECT * FROM inbox_messages
                    WHERE license_key_id = ?
                    AND (sender_contact IN ({placeholders}) OR sender_id IN ({placeholders}) OR sender_contact LIKE ?)
                    AND status != 'pending'
                    ORDER BY created_at DESC, id DESC
                    LIMIT ?
                """
                params = [license_id]
                params.extend(check_ids)
                params.extend(check_ids)
                params.append(f"%{sender_contact}%")
                params.append(limit + 1)
        else:
            # For loading newer messages (real-time updates)
            if cursor_created_at and cursor_id:
                query = f"""
                    SELECT * FROM inbox_messages
                    WHERE license_key_id = ?
                    AND (sender_contact IN ({placeholders}) OR sender_id IN ({placeholders}) OR sender_contact LIKE ?)
                    AND status != 'pending'
                    AND (created_at > ? OR (created_at = ? AND id > ?))
                    ORDER BY created_at ASC, id ASC
                    LIMIT ?
                """
                params = [license_id]
                params.extend(check_ids)
                params.extend(check_ids)
                params.append(f"%{sender_contact}%")
                params.extend([cursor_created_at, cursor_created_at, cursor_id, limit + 1])
            else:
                # No cursor for newer - return empty (must provide cursor)
                return {"messages": [], "next_cursor": None, "has_more": False}
        
        rows = await fetch_all(db, query, params)
        
        # Check if there are more results
        has_more = len(rows) > limit
        messages = list(rows[:limit])
        
        # Reverse for "older" direction to get chronological order
        if direction == "older":
            messages.reverse()
        
        # Generate next cursor from oldest message (for "older" direction)
        next_cursor = None
        if has_more and messages:
            if direction == "older":
                # Cursor for loading even older messages
                oldest = messages[0]
            else:
                # Cursor for loading even newer messages  
                oldest = messages[-1]
            
            ts = oldest.get("created_at")
            if hasattr(ts, 'isoformat'):
                ts = ts.isoformat()
            cursor_str = f"{ts}_{oldest['id']}"
            next_cursor = base64.b64encode(cursor_str.encode('utf-8')).decode('utf-8')
        
        return {
            "messages": messages,
            "next_cursor": next_cursor,
            "has_more": has_more
        }


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
        
        if result_count > 0:
            await upsert_conversation_state(license_id, sender_contact)
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
        # So we'll just return 0 or query count of all approved.
        
        await commit_db(db)
        await upsert_conversation_state(license_id, sender_contact)
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


async def mark_message_as_read(message_id: int, license_id: int) -> bool:
    """Mark a single inbox message as read."""
    async with get_db() as db:
        query = "UPDATE inbox_messages SET is_read = 1 WHERE id = ? AND license_key_id = ?"
        params = [message_id, license_id]
        if DB_TYPE == "postgresql":
            query = "UPDATE inbox_messages SET is_read = TRUE WHERE id = ? AND license_key_id = ?"
            
        await execute_sql(db, query, params)
        await commit_db(db)
        
        # After marking as read, update the conversation's unread_count
        row = await fetch_one(db, "SELECT sender_contact FROM inbox_messages WHERE id = ?", [message_id])
        if row and row["sender_contact"]:
            await upsert_conversation_state(license_id, row["sender_contact"])
        return True

async def mark_chat_read(license_id: int, sender_contact: str) -> int:
    """
    Mark all 'analyzed' messages from a sender as 'read'.
    This clears the unread badge for the conversation.
    Returns the count of messages updated.
    """
    async with get_db() as db:
        # Update all messages from this sender to is_read=1
        query = """
            UPDATE inbox_messages 
            SET is_read = 1
            WHERE license_key_id = ?
            AND (sender_contact = ? OR sender_id = ? OR sender_contact LIKE ?)
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
        await upsert_conversation_state(license_id, sender_contact)
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
        # Handle tg: prefix
        check_ids = [sender_contact]
        if sender_contact.startswith("tg:"):
            check_ids.append(sender_contact[3:])
            
        placeholders = ", ".join(["?" for _ in check_ids])
        
        # Get incoming messages (from client to us)
        # NOTE: Exclude 'pending' status - only show messages after AI responds
        
        # Build params
        params = [license_id]
        params.extend(check_ids) # sender_contact IN
        params.extend(check_ids) # sender_id IN
        params.append(f"%{sender_contact}%") # LIKE
        params.append(limit)
        
        inbox_rows = await fetch_all(
            db,
            f"""
            SELECT 
                id, channel, sender_name, sender_contact, sender_id, 
                subject, body, 
                intent, urgency, sentiment, language, dialect,
                ai_summary, ai_draft_response, status,
                created_at, received_at
            FROM inbox_messages
            WHERE license_key_id = ?
            AND (sender_contact IN ({placeholders}) OR sender_id IN ({placeholders}) OR sender_contact LIKE ?)
            AND status != 'pending'
            AND deleted_at IS NULL
            ORDER BY created_at ASC
            LIMIT ?
            """,
            params
        )
        
        # Get outgoing messages (from us to client) - sent replies
        # Include delivery_status for real receipt display
        # Get outgoing messages (from us to client) - sent replies
        # Include delivery_status for real receipt display
        
        # Build params for outbox
        # Outbox checks recipient_email or recipient_id
        out_params = [license_id]
        out_params.extend(check_ids) # recipient_email IN
        out_params.extend(check_ids) # recipient_id IN
        out_params.append(f"%{sender_contact}%") # LIKE
        out_params.append(limit)
        
        outbox_rows = await fetch_all(
            db,
            f"""
            SELECT 
                o.id, o.channel, o.recipient_email as sender_contact, o.recipient_id as sender_id,
                o.subject, o.body, o.status,
                o.created_at, o.sent_at,
                o.delivery_status,
                i.sender_name
            FROM outbox_messages o
            LEFT JOIN inbox_messages i ON o.inbox_message_id = i.id
            WHERE o.license_key_id = ?
            AND (o.recipient_email IN ({placeholders}) OR o.recipient_id IN ({placeholders}) OR o.recipient_email LIKE ?)
            AND o.status IN ('sent', 'approved')
            AND o.deleted_at IS NULL
            ORDER BY o.created_at ASC
            LIMIT ?
            """,
            out_params
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


# ============ Message Editing Functions ============

async def get_outbox_message_by_id(message_id: int, license_id: int) -> Optional[dict]:
    """Get a single outbox message by ID."""
    async with get_db() as db:
        row = await fetch_one(
            db,
            "SELECT * FROM outbox_messages WHERE id = ? AND license_key_id = ?",
            [message_id, license_id]
        )
        return row


async def edit_outbox_message(
    message_id: int,
    license_id: int,
    new_body: str,
    edit_window_minutes: int = 15
) -> dict:
    """
    Edit an outbox message (agent's sent message).
    
    Args:
        message_id: ID of the message to edit
        license_id: License ID for ownership verification
        new_body: New message content
        edit_window_minutes: Time window for editing (default 15 minutes)
        
    Returns:
        {"success": True/False, "message": str, "edited_at": str}
        
    Raises:
        ValueError: If message not found, not owned, or edit window expired
    """
    async with get_db() as db:
        # Get the message
        message = await fetch_one(
            db,
            "SELECT * FROM outbox_messages WHERE id = ? AND license_key_id = ?",
            [message_id, license_id]
        )
        
        if not message:
            raise ValueError("الرسالة غير موجودة")
        
        # Check if message was sent too long ago
        created_at = message.get("created_at")
        if created_at:
            if isinstance(created_at, str):
                from datetime import datetime
                try:
                    created_time = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                except:
                    created_time = datetime.utcnow()
            else:
                created_time = created_at
            
            from datetime import datetime, timezone, timedelta
            now = datetime.now(timezone.utc)
            if created_time.tzinfo is None:
                created_time = created_time.replace(tzinfo=timezone.utc)
            
            time_diff = now - created_time
            if time_diff > timedelta(minutes=edit_window_minutes):
                raise ValueError(f"لا يمكن تعديل الرسالة بعد {edit_window_minutes} دقيقة من الإرسال")
        
        # Store original body if this is the first edit
        original_body = message.get("original_body") or message.get("body", "")
        current_edit_count = message.get("edit_count", 0) or 0
        
        now = datetime.now(timezone.utc)
        ts_value = now if DB_TYPE == "postgresql" else now.isoformat()
        
        # Update the message
        await execute_sql(
            db,
            """
            UPDATE outbox_messages 
            SET body = ?, 
                edited_at = ?,
                original_body = COALESCE(original_body, ?),
                edit_count = ?
            WHERE id = ? AND license_key_id = ?
            """,
            [new_body, ts_value, original_body, current_edit_count + 1, message_id, license_id]
        )
        await commit_db(db)
        
        # Update conversation if this was the last message
        recipient = message.get("recipient_email") or message.get("recipient_id")
        if recipient:
             await upsert_conversation_state(license_id, recipient)
        
        return {
            "success": True,
            "message": "تم تعديل الرسالة بنجاح",
            "edited_at": now.isoformat(),
            "edit_count": current_edit_count + 1
        }


async def soft_delete_outbox_message(message_id: int, license_id: int) -> dict:
    """Soft delete an outbox message."""
    async with get_db() as db:
        # Check if message exists and is owned by this license
        message = await fetch_one(
            db,
            "SELECT id, deleted_at, recipient_email, recipient_id FROM outbox_messages WHERE id = ? AND license_key_id = ?",
            [message_id, license_id]
        )
        
        if not message:
            raise ValueError("الرسالة غير موجودة")
        
        if message.get("deleted_at"):
            raise ValueError("الرسالة محذوفة مسبقاً")
        
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        ts_value = now if DB_TYPE == "postgresql" else now.isoformat()
        
        # Soft delete
        await execute_sql(
            db,
            "UPDATE outbox_messages SET deleted_at = ? WHERE id = ? AND license_key_id = ?",
            [ts_value, message_id, license_id]
        )
        await commit_db(db)
        
        # Update conversation
        recipient = message.get("recipient_email") or message.get("recipient_id")
        if recipient:
             await upsert_conversation_state(license_id, recipient)
        
        return {
            "success": True,
            "message": "تم حذف الرسالة بنجاح",
            "deleted_at": now.isoformat()
        }


async def soft_delete_message(message_id: int, license_id: int) -> dict:
    """
    Unified delete function. Tries to delete from outbox first, then inbox.
    """
    try:
        # Try outbox first (most common for deletion)
        return await soft_delete_outbox_message(message_id, license_id)
    except ValueError as e:
        # If not found in outbox, try inbox
        if str(e) == "الرسالة غير موجودة":
            return await soft_delete_inbox_message(message_id, license_id)
        raise e


async def soft_delete_inbox_message(message_id: int, license_id: int) -> dict:
    """Soft delete an inbox message."""
    async with get_db() as db:
        message = await fetch_one(
            db,
            "SELECT id, deleted_at, sender_contact FROM inbox_messages WHERE id = ? AND license_key_id = ?",
            [message_id, license_id]
        )
        
        if not message:
            raise ValueError("الرسالة غير موجودة")
            
        if message.get("deleted_at"):
            raise ValueError("الرسالة محذوفة مسبقاً")
            
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        ts_value = now if DB_TYPE == "postgresql" else now.isoformat()
        
        await execute_sql(
            db,
            "UPDATE inbox_messages SET deleted_at = ? WHERE id = ? AND license_key_id = ?",
            [ts_value, message_id, license_id]
        )
        await commit_db(db)
        
        if message.get("sender_contact"):
            await upsert_conversation_state(license_id, message["sender_contact"])

        return {
            "success": True, 
            "message": "تم حذف الرسالة بنجاح",
            "deleted_at": now.isoformat()
        }


async def soft_delete_conversation(license_id: int, sender_contact: str) -> dict:
    """
    Soft delete an entire conversation (both inbox and outbox messages).
    Then updates conversation state (which should effectively remove it).
    """
    from datetime import datetime, timezone
    
    # Handle tg: prefix similar to other functions
    check_ids = [sender_contact]
    if sender_contact.startswith("tg:"):
        check_ids.append(sender_contact[3:])
        
    placeholders = ", ".join(["?" for _ in check_ids])
    
    # Params for queries
    # For inbox: sender_contact/id
    in_params = [license_id]
    in_params.extend(check_ids)
    in_params.extend(check_ids)
    in_params.append(f"%{sender_contact}%")
    
    # For outbox: recipient_email/id
    out_params = [license_id]
    out_params.extend(check_ids)
    out_params.extend(check_ids)
    out_params.append(f"%{sender_contact}%")
    
    now = datetime.now(timezone.utc)
    ts_value = now if DB_TYPE == "postgresql" else now.isoformat()
    
    # Add timestamp to params
    in_params.insert(0, ts_value) # UPDATE ... SET deleted_at = ? WHERE ...
    out_params.insert(0, ts_value)

    async with get_db() as db:
        # Update Inbox
        await execute_sql(
            db,
            f"""
            UPDATE inbox_messages 
            SET deleted_at = ?
            WHERE license_key_id = ?
            AND (sender_contact IN ({placeholders}) OR sender_id IN ({placeholders}) OR sender_contact LIKE ?)
            AND deleted_at IS NULL
            """,
            in_params
        )
        
        # Update Outbox
        await execute_sql(
            db,
            f"""
            UPDATE outbox_messages 
            SET deleted_at = ?
            WHERE license_key_id = ?
            AND (recipient_email IN ({placeholders}) OR recipient_id IN ({placeholders}) OR recipient_email LIKE ?)
             AND deleted_at IS NULL
            """,
            out_params
        )
        
        await commit_db(db)
        
    # Update state - this will see 0 messages and delete the conversation row
    await upsert_conversation_state(license_id, sender_contact)
    
    return {"success": True, "message": "تم حذف المحادثة بنجاح"}


async def restore_deleted_message(message_id: int, license_id: int) -> dict:
    """
    Restore a soft-deleted outbox message.
    
    Args:
        message_id: ID of the message to restore
        license_id: License ID for ownership verification
        
    Returns:
        {"success": True/False, "message": str}
    """
    async with get_db() as db:
        message = await fetch_one(
            db,
            "SELECT id, deleted_at FROM outbox_messages WHERE id = ? AND license_key_id = ?",
            [message_id, license_id]
        )
        
        if not message:
            raise ValueError("الرسالة غير موجودة")
        
        if not message.get("deleted_at"):
            raise ValueError("الرسالة غير محذوفة")
        
        await execute_sql(
            db,
            "UPDATE outbox_messages SET deleted_at = NULL WHERE id = ? AND license_key_id = ?",
            [message_id, license_id]
        )
        await commit_db(db)
        
        return {
            "success": True,
            "message": "تم استعادة الرسالة بنجاح"
        }


async def search_messages(
    license_id: int,
    query: str,
    sender_contact: str = None,
    limit: int = 50,
    offset: int = 0
) -> dict:
    """
    Search messages using Full-Text Search.
    Supports SQLite (FTS5) and PostgreSQL (TSVector).
    Returns a unified list of inbox/outbox messages sorted by relevance.
    """
    if not query:
        return {"messages": [], "total": 0}

    results = []
    
    async with get_db() as db:
        if DB_TYPE == "postgresql":
            # PostgreSQL Search
            params = [query, license_id, limit, offset]
            filter_clause = ""
            if sender_contact:
                params = [query, license_id, sender_contact, limit, offset]
                # We need to filter both parts of UNION
                # Use parameter $3 for contact
                filter_clause = "AND (sender_contact = $3 OR target = $3)" 
                # Wait, separate queries need correct param index?
                # Actually, simpler to inject param placeholder or adjust list.
                # Let's simple use formatted string for parameter index or careful construction.
                # $3 is contact.
                
                search_query = """
                WITH search_results AS (
                    SELECT 
                        'inbox' as source_table, 
                        id, 
                        body, 
                        sender_name, 
                        sender_contact,
                        received_at as timestamp, 
                        subject,
                        is_read::int as is_read,
                        ts_rank(search_vector, websearch_to_tsquery('english', $1)) as rank
                    FROM inbox_messages
                    WHERE search_vector @@ websearch_to_tsquery('english', $1) 
                      AND license_key_id = $2
                      AND ($3::text IS NULL OR sender_contact = $3)
                    
                    UNION ALL
                    
                    SELECT 
                        'outbox' as source_table, 
                        id, 
                        body, 
                        COALESCE(recipient_email, recipient_id) as sender_name, 
                        COALESCE(recipient_email, recipient_id) as sender_contact,
                        created_at as timestamp, 
                        NULL as subject,
                        1 as is_read,
                        ts_rank(search_vector, websearch_to_tsquery('english', $1)) as rank
                    FROM outbox_messages
                    WHERE search_vector @@ websearch_to_tsquery('english', $1) 
                      AND license_key_id = $2
                      AND ($3::text IS NULL OR COALESCE(recipient_email, recipient_id) = $3)
                )
                SELECT *, count(*) OVER() as full_count 
                FROM search_results
                ORDER BY rank DESC, timestamp DESC
                LIMIT $4 OFFSET $5
                """
                # Params: query, license_id, sender_contact, limit, offset
            else:
                 # No contact filter
                 search_query = """
                WITH search_results AS (
                    SELECT 
                        'inbox' as source_table, 
                        id, 
                        body, 
                        sender_name, 
                        sender_contact,
                        received_at as timestamp, 
                        subject,
                        is_read::int as is_read,
                        ts_rank(search_vector, websearch_to_tsquery('english', $1)) as rank
                    FROM inbox_messages
                    WHERE search_vector @@ websearch_to_tsquery('english', $1) 
                      AND license_key_id = $2
                    
                    UNION ALL
                    
                    SELECT 
                        'outbox' as source_table, 
                        id, 
                        body, 
                        COALESCE(recipient_email, recipient_id) as sender_name, 
                        COALESCE(recipient_email, recipient_id) as sender_contact,
                        created_at as timestamp, 
                        NULL as subject,
                        1 as is_read,
                        ts_rank(search_vector, websearch_to_tsquery('english', $1)) as rank
                    FROM outbox_messages
                    WHERE search_vector @@ websearch_to_tsquery('english', $1) 
                      AND license_key_id = $2
                )
                SELECT *, count(*) OVER() as full_count 
                FROM search_results
                ORDER BY rank DESC, timestamp DESC
                LIMIT $3 OFFSET $4
                """
            
            rows = await fetch_all(db, search_query, params)
            
        else:
            # SQLite Search
            params = [query, license_id, limit, offset]
            contact_filter = ""
            if sender_contact:
                params = [query, license_id, sender_contact, limit, offset]
                contact_filter = """
                    AND (
                        (m.source_table = 'inbox' AND i.sender_contact = ?)
                        OR
                        (m.source_table = 'outbox' AND COALESCE(o.recipient_email, o.recipient_id) = ?)
                    )
                """ 
                # But wait, parameter binding order!
                # query, license, contact, contact, limit, offset?
                # Or use named parameters? fetch_all usually positional.
                # Let's adjust params list manually.
                params = [query, license_id, sender_contact, sender_contact, limit, offset]

            search_query = f"""
                SELECT 
                    m.source_table,
                    m.source_id as id,
                    m.body,
                    m.sender_name,
                    CASE 
                        WHEN m.source_table = 'inbox' THEN i.sender_contact 
                        ELSE COALESCE(o.recipient_email, o.recipient_id) 
                    END as sender_contact,
                    CASE 
                        WHEN m.source_table = 'inbox' THEN i.received_at 
                        ELSE o.created_at 
                    END as timestamp,
                    CASE 
                        WHEN m.source_table = 'inbox' THEN i.subject 
                        ELSE NULL 
                    END as subject,
                    CASE
                        WHEN m.source_table = 'inbox' THEN COALESCE(i.is_read, 0)
                        ELSE 1
                    END as is_read
                FROM messages_fts m
                LEFT JOIN inbox_messages i ON m.source_table = 'inbox' AND m.source_id = i.id
                LEFT JOIN outbox_messages o ON m.source_table = 'outbox' AND m.source_id = o.id
                WHERE m.messages_fts MATCH ? 
                  AND m.license_id = ?
                  {contact_filter if sender_contact else ""}
                ORDER BY m.rank, timestamp DESC
                LIMIT ? OFFSET ?
            """
            
            rows = await fetch_all(db, search_query, params)

    # Formatting results
    formatted_messages = []
    full_count = 0
    
    if rows:
        # Try to get full_count from first row if available (Postgres)
        first_row = dict(rows[0])
        full_count = first_row.get("full_count", len(rows)) # Approx for SQLite if not implemented

        for row in rows:
            r = dict(row)
            formatted_messages.append({
                "id": r["id"],
                "type": r["source_table"], # 'inbox' or 'outbox'
                "body": r["body"],
                "sender_name": r["sender_name"],
                "sender_contact": r["sender_contact"],
                "subject": r.get("subject"),
                "timestamp": r["timestamp"], # datetime object or string
                "is_read": bool(r.get("is_read", True))
            })

    return {
        "results": formatted_messages,
        "count": full_count if full_count != 0 else len(formatted_messages)
    }


# ============ Conversation Optimization (Denormalized) ============

async def upsert_conversation_state(
    license_id: int, 
    sender_contact: str, 
    sender_name: Optional[str] = None,
    channel: Optional[str] = None
):
    """
    Recalculate and update the cached conversation state in `inbox_conversations`.
    To best maintain consistency, we re-calculate from source tables.
    This approach is "read-heavy write" but ensures accuracy vs incremental updates which can drift.
    """
    from db_helper import DB_TYPE
    
    async with get_db() as db:
        # 1. Get stats
        # Unread count: incoming messages that are analyzed but not read
        # Message count: all non-pending messages
        
        # Handle tg: prefix for accurate counts
        check_ids = [sender_contact]
        if sender_contact.startswith("tg:"):
            check_ids.append(sender_contact[3:])
            
        placeholders = ", ".join(["?" for _ in check_ids])
    
        # Calculate Unread Count
        unread_conditions = "is_read = 0 OR is_read IS NULL"
        if DB_TYPE == "postgresql":
            unread_conditions = "is_read IS FALSE OR is_read IS NULL"
            
        # Params for unread
        unread_params = [license_id]
        unread_params.extend(check_ids) # sender_contact IN
        unread_params.extend(check_ids) # sender_id IN
        unread_params.append(f"%{sender_contact}%") # LIKE
        
        row_unread = await fetch_one(db, f"""
            SELECT COUNT(*) as count FROM inbox_messages 
            WHERE license_key_id = ? 
            AND (sender_contact IN ({placeholders}) OR sender_id IN ({placeholders}) OR sender_contact LIKE ?)
            AND status = 'analyzed' 
            AND deleted_at IS NULL
            AND ({unread_conditions})
        """, unread_params)
        unread_count = row_unread["count"] if row_unread else 0
        
        # Calculate Total Message Count (excluding pending)
        # Params for total
        total_params = [license_id]
        total_params.extend(check_ids)
        total_params.extend(check_ids)
        total_params.append(f"%{sender_contact}%")
        
        row_count = await fetch_one(db, f"""
            SELECT COUNT(*) as count FROM inbox_messages 
            WHERE license_key_id = ? 
            AND (sender_contact IN ({placeholders}) OR sender_id IN ({placeholders}) OR sender_contact LIKE ?)
            AND status != 'pending'
            AND deleted_at IS NULL
        """, total_params)
        message_count = row_count["count"] if row_count else 0
        
        # 2. Get Last Message (Source of Truth)
        # Could be Inbox OR Outbox. We need the absolute latest.
        # Efficient querying: Get latest from each, compare.
        
        latest_inbox = await fetch_one(db, f"""
            SELECT id, body, received_at as created_at, status 
            FROM inbox_messages 
            WHERE license_key_id = ? 
            AND (sender_contact IN ({placeholders}) OR sender_id IN ({placeholders}) OR sender_contact LIKE ?)
            AND status != 'pending'
            AND deleted_at IS NULL
            ORDER BY created_at ASC LIMIT 1
        """, total_params)
        
        # Outbox params
        out_params = [license_id]
        out_params.extend(check_ids) # recipient_email IN
        out_params.extend(check_ids) # recipient_id IN
        out_params.append(f"%{sender_contact}%") # LIKE

        latest_outbox = await fetch_one(db, f"""
            SELECT id, body, created_at, status 
            FROM outbox_messages 
            WHERE license_key_id = ? 
            AND (recipient_email IN ({placeholders}) OR recipient_id IN ({placeholders}) OR recipient_email LIKE ?) 
            AND deleted_at IS NULL
            ORDER BY created_at DESC LIMIT 1
        """, out_params)
        
        # Determine winner
        last_message = None
        last_message_at = None
        
        last_inbox_time = None
        if latest_inbox:
            # Handle string/datetime differences
            last_inbox_time = latest_inbox["created_at"]
            if isinstance(last_inbox_time, str):
                try: last_inbox_time = datetime.fromisoformat(last_inbox_time.replace('Z', '+00:00'))
                except: pass
        
        last_outbox_time = None
        if latest_outbox:
             last_outbox_time = latest_outbox["created_at"]
             if isinstance(last_outbox_time, str):
                try: last_outbox_time = datetime.fromisoformat(last_outbox_time.replace('Z', '+00:00'))
                except: pass

        # Compare
        is_inbox_latest = False
        if last_inbox_time and last_outbox_time:
            if last_inbox_time >= last_outbox_time:
                last_message = latest_inbox
                last_message_at = last_inbox_time
                is_inbox_latest = True
            else:
                last_message = latest_outbox
                last_message_at = last_outbox_time
        elif last_inbox_time:
            last_message = latest_inbox
            last_message_at = last_inbox_time
            is_inbox_latest = True
        elif last_outbox_time:
             last_message = latest_outbox
             last_message_at = last_outbox_time
        
        if not last_message:
            # No valid messages? (Maybe all pending or deleted).
            # Clean up empty conversation entry to prevent ghost chats.
            # NOTE: We only delete if there are truly no messages left.
            if message_count == 0:
                await execute_sql(
                    db, 
                    "DELETE FROM inbox_conversations WHERE license_key_id = ? AND sender_contact = ?", 
                    [license_id, sender_contact]
                )
            return

        status = last_message["status"]
        body = last_message["body"]
        msg_id = last_message["id"]

        # 3. Upsert
        now = datetime.utcnow()
        ts_value = now if DB_TYPE == "postgresql" else now.isoformat()
        
        # Prepare timestamp for DB
        last_ts_value = last_message_at
        if DB_TYPE != "postgresql" and isinstance(last_message_at, datetime):
            last_ts_value = last_message_at.isoformat()
        
        fields = ["license_key_id", "sender_contact", "last_message_id", "last_message_body", 
                  "last_message_at", "status", "unread_count", "message_count", "updated_at"]
        params = [license_id, sender_contact, msg_id, body, last_ts_value, status, unread_count, message_count, ts_value]
        
        update_frame = """
            last_message_id = ?, last_message_body = ?, last_message_at = ?, 
            status = ?, unread_count = ?, message_count = ?, updated_at = ?
        """
        
        if sender_name:
            fields.append("sender_name")
            params.append(sender_name)
            update_frame += ", sender_name = ?"
            
        if channel:
            fields.append("channel")
            params.append(channel)
            update_frame += ", channel = ?"
            
        # Placeholders
        placeholders = ", ".join(["?" for _ in fields])
        cols = ", ".join(fields)
        
        if DB_TYPE == "postgresql":
            # Simple upsert
            sql = f"""
                INSERT INTO inbox_conversations ({cols}) VALUES ({placeholders})
                ON CONFLICT (license_key_id, sender_contact) DO UPDATE SET
                last_message_id = EXCLUDED.last_message_id,
                last_message_body = EXCLUDED.last_message_body,
                last_message_at = EXCLUDED.last_message_at,
                status = EXCLUDED.status,
                unread_count = EXCLUDED.unread_count,
                message_count = EXCLUDED.message_count,
                updated_at = EXCLUDED.updated_at
            """
            if sender_name: sql += ", sender_name = EXCLUDED.sender_name"
            if channel: sql += ", channel = EXCLUDED.channel"
            
            await execute_sql(db, sql, params)
            
        else:
             sql = f"""
                INSERT INTO inbox_conversations ({cols}) VALUES ({placeholders})
                ON CONFLICT(license_key_id, sender_contact) DO UPDATE SET
                last_message_id = excluded.last_message_id,
                last_message_body = excluded.last_message_body,
                last_message_at = excluded.last_message_at,
                status = excluded.status,
                unread_count = excluded.unread_count,
                message_count = excluded.message_count,
                updated_at = excluded.updated_at
            """
             if sender_name: sql += ", sender_name = excluded.sender_name"
             if channel: sql += ", channel = excluded.channel"
             
             await execute_sql(db, sql, params)
        
        await commit_db(db)
