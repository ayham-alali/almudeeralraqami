"""
Sync routes for offline operation support.

Provides batch sync endpoint for mobile clients to sync pending operations
with idempotency key support to prevent duplicate processing.
"""
from datetime import datetime, timezone
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from slowapi import Limiter
from slowapi.util import get_remote_address

from dependencies import get_license_context
from database import get_db
from routes.chat_routes import approve_inbox_message
from routes.integrations import send_customer_message
from routes.notifications import process_message_notifications

router = APIRouter(prefix="/api/v1/sync", tags=["sync"])

# Rate limiter for sync endpoints
limiter = Limiter(key_func=get_remote_address)


class SyncOperation(BaseModel):
    """A single operation to sync."""
    id: str = Field(..., description="Client-generated operation ID")
    type: str = Field(..., description="Operation type: approve, ignore, send, delete, etc.")
    idempotency_key: str = Field(..., description="Unique key to prevent duplicate processing")
    payload: dict = Field(default_factory=dict, description="Operation-specific data")
    client_timestamp: Optional[datetime] = Field(None, description="When operation was created on client")


class SyncRequest(BaseModel):
    """Batch sync request."""
    operations: List[SyncOperation]
    device_id: Optional[str] = None


class SyncResult(BaseModel):
    """Result of a single sync operation."""
    operation_id: str
    success: bool
    error: Optional[str] = None
    conflict: bool = False
    server_state: Optional[dict] = None
    server_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class SyncResponse(BaseModel):
    """Batch sync response."""
    results: List[SyncResult]
    processed_count: int
    failed_count: int
    conflict_count: int = 0
    server_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


# In-memory idempotency key cache (in production, use Redis)
# Key: idempotency_key, Value: (result, timestamp)
_idempotency_cache: dict = {}
IDEMPOTENCY_CACHE_TTL_HOURS = 24


def _check_idempotency(key: str) -> Optional[SyncResult]:
    """Check if operation was already processed."""
    if key in _idempotency_cache:
        result, timestamp = _idempotency_cache[key]
        # Check if still valid (within TTL)
        age = (datetime.now(timezone.utc) - timestamp).total_seconds() / 3600
        if age < IDEMPOTENCY_CACHE_TTL_HOURS:
            return result
        else:
            del _idempotency_cache[key]
    return None


def _store_idempotency(key: str, result: SyncResult):
    """Store operation result for idempotency."""
    _idempotency_cache[key] = (result, datetime.now(timezone.utc))
    
    # Clean old entries (simple cleanup - in production use proper TTL)
    if len(_idempotency_cache) > 10000:
        cutoff = datetime.now(timezone.utc)
        to_delete = [
            k for k, (_, t) in _idempotency_cache.items()
            if (cutoff - t).total_seconds() / 3600 > IDEMPOTENCY_CACHE_TTL_HOURS
        ]
        for k in to_delete:
            del _idempotency_cache[k]


@router.post("/batch", response_model=SyncResponse)
@limiter.limit("10/minute")
async def sync_batch(
    request: Request,
    sync_request: SyncRequest,
    license_context: dict = Depends(get_license_context),
):
    """
    Process a batch of offline operations with idempotency support.
    
    Each operation includes an idempotency_key to prevent duplicate processing.
    Operations are processed in order.
    
    Rate limited to 10 requests per minute per IP to prevent abuse.
    
    Args:
        sync_request: Batch of operations to sync
        
    Returns:
        Results for each operation including conflict detection
    """
    license_id = license_context["license_id"]
    results: List[SyncResult] = []
    
    for op in sync_request.operations:
        try:
            # Check idempotency first
            cached = _check_idempotency(op.idempotency_key)
            if cached:
                results.append(cached)
                continue
            
            # Process operation based on type
            result = await _process_operation(op, license_id)
            
            # Store for idempotency
            _store_idempotency(op.idempotency_key, result)
            results.append(result)
            
        except Exception as e:
            result = SyncResult(
                operation_id=op.id,
                success=False,
                error=str(e),
            )
            _store_idempotency(op.idempotency_key, result)
            results.append(result)
    
    return SyncResponse(
        results=results,
        processed_count=sum(1 for r in results if r.success),
        failed_count=sum(1 for r in results if not r.success),
        conflict_count=sum(1 for r in results if r.conflict),
    )


async def _process_operation(op: SyncOperation, license_id: int) -> SyncResult:
    """Process a single sync operation."""
    try:
        if op.type == "approve":
            message_id = op.payload.get("messageId")
            edited_body = op.payload.get("editedBody")
            
            async with get_db() as db:
                await _approve_message(db, license_id, message_id, "approve", edited_body)
            
            return SyncResult(operation_id=op.id, success=True)
            
        elif op.type == "ignore":
            message_id = op.payload.get("messageId")
            
            async with get_db() as db:
                await _approve_message(db, license_id, message_id, "ignore", None)
            
            return SyncResult(operation_id=op.id, success=True)
            
        elif op.type == "send":
            sender_contact = op.payload.get("senderContact")
            message = op.payload.get("body")
            
            async with get_db() as db:
                await _send_message(db, license_id, sender_contact, message)
            
            return SyncResult(operation_id=op.id, success=True)
            
        elif op.type == "delete":
            message_id = op.payload.get("messageId")
            
            async with get_db() as db:
                await _delete_message(db, license_id, message_id)
            
            return SyncResult(operation_id=op.id, success=True)
            
        elif op.type == "mark_read":
            sender_contact = op.payload.get("senderContact")
            
            async with get_db() as db:
                await _mark_conversation_read(db, license_id, sender_contact)
            
            return SyncResult(operation_id=op.id, success=True)
            
        else:
            return SyncResult(
                operation_id=op.id,
                success=False,
                error=f"Unknown operation type: {op.type}",
            )
            
    except Exception as e:
        return SyncResult(
            operation_id=op.id,
            success=False,
            error=str(e),
        )


async def _approve_message(db, license_id: int, message_id: int, action: str, edited_body: Optional[str]):
    """Approve or ignore a message."""
    # Get message and verify ownership
    message = await db.fetchrow(
        """
        SELECT id, license_id, status 
        FROM inbox_messages 
        WHERE id = $1 AND license_id = $2
        """,
        message_id, license_id
    )
    
    if not message:
        raise ValueError(f"Message {message_id} not found")
    
    new_status = "approved" if action == "approve" else "ignored"
    
    await db.execute(
        """
        UPDATE inbox_messages 
        SET status = $1, updated_at = NOW()
        WHERE id = $2
        """,
        new_status, message_id
    )


async def _send_message(db, license_id: int, sender_contact: str, message: str):
    """Send a message to a conversation."""
    # Get the channel for this contact
    existing = await db.fetchrow(
        """
        SELECT channel FROM inbox_messages 
        WHERE license_id = $1 AND sender_contact = $2
        ORDER BY created_at DESC LIMIT 1
        """,
        license_id, sender_contact
    )
    
    channel = existing["channel"] if existing else "whatsapp"
    
    # Create outbox message
    await db.execute(
        """
        INSERT INTO outbox_messages (license_id, sender_contact, body, channel, status, created_at)
        VALUES ($1, $2, $3, $4, 'pending', NOW())
        """,
        license_id, sender_contact, message, channel
    )


async def _delete_message(db, license_id: int, message_id: int):
    """Soft delete a message."""
    await db.execute(
        """
        UPDATE inbox_messages 
        SET is_deleted = true, deleted_at = NOW()
        WHERE id = $1 AND license_id = $2
        """,
        message_id, license_id
    )


async def _mark_conversation_read(db, license_id: int, sender_contact: str):
    """Mark all messages in a conversation as read."""
    await db.execute(
        """
        UPDATE inbox_messages 
        SET is_read = true, read_at = NOW()
        WHERE license_id = $1 AND sender_contact = $2 AND is_read = false
        """,
        license_id, sender_contact
    )


@router.get("/status")
async def get_sync_status(
    license_context: dict = Depends(get_license_context),
):
    """
    Get sync status for the current license.
    
    Returns last sync timestamp and any pending server-side events.
    """
    license_id = license_context["license_id"]
    
    async with get_db() as db:
        # Get count of pending messages
        pending = await db.fetchval(
            """
            SELECT COUNT(*) FROM inbox_messages 
            WHERE license_id = $1 AND status = 'analyzed'
            """,
            license_id
        )
        
        # Get last message timestamp
        last_message = await db.fetchval(
            """
            SELECT MAX(created_at) FROM inbox_messages 
            WHERE license_id = $1
            """,
            license_id
        )
    
    return {
        "pending_count": pending or 0,
        "last_message_at": last_message.isoformat() if last_message else None,
        "server_timestamp": datetime.now(timezone.utc).isoformat(),
    }
