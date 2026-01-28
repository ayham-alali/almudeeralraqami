"""
Sync routes for offline operation support.

Provides batch sync endpoint for mobile clients to sync pending operations
with idempotency key support to prevent duplicate processing.
"""
from datetime import datetime, timezone
from typing import List, Optional
from fastapi import APIRouter, Depends, Request, BackgroundTasks
from pydantic import BaseModel, Field

from dependencies import get_license_from_header

router = APIRouter(prefix="/api/v1/sync", tags=["sync"])


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
        age = (datetime.now(timezone.utc) - timestamp).total_seconds() / 3600
        if age < IDEMPOTENCY_CACHE_TTL_HOURS:
            return result
        else:
            del _idempotency_cache[key]
    return None


def _store_idempotency(key: str, result: SyncResult):
    """Store operation result for idempotency."""
    _idempotency_cache[key] = (result, datetime.now(timezone.utc))
    
    # Clean old entries
    if len(_idempotency_cache) > 10000:
        cutoff = datetime.now(timezone.utc)
        to_delete = [
            k for k, (_, t) in _idempotency_cache.items()
            if (cutoff - t).total_seconds() / 3600 > IDEMPOTENCY_CACHE_TTL_HOURS
        ]
        for k in to_delete:
            del _idempotency_cache[k]


@router.post("/batch", response_model=SyncResponse)
async def sync_batch(
    request: Request,
    sync_request: SyncRequest,
    background_tasks: BackgroundTasks,
    license_data: dict = Depends(get_license_from_header),
):
    """
    Process a batch of offline operations with idempotency support.
    
    Each operation includes an idempotency_key to prevent duplicate processing.
    Operations are processed in order.
    """
    license_id = license_data.get("license_id")
    results: List[SyncResult] = []
    
    for op in sync_request.operations:
        try:
            # Check idempotency first (in-memory cache)
            cached = _check_idempotency(op.idempotency_key)
            if cached:
                results.append(cached)
                continue
            
            # Process operation
            result = await _process_operation(op, license_id, background_tasks)
            
            _store_idempotency(op.idempotency_key, result)
            results.append(result)
            
        except Exception as e:
            # Log error but don't crash batch
            print(f"Sync error op {op.id}: {e}")
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


async def _process_operation(op: SyncOperation, license_id: int, background_tasks: BackgroundTasks) -> SyncResult:
    """Process a single sync operation using shared business logic."""
    from models import (
        create_outbox_message,
        approve_outbox_message,
        get_full_chat_history,
    )
    from models.inbox import (
        get_inbox_message_by_id,
        approve_chat_messages,
        update_inbox_status,
        soft_delete_message,
        mark_chat_read,
        soft_delete_conversation,
    )
    from routes.chat_routes import send_approved_message

    try:
        if op.type == "approve":
            message_id = op.payload.get("messageId")
            edited_body = op.payload.get("editedBody")
            
            message = await get_inbox_message_by_id(message_id, license_id)
            if not message:
                return SyncResult(operation_id=op.id, success=False, error="Message not found")
            
            body = edited_body or message.get("ai_draft_response")
            if not body:
                return SyncResult(operation_id=op.id, success=False, error="No response body")
            
            # Create and approve outbox message
            outbox_id = await create_outbox_message(
                inbox_message_id=message_id,
                license_id=license_id,
                channel=message["channel"],
                body=body,
                recipient_id=message.get("sender_id"),
                recipient_email=message.get("sender_contact")
            )
            await approve_outbox_message(outbox_id, body)
            await update_inbox_status(message_id, "approved")
            
            sender = message.get("sender_contact") or message.get("sender_id")
            if sender:
                await approve_chat_messages(license_id, sender)
            
            # Queue for sending
            background_tasks.add_task(send_approved_message, outbox_id, license_id)
            return SyncResult(operation_id=op.id, success=True)
            
        elif op.type == "ignore":
            # LEGACY: Treat ignore as 'approve' (handled) for unified inbox
            message_id = op.payload.get("messageId")
            message = await get_inbox_message_by_id(message_id, license_id)
            
            if message:
                sender = message.get("sender_contact") or message.get("sender_id")
                if sender:
                    # Mark all as approved (handled)
                    await approve_chat_messages(license_id, sender)
                else:
                    await update_inbox_status(message_id, "approved")
            
            return SyncResult(operation_id=op.id, success=True)
            
        elif op.type == "send":
            sender_contact = op.payload.get("senderContact")
            body = op.payload.get("body")
            
            if not body:
                return SyncResult(operation_id=op.id, success=False, error="Empty body")

            # Need to fetch conversation to get recipient_id and channel
            from models import get_full_chat_history
            history = await get_full_chat_history(license_id, sender_contact, limit=1)
            if not history:
                 return SyncResult(operation_id=op.id, success=False, error="Conversation not found")

            channel = history[0].get("channel", "whatsapp")
            recipient_id = history[0].get("sender_id")

            outbox_id = await create_outbox_message(
                inbox_message_id=0,
                license_id=license_id,
                channel=channel,
                body=body,
                recipient_id=recipient_id,
                recipient_email=sender_contact,
                attachments=None
            )
            
            await approve_outbox_message(outbox_id, body)
            background_tasks.add_task(send_approved_message, outbox_id, license_id)
            
            return SyncResult(operation_id=op.id, success=True)
            
        elif op.type == "delete":
            message_id = op.payload.get("messageId")
            await soft_delete_message(message_id, license_id)
            return SyncResult(operation_id=op.id, success=True)
            
        elif op.type == "mark_read":
            sender_contact = op.payload.get("senderContact")
            await mark_chat_read(license_id, sender_contact)
            return SyncResult(operation_id=op.id, success=True)
            
        elif op.type == "delete_conversation":
            sender_contact = op.payload.get("senderContact")
            await soft_delete_conversation(license_id, sender_contact)
            return SyncResult(operation_id=op.id, success=True)
            
        elif op.type == "add_customer":
            name = op.payload.get("name")
            phone = op.payload.get("phone")
            email = op.payload.get("email")
            
            from models.customers import get_or_create_customer
            customer = await get_or_create_customer(
                license_id=license_id,
                phone=phone,
                email=email,
                name=name
            )
            return SyncResult(
                operation_id=op.id, 
                success=True, 
                server_state={"customer_id": customer.get("id")}
            )
            
        elif op.type == "add_purchase":
            customer_id = op.payload.get("customer_id")
            product_name = op.payload.get("product_name")
            amount = op.payload.get("amount")
            currency = op.payload.get("currency", "SYP")
            notes = op.payload.get("notes")
            payment_type = op.payload.get("payment_type", "spot")
            
            from models.purchases import create_purchase
            purchase = await create_purchase(
                license_id=license_id,
                customer_id=customer_id,
                product_name=product_name,
                amount=amount,
                currency=currency,
                notes=notes,
                payment_type=payment_type
            )
            return SyncResult(
                operation_id=op.id, 
                success=True, 
                server_state={"purchase_id": purchase.get("id")}
            )
            
        else:
            return SyncResult(
                operation_id=op.id,
                success=False,
                error=f"Unknown operation type: {op.type}",
            )
            
    except Exception as e:
        print(f"Error processing op {op.type}: {e}")
        return SyncResult(
            operation_id=op.id,
            success=False,
            error=str(e),
        )


@router.get("/status")
async def get_sync_status(
    license_data: dict = Depends(get_license_from_header),
):
    """
    Get sync status for the current license.
    
    Returns server timestamp for client sync coordination.
    """
    return {
        "server_timestamp": datetime.now(timezone.utc).isoformat(),
    }


class DeltaSyncResponse(BaseModel):
    """Response for delta sync."""
    customers: List[dict]
    conversations: List[dict]
    server_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


@router.get("/delta", response_model=DeltaSyncResponse)
async def get_delta_sync(
    since: Optional[datetime] = None,
    limit: int = 50,
    license_data: dict = Depends(get_license_from_header),
):
    """
    Get incremental updates for customers and conversations.
    
    - `since`: ISO timestamp of last sync. If missing, returns recent active context.
    - `limit`: Maximum items to return per category.
    """
    license_id = license_data.get("license_id")
    
    # If no timestamp provided, default to recent history (e.g., 30 days ago)
    # or just rely on limit to get "Active Context"
    if not since:
        since = datetime.now(timezone.utc) - timedelta(days=30)
    elif since.tzinfo is None:
        # Assume UTC if naive
        since = since.replace(tzinfo=timezone.utc)
        
    from models.customers import get_customers_delta
    from models.inbox import get_conversations_delta
    
    customers = await get_customers_delta(license_id, since, limit)
    conversations = await get_conversations_delta(license_id, since, limit)
    
    return DeltaSyncResponse(
        customers=customers,
        conversations=conversations,
        server_timestamp=datetime.now(timezone.utc)
    )
