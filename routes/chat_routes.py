"""
Al-Mudeer - Chat Routes
Inbox management, conversation history, AI analysis, and message actions.
Modularized from legacy core_integrations.py
"""

import os
import re
import json
import base64
import tempfile
import asyncio
from typing import List, Optional
from fastapi import APIRouter, HTTPException, Depends, Request, BackgroundTasks
from pydantic import BaseModel, Field
from models.task_queue import enqueue_task

from models import (
    get_inbox_messages,
    get_inbox_messages_count,
    get_inbox_conversations,
    get_inbox_conversations_count,
    get_inbox_status_counts,
    get_conversation_messages_cursor,
    get_full_chat_history,
    search_messages,
    update_inbox_status,
    update_inbox_analysis,
    create_outbox_message,
    approve_outbox_message,
    get_pending_outbox,
    get_pending_outbox,
    mark_outbox_sent,
    mark_outbox_failed,
    get_email_oauth_tokens,
    get_whatsapp_config,
    get_telegram_phone_session_data,
)
from services import (
    GmailOAuthService,
    GmailAPIService,
    TelegramService,
    TelegramPhoneService,
)
from agent import process_message
from dependencies import get_license_from_header

router = APIRouter(prefix="/api/integrations", tags=["Chat"])

# --- Schemas ---
class ApprovalRequest(BaseModel):
    action: str = Field(..., description="approve or ignore")
    edited_body: Optional[str] = None
    reply_to_platform_id: Optional[str] = None
    reply_to_body_preview: Optional[str] = None

class ForwardRequest(BaseModel):
    target_channel: str
    target_contact: str

# --- Inbox Endpoints ---

@router.get("/inbox")
async def get_inbox_route(
    status: Optional[str] = None,
    channel: Optional[str] = None,
    limit: int = 25,
    offset: int = 0,
    license: dict = Depends(get_license_from_header)
):
    messages = await get_inbox_messages(license["license_id"], status, channel, limit, offset)
    total = await get_inbox_messages_count(license["license_id"], status, channel)
    return {"messages": messages, "total": total, "has_more": offset + len(messages) < total}

@router.get("/inbox/{message_id}")
async def get_inbox_message(
    message_id: int,
    license: dict = Depends(get_license_from_header)
):
    from models.inbox import get_inbox_message_by_id
    message = await get_inbox_message_by_id(message_id, license["license_id"])
    if not message:
        raise HTTPException(status_code=404, detail="الرسالة غير موجودة")
    return {"message": message}

# --- Conversations Endpoints ---

@router.get("/conversations")
async def get_conversations_route(
    status: Optional[str] = None,
    channel: Optional[str] = None,
    limit: int = 25,
    offset: int = 0,
    license: dict = Depends(get_license_from_header)
):
    conversations = await get_inbox_conversations(license["license_id"], status, channel, limit, offset)
    total = await get_inbox_conversations_count(license["license_id"], status, channel)
    status_counts = await get_inbox_status_counts(license["license_id"])
    return {"conversations": conversations, "total": total, "status_counts": status_counts}

@router.get("/conversations/stats")
async def get_conversations_stats(
    license: dict = Depends(get_license_from_header)
):
    """Lightweight endpoint for fetching unread counts"""
    return await get_inbox_status_counts(license["license_id"])

@router.get("/conversations/search")
async def search_user_messages(
    query: str,
    sender_contact: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    license: dict = Depends(get_license_from_header)
):
    return await search_messages(license["license_id"], query, sender_contact, limit, offset)

@router.get("/conversations/{sender_contact:path}/messages")
async def get_conversation_messages_paginated(
    sender_contact: str,
    cursor: Optional[str] = None,
    limit: int = 25,
    direction: str = "older",
    license: dict = Depends(get_license_from_header)
):
    limit = min(max(1, limit), 100)
    result = await get_conversation_messages_cursor(license["license_id"], sender_contact, limit, cursor, direction)
    return {**result, "sender_contact": sender_contact}

@router.get("/conversations/{sender_contact:path}")
async def get_conversation_detail(
    sender_contact: str,
    limit: int = 100,
    license: dict = Depends(get_license_from_header)
):
    from models.customers import get_customer_for_message
    messages = await get_full_chat_history(license["license_id"], sender_contact, limit)
    if not messages:
        raise HTTPException(status_code=404, detail="المحادثة غير موجودة")
    
    incoming_msgs = [m for m in messages if m.get("direction") == "incoming"]
    sender_name = incoming_msgs[0].get("sender_name", "عميل") if incoming_msgs else "عميل"
    
    lead_score = None
    if incoming_msgs:
        customer = await get_customer_for_message(license["license_id"], incoming_msgs[0].get("id"))
        if customer: lead_score = customer.get("lead_score")
    
    return {
        "sender_name": sender_name,
        "sender_contact": sender_contact,
        "messages": messages,
        "total": len(messages),
        "lead_score": lead_score
    }

@router.post("/conversations/{sender_contact:path}/typing")
async def send_typing_indicator(
    sender_contact: str,
    request: Request,
    license: dict = Depends(get_license_from_header)
):
    from services.websocket_manager import broadcast_typing_indicator
    data = await request.json()
    is_typing = data.get("is_typing", False)
    await broadcast_typing_indicator(license["license_id"], sender_contact, is_typing)
    return {"success": True}

@router.post("/conversations/{sender_contact:path}/send")
async def send_chat_message(
    sender_contact: str,
    request: Request,
    background_tasks: BackgroundTasks,
    license: dict = Depends(get_license_from_header)
):
    data = await request.json()
    body = data.get("message", "").strip()
    attachments = data.get("attachments", [])
    reply_to_platform_id = data.get("reply_to_platform_id")
    reply_to_body_preview = data.get("reply_to_body_preview")
    
    if not body and not attachments: raise HTTPException(status_code=400, detail="الرسالة فارغة")
    
    channel = data.get("channel")
    if not channel:
        if sender_contact == "__saved_messages__":
            channel = "saved"
        else:
            history = await get_full_chat_history(license["license_id"], sender_contact, limit=1)
            if not history: raise HTTPException(status_code=404, detail="المحادثة غير موجودة")
            channel = history[0].get("channel", "whatsapp")
    
    recipient_id = None
    if sender_contact == "__saved_messages__":
        recipient_id = "__saved_messages__"
    else:
        history = history if 'history' in locals() else await get_full_chat_history(license["license_id"], sender_contact, limit=1)
        if history:
            recipient_id = history[0].get("sender_id")
    
    outbox_id = await create_outbox_message(
        inbox_message_id=None,
        license_id=license["license_id"],
        channel=channel,
        body=body,
        recipient_id=recipient_id,
        recipient_email=sender_contact,
        attachments=attachments or None,
        reply_to_platform_id=reply_to_platform_id,
        reply_to_body_preview=reply_to_body_preview
    )
    
    await approve_outbox_message(outbox_id, body)
    background_tasks.add_task(send_approved_message, outbox_id, license["license_id"])
    return {"success": True, "outbox_id": outbox_id}

# --- Actions ---

@router.post("/inbox/{message_id}/approve")
async def approve_chat_message(
    message_id: int,
    approval: ApprovalRequest,
    background_tasks: BackgroundTasks,
    license: dict = Depends(get_license_from_header)
):
    from models.inbox import get_inbox_message_by_id, approve_chat_messages
    message = await get_inbox_message_by_id(message_id, license["license_id"])
    if not message: 
        from logging_config import get_logger
        get_logger(__name__).warning(f"Approve attempt for non-existent message: {message_id} (License ID: {license['license_id']})")
        raise HTTPException(status_code=404, detail="الرسالة غير موجودة")
    
    if approval.action == "approve":
        body = approval.edited_body or message.get("ai_draft_response")
        if not body: raise HTTPException(status_code=400, detail="لا يوجد رد للإرسال")
        
        outbox_id = await create_outbox_message(
            inbox_message_id=message_id,
            license_id=license["license_id"],
            channel=message["channel"],
            body=body,
            recipient_id=message.get("sender_id"),
            recipient_email=message.get("sender_contact"),
            reply_to_platform_id=approval.reply_to_platform_id or message.get("channel_message_id"),
            reply_to_body_preview=approval.reply_to_body_preview
        )
        await approve_outbox_message(outbox_id, body)
        await update_inbox_status(message_id, "approved")
        
        sender = message.get("sender_contact") or message.get("sender_id")
        if sender: await approve_chat_messages(license["license_id"], sender)
        
        background_tasks.add_task(send_approved_message, outbox_id, license["license_id"])
        return {"success": True, "message": "تم إرسال الرد"}

@router.post("/inbox/cleanup")
async def cleanup_inbox_status_route(license: dict = Depends(get_license_from_header)):
    from models.inbox import fix_stale_inbox_status
    count = await fix_stale_inbox_status(license["license_id"])
    return {"success": True, "message": "تم تنظيف المحادثات العالقة", "count": count}

@router.patch("/messages/{message_id}/edit")
async def edit_message_route(message_id: int, request: Request, license: dict = Depends(get_license_from_header)):
    from models.inbox import edit_outbox_message
    from services.websocket_manager import broadcast_message_edited
    data = await request.json()
    new_body = data.get("body", "").strip()
    if not new_body: raise HTTPException(status_code=400, detail="النص فارغ")
    
    result = await edit_outbox_message(message_id, license["license_id"], new_body)
    await broadcast_message_edited(license["license_id"], message_id, new_body, result["edited_at"])
    return result

@router.delete("/messages/{message_id}")
async def delete_message_route(
    message_id: int, 
    type: Optional[str] = None,
    license: dict = Depends(get_license_from_header)
):
    from models.inbox import soft_delete_message
    from services.websocket_manager import broadcast_message_deleted
    try:
        result = await soft_delete_message(message_id, license["license_id"], msg_type=type)
        await broadcast_message_deleted(license["license_id"], message_id)
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

# --- Conversations Actions ---

@router.delete("/conversations/{sender_contact:path}")
async def delete_conversation_route(
    sender_contact: str,
    license: dict = Depends(get_license_from_header)
):
    from models.inbox import soft_delete_conversation
    from services.websocket_manager import broadcast_conversation_deleted
    
    result = await soft_delete_conversation(license["license_id"], sender_contact)
    # Broadcast event so UI removes it instantly
    await broadcast_conversation_deleted(license["license_id"], sender_contact)
    return result

class BatchDeleteRequest(BaseModel):
    sender_contacts: List[str]

@router.delete("/conversations")
async def delete_multiple_conversations_route(
    request: BatchDeleteRequest,
    license: dict = Depends(get_license_from_header)
):
    from models.inbox import soft_delete_conversation
    from services.websocket_manager import broadcast_conversation_deleted
    
    if not request.sender_contacts:
        raise HTTPException(status_code=400, detail="قائمة المحادثات فارغة")
        
    for contact in request.sender_contacts:
        await soft_delete_conversation(license["license_id"], contact)
        await broadcast_conversation_deleted(license["license_id"], contact)
        
    return {"success": True, "count": len(request.sender_contacts), "message": "تم حذف المحادثات بنجاح"}


@router.post("/inbox/{message_id}/read")
async def mark_message_as_read_route(message_id: int, license: dict = Depends(get_license_from_header)):
    from models.inbox import mark_message_as_read
    await mark_message_as_read(message_id, license["license_id"])
    return {"success": True}


@router.post("/conversations/{sender_contact:path}/read")
async def mark_conversation_read_route(
    sender_contact: str,
    license: dict = Depends(get_license_from_header)
):
    from models.inbox import mark_chat_read
    count = await mark_chat_read(license["license_id"], sender_contact)
    return {"success": True, "count": count}


# --- Internal Background Tasks Implementation (Original core_integrations.py logic) ---

async def analyze_inbox_message(
    message_id: int,
    body: str,
    license_id: int,
    auto_reply: bool = False,
    telegram_chat_id: str = None,
    attachments: Optional[List[dict]] = None
):
    """
    Queue message for AI analysis.
    Replaces old direct processing with robust persistent queue.
    """
    await enqueue_task("analyze_message", {
        "message_id": message_id,
        "body": body,
        "license_id": license_id,
        "auto_reply": auto_reply,
        "telegram_chat_id": telegram_chat_id,
        "attachments": attachments
    })


async def send_approved_message(outbox_id: int, license_id: int):
    """Full implementation of sending logic with attachment and audio support"""
    from services.delivery_status import save_platform_message_id
    try:
        outbox = await get_pending_outbox(license_id)
        message = next((m for m in outbox if m["id"] == outbox_id), None)
        if not message or message["status"] != "approved": return
        
        body = message["body"]
        audio_path = None
        audio_match = re.search(r'\[AUDIO: (.*?)\]', body)
        if audio_match:
            audio_path = audio_match.group(1).strip()
            body = body.replace(audio_match.group(0), "").strip()
        
        sent_anything = False
        last_platform_id = None
        channel = message["channel"]
        
        # 0. SKIP EXTERNAL DELIVERY FOR SAVED MESSAGES (Self-chat)
        if channel == "saved":
            sent_anything = True
            last_platform_id = f"saved_{message['id']}"
        
        # ALMUDEER Internal Channel: Deliver to another license holder
        elif channel == "almudeer":
            recipient_username = message.get("recipient_email")
            if recipient_username:
                from db_helper import get_db, fetch_one
                async with get_db() as db:
                    # Find target license holder by username
                    target_license = await fetch_one(db, "SELECT id, company_name FROM license_keys WHERE username = ?", [recipient_username])
                    if target_license:
                        # Find sender username & company name
                        sender_license = await fetch_one(db, "SELECT username, company_name FROM license_keys WHERE id = ?", [license_id])
                        sender_username = (sender_license["username"] if sender_license else None) or "mudeer_user"
                        sender_company = (sender_license["company_name"] if sender_license else "Al-Mudeer User")
                        
                        # Deliver as INCOMING message to recipient's license
                        # Skip AI analysis and set status to 'analyzed' for visibility
                        from models.inbox import save_inbox_message
                        new_inbox_id = await save_inbox_message(
                            license_id=target_license["id"],
                            channel="almudeer",
                            body=body,
                            sender_contact=sender_username,
                            sender_name=sender_company,
                            sender_id=sender_username,
                            received_at=datetime.utcnow(),
                            status='analyzed'
                        )
                        
                        # Broadcast to recipient instantly if message was saved
                        if new_inbox_id:
                            from services.websocket_manager import broadcast_new_message
                            await broadcast_new_message(target_license["id"], {
                                "id": new_inbox_id,
                                "license_key_id": target_license["id"],
                                "channel": "almudeer",
                                "sender_contact": sender_username,
                                "sender_name": sender_company,
                                "body": body,
                                "received_at": datetime.utcnow().isoformat(),
                                "status": "analyzed",
                                "direction": "incoming"
                            })

                        # Mark as sent and notify the sender
                        from services.delivery_status import save_platform_message_id
                        last_platform_id = f"alm_{message['id']}"
                        await save_platform_message_id(outbox_id, last_platform_id)
                        
                        # Explicitly broadcast 'sent' status to the sender for real-time UI update
                        from services.websocket_manager import broadcast_message_status_update
                        await broadcast_message_status_update(license_id, {
                            "outbox_id": outbox_id,
                            "status": "sent",
                            "timestamp": datetime.utcnow().isoformat()
                        })

                        sent_anything = True
        
        # 1. SEND TEXT
        if body and not audio_path:
            try:
                if channel == "whatsapp":
                    config = await get_whatsapp_config(license_id)
                    if config:
                        from services.whatsapp_service import WhatsAppService
                        ws = WhatsAppService(config["phone_number_id"], config["access_token"])
                        res = await ws.send_message(to=message["recipient_id"], message=body)
                        if res["success"]:
                            sent_anything = True
                            last_platform_id = res.get("message_id")

                elif channel == "telegram_bot":
                    from db_helper import get_db, fetch_one
                    async with get_db() as db:
                        row = await fetch_one(db, "SELECT bot_token FROM telegram_configs WHERE license_key_id = ?", [license_id])
                        if row:
                            ts = TelegramService(row["bot_token"])
                            res = await ts.send_message(chat_id=message["recipient_id"], text=body)
                            sent_anything = True
                            if res: last_platform_id = str(res.get("message_id"))

                elif channel == "telegram":
                    session = await get_telegram_phone_session_data(license_id)
                    if session:
                        from services.telegram_listener_service import get_telegram_listener
                        listener = get_telegram_listener()
                        active_client = await listener.ensure_client_active(license_id)
                        
                        ps = TelegramPhoneService()
                        res = await ps.send_message(
                            session_string=session,
                            recipient_id=str(message["recipient_id"]),
                            text=body,
                            client=active_client
                        )
                        sent_anything = True
                        if res: last_platform_id = str(res.get("id"))

                elif channel == "email":
                    tokens = await get_email_oauth_tokens(license_id)
                    if tokens:
                        import json
                        email_attachments = []
                        if message.get("attachments"):
                            if isinstance(message["attachments"], str):
                                try: email_attachments = json.loads(message["attachments"])
                                except: pass
                            elif isinstance(message["attachments"], list):
                                email_attachments = message["attachments"]

                        gs = GmailAPIService(tokens["access_token"], tokens.get("refresh_token"), GmailOAuthService())
                        res = await gs.send_message(
                            to_email=message["recipient_email"], 
                            subject=message.get("subject", "رد"), 
                            body=body,
                            attachments=email_attachments
                        )
                        sent_anything = True
                        if res: last_platform_id = str(res.get("id"))
            except Exception as e:
                print(f"Error sending text via {channel}: {e}")

        # 2. SEND ATTACHMENTS
        if message.get("attachments"):
            import json
            import mimetypes
            
            attachments_list = []
            if isinstance(message["attachments"], str):
                try: attachments_list = json.loads(message["attachments"])
                except: pass
            elif isinstance(message["attachments"], list):
                attachments_list = message["attachments"]
                
            for att in attachments_list:
                if not att.get("base64") or not att.get("filename"): continue
                
                try:
                    file_data = base64.b64decode(att["base64"])
                    suffix = os.path.splitext(att["filename"])[1]
                    
                    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                        tmp.write(file_data)
                        tmp_path = tmp.name
                    
                    try:
                        mime_type = att.get("mime_type") or mimetypes.guess_type(att["filename"])[0] or "application/octet-stream"
                        is_voice_note = att.get("type") == "voice" or att.get("metadata", {}).get("is_voice_note") == True
                        
                        if channel == "whatsapp":
                            config = await get_whatsapp_config(license_id)
                            if config:
                                from services.whatsapp_service import WhatsAppService
                                ws = WhatsAppService(config["phone_number_id"], config["access_token"])
                                mid = await ws.upload_media(tmp_path, mime_type=mime_type)
                                if mid:
                                    res = None
                                    if mime_type.startswith("image/"):
                                        res = await ws.send_image_message(message["recipient_id"], mid)
                                    elif mime_type.startswith("video/"):
                                        res = await ws.send_video_message(message["recipient_id"], mid)
                                    elif mime_type.startswith("audio/"):
                                        res = await ws.send_audio_message(message["recipient_id"], mid)
                                    else:
                                        res = await ws.send_document_message(message["recipient_id"], mid, att["filename"])
                                    
                                    if res and res.get("success"):
                                        sent_anything = True
                                        last_platform_id = res.get("message_id")

                        elif channel == "telegram_bot":
                             from db_helper import get_db, fetch_one
                             async with get_db() as db:
                                row = await fetch_one(db, "SELECT bot_token FROM telegram_configs WHERE license_key_id = ?", [license_id])
                                if row:
                                    ts = TelegramService(row["bot_token"])
                                    res = None
                                    if mime_type.startswith("image/"):
                                        res = await ts.send_photo(chat_id=message["recipient_id"], photo_path=tmp_path)
                                    elif mime_type.startswith("video/"):
                                        res = await ts.send_video(chat_id=message["recipient_id"], video_path=tmp_path)
                                    elif mime_type.startswith("audio/"):
                                        if is_voice_note:
                                            res = await ts.send_voice(chat_id=message["recipient_id"], audio_path=tmp_path)
                                        else:
                                            res = await ts.send_audio(chat_id=message["recipient_id"], audio_path=tmp_path)
                                    else:
                                        res = await ts.send_document(chat_id=message["recipient_id"], document_path=tmp_path)
                                    
                                    if res:
                                        sent_anything = True
                                        last_platform_id = str(res.get("message_id"))

                        elif channel == "telegram":
                            session = await get_telegram_phone_session_data(license_id)
                            if session:
                                ps = TelegramPhoneService()
                                if is_voice_note:
                                    res = await ps.send_voice(
                                        session_string=session,
                                        recipient_id=str(message["recipient_id"]),
                                        audio_path=tmp_path
                                    )
                                else:
                                    res = await ps.send_file(
                                        session_string=session,
                                        recipient_id=str(message["recipient_id"]),
                                        file_path=tmp_path
                                    )
                                sent_anything = True
                                if res: last_platform_id = str(res.get("id"))
                    finally:
                        try: os.remove(tmp_path)
                        except: pass
                except Exception as att_e:
                    print(f"Error sending attachment: {att_e}")

        # 3. SEND AUDIO
        if audio_path:
            try:
                if channel == "whatsapp":
                    config = await get_whatsapp_config(license_id)
                    if config:
                        ws = WhatsAppService(config["phone_number_id"], config["access_token"])
                        mid = await ws.upload_media(audio_path)
                        if mid:
                            res = await ws.send_audio_message(to=message["recipient_id"], media_id=mid)
                            if res and res.get("success"):
                                sent_anything = True
                                last_platform_id = res.get("message_id")
                
                elif channel == "telegram_bot":
                    from db_helper import get_db, fetch_one
                    async with get_db() as db:
                        row = await fetch_one(db, "SELECT bot_token FROM telegram_configs WHERE license_key_id = ?", [license_id])
                        if row:
                            ts = TelegramService(row["bot_token"])
                            res = await ts.send_voice(chat_id=message["recipient_id"], audio_path=audio_path)
                            if res:
                                sent_anything = True
                                last_platform_id = str(res.get("message_id"))

                elif channel == "telegram":
                    session = await get_telegram_phone_session_data(license_id)
                    if session:
                        ps = TelegramPhoneService()
                        res = await ps.send_voice(
                            session_string=session,
                            recipient_id=str(message["recipient_id"]),
                            audio_path=audio_path
                        )
                        if res:
                            sent_anything = True
                            last_platform_id = str(res.get("id"))
            except Exception as e:
                print(f"Error sending audio: {e}")

        # Final Status Update
        if sent_anything:
            await mark_outbox_sent(outbox_id)
            if last_platform_id:
                await save_platform_message_id(outbox_id, last_platform_id)
        else:
            await mark_outbox_failed(outbox_id, "Failed to send message via any method")

    except Exception as e:
        print(f"Error sending message {outbox_id}: {e}")
        await mark_outbox_failed(outbox_id, str(e))
