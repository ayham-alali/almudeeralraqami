"""
Al-Mudeer WhatsApp Business Cloud API Integration
Uses Meta's official WhatsApp Business Cloud API
"""

import os
import hmac
import hashlib
import httpx
from typing import Optional, Dict, List
from datetime import datetime

# Configuration
WHATSAPP_API_VERSION = "v18.0"
WHATSAPP_API_BASE = f"https://graph.facebook.com/{WHATSAPP_API_VERSION}"


class WhatsAppService:
    def __init__(
        self,
        phone_number_id: str,
        access_token: str,
        verify_token: str = None,
        webhook_secret: str = None
    ):
        self.phone_number_id = phone_number_id
        self.access_token = access_token
        self.verify_token = verify_token or os.urandom(16).hex()
        self.webhook_secret = webhook_secret
        self.api_url = f"{WHATSAPP_API_BASE}/{phone_number_id}/messages"
    
    async def send_message(
        self,
        to: str,
        message: str,
        reply_to_message_id: str = None
    ) -> Dict:
        """Send a text message via WhatsApp"""
        payload = {
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "to": to,
            "type": "text",
            "text": {"body": message}
        }
        
        if reply_to_message_id:
            payload["context"] = {"message_id": reply_to_message_id}
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                self.api_url,
                headers={
                    "Authorization": f"Bearer {self.access_token}",
                    "Content-Type": "application/json"
                },
                json=payload
            )
            
            if response.status_code != 200:
                return {
                    "success": False,
                    "error": response.text
                }
            
            data = response.json()
            return {
                "success": True,
                "message_id": data.get("messages", [{}])[0].get("id"),
                "response": data
            }
    
    async def send_template_message(
        self,
        to: str,
        template_name: str,
        language_code: str = "ar",
        components: List[Dict] = None
    ) -> Dict:
        """Send a pre-approved template message"""
        payload = {
            "messaging_product": "whatsapp",
            "to": to,
            "type": "template",
            "template": {
                "name": template_name,
                "language": {"code": language_code}
            }
        }
        
        if components:
            payload["template"]["components"] = components
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                self.api_url,
                headers={
                    "Authorization": f"Bearer {self.access_token}",
                    "Content-Type": "application/json"
                },
                json=payload
            )
            
            return {
                "success": response.status_code == 200,
                "response": response.json() if response.status_code == 200 else response.text
            }
    
    async def mark_as_read(self, message_id: str) -> bool:
        """Mark a message as read"""
        payload = {
            "messaging_product": "whatsapp",
            "status": "read",
            "message_id": message_id
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                self.api_url,
                headers={
                    "Authorization": f"Bearer {self.access_token}",
                    "Content-Type": "application/json"
                },
                json=payload
            )
            return response.status_code == 200
    
    def verify_webhook(self, mode: str, token: str, challenge: str) -> Optional[str]:
        """Verify webhook subscription from Meta"""
        if mode == "subscribe" and token == self.verify_token:
            return challenge
        return None
    
    def verify_signature(self, payload: bytes, signature: str) -> bool:
        """Verify webhook payload signature"""
        if not self.webhook_secret:
            return True  # Skip verification if no secret set
        
        expected = hmac.new(
            self.webhook_secret.encode(),
            payload,
            hashlib.sha256
        ).hexdigest()
        
        # Signature format: sha256=...
        if signature.startswith("sha256="):
            signature = signature[7:]
        
        return hmac.compare_digest(expected, signature)
    
    def parse_webhook_message(self, payload: Dict) -> List[Dict]:
        """Parse incoming webhook payload into message objects"""
        messages = []
        
        try:
            entry = payload.get("entry", [])
            for e in entry:
                changes = e.get("changes", [])
                for change in changes:
                    value = change.get("value", {})
                    
                    # Get contact info
                    contacts = value.get("contacts", [{}])
                    contact = contacts[0] if contacts else {}
                    
                    # Get messages
                    wa_messages = value.get("messages", [])
                    for msg in wa_messages:
                        parsed = {
                            "message_id": msg.get("id"),
                            "from": msg.get("from"),
                            "timestamp": msg.get("timestamp"),
                            "type": msg.get("type"),
                            "sender_name": contact.get("profile", {}).get("name"),
                            "sender_phone": contact.get("wa_id"),
                        }
                        
                        # Extract message content based on type
                        msg_type = msg.get("type")
                        
                        if msg_type == "text":
                            parsed["body"] = msg.get("text", {}).get("body", "")
                        
                        elif msg_type == "image":
                            parsed["body"] = "[صورة]"
                            parsed["media_id"] = msg.get("image", {}).get("id")
                            parsed["caption"] = msg.get("image", {}).get("caption", "")
                        
                        elif msg_type == "audio":
                            parsed["body"] = "[رسالة صوتية]"
                            parsed["media_id"] = msg.get("audio", {}).get("id")
                            parsed["is_voice"] = msg.get("audio", {}).get("voice", False)
                        
                        elif msg_type == "document":
                            parsed["body"] = f"[مستند: {msg.get('document', {}).get('filename', 'ملف')}]"
                            parsed["media_id"] = msg.get("document", {}).get("id")
                        
                        elif msg_type == "location":
                            loc = msg.get("location", {})
                            parsed["body"] = f"[موقع: {loc.get('latitude')}, {loc.get('longitude')}]"
                        
                        elif msg_type == "contacts":
                            parsed["body"] = "[جهة اتصال]"
                        
                        elif msg_type == "button":
                            parsed["body"] = msg.get("button", {}).get("text", "")
                        
                        elif msg_type == "interactive":
                            interactive = msg.get("interactive", {})
                            if interactive.get("type") == "button_reply":
                                parsed["body"] = interactive.get("button_reply", {}).get("title", "")
                            elif interactive.get("type") == "list_reply":
                                parsed["body"] = interactive.get("list_reply", {}).get("title", "")
                        
                        else:
                            parsed["body"] = f"[{msg_type}]"
                        
                        messages.append(parsed)
                    
                    # Handle status updates
                    statuses = value.get("statuses", [])
                    for status in statuses:
                        messages.append({
                            "type": "status",
                            "message_id": status.get("id"),
                            "status": status.get("status"),  # sent, delivered, read, failed
                            "recipient": status.get("recipient_id"),
                            "timestamp": status.get("timestamp")
                        })
        
        except Exception as e:
            print(f"Error parsing WhatsApp webhook: {e}")
        
        return messages
    
    async def download_media(self, media_id: str) -> Optional[bytes]:
        """Download media file from WhatsApp"""
        # First, get the media URL
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{WHATSAPP_API_BASE}/{media_id}",
                headers={"Authorization": f"Bearer {self.access_token}"}
            )
            
            if response.status_code != 200:
                return None
            
            media_url = response.json().get("url")
            
            # Download the actual file
            file_response = await client.get(
                media_url,
                headers={"Authorization": f"Bearer {self.access_token}"}
            )
            
            if file_response.status_code == 200:
                return file_response.content
            
            return None

    async def upload_media(self, file_path: str, mime_type: str = "audio/mpeg") -> Optional[str]:
        """Upload media to WhatsApp and return media_id"""
        url = f"{WHATSAPP_API_BASE}/{self.phone_number_id}/media"
        
        try:
            # Prepare multipart form data
            files = {'file': (os.path.basename(file_path), open(file_path, 'rb'), mime_type)}
            data = {'messaging_product': 'whatsapp'}
            
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    url,
                    headers={"Authorization": f"Bearer {self.access_token}"},
                    files=files,
                    data=data
                )
                
                if response.status_code == 200:
                    return response.json().get("id")
                else:
                    print(f"Failed to upload media: {response.text}")
                    return None
        except Exception as e:
            print(f"Error uploading media: {e}")
            return None

    async def send_audio_message(self, to: str, media_id: str) -> Dict:
        """Send an audio message via WhatsApp"""
        payload = {
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "to": to,
            "type": "audio",
            "audio": {"id": media_id}
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                self.api_url,
                headers={
                    "Authorization": f"Bearer {self.access_token}",
                    "Content-Type": "application/json"
                },
                json=payload
            )
            
            if response.status_code != 200:
                return {
                    "success": False,
                    "error": response.text
                }
            
            data = response.json()
            return {
                "success": True,
                "message_id": data.get("messages", [{}])[0].get("id"),
                "response": data
            }

    async def send_image_message(self, to: str, media_id: str, caption: str = None) -> Dict:
        """Send an image message via WhatsApp"""
        payload = {
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "to": to,
            "type": "image",
            "image": {"id": media_id}
        }
        
        if caption:
            payload["image"]["caption"] = caption
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                self.api_url,
                headers={
                    "Authorization": f"Bearer {self.access_token}",
                    "Content-Type": "application/json"
                },
                json=payload
            )
            
            if response.status_code != 200:
                print(f"WhatsApp API Error (Image): {response.text}") # Debug log
                return {
                    "success": False,
                    "error": response.text
                }
            
            return {
                "success": True,
                "message_id": data.get("messages", [{}])[0].get("id"),
                "response": data
            }

    async def send_video_message(self, to: str, media_id: str, caption: str = None) -> Dict:
        """Send a video message via WhatsApp"""
        payload = {
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "to": to,
            "type": "video",
            "video": {"id": media_id}
        }
        if caption:
            payload["video"]["caption"] = caption
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                self.api_url,
                headers={"Authorization": f"Bearer {self.access_token}", "Content-Type": "application/json"},
                json=payload
            )
            data = response.json()
            return {"success": response.status_code == 200, "response": data}

    async def send_document_message(self, to: str, media_id: str, filename: str, caption: str = None) -> Dict:
        """Send a document message via WhatsApp"""
        payload = {
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "to": to,
            "type": "document",
            "document": {"id": media_id, "filename": filename}
        }
        if caption:
            payload["document"]["caption"] = caption
            
        async with httpx.AsyncClient() as client:
            response = await client.post(
                self.api_url,
                headers={"Authorization": f"Bearer {self.access_token}", "Content-Type": "application/json"},
                json=payload
            )
            data = response.json()
            return {"success": response.status_code == 200, "response": data}

    async def send_typing_indicator(self, to: str) -> bool:
        """
        Send typing indicator/chat state.
        NOTE: This functionality is not officially documented in v18+ but supported by many implementations via this payload.
        """
        # Common payload for "typing" status in WhatsApp API
        # Only works if the window is open or sometimes just ignored without error.
        payload = {
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "to": to,
            "type": "text", 
            "text": {"body": "..."} # We do NOT want to send actual text
        }
        
        # Better attempt for "typing" state if supported by the gateway
        # Note: We will use a request that attempts to set status without sending a message
        # If this fails, we catch it silently so we don't break the flow.
        
        # Actually, let's use the exact payload that works for some BSPs:
        # { "recipient_type": "individual", "to": "...", "type": "chat_state", "chat_state": "typing" }
        # If this is rejected by the specific Graph version, it will just return 400, which we catch.
        
        real_payload = {
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "to": to,
            "type": "chat_state", # Undocumented / Beta
            "chat_state": "typing"
        }
        
        try:
             async with httpx.AsyncClient() as client:
                response = await client.post(
                    self.api_url,
                    headers={
                        "Authorization": f"Bearer {self.access_token}",
                        "Content-Type": "application/json"
                    },
                    json=real_payload
                )
                return response.status_code == 200
        except:
            return False




# Database operations for WhatsApp config (SQLite & PostgreSQL via db_helper)
async def save_whatsapp_config(
    license_id: int,
    phone_number_id: str,
    access_token: str,
    business_account_id: str = None,
    verify_token: str = None,
    auto_reply_enabled: bool = False,
) -> int:
    """Save WhatsApp configuration in a database-agnostic way."""
    from db_helper import get_db, fetch_one, execute_sql, commit_db, DB_TYPE

    verify_token = verify_token or os.urandom(16).hex()
    # Use real datetime for PostgreSQL, ISO string for SQLite for compatibility
    updated_at = datetime.utcnow() if DB_TYPE == "postgresql" else datetime.utcnow().isoformat()

    async with get_db() as db:
        existing = await fetch_one(
            db,
            "SELECT id FROM whatsapp_configs WHERE license_key_id = ?",
            [license_id],
        )

        if existing:
            await execute_sql(
                db,
                """
                UPDATE whatsapp_configs SET
                    phone_number_id = ?,
                    access_token = ?,
                    business_account_id = ?,
                    verify_token = ?,
                    auto_reply_enabled = ?,
                    is_active = TRUE,
                    updated_at = ?
                WHERE license_key_id = ?
                """,
                [
                    phone_number_id,
                    access_token,
                    business_account_id,
                    verify_token,
                    auto_reply_enabled,
                    updated_at,
                    license_id,
                ],
            )
            await commit_db(db)
            return existing["id"]

        await execute_sql(
            db,
            """
            INSERT INTO whatsapp_configs 
                (license_key_id, phone_number_id, access_token, business_account_id,
                 verify_token, auto_reply_enabled, is_active)
            VALUES (?, ?, ?, ?, ?, ?, TRUE)
            """,
            [
                license_id,
                phone_number_id,
                access_token,
                business_account_id,
                verify_token,
                auto_reply_enabled,
            ],
        )

        row = await fetch_one(
            db,
            """
            SELECT id FROM whatsapp_configs
            WHERE license_key_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            [license_id],
        )
        await commit_db(db)
        return row["id"] if row else 0


async def get_whatsapp_config(license_id: int) -> Optional[Dict]:
    """Get WhatsApp configuration (SQLite & PostgreSQL compatible)."""
    from db_helper import get_db, fetch_one

    async with get_db() as db:
        row = await fetch_one(
            db,
            "SELECT * FROM whatsapp_configs WHERE license_key_id = ?",
            [license_id],
        )
        return row


async def delete_whatsapp_config(license_id: int) -> bool:
    """Delete WhatsApp configuration (SQLite & PostgreSQL compatible)."""
    from db_helper import get_db, execute_sql, commit_db

    async with get_db() as db:
        await execute_sql(
            db,
            "DELETE FROM whatsapp_configs WHERE license_key_id = ?",
            [license_id],
        )
        await commit_db(db)
    return True

