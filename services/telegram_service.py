"""
Al-Mudeer - Telegram Bot Service
Webhook-based Telegram integration for business messaging
"""

import httpx
from typing import Optional, Dict, Any, List
from datetime import datetime
import json


class TelegramService:
    """Service for Telegram Bot API interactions"""
    
    BASE_URL = "https://api.telegram.org/bot"
    FILE_BASE_URL = "https://api.telegram.org/file/bot"
    
    def __init__(self, bot_token: str):
        self.bot_token = bot_token
        self.api_url = f"{self.BASE_URL}{bot_token}"
        self.file_url = f"{self.FILE_BASE_URL}{bot_token}"
    
    async def _request(self, method: str, data: dict = None) -> dict:
        """Make request to Telegram Bot API"""
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{self.api_url}/{method}",
                json=data or {}
            )
            result = response.json()
            
            if not result.get("ok"):
                raise Exception(result.get("description", "Telegram API error"))
            
            return result.get("result", {})
    
    async def get_me(self) -> dict:
        """Get bot information"""
        return await self._request("getMe")
    
    async def set_webhook(self, webhook_url: str, secret_token: str = None) -> bool:
        """Set webhook URL for receiving updates"""
        data = {
            "url": webhook_url,
            "allowed_updates": ["message", "callback_query"]
        }
        if secret_token:
            data["secret_token"] = secret_token
        
        result = await self._request("setWebhook", data)
        return True
    
    async def delete_webhook(self) -> bool:
        """Delete webhook"""
        await self._request("deleteWebhook")
        return True
    
    async def get_webhook_info(self) -> dict:
        """Get current webhook info"""
        return await self._request("getWebhookInfo")
    
    async def get_file(self, file_id: str) -> dict:
        """Get file info (path) from file_id"""
        return await self._request("getFile", {"file_id": file_id})
        
    async def download_file(self, file_path: str) -> Optional[bytes]:
        """Download file content"""
        url = f"{self.file_url}/{file_path}"
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.get(url)
                response.raise_for_status()
                return response.content
        except Exception as e:
            print(f"Error downloading file {file_path}: {e}")
            return None
    
    async def send_message(
        self,
        chat_id: str,
        text: str,
        reply_to_message_id: int = None,
        parse_mode: str = None
    ) -> dict:
        """Send message to a chat"""
        data = {
            "chat_id": chat_id,
            "text": text
        }
        
        if reply_to_message_id:
            data["reply_to_message_id"] = reply_to_message_id
        
        if parse_mode:
            data["parse_mode"] = parse_mode
        
        return await self._request("sendMessage", data)
    
    async def send_typing_action(self, chat_id: str) -> bool:
        """Send typing indicator"""
        await self._request("sendChatAction", {
            "chat_id": chat_id,
            "action": "typing"
        })
        return True
    
    async def send_voice(self, chat_id: str, audio_path: str, caption: str = None) -> dict:
        """Send voice message (OGG with OPUS codec recommended, but MP3 works)"""
        async with httpx.AsyncClient(timeout=60.0) as client:
            with open(audio_path, "rb") as f:
                files = {"voice": (audio_path, f, "audio/mpeg")}
                data = {"chat_id": chat_id}
                if caption:
                    data["caption"] = caption
                
                response = await client.post(
                    f"{self.api_url}/sendVoice",
                    data=data,
                    files=files
                )
                result = response.json()
                
                if not result.get("ok"):
                    raise Exception(result.get("description", "Telegram API error"))
                
                return result.get("result", {})
    
    async def send_audio(self, chat_id: str, audio_path: str, title: str = None) -> dict:
        """Send audio file (MP3, etc.)"""
        async with httpx.AsyncClient(timeout=60.0) as client:
            with open(audio_path, "rb") as f:
                files = {"audio": (audio_path, f, "audio/mpeg")}
                data = {"chat_id": chat_id}
                if title:
                    data["title"] = title
                
                response = await client.post(
                    f"{self.api_url}/sendAudio",
                    data=data,
                    files=files
                )
                result = response.json()
                
                if not result.get("ok"):
                    raise Exception(result.get("description", "Telegram API error"))
                
                return result.get("result", {})
    
    async def send_photo(self, chat_id: str, photo_path: str, caption: str = None) -> dict:
        """Send photo"""
        async with httpx.AsyncClient(timeout=60.0) as client:
            with open(photo_path, "rb") as f:
                files = {"photo": (photo_path, f, "image/jpeg")}
                data = {"chat_id": chat_id}
                if caption:
                    data["caption"] = caption
                
                response = await client.post(
                    f"{self.api_url}/sendPhoto",
                    data=data,
                    files=files
                )
                result = response.json()
                
                if not result.get("ok"):
                    raise Exception(result.get("description", "Telegram API error"))
                
                return result.get("result", {})

    async def send_video(self, chat_id: str, video_path: str, caption: str = None) -> dict:
        """Send video"""
        async with httpx.AsyncClient(timeout=120.0) as client:
            with open(video_path, "rb") as f:
                files = {"video": (video_path, f, "video/mp4")}
                data = {"chat_id": chat_id}
                if caption:
                    data["caption"] = caption
                
                response = await client.post(
                    f"{self.api_url}/sendVideo",
                    data=data,
                    files=files
                )
                result = response.json()
                if not result.get("ok"):
                    raise Exception(result.get("description", "Telegram API error"))
                return result.get("result", {})

    async def send_document(self, chat_id: str, document_path: str, caption: str = None) -> dict:
        """Send document"""
        async with httpx.AsyncClient(timeout=120.0) as client:
            with open(document_path, "rb") as f:
                files = {"document": (document_path, f, "application/octet-stream")}
                data = {"chat_id": chat_id}
                if caption:
                    data["caption"] = caption
                
                response = await client.post(
                    f"{self.api_url}/sendDocument",
                    data=data,
                    files=files
                )
                result = response.json()
                if not result.get("ok"):
                    raise Exception(result.get("description", "Telegram API error"))
                return result.get("result", {})
    
    async def test_connection(self) -> tuple[bool, str, dict]:
        """Test bot token and get bot info"""
        try:
            bot_info = await self.get_me()
            return True, "تم الاتصال بنجاح", bot_info
        except Exception as e:
            return False, f"خطأ: {str(e)}", {}
    
    @staticmethod
    def parse_update(update: dict) -> Optional[dict]:
        """Parse incoming webhook update"""
        # Handle different update types
        message = (
            update.get("message") or 
            update.get("edited_message") or 
            update.get("channel_post") or 
            update.get("edited_channel_post")
        )
        
        # Also check for my_chat_member updates (bot added/removed from groups)
        # We generally want to ignore these for inbox purposes, but if we do parse them,
        # we must ensure they are marked as non-private.
        if not message:
            # Check for my_chat_member to avoid crashing or false positives if we ever decided to handle them
            # For now, return None so they are ignored by the inbox logic.
            return None
        
        chat = message.get("chat", {})
        from_user = message.get("from", {})
        
        # In channels, "from" might be missing, so we use chat title/username
        if not from_user and chat.get("type") == "channel":
            from_user = {
                "id": chat.get("id"),
                "username": chat.get("username"),
                "first_name": chat.get("title"),
                "is_bot": False # Treat as user for the sake of the system
            }

        # Media extraction
        attachments = []
        
        # Photos (get largest)
        if message.get("photo"):
            # Photos are list of sizes, last one is largest
            largest = message["photo"][-1]
            attachments.append({
                "type": "photo",
                "file_id": largest["file_id"],
                "file_size": largest.get("file_size", 0)
            })
            
        # Voice
        if message.get("voice"):
            voice = message["voice"]
            attachments.append({
                "type": "voice", 
                "file_id": voice["file_id"],
                "mime_type": voice.get("mime_type", "audio/ogg"),
                "file_size": voice.get("file_size", 0)
            })
            
        # Audio
        if message.get("audio"):
            audio = message["audio"]
            attachments.append({
                "type": "audio",
                "file_id": audio["file_id"],
                "mime_type": audio.get("mime_type", "audio/mpeg"),
                "file_size": audio.get("file_size", 0)
            })
            
        # Document
        if message.get("document"):
            doc = message["document"]
            attachments.append({
                "type": "document",
                "file_id": doc["file_id"],
                "mime_type": doc.get("mime_type", "application/octet-stream"),
                "file_size": doc.get("file_size", 0),
                "file_name": doc.get("file_name")
            })

        return {
            "update_id": update.get("update_id"),
            "message_id": message.get("message_id"),
            "chat_id": str(chat.get("id")),
            "chat_type": chat.get("type"),  # private, group, supergroup, channel
            "user_id": str(from_user.get("id")),
            "username": from_user.get("username"),
            "first_name": from_user.get("first_name", ""),
            "last_name": from_user.get("last_name", ""),
            "text": message.get("text", "") or message.get("caption", ""), 
            "date": datetime.fromtimestamp(message.get("date", 0)),
            "is_bot": from_user.get("is_bot", False),
            "attachments": attachments
        }
        
        # Add fallback body if empty but has attachments (for Inbox visibility)
        if not result["text"] and attachments:
            first_type = attachments[0]["type"]
            if first_type == "photo":
                result["text"] = "[صورة]"
            elif first_type == "voice":
                result["text"] = "[رسالة صوتية]"
            elif first_type == "audio":
                result["text"] = "[ملف صوتي]"
            elif first_type == "video":
                result["text"] = "[فيديو]"
            else:
                result["text"] = "[ملف]"
                
        return result


class TelegramBotManager:
    """Manager for multiple Telegram bots (one per business)"""
    
    _instances: Dict[int, TelegramService] = {}
    
    @classmethod
    def get_bot(cls, license_id: int, bot_token: str) -> TelegramService:
        """Get or create bot instance for a license"""
        if license_id not in cls._instances:
            cls._instances[license_id] = TelegramService(bot_token)
        return cls._instances[license_id]
    
    @classmethod
    def remove_bot(cls, license_id: int):
        """Remove bot instance"""
        if license_id in cls._instances:
            del cls._instances[license_id]


# Telegram bot setup guide (in Arabic)
TELEGRAM_SETUP_GUIDE = """
## كيفية إنشاء بوت تيليجرام

### الخطوة 1: إنشاء البوت
1. افتح تيليجرام وابحث عن @BotFather
2. أرسل الأمر /newbot
3. اختر اسماً للبوت (مثال: مساعد شركة رؤية)
4. اختر معرّف فريد ينتهي بـ bot (مثال: roya_assistant_bot)

### الخطوة 2: الحصول على التوكن
بعد إنشاء البوت، سيرسل لك BotFather رسالة تحتوي على:
```
Use this token to access the HTTP API:
123456789:ABCdefGHIjklMNOpqrsTUVwxyz
```
انسخ هذا التوكن وألصقه في الحقل أدناه.

### الخطوة 3: تخصيص البوت (اختياري)
يمكنك إرسال هذه الأوامر لـ BotFather:
- /setdescription - لتعيين وصف البوت
- /setabouttext - لتعيين نص "حول"
- /setuserpic - لتعيين صورة البوت

### ملاحظات مهمة
- احفظ التوكن في مكان آمن
- لا تشارك التوكن مع أي شخص
- يمكنك إنشاء توكن جديد بإرسال /revoke لـ BotFather
"""

