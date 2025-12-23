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
        message = update.get("message") or update.get("edited_message")
        
        if not message:
            return None
        
        chat = message.get("chat", {})
        from_user = message.get("from", {})
        
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
            "text": message.get("text", "") or message.get("caption", ""), # Use caption if text is empty
            "date": datetime.fromtimestamp(message.get("date", 0)),
            "is_bot": from_user.get("is_bot", False),
            "attachments": attachments
        }


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

