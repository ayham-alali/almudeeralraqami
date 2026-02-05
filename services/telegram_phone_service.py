"""
Al-Mudeer - Telegram Phone Number Service
MTProto client for Telegram user accounts (phone numbers)
"""

import asyncio
import os
from typing import Optional, Dict, Tuple, List, Any
from datetime import datetime, timedelta, timezone
import time
# Telethon imports moved to methods to avoid import-time side effects
# from telethon import TelegramClient
# from telethon.sessions import StringSession
# from telethon.errors import ...
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from telethon import TelegramClient
    from telethon.sessions import StringSession
import json
from security import decrypt_sensitive_data
import base64
import io
from services.file_storage_service import get_file_storage

# MIME type mapping for Telegram media
def get_mime_type_from_ext(file_ext: str) -> str:
    ext = file_ext.lower().replace('.', '')
    
    # Images
    if ext in ['jpg', 'jpeg']: return 'image/jpeg'
    if ext == 'png': return 'image/png'
    if ext == 'gif': return 'image/gif'
    if ext == 'webp': return 'image/webp'
    if ext == 'bmp': return 'image/bmp'
    if ext == 'svg': return 'image/svg+xml'
    
    # Video
    if ext == 'mp4': return 'video/mp4'
    if ext == 'mov': return 'video/quicktime'
    if ext == 'avi': return 'video/x-msvideo'
    if ext == 'mkv': return 'video/x-matroska'
    if ext == 'webm': return 'video/webm'
    
    # Audio
    if ext == 'mp3': return 'audio/mpeg'
    if ext in ['m4a', 'ogg']: return 'audio/ogg'
    if ext == 'wav': return 'audio/wav'
    if ext == 'flac': return 'audio/flac'
    if ext == 'aac': return 'audio/aac'
    
    # Documents
    if ext == 'pdf': return 'application/pdf'
    if ext in ['doc', 'docx']: return 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
    if ext in ['xls', 'xlsx']: return 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    if ext in ['ppt', 'pptx']: return 'application/vnd.openxmlformats-officedocument.presentationml.presentation'
    if ext == 'csv': return 'text/csv'
    
    # Archives
    if ext == 'zip': return 'application/zip'
    if ext == 'rar': return 'application/vnd.rar'
    if ext in ['7z', 'tar', 'gz']: return 'application/x-compressed'
    
    # Android
    if ext == 'apk': return 'application/vnd.android.package-archive'
    if ext == 'aab': return 'application/octet-stream'
    
    # Code files (treat as text for proper handling)
    if ext in ['dart', 'py', 'pyw', 'js', 'jsx', 'mjs', 'ts', 'tsx',
               'java', 'kt', 'kts', 'swift', 'go', 'rs', 'rb', 'php',
               'c', 'h', 'cpp', 'hpp', 'cc', 'cxx', 'm', 'mm',
               'scala', 'groovy', 'lua', 'pl', 'r']:
        return 'text/plain'
    
    # Web
    if ext in ['html', 'htm']: return 'text/html'
    if ext in ['css', 'scss', 'less']: return 'text/css'
    if ext in ['vue', 'svelte']: return 'text/plain'
    
    # Shell scripts
    if ext in ['sh', 'bash', 'zsh', 'bat', 'cmd', 'ps1']:
        return 'text/plain'
    
    # Config/Data
    if ext == 'json': return 'application/json'
    if ext in ['yaml', 'yml']: return 'text/yaml'
    if ext == 'xml': return 'application/xml'
    if ext in ['ini', 'conf', 'cfg', 'env', 'properties']:
        return 'text/plain'
    if ext == 'sql': return 'text/plain'
    
    # Markdown/Text
    if ext in ['md', 'markdown']: return 'text/markdown'
    if ext in ['txt', 'log']: return 'text/plain'
    
    return 'application/octet-stream'

class TelegramPhoneService:
    """Service for Telegram MTProto client (phone number authentication)"""
    
    def __init__(self):
        """
        Initialize Telegram Phone service
        
        Note: Telegram API credentials are required.
        Get them from https://my.telegram.org/apps
        """
        # Telegram API credentials (same for all users)
        # These should be set as environment variables
        self.api_id = os.getenv("TELEGRAM_API_ID")
        self.api_hash = os.getenv("TELEGRAM_API_HASH")
        
        if not self.api_id or not self.api_hash:
            raise ValueError(
                "Telegram API credentials not configured. "
                "Set TELEGRAM_API_ID and TELEGRAM_API_HASH environment variables. "
                "Get them from https://my.telegram.org/apps"
            )

    # ============ Helper methods for stateless session payload ============

    @staticmethod
    def _encode_session_state(state: Dict) -> str:
        """Encode session state (phone, session, hash, ts) into URL-safe token."""
        raw = json.dumps(state, ensure_ascii=False).encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("ascii")

    @staticmethod
    def _decode_session_state(token: str) -> Dict:
        """Decode URL-safe token back into session state dict."""
        raw = base64.urlsafe_b64decode(token.encode("ascii"))
        return json.loads(raw.decode("utf-8"))

    async def _execute_with_retry(self, func, *args, **kwargs):
        """Execute a function with retry logic for transient errors"""
        from telethon.errors import RpcCallFailError, ServerError
        
        retries = 3
        last_error = None
        
        for attempt in range(retries):
            try:
                # If func is coroutine, await it
                if asyncio.iscoroutinefunction(func):
                   return await func(*args, **kwargs)
                
                # If result is awaitable, await it
                result = func(*args, **kwargs)
                if asyncio.iscoroutine(result):
                    return await result
                return result
            except (RpcCallFailError, ServerError) as e:
                last_error = e
                # Exponential backoff: 1s, 2s, 4s
                wait_time = 2 ** attempt
                from logging_config import get_logger
                logger = get_logger(__name__)
                logger.warning(f"Telegram RPC error (attempt {attempt+1}/{retries}): {e}. Retrying in {wait_time}s...")
                await asyncio.sleep(wait_time)
            except Exception as e:
                # Don't retry other errors
                raise e
                
        # If we exhausted retries
        raise last_error

    async def _resolve_telegram_entity(self, client: "TelegramClient", recipient_id: str, logger: Any) -> Optional[Any]:
        """
        Robustly resolve a Telegram entity (User, Chat, or Channel).
        Uses direct resolution, DB resolution (via access_hash), DB aliases, and dialog search.
        """
        logger.info(f"Resolving Telegram entity for ID: '{recipient_id}'")
        
        # 0. Cleanup recipient_id
        clean_id = str(recipient_id).strip()
        if clean_id.startswith("tg:"):
            clean_id = clean_id[3:]
        
        chat_id_int = None
        try:
            chat_id_int = int(clean_id)
        except (ValueError, TypeError):
            pass

        # 1. Direct Resolution (Works if in memory cache)
        try:
            if chat_id_int:
                entity = await client.get_entity(chat_id_int)
            else:
                entity = await client.get_entity(clean_id)
            if entity:
                return entity
        except Exception:
            pass

        # 2. DB Persistence Resolution (Senior Backend Fix)
        # If we have the access_hash in our DB, we can manually construct the InputPeer
        if chat_id_int:
            try:
                from models import get_telegram_entity
                # We need the license ID. We can get it from the client if needed, 
                # but better to pass it or infer it. 
                # For now, we search all known hashes for this ID across our sessions.
                # Actually, access_hashes are account-specific. 
                # We need to know WHICH account we are using.
                me = await client.get_me()
                from db_helper import get_db, fetch_one
                async with get_db() as db:
                    sess_row = await fetch_one(db, "SELECT license_key_id FROM telegram_phone_sessions WHERE user_id = ?", [str(me.id)])
                    if sess_row:
                        lic_id = sess_row['license_key_id']
                        ent_row = await get_telegram_entity(lic_id, str(chat_id_int))
                        if ent_row and ent_row.get('access_hash'):
                            from telethon.tl.types import InputPeerUser, InputPeerChannel
                            ah = int(ent_row['access_hash'])
                            if ent_row['entity_type'] == 'user':
                                peer = InputPeerUser(chat_id_int, ah)
                            else:
                                peer = InputPeerChannel(chat_id_int, ah)
                            
                            entity = await client.get_entity(peer)
                            if entity:
                                logger.info(f"Resolved entity {clean_id} using persisted access_hash from DB.")
                                return entity
            except Exception as e:
                logger.debug(f"DB Hash resolution failed: {e}")

        # 3. DB Alias (Phone/Username)
        if chat_id_int:
            from db_helper import get_db, fetch_one
            async with get_db() as db:
                row = await fetch_one(db, "SELECT sender_contact FROM inbox_messages WHERE sender_id = ? AND sender_contact != sender_id AND sender_contact != '' LIMIT 1", [str(chat_id_int)])
                if row:
                    alias = row.get("sender_contact") or row[0]
                    try:
                        entity = await client.get_entity(alias)
                        if entity: return entity
                    except: pass
        
        # 4. Dialogs (Deep Scan)
        try:
            logger.info(f"Deep scanning dialogs for {clean_id}...")
            # We don't limit here because the contact might be archived or old
            async for dialog in client.iter_dialogs(limit=None):
                if (chat_id_int and (dialog.id == chat_id_int or abs(dialog.id) == chat_id_int or str(dialog.id).endswith(str(chat_id_int)))) or \
                   (not chat_id_int and dialog.name == clean_id):
                    # SAVE THE HASH NOW that we found it!
                    if hasattr(dialog.entity, 'access_hash'):
                        try:
                            from models import save_telegram_entity
                            me = await client.get_me()
                            async with get_db() as db:
                                sess_row = await fetch_one(db, "SELECT license_key_id FROM telegram_phone_sessions WHERE user_id = ?", [str(me.id)])
                                if sess_row:
                                    await save_telegram_entity(
                                        license_key_id=sess_row['license_key_id'],
                                        entity_id=str(dialog.id),
                                        access_hash=str(dialog.entity.access_hash),
                                        entity_type='user' if hasattr(dialog.entity, 'first_name') else 'channel',
                                        username=getattr(dialog.entity, 'username', None)
                                    )
                        except: pass
                    return dialog.entity
        except Exception as e:
            logger.debug(f"Dialog scan failed: {e}")

        # 5. Force Resolve (Force Resolve)
        if chat_id_int:
            from telethon.tl.functions.users import GetFullUserRequest
            from telethon.tl.types import PeerUser
            try:
                full_user = await client(GetFullUserRequest(PeerUser(chat_id_int)))
                if full_user and full_user.users: return full_user.users[0]
            except: pass
            
            from telethon.tl.functions.channels import GetFullChannelRequest
            from telethon.tl.types import PeerChannel
            try:
                full_channel = await client(GetFullChannelRequest(PeerChannel(chat_id_int)))
                if full_channel and full_channel.chats: return full_channel.chats[0]
            except: pass
        
        return None

    async def start_login(self, phone_number: str) -> Dict[str, str]:
        """
        Start login process by requesting verification code
        
        Args:
            phone_number: Phone number in international format (e.g., +963912345678)
        
        Returns:
            Dict with message indicating code was sent
        """
        if not phone_number.startswith('+'):
            phone_number = '+' + phone_number

        # Use an in-memory StringSession and encode its state into a token that
        # the frontend sends back on verification. This makes the flow
        # stateless and resilient to multi-process deployments (Railway).
        from telethon.sessions import StringSession
        from telethon import TelegramClient
        from telethon.errors import PhoneNumberUnoccupiedError, FloodWaitError
        
        session = StringSession()
        client = TelegramClient(session, int(self.api_id), self.api_hash)

        try:
            await client.connect()

            # Always request a new code
            sent = await client.send_code_request(phone_number)
            phone_code_hash = getattr(sent, "phone_code_hash", None)

            # Persist minimal state in a signed/encoded token returned to client
            session_state = {
                "phone_number": phone_number,
                "session": session.save(),
                "phone_code_hash": phone_code_hash,
                "created_at": datetime.utcnow().isoformat(),
            }
            session_id = self._encode_session_state(session_state)

            return {
                "success": True,
                "message": f"تم إرسال كود التحقق إلى Telegram الخاص برقم {phone_number}",
                "session_id": session_id,
                "phone_number": phone_number,
            }

        except PhoneNumberUnoccupiedError:
            raise ValueError(f"الرقم {phone_number} غير مسجل في Telegram")

        except FloodWaitError as e:
            raise ValueError(f"تم إرسال عدد كبير من الطلبات. يرجى الانتظار {e.seconds} ثانية")

        except Exception as e:
            raise ValueError(f"خطأ في طلب الكود: {str(e)}")

        finally:
            try:
                await client.disconnect()
            except Exception:
                pass
    
    async def verify_code(
        self,
        phone_number: str,
        code: str,
        session_id: Optional[str] = None,
        password: Optional[str] = None
    ) -> Tuple[str, Dict]:
        """
        Verify code and complete login, returning session string
        
        Args:
            phone_number: Phone number (same as used in start_login)
            code: Verification code received in Telegram
            session_id: Optional session ID from start_login
        
        Returns:
            Tuple of (session_string, user_info_dict)
        """
        if not phone_number.startswith('+'):
            phone_number = '+' + phone_number

        if not session_id:
            raise ValueError("انتهت صلاحية طلب تسجيل الدخول. يرجى طلب كود جديد")

        # Decode state from token returned by start_login
        try:
            state = self._decode_session_state(session_id)
        except Exception:
            raise ValueError("انتهت صلاحية طلب تسجيل الدخول. يرجى طلب كود جديد")

        if state.get("phone_number") != phone_number:
            raise ValueError("حدث تعارض في رقم الهاتف. يرجى طلب كود جديد")

        # Recreate client from stored StringSession
        from telethon.sessions import StringSession
        from telethon import TelegramClient
        from telethon.errors import (
            PhoneCodeInvalidError,
            PhoneCodeExpiredError,
            SessionPasswordNeededError
        )

        session = StringSession(state.get("session") or "")
        client = TelegramClient(session, int(self.api_id), self.api_hash)

        try:
            await client.connect()

            # If we are not yet authorized, complete sign-in
            if not await client.is_user_authorized():
                try:
                    # First step: code verification
                    await client.sign_in(
                        phone_number,
                        code,
                        phone_code_hash=state.get("phone_code_hash"),
                    )
                except SessionPasswordNeededError:
                    # 2FA enabled - ask for password on next call
                    if not password:
                        raise ValueError(
                            "حسابك محمي بكلمة مرور ثنائية (2FA). "
                            "يرجى إدخال كلمة المرور الثنائية لإكمال تسجيل الدخول."
                        )
                    # Second step: provide 2FA password
                    try:
                        await client.sign_in(password=password)
                    except Exception as e:
                        raise ValueError(f"كلمة المرور الثنائية غير صحيحة: {str(e)}")

            # Now we should be fully authorized
            session_string = client.session.save()
            me = await client.get_me()

            await client.disconnect()

            return session_string, {
                "id": me.id,
                "phone": me.phone,
                "first_name": me.first_name,
                "last_name": me.last_name,
                "username": me.username,
            }

        except PhoneCodeInvalidError:
            raise ValueError("كود التحقق غير صحيح")
        except PhoneCodeExpiredError:
            # Code expired; user must request a new one
            self._pending_logins.pop(phone_number, None)
            raise ValueError("انتهت صلاحية كود التحقق. يرجى طلب كود جديد")
        except Exception as e:
            try:
                await client.disconnect()
            except Exception:
                pass

            # For known ValueError cases, bubble up as-is
            if isinstance(e, ValueError):
                raise

            # Hide low-level details (like temp file paths) behind a clean message
            raise ValueError(
                "حدث خطأ غير متوقع أثناء التحقق من رمز Telegram. "
                "يرجى طلب كود جديد والمحاولة مرة أخرى."
            )
    
    async def create_client_from_session(self, session_string: str) -> "TelegramClient":
        """
        Create TelegramClient from session string
        
        Args:
            session_string: Session string saved from verify_code (StringSession format)
        
        Returns:
            Connected TelegramClient instance
        
        Note: Uses StringSession for in-memory session handling, no temp files needed.
        """
        client = None
        try:
            # Use StringSession directly - no temp file needed
            # flood_sleep_threshold=0 means raise FloodWaitError immediately instead of sleeping
            from telethon.sessions import StringSession
            from telethon import TelegramClient
            
            session = StringSession(session_string)
            client = TelegramClient(
                session, 
                int(self.api_id), 
                self.api_hash,
                flood_sleep_threshold=0
            )
            await client.connect()
            
            if not await client.is_user_authorized():
                await client.disconnect()
                raise ValueError("Session expired or invalid. Please re-authenticate.")
            
            return client
        except Exception:
            # Cleanup on error
            if client:
                try:
                    await client.disconnect()
                except:
                    pass
            raise
    
    async def test_connection(self, session_string: str, client: Optional["TelegramClient"] = None) -> Tuple[bool, str, Dict]:
        """
        Test if session is still valid
        
        Args:
            session_string: Session string to test
            client: Optional existing TelegramClient to reuse
        
        Returns:
            Tuple of (success, message, user_info)
        """
        local_client = None
        try:
            if client and client.is_connected():
                active_client = client
                should_disconnect = False
            else:
                active_client = await self.create_client_from_session(session_string)
                local_client = active_client
                should_disconnect = True

            me = await active_client.get_me()
            
            user_info = {
                "id": me.id,
                "phone": getattr(me, 'phone', None),
                "first_name": getattr(me, 'first_name', None),
                "last_name": getattr(me, 'last_name', None),
                "username": getattr(me, 'username', None)
            }
            
            if should_disconnect:
                await active_client.disconnect()
            
            return True, "الاتصال ناجح", user_info
        
        except Exception as e:
            if local_client:
                try:
                    await local_client.disconnect()
                except:
                    pass
            return False, f"فشل الاتصال: {str(e)}", {}
    
    async def get_recent_messages(
        self,
        session_string: str,
        limit: int = 50,
        since_hours: int = 72,
        exclude_ids: Optional[List[str]] = None,
        skip_replied: bool = False,
        client: Optional["TelegramClient"] = None
    ) -> List[Dict]:
        """
        Get recent messages from Telegram account (INCOMING only)
        
        Args:
            session_string: Session string
            limit: Maximum number of messages to fetch
            since_hours: Only fetch messages from last N hours
            exclude_ids: Optional list of message IDs to skip (bandwidth optimization)
            client: Optional existing TelegramClient to reuse
        
        Returns:
            List of message dicts (only incoming messages from other users)
        """
        from datetime import timedelta
        from logging_config import get_logger
        
        logger = get_logger(__name__)
        # client is passed as arg or created locally
        client_created_locally = False
        messages_data = []
        
        try:
            if not client:
                client = await self.create_client_from_session(session_string)
                client_created_locally = True
            
            # Get our own user ID to filter out self-messages
            # This is a safety net in case message.out doesn't work correctly
            me = await client.get_me()
            my_user_id = me.id if me else None
            logger.debug(f"Telegram phone session user ID: {my_user_id}")
            
            # Calculate time threshold (timezone-aware)
            since_time = datetime.now(timezone.utc) - timedelta(hours=since_hours)
            
            # Get dialogs (conversations) with retry
            dialogs = await self._execute_with_retry(
                client.get_dialogs,
                limit=limit * 2
            )
            
            for dialog in dialogs[:limit]:
                # Skip if it's a channel/group where we're not admin
                if dialog.is_channel or dialog.is_group:
                    continue
                
                # OPTIMIZATION: Skip dialogs where the last message is older than since_time
                if dialog.message and dialog.message.date and dialog.message.date < since_time:
                    logger.debug(f"Skipping dialog {dialog.id} - no recent messages (last: {dialog.message.date})")
                    continue
                
                # Filter out chats that are already replied to (last message is from us)
                if skip_replied and dialog.message and dialog.message.out:
                    logger.debug(f"Skipping dialog {dialog.id} because it is already replied (last message out)")
                    continue
                
                # Get recent messages from this dialog
                try:
                    # Note: iter_messages default order is newest first
                    # offset_date returns messages BEFORE that date, so we don't use it
                    # Instead, we filter by date manually
                    async for message in client.iter_messages(
                        dialog.entity,
                        limit=20,  # Get more to filter
                    ):
                        msg_id_str = str(message.id)
                        
                        # Optimization: Skip if ID is in exclude list (already processed)
                        if exclude_ids and msg_id_str in exclude_ids:
                            continue

                        # Skip messages without text AND without media
                        # We want text OR media (or both)
                        if not message.text and not message.media:
                            continue
                        
                        # CRITICAL: explicit check for group/channel messages
                        # Even if we filtered dialogs, some messages might slip through context references
                        if message.is_group or message.is_channel:
                            logger.debug(f"Skipping group/channel message: {message.id}")
                            continue

                        # Handle OUTGOING messages (syncing messages sent from Telegram app)
                        if message.out:
                            # EXCLUDE Saved Messages (self-chat)
                            # If the dialog is with ourselves, skip it
                            if my_user_id and dialog.id == my_user_id:
                                logger.debug(f"Skipping outgoing 'Saved Messages' (self-chat): {message.id}")
                                continue
                            
                            # Skip messages older than since_time
                            if message.date and message.date < since_time:
                                break  # Messages are in reverse chronological order
                            
                            # For outgoing messages, the "contact" is the recipient (the dialog entity)
                            recipient = dialog.entity
                            recipient_name = ""
                            recipient_contact = ""
                            
                            if recipient:
                                fn = getattr(recipient, 'first_name', None) or ""
                                ln = getattr(recipient, 'last_name', None) or ""
                                recipient_name = getattr(recipient, 'title', None) or f"{fn} {ln}".strip()
                                
                                # Normalize phone
                                phone = getattr(recipient, 'phone', None)
                                if phone and phone.isdigit():
                                    phone = "+" + phone
                                recipient_contact = phone or (f"@{recipient.username}" if getattr(recipient, 'username', None) else str(dialog.id))
                            
                            messages_data.append({
                                "channel_message_id": str(message.id),
                                "sender_id": str(dialog.id),  # For outgoing, this is the recipient ID
                                "sender_name": recipient_name or "Unknown",
                                "sender_contact": recipient_contact or str(dialog.id),
                                "body": message.text or "",
                                "subject": None,
                                "received_at": message.date,
                                "chat_id": str(dialog.id),
                                "is_channel": dialog.is_channel,
                                "is_group": dialog.is_group,
                                "attachments": [],
                                "direction": "outgoing"  # Mark as outgoing for sync logic
                            })
                            continue  # Skip to next message, don't process as incoming
                        
                        # INCOMING messages processing
                        sender = await message.get_sender()
                        
                        # SAFETY NET: Skip if sender_id matches our user ID (edge case)
                        if sender and my_user_id and sender.id == my_user_id:
                            logger.debug(f"Skipping self-message (sender_id matches our ID): {message.id}")
                            continue
                        
                        # CRITICAL: Skip messages from Telegram Bots
                        # This prevents promotional bots and gaming bots from entering inbox
                        if sender:
                            # Check the is_bot property (Telethon User object)
                            sender_is_bot = getattr(sender, 'bot', False) or getattr(sender, 'is_bot', False)
                            sender_username = getattr(sender, 'username', '') or ''
                            
                            # Also check username pattern (bots typically end with 'bot')
                            username_is_bot = sender_username.lower().endswith('bot') if sender_username else False
                            
                            if sender_is_bot or username_is_bot:
                                logger.debug(f"Skipping bot message from {sender_username or sender.id}: is_bot={sender_is_bot}, username_ends_bot={username_is_bot}")
                                continue
                        
                        # Skip messages older than since_time
                        if message.date and message.date < since_time:
                            break  # Messages are in reverse chronological order
                        
                        # Extract sender info for incoming messages
                        sender_name = ""
                        sender_contact = ""
                        
                        if sender:
                            sender_name = f"{sender.first_name or ''} {sender.last_name or ''}".strip()
                            # Normalize phone: always add + prefix if it looks like a phone number
                            phone = sender.phone
                            if phone and phone.isdigit():
                                phone = "+" + phone
                            sender_contact = phone or (f"@{sender.username}" if sender.username else str(sender.id))
                        
                        messages_data.append({
                            "channel_message_id": str(message.id),
                            "sender_id": str(sender.id) if sender else None,
                            "sender_name": sender_name or sender_contact.split('@')[0] if sender_contact else "Unknown",
                            "sender_contact": sender_contact,
                            "body": message.text or "", # Allow empty body if media present
                            "subject": None,
                            "received_at": message.date,
                            "chat_id": str(dialog.id),
                            "is_channel": dialog.is_channel,
                            "is_group": dialog.is_group,
                            "attachments": [],
                            "direction": "incoming"  # Mark as incoming
                        })
                        
                        # Handle Media (Photo/Voice/Video/Document)
                        if message.media:
                            try:
                                # Skip huge files to prevent memory issues > 20MB (Increased from 10MB)
                                if hasattr(message.media, "document") and message.media.document.size > 20 * 1024 * 1024:
                                    logger.info(f"Skipping large media file in message {message.id} (size: {message.media.document.size} bytes)")
                                    # Mark message as having skipped media so it's not retried infinitely
                                    messages_data[-1]["media_skipped"] = True
                                    messages_data[-1]["media_skip_reason"] = "file_too_large"
                                    
                                    # Still set a body placeholder so user knows there is a file
                                    if not messages_data[-1]["body"]:
                                        mime = message.media.document.mime_type
                                        if mime.startswith('video'): messages_data[-1]["body"] = "[فيديو كبير]"
                                        elif mime.startswith('audio'): messages_data[-1]["body"] = "[صوت كبير]"
                                        else: messages_data[-1]["body"] = "[ملف كبير]"
                                        
                                    continue  # Skip downloading but keep the message
                                    
                                # Download media to memory
                                file_bytes = await message.download_media(file=bytes)
                                
                                if file_bytes:
                                    # Detect content type and generic type
                                    mime_type = "application/octet-stream"
                                    generic_type = "file"
                                    
                                    if hasattr(message.media, "photo"):
                                        mime_type = "image/jpeg"
                                        generic_type = "image"
                                    elif hasattr(message.media, "document"):
                                        mime_type = message.media.document.mime_type
                                        # Map MIME to generic type for mobile app
                                        if mime_type.startswith('image/'):
                                            generic_type = "image"
                                        elif mime_type.startswith('video/'):
                                            generic_type = "video"
                                        elif mime_type.startswith('audio/'):
                                            generic_type = "audio"
                                        else:
                                            generic_type = "file"

                                    # Set fallback body if empty (for Inbox visibility)
                                    if not messages_data[-1]["body"]:
                                        if generic_type == "image":
                                            messages_data[-1]["body"] = "[صورة]"
                                        elif generic_type == "video":
                                            messages_data[-1]["body"] = "[فيديو]"
                                        elif generic_type == "audio":
                                            # Check if voice note
                                            is_voice = False
                                            if hasattr(message.media, "document"):
                                                 # check attributes for voice
                                                 for attr in message.media.document.attributes:
                                                     if hasattr(attr, 'voice'):
                                                         is_voice = True
                                                         break
                                            
                                            messages_data[-1]["body"] = "[رسالة صوتية]" if is_voice else "[ملف صوتي]"
                                        else:
                                            messages_data[-1]["body"] = "[ملف]"

                                    # Save to file system (Premium Storage)
                                    filename = f"tg_phone_{message.id}"
                                    rel_path, abs_url = get_file_storage().save_file(
                                        content=file_bytes,
                                        filename=filename,
                                        mime_type=mime_type
                                    )

                                    # Hybrid storage: small files get base64 for instant loading
                                    b64_data = None
                                    if len(file_bytes) < 1 * 1024 * 1024:
                                        b64_data = base64.b64encode(file_bytes).decode('utf-8')
                                    
                                    # Add to attachments
                                    messages_data[-1]["attachments"].append({
                                        "type": generic_type,        # 'image', 'video', 'audio', 'file'
                                        "mime_type": mime_type,      # 'image/jpeg', 'video/mp4', etc.
                                        "url": abs_url,
                                        "path": rel_path,
                                        "base64": b64_data,
                                        "filename": filename,
                                        "size": len(file_bytes)
                                    })
                                    logger.debug(f"Saved media for message {message.id} ({len(file_bytes)} bytes) to {rel_path}")
                                    
                            except Exception as media_e:
                                # logger.error(f"Failed to download media for message {message.id}: {media_e}")
                                pass
                
                except Exception as e:
                    # Skip this dialog if error
                    continue
            
            # Sort by received_at, newest first
            messages_data.sort(key=lambda x: x.get("received_at", datetime.min), reverse=True)
            
            return messages_data[:limit]
        
        except Exception as e:
            raise ValueError(f"خطأ في جلب الرسائل: {str(e)}")
        finally:
            if client and client_created_locally:
                try:
                    await client.disconnect()
                except:
                    pass
    
    async def send_message(
        self,
        session_string: str,
        recipient_id: str,
        text: str,
        reply_to_message_id: Optional[int] = None,
        client: Optional["TelegramClient"] = None
    ) -> Dict:
        """
        Send a message via Telegram
        """
        from logging_config import get_logger
        logger = get_logger(__name__)
        
        client_created_locally = False
        try:
            if not client:
                client = await self.create_client_from_session(session_string)
                client_created_locally = True
            
            # Log identity for debugging
            try:
                me = await client.get_me()
                logger.info(f"Using Telegram account: {me.first_name} (@{me.username or 'NoUsername'}) ID: {me.id}")
            except Exception as ident_e:
                logger.warning(f"Failed to fetch identity: {ident_e}")

            # CRITICAL: Populate entity cache from recent dialogs first.
            await self._execute_with_retry(client.get_dialogs, limit=200)
            
            # 1. First resolve the entity (User, Chat, Channel)
            entity = await self._resolve_telegram_entity(client, recipient_id, logger)
            
            if entity is None:
                logger.error(f"Cannot find any entity corresponding to '{recipient_id}'")
                raise ValueError(f"لم نتمكن من العثور على جهة الاتصال {recipient_id}. هل قام بمراسلتك مؤخراً؟")
            
            sent_message = await self._execute_with_retry(
                client.send_message,
                entity,
                text,
                reply_to=reply_to_message_id
            )
            
            return {
                "id": sent_message.id,
                "chat_id": str(sent_message.peer_id.channel_id if hasattr(sent_message.peer_id, 'channel_id') else sent_message.peer_id.user_id),
                "text": sent_message.text,
                "date": sent_message.date.isoformat() if sent_message.date else None
            }
        
        except Exception as e:
            raise ValueError(f"خطأ في إرسال الرسالة: {str(e)}")
        finally:
            if client and client_created_locally:
                try:
                    await client.disconnect()
                except:
                    pass

    async def send_voice(
        self,
        session_string: str,
        recipient_id: str,
        audio_path: str,
        client: Optional["TelegramClient"] = None
    ) -> Dict:
        """
        Send a voice message via Telegram
        """
        from logging_config import get_logger
        logger = get_logger(__name__)
        
        client_created_locally = False
        try:
            if not client:
                client = await self.create_client_from_session(session_string)
                client_created_locally = True
            
            # Fetch dialogs first to populate entity cache
            await client.get_dialogs(limit=100)
            
            # Resolve entity (will try DB hash if not in cache)
            entity = await self._resolve_telegram_entity(client, recipient_id, logger)
            
            if entity is None:
                logger.error(f"Cannot find entity '{recipient_id}'")
                raise ValueError(f"Cannot find entity '{recipient_id}'")
            
            # Send voice message
            sent_message = await client.send_file(
                entity,
                audio_path,
                voice_note=True  # This makes it appear as a voice message
            )
            
            return {
                "id": sent_message.id,
                "chat_id": str(recipient_id),
                "date": sent_message.date.isoformat() if sent_message.date else None
            }
        
        except Exception as e:
            raise ValueError(f"خطأ في إرسال الفويس: {str(e)}")
        finally:
            if client and client_created_locally:
                try:
                    await client.disconnect()
                except:
                    pass

    async def send_file(
        self,
        session_string: str,
        recipient_id: str,
        file_path: str,
        caption: str = None,
        client: Optional["TelegramClient"] = None
    ) -> Dict:
        """
        Send a general file (video, document, photo) via Telegram
        """
        from logging_config import get_logger
        logger = get_logger(__name__)
        
        client_created_locally = False
        try:
            if not client:
                client = await self.create_client_from_session(session_string)
                client_created_locally = True
            
            # Fetch dialogs first
            await client.get_dialogs(limit=100)
            
            # Resolve entity
            entity = await self._resolve_telegram_entity(client, recipient_id, logger)
            
            if entity is None:
                logger.error(f"Cannot find entity '{recipient_id}'")
                raise ValueError(f"Cannot find entity '{recipient_id}'")
            
            # Send file
            sent_message = await client.send_file(
                entity,
                file_path,
                caption=caption
            )
            
            return {
                "id": sent_message.id,
                "chat_id": str(recipient_id),
                "date": sent_message.date.isoformat() if sent_message.date else None
            }
        
        except Exception as e:
            raise ValueError(f"خطأ في إرسال الملف: {str(e)}")
        finally:
            if client and client_created_locally:
                try:
                    await client.disconnect()
                except:
                    pass

    async def mark_as_read(
        self,
        session_string: str,
        chat_id: str,
        max_id: int = 0,
        client: Optional["TelegramClient"] = None
    ) -> bool:
        """
        Mark messages in a chat as read (triggers double check for sender)
        """
        from logging_config import get_logger
        logger = get_logger(__name__)
        
        client_created_locally = False
        try:
            if not client:
                client = await self.create_client_from_session(session_string)
                client_created_locally = True
            
            # Fetch dialogs first
            await client.get_dialogs(limit=100)
            
            # Resolve entity
            entity = await self._resolve_telegram_entity(client, chat_id, logger)
            
            if not entity:
                logger.warning(f"Failed to resolve chat {chat_id} for mark_read")
                return False
            
            # Send read acknowledgment
            await client.send_read_acknowledge(entity, max_id=max_id)
            logger.info(f"Marked chat {chat_id} as read up to {max_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to mark as read for {chat_id}: {e}")
            return False
        finally:
            if client and client_created_locally:
                try:
                    await client.disconnect()
                except:
                    pass

    async def get_messages_read_status(
        self,
        session_string: str,
        channel_message_ids: List[str],
        client: Optional["TelegramClient"] = None
    ) -> Dict[str, str]:
        """
        Get read status for specific messages
        """
        from logging_config import get_logger
        logger = get_logger(__name__)
        
        client_created_locally = False
        statuses = {}
        
        try:
            # Parse IDs to integers
            msg_ids = []
            for mid in channel_message_ids:
                try:
                    msg_ids.append(int(mid))
                except (ValueError, TypeError):
                    continue
            
            if not msg_ids:
                return {}

            if not client:
                client = await self.create_client_from_session(session_string)
                client_created_locally = True
            
            # Optimization: First get messages to find their chats
            messages = await client.get_messages(ids=msg_ids)
            
            # Map valid messages to their chats
            chat_map: Dict[int, List[int]] = {}
            valid_messages = []
            
            for msg in messages:
                if not msg:
                    continue
                valid_messages.append(msg)
                if msg.chat_id not in chat_map:
                    chat_map[msg.chat_id] = []
                chat_map[msg.chat_id].append(msg.id)
            
            if not valid_messages:
                return {}
                
            # Fetch dialogs for these chats
            dialogs = await client.get_dialogs(limit=100)
            
            read_max_ids = {} # chat_id -> max_id
            for d in dialogs:
                read_max_ids[d.id] = d.read_outbox_max_id
                
            for msg in valid_messages:
                chat_id = msg.chat_id
                # Determine status
                max_id = read_max_ids.get(chat_id, 0)
                
                if max_id > 0 and msg.id <= max_id:
                    statuses[str(msg.id)] = "read"
                else:
                    statuses[str(msg.id)] = "sent"
                    
            return statuses
            
        except Exception as e:
            logger.error(f"Failed to check read status: {e}")
            return {}
        finally:
            if client and client_created_locally:
                try:
                    await client.disconnect()
                except:
                    pass

# Singleton instance for Telegram Phone Service
_telegram_phone_instance: Optional[TelegramPhoneService] = None

def get_telegram_phone_service() -> TelegramPhoneService:
    """Get or create singleton instance of TelegramPhoneService"""
    global _telegram_phone_instance
    if _telegram_phone_instance is None:
        _telegram_phone_instance = TelegramPhoneService()
    return _telegram_phone_instance
