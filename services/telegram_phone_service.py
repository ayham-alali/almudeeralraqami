"""
Al-Mudeer - Telegram Phone Number Service
MTProto client for Telegram user accounts (phone numbers)
"""

import asyncio
import os
from typing import Optional, Dict, Tuple, List
from datetime import datetime
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import (
    PhoneCodeInvalidError,
    PhoneCodeExpiredError,
    SessionPasswordNeededError,
    PhoneNumberUnoccupiedError,
    FloodWaitError,
    ApiIdInvalidError
)
import json
import tempfile


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
        
        # In-memory storage for temporary login states (kept for backward compatibility)
        # NOTE: The new implementation relies primarily on deterministic session files,
        # so that any worker process can handle the verification step.
        self._pending_logins: Dict[str, Dict] = {}

    def _get_session_path(self, phone_number: str) -> str:
        """Get deterministic session file path for a phone number."""
        safe_phone = phone_number.replace("+", "").replace(" ", "")
        tmp_dir = tempfile.gettempdir()
        return os.path.join(tmp_dir, f"almudeer_tg_phone_{safe_phone}.session")
    
    def _get_meta_path(self, phone_number: str) -> str:
        """Get path for storing temporary metadata (e.g. phone_code_hash)."""
        return self._get_session_path(phone_number) + ".json"
    
    def _cleanup_expired_logins(self, max_age_minutes: int = 10):
        """Clean up expired login sessions (older than max_age_minutes)"""
        from datetime import timedelta
        now = datetime.now()
        expired_ids = []
        
        for session_id, data in self._pending_logins.items():
            created_at = data.get("created_at")
            if created_at:
                age = now - created_at
                if age > timedelta(minutes=max_age_minutes):
                    expired_ids.append(session_id)
                    # Cleanup client if exists
                    if "client" in data:
                        client = data["client"]
                        try:
                            # Schedule disconnect (can't await in sync method)
                            import asyncio
                            if client.is_connected():
                                asyncio.create_task(client.disconnect())
                        except:
                            pass
                    # Cleanup session file
                    session_path = data.get("session_path")
                    if session_path and os.path.exists(session_path):
                        try:
                            os.unlink(session_path)
                        except:
                            pass
        
        # Remove expired entries
        for session_id in expired_ids:
            self._pending_logins.pop(session_id, None)
    
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
        
        # Use deterministic session + meta files per phone so any worker can verify
        session_path = self._get_session_path(phone_number)
        meta_path = self._get_meta_path(phone_number)
        
        # Remove any old files for this phone
        for path in (session_path, meta_path):
            if os.path.exists(path):
                try:
                    os.unlink(path)
                except Exception:
                    pass
        
        try:
            client = TelegramClient(session_path, int(self.api_id), self.api_hash)
            await client.connect()
            
            if not await client.is_user_authorized():
                # Request code and capture phone_code_hash
                sent = await client.send_code_request(phone_number)
                
                # Persist phone_code_hash so that any worker can verify later
                try:
                    with open(meta_path, "w", encoding="utf-8") as f:
                        json.dump(
                            {
                                "phone_number": phone_number,
                                "phone_code_hash": getattr(sent, "phone_code_hash", None),
                                "created_at": datetime.now().isoformat(),
                            },
                            f,
                            ensure_ascii=False,
                        )
                except Exception:
                    # Best-effort; if this fails, verification will fail with a clear error
                    pass
                
                # We can safely disconnect here; verify step will reopen the session file
                await client.disconnect()
                
                return {
                    "success": True,
                    "message": f"تم إرسال كود التحقق إلى Telegram الخاص برقم {phone_number}",
                    # Keep the same field name for backward compatibility; we now
                    # store the deterministic session_path in it (unused by backend).
                    "session_id": session_path,
                    "phone_number": phone_number
                }
            else:
                # Already authorized, convert file session to string
                client.session.save()
                
                # Convert file session to StringSession format
                me = await client.get_me()
                string_session = StringSession()
                string_session.set_dc(
                    client.session.dc_id,
                    client.session.server_address,
                    client.session.port
                )
                if hasattr(client.session, '_auth_key') and client.session._auth_key:
                    string_session._auth_key = client.session._auth_key
                elif hasattr(client.session, 'auth_key') and client.session.auth_key:
                    string_session.auth_key = client.session.auth_key
                
                session_string = string_session.save()
                
                await client.disconnect()
                
                return {
                    "success": True,
                    "message": "تم تسجيل الدخول مسبقاً",
                    "session_string": session_string,
                    "phone_number": phone_number
                }
        
        except PhoneNumberUnoccupiedError:
            if 'client' in locals():
                await client.disconnect()
            # Clean up deterministic session file if it exists
            if os.path.exists(session_path):
                try:
                    os.unlink(session_path)
                except Exception:
                    pass
            raise ValueError(f"الرقم {phone_number} غير مسجل في Telegram")
        
        except FloodWaitError as e:
            if 'client' in locals():
                await client.disconnect()
            if os.path.exists(session_path):
                try:
                    os.unlink(session_path)
                except Exception:
                    pass
            raise ValueError(f"تم إرسال عدد كبير من الطلبات. يرجى الانتظار {e.seconds} ثانية")
        
        except Exception as e:
            if 'client' in locals():
                await client.disconnect()
            if os.path.exists(session_path):
                try:
                    os.unlink(session_path)
                except Exception:
                    pass
            raise ValueError(f"خطأ في طلب الكود: {str(e)}")
    
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
        
        # Use deterministic session + meta files (independent of in‑memory state)
        session_path = self._get_session_path(phone_number)
        meta_path = self._get_meta_path(phone_number)
        
        if not os.path.exists(session_path) or not os.path.exists(meta_path):
            # No active login request found for this phone
            raise ValueError("انتهت صلاحية طلب تسجيل الدخول. يرجى طلب كود جديد")
        
        # Load phone_code_hash from meta file
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
            phone_code_hash = meta.get("phone_code_hash")
            if not phone_code_hash:
                raise ValueError("بيانات التحقق غير مكتملة، يرجى طلب كود جديد")
        except Exception as e:
            raise ValueError("تعذر قراءة بيانات التحقق، يرجى طلب كود جديد") from e
        
        # Open client from session file
        client: Optional[TelegramClient] = None
        try:
            client = TelegramClient(session_path, int(self.api_id), self.api_hash)
            await client.connect()
            if await client.is_user_authorized():
                # Already authorized, convert file session to string
                client.session.save()
                
                # Convert to StringSession format
                string_session = StringSession()
                string_session.set_dc(
                    client.session.dc_id,
                    client.session.server_address,
                    client.session.port
                )
                # Copy auth key
                if hasattr(client.session, '_auth_key') and client.session._auth_key:
                    string_session._auth_key = client.session._auth_key
                elif hasattr(client.session, 'auth_key') and client.session.auth_key:
                    string_session.auth_key = client.session.auth_key
                
                session_string = string_session.save()
                me = await client.get_me()
                await client.disconnect()
                
                # Cleanup
                if session_id:
                    self._pending_logins.pop(session_id, None)
                os.unlink(session_path)
                
                return session_string, {
                    "id": me.id,
                    "phone": me.phone,
                    "first_name": me.first_name,
                    "last_name": me.last_name,
                    "username": me.username
                }
            
            # Sign in with code (using stored phone_code_hash)
            try:
                await client.sign_in(phone=phone_number, code=code, phone_code_hash=phone_code_hash)
            except SessionPasswordNeededError:
                # 2FA enabled - request password
                if not password:
                    # Keep session alive for password entry
                    # Store client temporarily (don't disconnect yet)
                    if session_id:
                        # Update pending login to indicate 2FA needed
                        self._pending_logins[session_id]["needs_2fa"] = True
                        self._pending_logins[session_id]["client"] = client
                    raise ValueError(
                        "حسابك محمي بكلمة مرور ثنائية (2FA). "
                        "يرجى إدخال كلمة المرور الثنائية لإكمال تسجيل الدخول."
                    )
                else:
                    # Sign in with password
                    try:
                        await client.sign_in(password=password)
                    except Exception as e:
                        await client.disconnect()
                        os.unlink(session_path)
                        if session_id:
                            self._pending_logins.pop(session_id, None)
                        raise ValueError(f"كلمة المرور الثنائية غير صحيحة: {str(e)}")
            except PhoneCodeInvalidError:
                raise ValueError("كود التحقق غير صحيح")
            except PhoneCodeExpiredError:
                if session_id:
                    self._pending_logins.pop(session_id, None)
                if os.path.exists(session_path):
                    os.unlink(session_path)
                raise ValueError("انتهت صلاحية كود التحقق. يرجى طلب كود جديد")
            
            # Get session string - convert file session to StringSession format
            client.session.save()
            
            # Convert to StringSession format
            string_session = StringSession()
            string_session.set_dc(
                client.session.dc_id,
                client.session.server_address,
                client.session.port
            )
            # Copy auth key
            if hasattr(client.session, '_auth_key') and client.session._auth_key:
                string_session._auth_key = client.session._auth_key
            elif hasattr(client.session, 'auth_key') and client.session.auth_key:
                string_session.auth_key = client.session.auth_key
            
            session_string = string_session.save()
            me = await client.get_me()
            
            await client.disconnect()
            
            # Cleanup
            if os.path.exists(session_path):
                try:
                    os.unlink(session_path)
                except Exception:
                    pass
            if os.path.exists(meta_path):
                try:
                    os.unlink(meta_path)
                except Exception:
                    pass
            
            return session_string, {
                "id": me.id,
                "phone": me.phone,
                "first_name": me.first_name,
                "last_name": me.last_name,
                "username": me.username
            }
        
        except Exception as e:
            if client:
                try:
                    await client.disconnect()
                except:
                    pass
            if os.path.exists(session_path):
                try:
                    os.unlink(session_path)
                except Exception:
                    pass
            if os.path.exists(meta_path):
                try:
                    os.unlink(meta_path)
                except Exception:
                    pass
            
            if isinstance(e, ValueError):
                raise
            raise ValueError(f"خطأ في التحقق: {str(e)}")
    
    async def create_client_from_session(self, session_string: str) -> TelegramClient:
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
            session = StringSession(session_string)
            client = TelegramClient(session, int(self.api_id), self.api_hash)
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
    
    async def test_connection(self, session_string: str) -> Tuple[bool, str, Dict]:
        """
        Test if session is still valid
        
        Args:
            session_string: Session string to test
        
        Returns:
            Tuple of (success, message, user_info)
        """
        client = None
        try:
            client = await self.create_client_from_session(session_string)
            me = await client.get_me()
            
            user_info = {
                "id": me.id,
                "phone": me.phone,
                "first_name": me.first_name,
                "last_name": me.last_name,
                "username": me.username
            }
            
            await client.disconnect()
            
            return True, "الاتصال ناجح", user_info
        
        except Exception as e:
            if client:
                try:
                    await client.disconnect()
                except:
                    pass
            return False, f"فشل الاتصال: {str(e)}", {}
    
    async def get_recent_messages(
        self,
        session_string: str,
        limit: int = 50,
        since_hours: int = 24
    ) -> List[Dict]:
        """
        Get recent messages from Telegram account
        
        Args:
            session_string: Session string
            limit: Maximum number of messages to fetch
            since_hours: Only fetch messages from last N hours
        
        Returns:
            List of message dicts
        """
        from datetime import timedelta
        
        client = None
        messages_data = []
        
        try:
            client = await self.create_client_from_session(session_string)
            
            # Calculate time threshold
            since_time = datetime.now() - timedelta(hours=since_hours)
            
            # Get dialogs (conversations)
            dialogs = await client.get_dialogs(limit=limit * 2)  # Get more to filter
            
            for dialog in dialogs[:limit]:
                # Skip if it's a channel/group where we're not admin
                if dialog.is_channel or dialog.is_group:
                    continue
                
                # Get recent messages from this dialog
                try:
                    async for message in client.iter_messages(
                        dialog.entity,
                        limit=10,
                        offset_date=since_time
                    ):
                        if not message.text or message.out:  # Skip outgoing messages
                            continue
                        
                        sender = await message.get_sender()
                        sender_name = ""
                        sender_contact = ""
                        
                        if sender:
                            sender_name = f"{sender.first_name or ''} {sender.last_name or ''}".strip()
                            sender_contact = sender.phone or (f"@{sender.username}" if sender.username else str(sender.id))
                        
                        messages_data.append({
                            "channel_message_id": str(message.id),
                            "sender_id": str(sender.id) if sender else None,
                            "sender_name": sender_name or sender_contact.split('@')[0] if sender_contact else "Unknown",
                            "sender_contact": sender_contact,
                            "body": message.text,
                            "subject": None,
                            "received_at": message.date,
                            "chat_id": str(dialog.id),
                        })
                
                except Exception as e:
                    # Skip this dialog if error
                    continue
            
            await client.disconnect()
            
            # Sort by received_at, newest first
            messages_data.sort(key=lambda x: x.get("received_at", datetime.min), reverse=True)
            
            return messages_data[:limit]
        
        except Exception as e:
            if client:
                try:
                    await client.disconnect()
                except:
                    pass
            raise ValueError(f"خطأ في جلب الرسائل: {str(e)}")
    
    async def send_message(
        self,
        session_string: str,
        recipient_id: str,
        text: str,
        reply_to_message_id: Optional[int] = None
    ) -> Dict:
        """
        Send a message via Telegram
        
        Args:
            session_string: Session string
            recipient_id: Recipient chat ID or username
            text: Message text
            reply_to_message_id: Optional message ID to reply to
        
        Returns:
            Dict with sent message info
        """
        client = None
        try:
            client = await self.create_client_from_session(session_string)
            
            # Try to parse as int (chat ID) or use as username
            try:
                chat_id = int(recipient_id)
                entity = await client.get_entity(chat_id)
            except (ValueError, Exception):
                # Try as username
                entity = await client.get_entity(recipient_id)
            
            sent_message = await client.send_message(
                entity,
                text,
                reply_to=reply_to_message_id
            )
            
            await client.disconnect()
            
            return {
                "id": sent_message.id,
                "chat_id": str(sent_message.peer_id.channel_id if hasattr(sent_message.peer_id, 'channel_id') else sent_message.peer_id.user_id),
                "text": sent_message.text,
                "date": sent_message.date.isoformat() if sent_message.date else None
            }
        
        except Exception as e:
            if client:
                try:
                    await client.disconnect()
                except:
                    pass
            raise ValueError(f"خطأ في إرسال الرسالة: {str(e)}")

