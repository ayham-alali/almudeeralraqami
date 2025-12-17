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

        # Clean up any previous pending login for this phone
        pending = self._pending_logins.pop(phone_number, None)
        if pending and "client" in pending:
            try:
                await pending["client"].disconnect()
            except Exception:
                pass

        # Use an in-memory StringSession so we don't rely on temp files
        session = StringSession()
        client = TelegramClient(session, int(self.api_id), self.api_hash)

        try:
            await client.connect()

            # Always request a new code
            sent = await client.send_code_request(phone_number)
            phone_code_hash = getattr(sent, "phone_code_hash", None)

            # Store pending login info in memory
            self._pending_logins[phone_number] = {
                "phone_number": phone_number,
                "created_at": datetime.now(),
                "client": client,
                "phone_code_hash": phone_code_hash,
            }

            return {
                "success": True,
                "message": f"تم إرسال كود التحقق إلى Telegram الخاص برقم {phone_number}",
                "session_id": None,
                "phone_number": phone_number,
            }

        except PhoneNumberUnoccupiedError:
            try:
                await client.disconnect()
            except Exception:
                pass
            raise ValueError(f"الرقم {phone_number} غير مسجل في Telegram")

        except FloodWaitError as e:
            try:
                await client.disconnect()
            except Exception:
                pass
            raise ValueError(f"تم إرسال عدد كبير من الطلبات. يرجى الانتظار {e.seconds} ثانية")

        except Exception as e:
            try:
                await client.disconnect()
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

        pending = self._pending_logins.get(phone_number)
        if not pending:
            raise ValueError("انتهت صلاحية طلب تسجيل الدخول. يرجى طلب كود جديد")

        client: TelegramClient = pending["client"]

        try:
            # If we are not yet authorized, complete sign-in
            if not await client.is_user_authorized():
                try:
                    # First step: code verification
                    await client.sign_in(phone_number, code)
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
            # Remove pending login
            self._pending_logins.pop(phone_number, None)

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
            # Ensure cleanup
            self._pending_logins.pop(phone_number, None)

            # For known ValueError cases, bubble up as-is
            if isinstance(e, ValueError):
                raise

            # Hide low-level details (like temp file paths) behind a clean message
            raise ValueError(
                "حدث خطأ غير متوقع أثناء التحقق من رمز Telegram. "
                "يرجى طلب كود جديد والمحاولة مرة أخرى."
            )
    
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

