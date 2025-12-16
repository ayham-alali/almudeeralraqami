"""
Al-Mudeer - Email Service
IMAP fetching and SMTP sending for business email integration
"""

import imaplib
import smtplib
import email
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import decode_header
from datetime import datetime, timedelta
from typing import List, Optional, Tuple
import asyncio
import re


class EmailService:
    """Service for fetching and sending emails"""
    
    def __init__(
        self,
        email_address: str,
        password: str,
        imap_server: str,
        smtp_server: str,
        imap_port: int = 993,
        smtp_port: int = 587
    ):
        self.email_address = email_address
        self.password = password
        self.imap_server = imap_server
        self.smtp_server = smtp_server
        self.imap_port = imap_port
        self.smtp_port = smtp_port
    
    def _decode_header_value(self, value: str) -> str:
        """Decode email header value (handles Arabic and other encodings)"""
        if not value:
            return ""
        
        decoded_parts = decode_header(value)
        result = []
        for part, encoding in decoded_parts:
            if isinstance(part, bytes):
                try:
                    result.append(part.decode(encoding or 'utf-8', errors='replace'))
                except:
                    result.append(part.decode('utf-8', errors='replace'))
            else:
                result.append(part)
        return ' '.join(result)
    
    def _extract_email_address(self, from_header: str) -> Tuple[str, str]:
        """Extract name and email from From header"""
        if not from_header:
            return "", ""
        
        # Try to match "Name <email@domain.com>" format
        match = re.match(r'(.+?)\s*<(.+?)>', from_header)
        if match:
            name = self._decode_header_value(match.group(1).strip().strip('"'))
            email_addr = match.group(2).strip()
            return name, email_addr
        
        # Just email address
        return "", from_header.strip()
    
    def _get_email_body(self, msg) -> str:
        """Extract email body (plain text preferred)"""
        body = ""
        
        if msg.is_multipart():
            for part in msg.walk():
                content_type = part.get_content_type()
                content_disposition = str(part.get("Content-Disposition", ""))
                
                # Skip attachments
                if "attachment" in content_disposition:
                    continue
                
                if content_type == "text/plain":
                    try:
                        charset = part.get_content_charset() or 'utf-8'
                        body = part.get_payload(decode=True).decode(charset, errors='replace')
                        break  # Prefer plain text
                    except:
                        pass
                elif content_type == "text/html" and not body:
                    try:
                        charset = part.get_content_charset() or 'utf-8'
                        html = part.get_payload(decode=True).decode(charset, errors='replace')
                        # Simple HTML to text conversion
                        body = re.sub(r'<[^>]+>', '', html)
                        body = re.sub(r'\s+', ' ', body).strip()
                    except:
                        pass
        else:
            try:
                charset = msg.get_content_charset() or 'utf-8'
                body = msg.get_payload(decode=True).decode(charset, errors='replace')
            except:
                body = str(msg.get_payload())
        
        return body.strip()
    
    async def fetch_new_emails(
        self,
        since_hours: int = 24,
        folder: str = "INBOX",
        limit: int = 50
    ) -> List[dict]:
        """Fetch new emails from IMAP server"""
        
        emails = []
        
        def _fetch():
            nonlocal emails
            mail = None
            try:
                # Connect to IMAP with timeout
                mail = imaplib.IMAP4_SSL(self.imap_server, self.imap_port, timeout=30)
                mail.login(self.email_address, self.password)
                mail.select(folder)
                
                # Search for recent emails
                since_date = (datetime.now() - timedelta(hours=since_hours)).strftime("%d-%b-%Y")
                _, message_numbers = mail.search(None, f'(SINCE {since_date})')
                
                message_ids = message_numbers[0].split()
                
                # Get latest emails (limit)
                for msg_id in message_ids[-limit:]:
                    try:
                        _, msg_data = mail.fetch(msg_id, "(RFC822)")
                        email_body = msg_data[0][1]
                        msg = email.message_from_bytes(email_body)
                        
                        # Extract info
                        subject = self._decode_header_value(msg.get("Subject", ""))
                        from_header = self._decode_header_value(msg.get("From", ""))
                        sender_name, sender_email = self._extract_email_address(from_header)
                        date_str = msg.get("Date", "")
                        message_id = msg.get("Message-ID", "")
                        body = self._get_email_body(msg)
                        
                        # Parse date
                        try:
                            received_at = email.utils.parsedate_to_datetime(date_str)
                        except:
                            received_at = datetime.now()
                        
                        emails.append({
                            "channel_message_id": message_id,
                            "subject": subject,
                            "sender_name": sender_name or sender_email.split('@')[0],
                            "sender_contact": sender_email,
                            "body": body,
                            "received_at": received_at,
                            "raw_from": from_header
                        })
                        
                    except Exception as e:
                        print(f"Error parsing email {msg_id}: {e}")
                        continue
                
                mail.logout()
                mail = None
                
            except Exception as e:
                if mail:
                    try:
                        mail.logout()
                    except:
                        pass
                print(f"IMAP Error: {e}")
                raise
        
        # Run in thread pool to not block async
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _fetch)
        
        return emails
    
    async def send_email(
        self,
        to_email: str,
        subject: str,
        body: str,
        reply_to_message_id: str = None
    ) -> bool:
        """Send email via SMTP"""
        
        def _send():
            try:
                # Create message
                msg = MIMEMultipart('alternative')
                msg['From'] = self.email_address
                msg['To'] = to_email
                msg['Subject'] = subject
                
                if reply_to_message_id:
                    msg['In-Reply-To'] = reply_to_message_id
                    msg['References'] = reply_to_message_id
                
                # Add plain text body
                text_part = MIMEText(body, 'plain', 'utf-8')
                msg.attach(text_part)
                
                # Connect and send with timeout
                server = None
                if self.smtp_port == 465:
                    server = smtplib.SMTP_SSL(self.smtp_server, self.smtp_port, timeout=30)
                else:
                    server = smtplib.SMTP(self.smtp_server, self.smtp_port, timeout=30)
                    server.starttls()
                
                server.login(self.email_address, self.password)
                server.send_message(msg)
                server.quit()
                server = None
                
                return True
                
            except Exception as e:
                if server:
                    try:
                        server.quit()
                    except:
                        pass
                print(f"SMTP Error: {e}")
                raise
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _send)
    
    async def test_connection(self, timeout: int = 15) -> Tuple[bool, str]:
        """Test IMAP and SMTP connections with timeout"""
        
        def _test():
            errors = []
            mail = None
            server = None
            
            try:
                # Test IMAP with timeout
                try:
                    mail = imaplib.IMAP4_SSL(self.imap_server, self.imap_port, timeout=timeout)
                    mail.login(self.email_address, self.password)
                    mail.logout()
                    mail = None
                except Exception as e:
                    if mail:
                        try:
                            mail.logout()
                        except:
                            pass
                    errors.append(f"IMAP: {str(e)}")
                
                # Test SMTP with timeout
                try:
                    if self.smtp_port == 465:
                        server = smtplib.SMTP_SSL(self.smtp_server, self.smtp_port, timeout=timeout)
                    else:
                        server = smtplib.SMTP(self.smtp_server, self.smtp_port, timeout=timeout)
                        server.starttls()
                    server.login(self.email_address, self.password)
                    server.quit()
                    server = None
                except Exception as e:
                    if server:
                        try:
                            server.quit()
                        except:
                            pass
                    errors.append(f"SMTP: {str(e)}")
                
            except Exception as e:
                # Cleanup on any unexpected error
                if mail:
                    try:
                        mail.logout()
                    except:
                        pass
                if server:
                    try:
                        server.quit()
                    except:
                        pass
                errors.append(f"خطأ غير متوقع: {str(e)}")
            
            return errors
        
        try:
            loop = asyncio.get_event_loop()
            # Add additional timeout wrapper to prevent hanging
            errors = await asyncio.wait_for(
                loop.run_in_executor(None, _test),
                timeout=timeout + 5  # Add 5 seconds buffer
            )
        except asyncio.TimeoutError:
            return False, f"انتهت مهلة الاتصال ({timeout} ثانية). تحقق من صحة الإعدادات والخادم."
        except Exception as e:
            return False, f"خطأ في الاتصال: {str(e)}"
        
        if errors:
            return False, "; ".join(errors)
        return True, "تم الاتصال بنجاح"


# Common email provider settings
EMAIL_PROVIDERS = {
    "gmail": {
        "imap_server": "imap.gmail.com",
        "smtp_server": "smtp.gmail.com",
        "imap_port": 993,
        "smtp_port": 587,
        "note": "يجب تفعيل 'كلمات مرور التطبيقات' في حساب Google"
    },
    "outlook": {
        "imap_server": "outlook.office365.com",
        "smtp_server": "smtp.office365.com",
        "imap_port": 993,
        "smtp_port": 587,
        "note": "يعمل مع Outlook و Hotmail"
    },
    "yahoo": {
        "imap_server": "imap.mail.yahoo.com",
        "smtp_server": "smtp.mail.yahoo.com",
        "imap_port": 993,
        "smtp_port": 587,
        "note": "يجب إنشاء كلمة مرور للتطبيق"
    },
    "custom": {
        "imap_server": "",
        "smtp_server": "",
        "imap_port": 993,
        "smtp_port": 587,
        "note": "أدخل إعدادات الخادم يدوياً"
    }
}

