"""
Al-Mudeer - Gmail API Service
Uses Gmail API with OAuth 2.0 tokens for fetching and sending emails
"""

import base64
import json
import httpx
from typing import List, Dict, Optional
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


class GmailAPIService:
    """Service for Gmail API operations using OAuth 2.0"""
    
    GMAIL_API_BASE = "https://gmail.googleapis.com/gmail/v1"
    
    def __init__(self, access_token: str, refresh_token: str = None, oauth_service=None):
        """
        Initialize Gmail API service
        
        Args:
            access_token: OAuth 2.0 access token
            refresh_token: OAuth 2.0 refresh token (optional, for auto-refresh)
            oauth_service: GmailOAuthService instance for token refresh
        """
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.oauth_service = oauth_service
        self._headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
    
    async def _refresh_token_if_needed(self):
        """Refresh access token if it's expired"""
        if self.oauth_service and self.refresh_token:
            try:
                new_tokens = await self.oauth_service.refresh_access_token(self.refresh_token)
                self.access_token = new_tokens["access_token"]
                self._headers["Authorization"] = f"Bearer {self.access_token}"
                return new_tokens
            except Exception as e:
                print(f"Failed to refresh token: {e}")
                raise
    
    async def _request(self, method: str, endpoint: str, **kwargs) -> Dict:
        """Make authenticated request to Gmail API"""
        url = f"{self.GMAIL_API_BASE}/{endpoint}"
        
        async with httpx.AsyncClient() as client:
            try:
                response = await client.request(
                    method,
                    url,
                    headers=self._headers,
                    timeout=30.0,
                    **kwargs
                )
                
                # If unauthorized, try refreshing token
                if response.status_code == 401 and self.refresh_token:
                    await self._refresh_token_if_needed()
                    # Retry request with new token
                    response = await client.request(
                        method,
                        url,
                        headers=self._headers,
                        timeout=30.0,
                        **kwargs
                    )
                
                if response.status_code != 200:
                    error_data = response.json() if response.content else {}
                    error_msg = error_data.get("error", {}).get("message", response.text)
                    raise Exception(f"Gmail API error: {error_msg}")
                
                return response.json() if response.content else {}
                
            except httpx.HTTPStatusError as e:
                error_data = e.response.json() if e.response.content else {}
                error_msg = error_data.get("error", {}).get("message", str(e))
                raise Exception(f"Gmail API error: {error_msg}")
    
    async def get_profile(self) -> Dict:
        """Get Gmail user profile"""
        return await self._request("GET", "users/me/profile")
    
    async def list_messages(
        self,
        query: str = "",
        max_results: int = 50,
        page_token: str = None
    ) -> Dict:
        """
        List messages from Gmail
        
        Args:
            query: Gmail search query (e.g., "is:unread", "newer_than:1d")
            max_results: Maximum number of messages to return
            page_token: Token for pagination
        
        Returns:
            Dictionary with messages list and nextPageToken
        """
        params = {"maxResults": max_results}
        if query:
            params["q"] = query
        if page_token:
            params["pageToken"] = page_token
        
        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        return await self._request("GET", f"users/me/messages?{query_string}")
    
    async def get_message(self, message_id: str, format: str = "full") -> Dict:
        """
        Get a specific message by ID
        
        Args:
            message_id: Gmail message ID
            format: Format to return (full, raw, metadata, minimal)
        
        Returns:
            Message object
        """
        return await self._request("GET", f"users/me/messages/{message_id}?format={format}")

    async def get_attachment_data(self, message_id: str, attachment_id: str) -> Optional[bytes]:
        """Download attachment content"""
        try:
            data = await self._request("GET", f"users/me/messages/{message_id}/attachments/{attachment_id}")
            if data and "data" in data:
                return base64.urlsafe_b64decode(data["data"])
        except Exception as e:
            print(f"Error downloading attachment {attachment_id}: {e}")
        return None
    
    async def send_message(
        self,
        to_email: str,
        subject: str,
        body: str,
        reply_to_message_id: str = None,
        attachments: List[Dict] = None
    ) -> Dict:
        """
        Send an email message
        
        Args:
            to_email: Recipient email address
            subject: Email subject
            subject: Email subject
            body: Email body (plain text)
            reply_to_message_id: Optional Gmail message ID to reply to
            attachments: Optional list of dicts with 'filename' and 'base64' keys
        
        Returns:
            Dictionary with sent message info
        """
        # Create MIME message
        msg = MIMEMultipart('mixed') if attachments else MIMEMultipart('alternative')
        msg['To'] = to_email
        msg['Subject'] = subject
        
        if reply_to_message_id:
            # Get original message for threading
            try:
                original = await self.get_message(reply_to_message_id, format="metadata")
                thread_id = original.get("threadId")
                # Get original message headers
                headers = original.get("payload", {}).get("headers", [])
                msg_id = next((h["value"] for h in headers if h["name"].lower() == "message-id"), None)
                references = next((h["value"] for h in headers if h["name"].lower() == "references"), None)
                
                if msg_id:
                    msg['In-Reply-To'] = msg_id
                    if references:
                        msg['References'] = f"{references} {msg_id}"
                    else:
                        msg['References'] = msg_id
                if thread_id:
                    msg['X-Gmail-Thread-ID'] = thread_id
            except:
                pass  # Continue even if we can't get thread info
        
        # Add plain text body
        # If it's mixed (has attachments), we need to nest the alternative part (text/html)
        if attachments:
            msg_alt = MIMEMultipart('alternative')
            msg_alt.attach(MIMEText(body, 'plain', 'utf-8'))
            msg.attach(msg_alt)
        else:
            msg.attach(MIMEText(body, 'plain', 'utf-8'))
        
        # Add attachments
        if attachments:
            from email.mime.base import MIMEBase
            from email import encoders
            import mimetypes
            
            for att in attachments:
                if not att.get("base64") or not att.get("filename"):
                    continue
                    
                content_type, encoding = mimetypes.guess_type(att["filename"])
                if content_type is None or encoding is not None:
                    # No guess could be made, or the file is encoded (compressed), so
                    # use a generic bag-of-bits type.
                    content_type = 'application/octet-stream'
                
                main_type, sub_type = content_type.split('/', 1)
                
                try:
                    file_data = base64.b64decode(att["base64"])
                    
                    part = MIMEBase(main_type, sub_type)
                    part.set_payload(file_data)
                    encoders.encode_base64(part)
                    part.add_header(
                        'Content-Disposition',
                        f'attachment; filename="{att["filename"]}"'
                    )
                    msg.attach(part)
                except Exception as e:
                    print(f"Error attaching file {att['filename']}: {e}")

        
        # Encode message as base64url
        raw_message = base64.urlsafe_b64encode(msg.as_bytes()).decode()
        
        payload = {"raw": raw_message}
        
        return await self._request("POST", "users/me/messages/send", json=payload)
    
    async def fetch_unreplied_threads(
        self,
        days: int = 30,
        limit: int = 50
    ) -> List[Dict]:
        """
        Fetch emails from threads where the last message is NOT from the user (unreplied).
        
        Args:
            days: Number of days to look back
            limit: Maximum number of threads to check
            
        Returns:
            List of parsed email dictionaries
        """
        # Search for threads with activity in last N days
        # We cannot use -label:SENT in query because that would exclude threads 
        # where we replied in the past but customer replied again.
        query = f"newer_than:{days}d"
        
        # Get threads
        # Note: threads.list provides snippet and ID. We need full details.
        result = await self._request("GET", f"users/me/threads?q={query}&maxResults={limit}")
        threads_meta = result.get("threads", [])
        
        emails = []
        
        for thread_meta in threads_meta:
            try:
                # Fetch full thread details to check messages
                thread_data = await self._request("GET", f"users/me/threads/{thread_meta['id']}?format=full")
                messages = thread_data.get("messages", [])
                
                if not messages:
                    continue
                
                # Check the very last message in the thread
                last_msg = messages[-1]
                label_ids = last_msg.get("labelIds", [])
                
                # If the last message is SENT, it means we replied. Skip this thread.
                if "SENT" in label_ids:
                    continue
                    
                # If we are here, the thread is "awaiting reply"
                # We should import the incoming messages from this thread 
                # that are within the time window.
                
                cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
                
                for msg in messages:
                    # Skip our own sent messages in the import
                    if "SENT" in msg.get("labelIds", []):
                        continue
                        
                    # Parse and check date
                    parsed_email = await self._parse_message(msg)
                    
                    if parsed_email["received_at"] > cutoff_date:
                        emails.append(parsed_email)
                        
            except Exception as e:
                print(f"Error fetching thread {thread_meta['id']}: {e}")
                continue
                
        return emails

    async def fetch_new_emails(
        self,
        since_hours: int = 24,
        limit: int = 50
    ) -> List[Dict]:
        """
        Fetch new emails from Gmail
        
        Args:
            since_hours: Hours to look back for messages
            limit: Maximum number of messages to fetch
        
        Returns:
            List of email dictionaries
        """
        # Build Gmail search query
        if since_hours == 24:
            query = "is:unread OR newer_than:1d"
        else:
            query = f"newer_than:{since_hours}h"
        
        # List messages
        messages_result = await self.list_messages(query=query, max_results=limit)
        message_ids = [msg["id"] for msg in messages_result.get("messages", [])]
        
        emails = []
        for msg_id in message_ids:
            try:
                message = await self.get_message(msg_id, format="full")
                emails.append(await self._parse_message(message))
            except Exception as e:
                print(f"Error parsing message {msg_id}: {e}")
                continue
        
        return emails
    
    async def _parse_message(self, message: Dict) -> Dict:
        """Parse Gmail API message format into our standard format"""
        payload = message.get("payload", {})
        headers = {h["name"].lower(): h["value"] for h in payload.get("headers", [])}
        
        # Extract email address from From header
        from_header = headers.get("from", "")
        sender_email = self._extract_email_address(from_header)
        sender_name = self._extract_name(from_header)
        
        # Get body
        body = self._extract_body(payload)
        
        # Parse date
        date_str = headers.get("date", "")
        try:
            from email.utils import parsedate_to_datetime
            received_at = parsedate_to_datetime(date_str)
        except:
            received_at = datetime.now(timezone.utc)

        # Extract attachments metadata
        attachments = await self._extract_attachments_meta(payload, message.get("id"))
        
        return {
            "channel_message_id": message.get("id"),
            "subject": headers.get("subject", ""),
            "sender_name": sender_name or sender_email.split("@")[0],
            "sender_contact": sender_email,
            "body": body,
            "received_at": received_at,
            "raw_from": from_header,
            "raw_from": from_header,
            "attachments": attachments
        }
        
        # Add fallback body if empty but has attachments
        if not result["body"] and attachments:
             result["body"] = f"[مرفق: {len(attachments)} ملفات]"
             
        return result
    
    def _extract_email_address(self, from_header: str) -> str:
        """Extract email address from From header"""
        import re
        # Match email in <email@domain.com> format or plain email
        match = re.search(r'<(.+?)>|([^\s<>]+@[^\s<>]+)', from_header)
        if match:
            return match.group(1) or match.group(2)
        return from_header.strip()
    
    def _extract_name(self, from_header: str) -> str:
        """Extract name from From header"""
        import re
        match = re.match(r'(.+?)\s*<', from_header)
        if match:
            name = match.group(1).strip().strip('"')
            return name
        return ""
    
    def _extract_body(self, payload: Dict) -> str:
        """Extract body text from Gmail message payload (handles nested multipart)"""
        
        def find_body_recursive(part: Dict) -> tuple[str, str]:
            """
            Recursively search for text content in email parts.
            Returns (plain_text, html_text) tuple.
            """
            plain_text = ""
            html_text = ""
            
            mime_type = part.get("mimeType", "")
            
            # If this part has nested parts, recurse into them
            if part.get("parts"):
                for sub_part in part["parts"]:
                    sub_plain, sub_html = find_body_recursive(sub_part)
                    if sub_plain and not plain_text:
                        plain_text = sub_plain
                    if sub_html and not html_text:
                        html_text = sub_html
            
            # Extract content from this part if it's a text type
            elif mime_type == "text/plain":
                data = part.get("body", {}).get("data")
                if data:
                    try:
                        plain_text = base64.urlsafe_b64decode(data).decode("utf-8", errors="replace")
                    except Exception:
                        pass
                        
            elif mime_type == "text/html":
                data = part.get("body", {}).get("data")
                if data:
                    try:
                        html_text = base64.urlsafe_b64decode(data).decode("utf-8", errors="replace")
                    except Exception:
                        pass
            
            return plain_text, html_text
        
        # Start recursive search from payload
        plain_text, html_text = find_body_recursive(payload)
        
        # Prefer plain text, fall back to HTML with tags stripped
        if plain_text.strip():
            return plain_text.strip()
        elif html_text:
            import re
            # Strip HTML tags
            text = re.sub(r'<[^>]+>', '', html_text)
            # Clean up excessive whitespace
            text = re.sub(r'\s+', ' ', text)
            return text.strip()
        
        return ""

    async def _extract_attachments_meta(self, payload: Dict, message_id: str) -> List[Dict]:
        """Extract attachment metadata and save content"""
        from services.file_storage_service import get_file_storage
        
        attachments = []
        parts = payload.get("parts", [])
        
        # Helper to recurse
        async def scan_parts(parts_list):
            for part in parts_list:
                if part.get("filename") and part.get("body", {}).get("attachmentId"):
                    att_id = part["body"]["attachmentId"]
                    filename = part["filename"]
                    mime_type = part.get("mimeType")
                    size = part["body"].get("size", 0)
                    
                    # Infer type
                    type_ = "document"
                    if mime_type:
                        if mime_type.startswith("image/"): type_ = "image"
                        elif mime_type.startswith("video/"): type_ = "video"
                        elif mime_type.startswith("audio/"): type_ = "audio"
                    
                    att_data = {
                        "file_id": att_id,
                        "file_name": filename,
                        "mime_type": mime_type,
                        "file_size": size,
                        "type": type_
                    }

                    # Download and save if we have a message ID
                    if message_id:
                        try:
                            content = await self.get_attachment_data(message_id, att_id)
                            if content:
                                rel_path, abs_url = get_file_storage().save_file(
                                    content=content,
                                    filename=filename,
                                    mime_type=mime_type
                                )
                                att_data["url"] = abs_url
                                att_data["path"] = rel_path
                                
                                # Optional: Base64 for small images
                                if size < 200 * 1024 and type_ == "image":
                                    att_data["base64"] = base64.b64encode(content).decode('utf-8')
                        except Exception as e:
                            print(f"Error saving attachment {filename}: {e}")

                    attachments.append(att_data)
                
                # Recurse if multipart
                if part.get("parts"):
                    await scan_parts(part["parts"])
        
        await scan_parts(parts)
        return attachments

