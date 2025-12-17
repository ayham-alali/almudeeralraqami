"""
Security utilities for Al-Mudeer
Input sanitization and security helpers
"""

import html
import re
from typing import Optional


def sanitize_string(text: str, max_length: Optional[int] = None) -> str:
    """
    Sanitize user input string to prevent XSS attacks.
    
    Args:
        text: Input string to sanitize
        max_length: Optional maximum length to truncate
        
    Returns:
        Sanitized string
    """
    if not text:
        return ""
    
    # Remove null bytes
    text = text.replace('\x00', '')
    
    # HTML escape to prevent XSS
    text = html.escape(text)
    
    # Truncate if max_length specified
    if max_length and len(text) > max_length:
        text = text[:max_length]
    
    return text.strip()


def sanitize_email(email: str) -> Optional[str]:
    """
    Sanitize and validate email address.
    
    Args:
        email: Email string to sanitize
        
    Returns:
        Sanitized email or None if invalid
    """
    if not email:
        return None
    
    # Basic email validation pattern
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    
    # Sanitize
    email = email.strip().lower()
    
    # Validate format
    if not re.match(email_pattern, email):
        return None
    
    # Additional length check
    if len(email) > 254:  # RFC 5321 limit
        return None
    
    return email


def sanitize_phone(phone: str) -> Optional[str]:
    """
    Sanitize phone number (basic cleaning).
    
    Args:
        phone: Phone number string
        
    Returns:
        Sanitized phone number or None
    """
    if not phone:
        return None
    
    # Remove all non-digit characters except + at start
    phone = phone.strip()
    if phone.startswith('+'):
        cleaned = '+' + re.sub(r'\D', '', phone[1:])
    else:
        cleaned = re.sub(r'\D', '', phone)
    
    # Basic length validation (5-15 digits is reasonable)
    if len(cleaned.replace('+', '')) < 5 or len(cleaned.replace('+', '')) > 15:
        return None
    
    return cleaned


def sanitize_message(message: str, max_length: int = 10000) -> str:
    """
    Sanitize message content (allows more characters than basic string).
    
    Args:
        message: Message text to sanitize
        max_length: Maximum allowed length
        
    Returns:
        Sanitized message
    """
    if not message:
        return ""
    
    # Remove null bytes and control characters (except newlines and tabs)
    message = re.sub(r'[\x00-\x08\x0B-\x0C\x0E-\x1F]', '', message)
    
    # HTML escape
    message = html.escape(message)
    
    # Truncate if too long
    if len(message) > max_length:
        message = message[:max_length]
    
    return message.strip()

