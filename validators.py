"""
Al-Mudeer Input Validation Utilities
Comprehensive validation helpers for API inputs
"""

import re
from typing import Optional, List, Tuple
from pydantic import validator, field_validator
from errors import ValidationError


# ============ Phone Number Validation ============

# Country codes for Arab region
ARAB_COUNTRY_CODES = {
    "+963": "Syria",
    "+966": "Saudi Arabia",
    "+971": "UAE",
    "+962": "Jordan",
    "+961": "Lebanon",
    "+20": "Egypt",
    "+965": "Kuwait",
    "+974": "Qatar",
    "+973": "Bahrain",
    "+968": "Oman",
    "+967": "Yemen",
    "+218": "Libya",
    "+213": "Algeria",
    "+216": "Tunisia",
    "+212": "Morocco",
    "+249": "Sudan",
    "+964": "Iraq",
}


def validate_phone_number(phone: str) -> Tuple[bool, str, Optional[str]]:
    """
    Validate phone number format.
    Returns: (is_valid, cleaned_phone, country_name)
    """
    # Remove spaces and dashes
    cleaned = re.sub(r'[\s\-\(\)]', '', phone)
    
    # Ensure starts with +
    if not cleaned.startswith('+'):
        cleaned = '+' + cleaned
    
    # Check if it's a valid phone format
    if not re.match(r'^\+\d{8,15}$', cleaned):
        return False, phone, None
    
    # Check for Arab country codes
    country = None
    for code, name in ARAB_COUNTRY_CODES.items():
        if cleaned.startswith(code):
            country = name
            break
    
    return True, cleaned, country


def require_valid_phone(phone: str, field_name: str = "phone") -> str:
    """Validate phone and raise ValidationError if invalid"""
    is_valid, cleaned, _ = validate_phone_number(phone)
    if not is_valid:
        raise ValidationError(
            message=f"Invalid phone number format",
            field=field_name,
            message_ar="رقم الهاتف غير صالح"
        )
    return cleaned


# ============ Email Validation ============

EMAIL_REGEX = re.compile(
    r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
)


def validate_email(email: str) -> bool:
    """Validate email format"""
    return bool(EMAIL_REGEX.match(email.strip().lower()))


def require_valid_email(email: str, field_name: str = "email") -> str:
    """Validate email and raise ValidationError if invalid"""
    email = email.strip().lower()
    if not validate_email(email):
        raise ValidationError(
            message=f"Invalid email format",
            field=field_name,
            message_ar="البريد الإلكتروني غير صالح"
        )
    return email


# ============ Text Validation ============

def validate_text_length(
    text: str,
    min_length: int = 0,
    max_length: int = 10000,
    field_name: str = "text"
) -> str:
    """Validate text length"""
    if len(text) < min_length:
        raise ValidationError(
            message=f"{field_name} must be at least {min_length} characters",
            field=field_name,
            message_ar=f"{field_name} يجب أن يكون على الأقل {min_length} حرف"
        )
    if len(text) > max_length:
        raise ValidationError(
            message=f"{field_name} must be at most {max_length} characters",
            field=field_name,
            message_ar=f"{field_name} يجب أن لا يتجاوز {max_length} حرف"
        )
    return text


def sanitize_html(text: str) -> str:
    """Remove HTML tags from text"""
    return re.sub(r'<[^>]+>', '', text)


def sanitize_for_sql(text: str) -> str:
    """Basic SQL injection prevention (use parameterized queries instead!)"""
    dangerous = ["'", '"', ";", "--", "/*", "*/", "xp_", "sp_"]
    result = text
    for char in dangerous:
        result = result.replace(char, "")
    return result


# ============ License Key Validation ============

LICENSE_KEY_REGEX = re.compile(r'^MUDEER-[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{4}$')


def validate_license_key_format(key: str) -> bool:
    """Validate license key format"""
    return bool(LICENSE_KEY_REGEX.match(key.strip().upper()))


def require_valid_license_format(key: str) -> str:
    """Validate license key format and raise ValidationError if invalid"""
    key = key.strip().upper()
    if not validate_license_key_format(key):
        raise ValidationError(
            message="Invalid license key format",
            field="license_key",
            message_ar="صيغة مفتاح الاشتراك غير صحيحة"
        )
    return key


# ============ Pagination Validation ============

def validate_pagination(
    page: int = 1,
    page_size: int = 20,
    max_page_size: int = 100
) -> Tuple[int, int]:
    """Validate and normalize pagination parameters"""
    if page < 1:
        page = 1
    if page_size < 1:
        page_size = 20
    if page_size > max_page_size:
        page_size = max_page_size
    return page, page_size
