"""
Al-Mudeer Security Configuration
Security headers, CORS, and hardening settings
"""

import os
from typing import List


# ============ CORS Configuration ============

def get_allowed_origins() -> List[str]:
    """
    Get list of allowed CORS origins.
    Configurable via CORS_ORIGINS environment variable.
    """
    # Get from environment (comma-separated)
    env_origins = os.getenv("CORS_ORIGINS", "")
    if env_origins:
        return [o.strip() for o in env_origins.split(",") if o.strip()]
    
    # Get frontend URL
    frontend_url = os.getenv("FRONTEND_URL", "")
    
    # Default origins for development
    default_origins = [
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ]
    
    if frontend_url:
        default_origins.append(frontend_url)
        # Also add without trailing slash
        default_origins.append(frontend_url.rstrip("/"))
    
    return list(set(default_origins))


# ============ Security Headers ============

SECURITY_HEADERS = {
    # Prevent clickjacking
    "X-Frame-Options": "DENY",
    
    # Prevent MIME type sniffing
    "X-Content-Type-Options": "nosniff",
    
    # Enable browser XSS protection
    "X-XSS-Protection": "1; mode=block",
    
    # Referrer policy
    "Referrer-Policy": "strict-origin-when-cross-origin",
    
    # Permissions policy (disable dangerous features)
    "Permissions-Policy": "geolocation=(), microphone=(), camera=()",
}


# Content Security Policy for API responses
CSP_HEADER = (
    "default-src 'self'; "
    "script-src 'self'; "
    "style-src 'self' 'unsafe-inline'; "
    "img-src 'self' data: https:; "
    "font-src 'self' https://fonts.gstatic.com; "
    "connect-src 'self' https://api.telegram.org; "
    "frame-ancestors 'none';"
)


# ============ Cookie Settings ============

COOKIE_SETTINGS = {
    "httponly": True,
    "secure": os.getenv("ENVIRONMENT", "development") == "production",
    "samesite": "lax",
    "max_age": 86400 * 7,  # 7 days
}


# ============ Password Policy ============

PASSWORD_MIN_LENGTH = int(os.getenv("PASSWORD_MIN_LENGTH", "8"))
PASSWORD_REQUIRE_UPPERCASE = os.getenv("PASSWORD_REQUIRE_UPPERCASE", "true").lower() == "true"
PASSWORD_REQUIRE_LOWERCASE = os.getenv("PASSWORD_REQUIRE_LOWERCASE", "true").lower() == "true"
PASSWORD_REQUIRE_DIGITS = os.getenv("PASSWORD_REQUIRE_DIGITS", "true").lower() == "true"
PASSWORD_REQUIRE_SPECIAL = os.getenv("PASSWORD_REQUIRE_SPECIAL", "false").lower() == "true"


def validate_password_strength(password: str) -> tuple:
    """
    Validate password against policy.
    Returns: (is_valid, error_message)
    """
    if len(password) < PASSWORD_MIN_LENGTH:
        return False, f"Password must be at least {PASSWORD_MIN_LENGTH} characters"
    
    if PASSWORD_REQUIRE_UPPERCASE and not any(c.isupper() for c in password):
        return False, "Password must contain at least one uppercase letter"
    
    if PASSWORD_REQUIRE_LOWERCASE and not any(c.islower() for c in password):
        return False, "Password must contain at least one lowercase letter"
    
    if PASSWORD_REQUIRE_DIGITS and not any(c.isdigit() for c in password):
        return False, "Password must contain at least one digit"
    
    if PASSWORD_REQUIRE_SPECIAL and not any(c in "!@#$%^&*(),.?\":{}|<>" for c in password):
        return False, "Password must contain at least one special character"
    
    return True, ""


# ============ API Key Settings ============

API_KEY_LENGTH = 32
ADMIN_KEY = os.getenv("ADMIN_KEY", "admin-secret-key-change-me")


def is_admin_key_valid(key: str) -> bool:
    """Check if provided key matches admin key"""
    # Use constant-time comparison to prevent timing attacks
    import hmac
    return hmac.compare_digest(key, ADMIN_KEY)


# ============ Session Settings ============

SESSION_MAX_AGE = int(os.getenv("SESSION_MAX_AGE", str(86400 * 7)))  # 7 days
SESSION_RENEW_THRESHOLD = int(os.getenv("SESSION_RENEW_THRESHOLD", str(86400)))  # 1 day
