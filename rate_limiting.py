"""
Al-Mudeer Rate Limiting Configuration
Configurable rate limits for different endpoint types
"""

import os
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from fastapi import Request
from fastapi.responses import JSONResponse


def get_license_key_or_ip(request: Request) -> str:
    """
    Get rate limit key from license key (preferred) or client IP.
    This allows licensed users to have separate rate limits.
    """
    license_key = request.headers.get("X-License-Key", "")
    if license_key:
        # Use first 20 chars of license key as identifier
        return f"license:{license_key[:20]}"
    return f"ip:{get_remote_address(request)}"


# Create limiter instance
limiter = Limiter(key_func=get_license_key_or_ip)


# ============ Rate Limit Configurations ============

class RateLimits:
    """
    Configurable rate limits for different endpoint types.
    Values can be overridden via environment variables.
    """
    
    # Authentication endpoints (stricter to prevent brute force)
    AUTH = os.getenv("RATE_LIMIT_AUTH", "5/minute")
    
    # General API endpoints
    API = os.getenv("RATE_LIMIT_API", "60/minute")
    
    # AI processing endpoints (expensive operations)
    AI_PROCESS = os.getenv("RATE_LIMIT_AI", "10/minute")
    
    # Message sending endpoints
    SEND_MESSAGE = os.getenv("RATE_LIMIT_SEND", "30/minute")
    
    # Data export endpoints
    EXPORT = os.getenv("RATE_LIMIT_EXPORT", "5/minute")
    
    # Admin endpoints
    ADMIN = os.getenv("RATE_LIMIT_ADMIN", "30/minute")
    
    # Webhook endpoints (higher limits for external services)
    WEBHOOK = os.getenv("RATE_LIMIT_WEBHOOK", "100/minute")


# ============ Rate Limit Error Handler ============

async def rate_limit_handler(request: Request, exc: RateLimitExceeded) -> JSONResponse:
    """
    Custom rate limit exceeded handler with Arabic message.
    """
    # Extract retry-after from the exception if available
    retry_after = getattr(exc, 'retry_after', 60)
    
    return JSONResponse(
        status_code=429,
        content={
            "error": True,
            "error_code": "RATE_LIMIT_EXCEEDED",
            "message": f"Rate limit exceeded. Retry after {retry_after} seconds",
            "message_ar": f"تم تجاوز الحد المسموح. حاول مرة أخرى بعد {retry_after} ثانية",
            "details": {
                "retry_after": retry_after,
                "limit": str(exc.detail) if hasattr(exc, 'detail') else None,
            },
        },
        headers={"Retry-After": str(retry_after)},
    )


def setup_rate_limiting(app):
    """
    Configure rate limiting for the FastAPI app.
    
    Usage in routes:
        from rate_limiting import limiter, RateLimits
        
        @router.post("/api/analyze")
        @limiter.limit(RateLimits.AI_PROCESS)
        async def analyze_message(request: Request, ...):
            ...
    """
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, rate_limit_handler)


# ============ Decorator Helpers ============

def limit_auth(func):
    """Decorator for authentication endpoints"""
    return limiter.limit(RateLimits.AUTH)(func)


def limit_api(func):
    """Decorator for general API endpoints"""
    return limiter.limit(RateLimits.API)(func)


def limit_ai(func):
    """Decorator for AI processing endpoints"""
    return limiter.limit(RateLimits.AI_PROCESS)(func)


def limit_send(func):
    """Decorator for message sending endpoints"""
    return limiter.limit(RateLimits.SEND_MESSAGE)(func)


def limit_export(func):
    """Decorator for data export endpoints"""
    return limiter.limit(RateLimits.EXPORT)(func)


def limit_admin(func):
    """Decorator for admin endpoints"""
    return limiter.limit(RateLimits.ADMIN)(func)
