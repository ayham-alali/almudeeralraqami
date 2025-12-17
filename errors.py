"""
Al-Mudeer Error Handling Module
Structured error responses for API consistency
"""

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse
from typing import Optional, Dict, Any
import traceback
import logging

logger = logging.getLogger(__name__)


class APIError(Exception):
    """Base API Error with structured response"""
    
    def __init__(
        self,
        message: str,
        error_code: str = "UNKNOWN_ERROR",
        status_code: int = 500,
        details: Optional[Dict[str, Any]] = None,
        message_ar: Optional[str] = None,
    ):
        self.message = message
        self.message_ar = message_ar or message
        self.error_code = error_code
        self.status_code = status_code
        self.details = details or {}
        super().__init__(self.message)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "error": True,
            "error_code": self.error_code,
            "message": self.message,
            "message_ar": self.message_ar,
            "details": self.details,
        }


# Common API Errors
class ValidationError(APIError):
    """Input validation failed"""
    def __init__(self, message: str, field: Optional[str] = None, message_ar: Optional[str] = None):
        super().__init__(
            message=message,
            message_ar=message_ar or "بيانات غير صالحة",
            error_code="VALIDATION_ERROR",
            status_code=422,
            details={"field": field} if field else {},
        )


class NotFoundError(APIError):
    """Resource not found"""
    def __init__(self, resource: str, resource_id: Optional[str] = None):
        super().__init__(
            message=f"{resource} not found",
            message_ar=f"لم يتم العثور على {resource}",
            error_code="NOT_FOUND",
            status_code=404,
            details={"resource": resource, "id": resource_id},
        )


class AuthenticationError(APIError):
    """Authentication failed"""
    def __init__(self, message: str = "Authentication required", message_ar: Optional[str] = None):
        super().__init__(
            message=message,
            message_ar=message_ar or "يرجى تسجيل الدخول",
            error_code="AUTH_REQUIRED",
            status_code=401,
        )


class AuthorizationError(APIError):
    """Permission denied"""
    def __init__(self, message: str = "Permission denied", message_ar: Optional[str] = None):
        super().__init__(
            message=message,
            message_ar=message_ar or "ليس لديك صلاحية للوصول",
            error_code="FORBIDDEN",
            status_code=403,
        )


class RateLimitError(APIError):
    """Rate limit exceeded"""
    def __init__(self, retry_after: int = 60):
        super().__init__(
            message=f"Rate limit exceeded. Retry after {retry_after} seconds",
            message_ar=f"تم تجاوز الحد المسموح. حاول مرة أخرى بعد {retry_after} ثانية",
            error_code="RATE_LIMIT_EXCEEDED",
            status_code=429,
            details={"retry_after": retry_after},
        )


class ExternalServiceError(APIError):
    """External service (Telegram, Gmail, etc.) error"""
    def __init__(self, service: str, message: str, message_ar: Optional[str] = None):
        super().__init__(
            message=f"{service} error: {message}",
            message_ar=message_ar or f"خطأ في خدمة {service}",
            error_code="EXTERNAL_SERVICE_ERROR",
            status_code=502,
            details={"service": service},
        )


class DatabaseError(APIError):
    """Database operation failed"""
    def __init__(self, operation: str = "query"):
        super().__init__(
            message=f"Database {operation} failed",
            message_ar="حدث خطأ في قاعدة البيانات",
            error_code="DATABASE_ERROR",
            status_code=500,
            details={"operation": operation},
        )


# Exception handlers for FastAPI
async def api_error_handler(request: Request, exc: APIError) -> JSONResponse:
    """Handle APIError exceptions"""
    logger.warning(f"API Error: {exc.error_code} - {exc.message}")
    return JSONResponse(
        status_code=exc.status_code,
        content=exc.to_dict(),
    )


async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    """Handle FastAPI HTTPException"""
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": True,
            "error_code": "HTTP_ERROR",
            "message": exc.detail,
            "message_ar": exc.detail,
            "details": {},
        },
    )


async def generic_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Handle unexpected exceptions"""
    error_id = id(exc)
    logger.exception(f"Unhandled exception [{error_id}]: {exc}")
    
    return JSONResponse(
        status_code=500,
        content={
            "error": True,
            "error_code": "INTERNAL_ERROR",
            "message": "An unexpected error occurred",
            "message_ar": "حدث خطأ غير متوقع",
            "details": {"error_id": error_id},
        },
    )


def register_error_handlers(app):
    """Register all error handlers with the FastAPI app"""
    app.add_exception_handler(APIError, api_error_handler)
    app.add_exception_handler(HTTPException, http_exception_handler)
    # Only enable generic handler in production
    # app.add_exception_handler(Exception, generic_exception_handler)
