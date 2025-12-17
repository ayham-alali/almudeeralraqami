"""
Al-Mudeer API Documentation Utilities
OpenAPI schema enhancements and common response models
"""

from typing import Any, Dict, Generic, List, Optional, TypeVar
from pydantic import BaseModel, Field


T = TypeVar("T")


# ============ Generic Response Models ============

class SuccessResponse(BaseModel):
    """Standard success response"""
    success: bool = True
    message: str = Field(..., description="Success message")
    message_ar: Optional[str] = Field(None, description="Arabic message")


class ErrorResponse(BaseModel):
    """Standard error response"""
    error: bool = True
    error_code: str = Field(..., description="Machine-readable error code")
    message: str = Field(..., description="Error message")
    message_ar: str = Field(..., description="Arabic error message")
    details: Dict[str, Any] = Field(default_factory=dict, description="Additional error details")


class PaginatedResponse(BaseModel, Generic[T]):
    """Paginated list response"""
    items: List[T]
    total: int = Field(..., description="Total number of items")
    page: int = Field(..., description="Current page (1-indexed)")
    page_size: int = Field(..., description="Items per page")
    has_more: bool = Field(..., description="Whether there are more pages")


class DataResponse(BaseModel, Generic[T]):
    """Generic data wrapper response"""
    success: bool = True
    data: T


# ============ Common Field Descriptions ============

LICENSE_KEY_HEADER = {
    "description": "License key for authentication",
    "example": "MUDEER-XXXX-XXXX-XXXX",
}

ADMIN_KEY_HEADER = {
    "description": "Admin key for privileged operations",
    "example": "admin-secret-key",
}


# ============ OpenAPI Tags ============

OPENAPI_TAGS = [
    {
        "name": "Authentication",
        "description": "License key validation and authentication",
    },
    {
        "name": "Integrations",
        "description": "Email, Telegram, and WhatsApp channel integrations",
    },
    {
        "name": "Inbox",
        "description": "Unified inbox for all channels",
    },
    {
        "name": "Customers",
        "description": "Customer profiles and CRM functionality",
    },
    {
        "name": "Analytics",
        "description": "Business analytics and reporting",
    },
    {
        "name": "Team",
        "description": "Team management and permissions",
    },
    {
        "name": "Notifications",
        "description": "Smart notifications and alerts",
    },
    {
        "name": "Admin",
        "description": "Administrative operations (requires admin key)",
    },
    {
        "name": "Health",
        "description": "Health check and monitoring endpoints",
    },
]


# ============ Example Responses ============

EXAMPLE_RESPONSES = {
    401: {
        "description": "Authentication required",
        "content": {
            "application/json": {
                "example": {
                    "error": True,
                    "error_code": "AUTH_REQUIRED",
                    "message": "Invalid or missing license key",
                    "message_ar": "مفتاح الاشتراك غير صالح أو مفقود",
                    "details": {},
                }
            }
        },
    },
    403: {
        "description": "Permission denied",
        "content": {
            "application/json": {
                "example": {
                    "error": True,
                    "error_code": "FORBIDDEN",
                    "message": "You don't have permission to access this resource",
                    "message_ar": "ليس لديك صلاحية للوصول",
                    "details": {},
                }
            }
        },
    },
    404: {
        "description": "Resource not found",
        "content": {
            "application/json": {
                "example": {
                    "error": True,
                    "error_code": "NOT_FOUND",
                    "message": "Resource not found",
                    "message_ar": "لم يتم العثور على المورد",
                    "details": {"resource": "Customer", "id": "123"},
                }
            }
        },
    },
    422: {
        "description": "Validation error",
        "content": {
            "application/json": {
                "example": {
                    "error": True,
                    "error_code": "VALIDATION_ERROR",
                    "message": "Invalid input data",
                    "message_ar": "بيانات غير صالحة",
                    "details": {"field": "email"},
                }
            }
        },
    },
    429: {
        "description": "Rate limit exceeded",
        "content": {
            "application/json": {
                "example": {
                    "error": True,
                    "error_code": "RATE_LIMIT_EXCEEDED",
                    "message": "Rate limit exceeded. Retry after 60 seconds",
                    "message_ar": "تم تجاوز الحد المسموح",
                    "details": {"retry_after": 60},
                }
            }
        },
    },
    500: {
        "description": "Internal server error",
        "content": {
            "application/json": {
                "example": {
                    "error": True,
                    "error_code": "INTERNAL_ERROR",
                    "message": "An unexpected error occurred",
                    "message_ar": "حدث خطأ غير متوقع",
                    "details": {"error_id": 12345},
                }
            }
        },
    },
}
