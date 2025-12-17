"""
Al-Mudeer - Integrations Package
Modular integration routes split by channel type
"""

from fastapi import APIRouter

# Create main router for integrations
router = APIRouter(prefix="/api/integrations", tags=["Integrations"])

# Import and include sub-routers
# Note: These are imported and included for backward compatibility
# The monolithic integrations.py file remains as the primary router for now
# Future refactoring will move routes here progressively

# Re-export schemas for convenience
from .schemas import (
    EmailConfigRequest,
    TelegramConfigRequest,
    TelegramPhoneStartRequest,
    TelegramPhoneVerifyRequest,
    ApprovalRequest,
    InboxMessageResponse,
    WorkerStatusResponse,
    IntegrationAccount,
    InboxCustomerResponse,
)

__all__ = [
    "router",
    "EmailConfigRequest",
    "TelegramConfigRequest", 
    "TelegramPhoneStartRequest",
    "TelegramPhoneVerifyRequest",
    "ApprovalRequest",
    "InboxMessageResponse",
    "WorkerStatusResponse",
    "IntegrationAccount",
    "InboxCustomerResponse",
]
