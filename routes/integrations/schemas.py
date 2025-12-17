"""
Al-Mudeer - Integration Schemas
Shared Pydantic models for integration routes
"""

from pydantic import BaseModel, Field
from typing import Optional


class EmailConfigRequest(BaseModel):
    provider: str = Field(..., description="gmail (OAuth 2.0 only)")
    email_address: str  # Will be set from OAuth token
    auto_reply_enabled: bool = False
    check_interval_minutes: int = 5


class TelegramConfigRequest(BaseModel):
    bot_token: str
    auto_reply_enabled: bool = False


class TelegramPhoneStartRequest(BaseModel):
    phone_number: str


class TelegramPhoneVerifyRequest(BaseModel):
    phone_number: str
    code: str
    session_id: Optional[str] = None
    password: Optional[str] = None  # 2FA password


class ApprovalRequest(BaseModel):
    action: str = Field(..., description="approve, reject, edit")
    edited_body: Optional[str] = None


class InboxMessageResponse(BaseModel):
    id: int
    channel: str
    sender_name: Optional[str]
    sender_contact: Optional[str]
    subject: Optional[str]
    body: str
    received_at: Optional[str]
    intent: Optional[str]
    urgency: Optional[str]
    sentiment: Optional[str]
    ai_summary: Optional[str]
    ai_draft_response: Optional[str]
    status: str
    created_at: str


class WorkerStatusResponse(BaseModel):
    email_polling: dict
    telegram_polling: dict


class IntegrationAccount(BaseModel):
    id: str
    channel_type: str
    display_name: str
    is_active: bool
    details: Optional[str] = None


class InboxCustomerResponse(BaseModel):
    customer: Optional[dict]
