"""
Al-Mudeer - Pydantic Schemas
Request and Response models for the API
"""

from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime


# ============ License Key Schemas ============

class LicenseKeyValidation(BaseModel):
    """Request to validate a license key"""
    key: str = Field(..., description="مفتاح الاشتراك", min_length=10)


class LicenseKeyResponse(BaseModel):
    """Response for license key validation"""
    valid: bool
    company_name: Optional[str] = None
    created_at: Optional[str] = None
    expires_at: Optional[str] = None
    requests_remaining: Optional[int] = None
    error: Optional[str] = None


class LicenseKeyCreate(BaseModel):
    """Request to create a new license key (admin only)"""
    company_name: str = Field(..., description="اسم الشركة")
    contact_email: Optional[str] = Field(None, description="البريد الإلكتروني")
    days_valid: int = Field(365, description="مدة الصلاحية بالأيام")
    max_requests_per_day: int = Field(100, description="الحد الأقصى للطلبات اليومية")


# ============ Message Processing Schemas ============

class MessageInput(BaseModel):
    """Input for message processing"""
    message: str = Field(..., description="نص الرسالة", min_length=10)
    message_type: Optional[str] = Field(None, description="نوع الرسالة: email, whatsapp, general")
    sender_name: Optional[str] = Field(None, description="اسم المرسل")
    sender_contact: Optional[str] = Field(None, description="بيانات التواصل")


class AnalysisResult(BaseModel):
    """Result of message analysis"""
    intent: str = Field(..., description="النية: استفسار، طلب خدمة، شكوى، متابعة، عرض، أخرى")
    urgency: str = Field(..., description="الأهمية: عاجل، عادي، منخفض")
    sentiment: str = Field(..., description="المشاعر: إيجابي، محايد، سلبي")
    sender_name: Optional[str] = None
    sender_contact: Optional[str] = None
    key_points: List[str] = Field(default_factory=list)
    action_items: List[str] = Field(default_factory=list)
    extracted_entities: dict = Field(default_factory=dict)
    summary: str = ""
    draft_response: str = ""
    suggested_actions: List[str] = Field(default_factory=list)
    message_type: str = "general"


class ProcessingResponse(BaseModel):
    """Response for message processing"""
    success: bool
    data: Optional[AnalysisResult] = None
    error: Optional[str] = None


# ============ CRM Schemas ============

class CRMEntryCreate(BaseModel):
    """Request to save a CRM entry"""
    sender_name: Optional[str] = Field(None, description="اسم المرسل")
    sender_contact: Optional[str] = Field(None, description="بيانات التواصل")
    message_type: str = Field("general", description="نوع الرسالة")
    intent: str = Field(..., description="النية")
    extracted_data: str = Field("", description="البيانات المستخرجة")
    original_message: str = Field(..., description="الرسالة الأصلية")
    draft_response: str = Field("", description="الرد المقترح")


class CRMEntry(BaseModel):
    """CRM entry response"""
    id: int
    sender_name: Optional[str]
    sender_contact: Optional[str]
    message_type: str
    intent: str
    extracted_data: str
    original_message: str
    draft_response: str
    status: str
    created_at: str
    updated_at: Optional[str]


class CRMListResponse(BaseModel):
    """Response for CRM entries list"""
    entries: List[CRMEntry]
    total: int


# ============ Health Check ============

class HealthCheck(BaseModel):
    """Health check response"""
    status: str = Field("healthy", description="Service status: healthy, degraded, unhealthy")
    timestamp: Optional[float] = Field(None, description="Unix timestamp of health check")
    database: str = Field("connected", description="Database connection status")
    cache: str = Field("available", description="Cache availability status")
    version: str = Field("1.0.0", description="API version")
    service: str = Field("Al-Mudeer API", description="Service name")
