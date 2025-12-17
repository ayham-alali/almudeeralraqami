"""
Al-Mudeer Constants
Application-wide constants and configuration values
"""

# ============ Application Info ============

APP_NAME = "Al-Mudeer"
APP_NAME_AR = "المدير"
APP_VERSION = "1.0.0"
APP_DESCRIPTION = "B2B AI-Powered Customer Communication Platform"
APP_DESCRIPTION_AR = "منصة ذكاء اصطناعي لإدارة التواصل مع العملاء"


# ============ Channel Types ============

class Channels:
    EMAIL = "email"
    TELEGRAM_BOT = "telegram_bot"
    TELEGRAM_PHONE = "telegram_phone"
    WHATSAPP = "whatsapp"
    
    ALL = [EMAIL, TELEGRAM_BOT, TELEGRAM_PHONE, WHATSAPP]
    
    DISPLAY_NAMES = {
        EMAIL: "البريد الإلكتروني",
        TELEGRAM_BOT: "روبوت تيليجرام",
        TELEGRAM_PHONE: "رقم تيليجرام",
        WHATSAPP: "واتساب",
    }


# ============ Message Status ============

class MessageStatus:
    NEW = "new"
    ANALYZED = "analyzed"
    DRAFT_READY = "draft_ready"
    APPROVED = "approved"
    SENT = "sent"
    FAILED = "failed"
    ARCHIVED = "archived"
    
    ALL = [NEW, ANALYZED, DRAFT_READY, APPROVED, SENT, FAILED, ARCHIVED]
    
    DISPLAY_NAMES = {
        NEW: "جديد",
        ANALYZED: "تم التحليل",
        DRAFT_READY: "مسودة جاهزة",
        APPROVED: "تمت الموافقة",
        SENT: "تم الإرسال",
        FAILED: "فشل",
        ARCHIVED: "مؤرشف",
    }


# ============ Intent Types ============

class IntentTypes:
    INQUIRY = "inquiry"
    ORDER = "order"
    COMPLAINT = "complaint"
    SUPPORT = "support"
    FEEDBACK = "feedback"
    GENERAL = "general"
    
    ALL = [INQUIRY, ORDER, COMPLAINT, SUPPORT, FEEDBACK, GENERAL]
    
    DISPLAY_NAMES = {
        INQUIRY: "استفسار",
        ORDER: "طلب",
        COMPLAINT: "شكوى",
        SUPPORT: "دعم فني",
        FEEDBACK: "ملاحظات",
        GENERAL: "عام",
    }


# ============ Urgency Levels ============

class UrgencyLevels:
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"
    
    ALL = [LOW, MEDIUM, HIGH, CRITICAL]
    
    DISPLAY_NAMES = {
        LOW: "منخفضة",
        MEDIUM: "متوسطة",
        HIGH: "عالية",
        CRITICAL: "عاجلة جداً",
    }


# ============ Sentiment Values ============

class Sentiments:
    POSITIVE = "positive"
    NEUTRAL = "neutral"
    NEGATIVE = "negative"
    
    ALL = [POSITIVE, NEUTRAL, NEGATIVE]
    
    DISPLAY_NAMES = {
        POSITIVE: "إيجابي",
        NEUTRAL: "محايد",
        NEGATIVE: "سلبي",
    }


# ============ Team Roles ============

class TeamRoles:
    OWNER = "owner"
    ADMIN = "admin"
    MEMBER = "member"
    VIEWER = "viewer"
    
    ALL = [OWNER, ADMIN, MEMBER, VIEWER]
    
    DISPLAY_NAMES = {
        OWNER: "المالك",
        ADMIN: "مدير",
        MEMBER: "عضو",
        VIEWER: "مشاهد",
    }


# ============ Subscription Tiers ============

class SubscriptionTiers:
    TRIAL = "trial"
    BASIC = "basic"
    PROFESSIONAL = "professional"
    ENTERPRISE = "enterprise"
    
    ALL = [TRIAL, BASIC, PROFESSIONAL, ENTERPRISE]
    
    LIMITS = {
        TRIAL: {"requests_per_day": 50, "channels": 1, "team_members": 1},
        BASIC: {"requests_per_day": 200, "channels": 2, "team_members": 3},
        PROFESSIONAL: {"requests_per_day": 1000, "channels": 4, "team_members": 10},
        ENTERPRISE: {"requests_per_day": -1, "channels": -1, "team_members": -1},  # Unlimited
    }


# ============ Time Constants ============

SECONDS_PER_MINUTE = 60
SECONDS_PER_HOUR = 3600
SECONDS_PER_DAY = 86400
SECONDS_PER_WEEK = 604800


# ============ Default Values ============

DEFAULT_PAGE_SIZE = 20
MAX_PAGE_SIZE = 100
DEFAULT_CACHE_TTL = 300  # 5 minutes
DEFAULT_SESSION_TTL = 86400 * 7  # 7 days


# ============ Error Codes ============

class ErrorCodes:
    VALIDATION_ERROR = "VALIDATION_ERROR"
    AUTH_REQUIRED = "AUTH_REQUIRED"
    FORBIDDEN = "FORBIDDEN"
    NOT_FOUND = "NOT_FOUND"
    RATE_LIMIT_EXCEEDED = "RATE_LIMIT_EXCEEDED"
    EXTERNAL_SERVICE_ERROR = "EXTERNAL_SERVICE_ERROR"
    DATABASE_ERROR = "DATABASE_ERROR"
    INTERNAL_ERROR = "INTERNAL_ERROR"
