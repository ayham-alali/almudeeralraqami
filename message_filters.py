"""
Al-Mudeer - Message Filtering System
Advanced filtering rules for messages before processing
"""

from typing import Dict, List, Optional, Callable
from datetime import datetime
import re
from logging_config import get_logger

logger = get_logger(__name__)


class MessageFilter:
    """Filter messages based on configurable rules"""
    
    def __init__(self):
        self.rules: List[Callable] = []
    
    def add_rule(self, rule_func: Callable):
        """Add a filtering rule"""
        self.rules.append(rule_func)
    
    def should_process(self, message: Dict) -> tuple[bool, Optional[str]]:
        """
        Check if message should be processed.
        
        Returns:
            Tuple of (should_process, reason_if_rejected)
        """
        for rule in self.rules:
            result = rule(message)
            if isinstance(result, tuple):
                should_process, reason = result
                if not should_process:
                    return False, reason
            elif not result:
                return False, "Filtered by rule"
        
        return True, None


# ============ Built-in Filter Rules ============

def filter_spam(message: Dict) -> tuple[bool, Optional[str]]:
    """Filter spam messages"""
    body = message.get("body", "").lower()
    
    # Common spam indicators
    spam_keywords = [
        "click here", "limited time", "act now", "urgent action required",
        "you have won", "congratulations", "free money", "click this link",
        "اضغط هنا", "عرض محدود", "فوز", "جائزة", "مجاني"
    ]
    
    # Check for excessive links
    link_count = len(re.findall(r'http[s]?://', body))
    
    # Check for excessive caps
    caps_ratio = sum(1 for c in body if c.isupper()) / max(len(body), 1)
    
    # Spam score
    spam_score = 0
    if any(keyword in body for keyword in spam_keywords):
        spam_score += 2
    if link_count > 3:
        spam_score += 1
    if caps_ratio > 0.5 and len(body) > 50:
        spam_score += 1
    
    if spam_score >= 3:
        return False, "Spam detected"
    
    return True, None


def filter_empty(message: Dict) -> tuple[bool, Optional[str]]:
    """Filter empty or very short messages"""
    body = message.get("body", "").strip()
    
    if len(body) < 3:
        return False, "Message too short"
    
    # Check if message is only whitespace or special characters
    if not re.search(r'[a-zA-Z\u0600-\u06FF]', body):
        return False, "No meaningful content"
    
    return True, None


def filter_duplicate(message: Dict, recent_messages: List[Dict], time_window_minutes: int = 5) -> tuple[bool, Optional[str]]:
    """Filter duplicate messages from same sender"""
    sender = message.get("sender_contact") or message.get("sender_id")
    body = message.get("body", "").strip()[:100]  # First 100 chars
    
    if not sender:
        return True, None
    
    # Check recent messages
    now = datetime.now()
    for recent in recent_messages:
        if recent.get("sender_contact") == sender or recent.get("sender_id") == sender:
            recent_body = recent.get("body", "").strip()[:100]

            raw_received = recent.get("received_at")
            if isinstance(raw_received, str):
                try:
                    recent_time = datetime.fromisoformat(raw_received)
                except ValueError:
                    recent_time = now
            elif isinstance(raw_received, datetime):
                recent_time = raw_received
            else:
                recent_time = now
            
            # Check if same content and within time window
            if recent_body == body:
                time_diff = (now - recent_time).total_seconds() / 60
                if time_diff < time_window_minutes:
                    return False, "Duplicate message"
    
    return True, None


def filter_blocked_senders(message: Dict, blocked_list: List[str]) -> tuple[bool, Optional[str]]:
    """Filter messages from blocked senders"""
    sender = message.get("sender_contact") or message.get("sender_id", "")
    
    if sender in blocked_list:
        return False, "Sender is blocked"
    
    return True, None

def filter_automated_messages(message: Dict) -> tuple[bool, Optional[str]]:
    """
    Filter automated messages (OTP, Marketing, System Info, Ads, Special Offers, 
    Account Info, Warnings, Newsletters, Transactional).
    
    Returns (True, None) if message is CLEAN (from a real customer).
    Returns (False, Reason) if message should be BLOCKED (automated/marketing).
    """
    body = message.get("body", "").lower()
    sender_contact = (message.get("sender_contact") or "").lower()
    sender_name = (message.get("sender_name") or "").lower()
    subject = (message.get("subject") or "").lower()
    
    # Combine all searchable text
    full_text = f"{body} {subject} {sender_name}"
    
    # ============ SENDER-BASED FILTERING ============
    # Common automated sender patterns (email addresses)
    automated_sender_patterns = [
        r"^noreply@", r"^no-reply@", r"^no\.reply@",
        r"^notifications?@", r"^newsletter@", r"^newsletters@",
        r"^marketing@", r"^promo@", r"^promotions?@",
        r"^ads?@", r"^advertising@", r"^campaign@",
        r"^info@", r"^support@.*noreply", r"^alerts?@",
        r"^security@", r"^account@", r"^billing@",
        r"^mailer-daemon@", r"^postmaster@", r"^bounce@",
        r"^updates?@", r"^news@", r"^digest@",
        r"^subscriptions?@", r"^automated@", r"^system@",
        r"^donotreply@", r"^do-not-reply@", r"^reply-.*@",
        r"@.*\.noreply\.", r"@bounce\.", r"@email\.",
        r"@mail\.", r"@mailer\.", r"@notifications?\.",
        r"@campaign\.", r"@newsletter\.", r"@promo\.",
    ]
    
    for pattern in automated_sender_patterns:
        if re.search(pattern, sender_contact):
            return False, "Automated: Sender pattern detected"
    
    # ============ 1. OTP / VERIFICATION CODES ============
    otp_patterns = [
        r"code\s*is\s*\d+", r"code\s*:\s*\d+",
        r"verification\s*code", r"one-time\s*password",
        r"\botp\b", r"passcode", r"pin\s*code",
        r"رمز\s*التحقق", r"كود\s*التفعيل", r"كلمة\s*المرور\s*المؤقتة",
        r"رمز\s*الدخول", r"كود\s*التأكيد", r"رمز\s*التأكيد",
        r"\b\d{4,6}\b.*code", r"code.*\b\d{4,6}\b",
        r"رقم\s*سري", r"رمز\s*أمان",
    ]
    if any(re.search(p, full_text) for p in otp_patterns):
        return False, "Automated: OTP/Verification"

    # ============ 2. MARKETING / ADS / OFFERS ============
    marketing_keywords = [
        # English
        "unsubscribe", "opt-out", "stop to end", "manage preferences",
        "promotional", "limited time offer", "special offer", "discount",
        "click here", "click below", "exclusive deal", "act now",
        "advertisement", "sponsored", "promoted", "ad:", "[ad]",
        "flash sale", "today only", "sale ends", "hurry",
        "% off", "save now", "deal of the day", "best price",
        "clearance", "buy now", "shop now", "order now",
        "free shipping", "free trial", "free gift", "bonus",
        "coupon", "voucher", "promo code", "discount code",
        "you've been selected", "congratulations", "winner",
        "claim your", "redeem", "expires soon", "last chance",
        # Arabic
        "إلغاء الاشتراك", "أرسل توقف", "عرض خاص", "لفترة محدودة",
        "تخفيضات", "خصم خاص", "اشترك الآن", "عرض حصري",
        "تسوق الآن", "اطلب الآن", "خصم", "عرض اليوم",
        "تنزيلات", "خصم حصري", "أسعار مخفضة", "فرصة لا تعوض",
        "احصل على", "مجاني", "هدية", "جائزة", "فائز", "فوز",
        "كوبون", "قسيمة", "رمز الخصم", "برعاية", "إعلان", "ترويج",
    ]
    if any(k in full_text for k in marketing_keywords):
        return False, "Automated: Marketing/Ad"

    # ============ 3. SYSTEM / INFO / TRANSACTIONAL ============
    info_keywords = [
        # English  
        "do not reply", "auto-generated", "system message",
        "no-reply", "noreply", "automated message", "this is an automated",
        "order confirmation", "shipping update", "delivery update",
        "tracking number", "your order has", "has been shipped",
        "payment received", "payment confirmed", "receipt",
        "invoice", "statement", "transaction", "purchase confirmation",
        # Arabic
        "لا ترد", "رسالة تلقائية", "تمت العملية بنجاح",
        "عزيزي العميل، تم", "تم سحب", "تم إيداع",
        "تأكيد الطلب", "تحديث الشحن", "رقم التتبع",
        "تم شحن", "إيصال", "فاتورة", "كشف حساب",
        "تم الدفع", "تأكيد الدفع", "عملية ناجحة",
    ]
    if any(k in full_text for k in info_keywords):
        return False, "Automated: System/Transactional"

    # ============ 4. ACCOUNT NOTIFICATIONS ============
    account_keywords = [
        # English
        "password reset", "reset your password", "forgot password",
        "account update", "account created", "account activated",
        "login attempt", "new sign-in", "new device", "new login",
        "verify your email", "confirm your email", "email verification",
        "two-factor", "2fa", "mfa", "authenticator",
        "security code", "access code", "account security",
        "profile update", "settings changed", "preferences updated",
        # Arabic
        "تحديث الحساب", "تسجيل دخول جديد", "جهاز جديد",
        "إعادة تعيين كلمة المرور", "استعادة كلمة المرور",
        "تفعيل الحساب", "تأكيد البريد", "التحقق من البريد",
        "رمز الأمان", "رمز الوصول", "أمان الحساب",
        "تم تحديث الملف", "تم تغيير الإعدادات",
    ]
    if any(k in full_text for k in account_keywords):
        return False, "Automated: Account Notification"

    # ============ 5. SECURITY WARNINGS / ALERTS ============
    security_keywords = [
        # English
        "security alert", "security notice", "security warning",
        "suspicious activity", "unusual activity", "unauthorized",
        "breach", "compromised", "hacked", "fraud alert",
        "action required", "immediate action", "urgent action",
        "your account may", "we noticed", "we detected",
        "blocked", "restricted", "suspended", "locked",
        # Arabic
        "تنبيه أمني", "تحذير أمني", "إشعار أمني",
        "نشاط مشبوه", "نشاط غير عادي", "غير مصرح به",
        "اختراق", "تم حظر", "تم تعليق", "تم تقييد",
        "إجراء مطلوب", "إجراء فوري", "إجراء عاجل",
    ]
    if any(k in full_text for k in security_keywords):
        return False, "Automated: Security Alert"

    # ============ 6. NEWSLETTERS / DIGESTS ============
    newsletter_keywords = [
        # English
        "newsletter", "weekly digest", "daily digest", "monthly digest",
        "weekly update", "daily update", "monthly update",
        "news roundup", "news summary", "this week in",
        "top stories", "headlines", "what's new",
        "edition", "issue #", "issue no",
        "curator", "curated", "editorial",
        # Arabic
        "النشرة الإخبارية", "ملخص أسبوعي", "ملخص يومي",
        "تحديث أسبوعي", "تحديث يومي", "أخبار الأسبوع",
        "أهم الأخبار", "عناوين اليوم", "ما الجديد",
    ]
    if any(k in full_text for k in newsletter_keywords):
        return False, "Automated: Newsletter"

    # ============ 7. TERMS / POLICY UPDATES ============
    policy_keywords = [
        # English
        "terms of use", "terms of service", "privacy policy",
        "policy update", "terms update", "legal update",
        "we've updated", "we have updated", "changes to our",
        "updated our terms", "updated our policy", "updated our privacy",
        "service agreement", "user agreement", "license agreement",
        "effective date", "these changes will take effect",
        "by continuing to use", "data protection", "gdpr",
        # Arabic
        "شروط الاستخدام", "سياسة الخصوصية", "تحديث الشروط",
        "تغييرات على", "تم تحديث", "الاتفاقية",
    ]
    if any(k in full_text for k in policy_keywords):
        return False, "Automated: Terms/Policy Update"

    # ============ 8. WELCOME / ONBOARDING EMAILS ============
    welcome_keywords = [
        # English
        "welcome to", "thanks for signing up", "thank you for signing up",
        "thanks for joining", "thank you for joining", "get started",
        "getting started", "welcome aboard", "you're in", "you are in",
        "account is ready", "account has been created",
        "first steps", "next steps", "start using",
        "activate your", "complete your profile", "set up your",
        "explore our", "discover our", "learn how to",
        # Arabic
        "مرحبا بك في", "أهلا بك في", "شكرا للتسجيل",
        "شكرا للانضمام", "ابدأ الآن", "حسابك جاهز",
        "الخطوات الأولى", "أكمل ملفك الشخصي",
    ]
    if any(k in full_text for k in welcome_keywords):
        return False, "Automated: Welcome/Onboarding"

    # ============ 9. CI/CD / DEVOPS NOTIFICATIONS ============
    devops_keywords = [
        # Build notifications
        "build failed", "build succeeded", "build passed", "build completed",
        "deployment failed", "deployment succeeded", "deploy failed",
        "pipeline failed", "pipeline succeeded", "pipeline completed",
        "workflow failed", "workflow succeeded", "workflow completed",
        # Git notifications
        "pull request", "merge request", "commit", "push notification",
        "code review", "branch", "repository",
        # CI/CD platforms
        "github actions", "gitlab ci", "jenkins", "travis ci",
        "circleci", "azure devops", "bitbucket pipelines",
        "railway", "vercel", "netlify", "heroku", "aws codebuild",
        # Server/monitoring
        "server alert", "server down", "server error", "uptime",
        "monitoring alert", "health check", "crash report",
        "cpu usage", "memory usage", "disk space",
        "error rate", "latency alert",
    ]
    if any(k in full_text for k in devops_keywords):
        return False, "Automated: CI/CD/DevOps"

    # ============ 10. SERVICE PROVIDER NOREPLY ============
    # Check for additional service-specific noreply patterns
    service_noreply_patterns = [
        r"googleone-noreply", r"google-noreply", r"@google\.com$",
        r"@notify\.railway\.app", r"@github\.com", r"@gitlab\.com",
        r"@microsoft\.com", r"clarity@microsoft", r"@azure\.com",
        r"@vercel\.com", r"@netlify\.com", r"@heroku\.com",
        r"@dropbox\.com", r"@slack\.com", r"@zoom\.us",
        r"@stripe\.com", r"@paypal\.com", r"@linkedin\.com",
        r"@twitter\.com", r"@x\.com", r"@facebook\.com",
        r"@meta\.com", r"@apple\.com", r"@amazon\.com",
    ]
    for pattern in service_noreply_patterns:
        if re.search(pattern, sender_contact):
            return False, "Automated: Service Provider"

    return True, None


def filter_keywords(message: Dict, keywords: List[str], mode: str = "block") -> tuple[bool, Optional[str]]:
    """
    Filter messages based on keywords.
    
    Args:
        message: Message dict
        keywords: List of keywords to check
        mode: "block" to block messages with keywords, "allow" to only allow messages with keywords
    """
    body = message.get("body", "").lower()
    
    has_keyword = any(keyword.lower() in body for keyword in keywords)
    
    if mode == "block" and has_keyword:
        return False, "Contains blocked keyword"
    
    if mode == "allow" and not has_keyword:
        return False, "Does not contain required keyword"
    
    return True, None


def filter_urgency(message: Dict, min_urgency: str = "normal") -> tuple[bool, Optional[str]]:
    """Filter messages based on urgency level"""
    urgency_levels = {"low": 0, "normal": 1, "high": 2, "urgent": 3}
    
    message_urgency = message.get("urgency", "normal").lower()
    min_level = urgency_levels.get(min_urgency.lower(), 1)
    msg_level = urgency_levels.get(message_urgency, 1)
    
    if msg_level < min_level:
        return False, f"Urgency too low (required: {min_urgency})"
    
    return True, None


# ============ Filter Manager ============

class FilterManager:
    """Manage message filters for a license"""
    
    def __init__(self, license_id: int):
        self.license_id = license_id
        self.filter = MessageFilter()
        self._setup_default_filters()
    
    def _setup_default_filters(self):
        """Setup default filter rules"""
        self.filter.add_rule(filter_spam)
        self.filter.add_rule(filter_empty)
        # Add the new automated message filter by default
        self.filter.add_rule(filter_automated_messages)
    
    def add_custom_rule(self, rule_func: Callable):
        """Add a custom filter rule"""
        self.filter.add_rule(rule_func)
    
    def should_process(self, message: Dict, recent_messages: List[Dict] = None) -> tuple[bool, Optional[str]]:
        """Check if message should be processed"""
        # Add duplicate check if recent messages provided
        if recent_messages:
            duplicate_check = lambda msg: filter_duplicate(msg, recent_messages)
            self.filter.add_rule(duplicate_check)
        
        return self.filter.should_process(message)
    
    def get_blocked_senders(self) -> List[str]:
        """Get list of blocked senders for this license"""
        # This would typically come from database
        # For now, return empty list
        return []
    
    def get_keyword_filters(self) -> Dict:
        """Get keyword filter configuration"""
        # This would typically come from database
        return {
            "blocked_keywords": [],
            "required_keywords": [],
            "mode": "block"
        }


# ============ Integration with Agent ============

async def apply_filters(message: Dict, license_id: int, recent_messages: List[Dict] = None) -> tuple[bool, Optional[str]]:
    """
    Apply all filters to a message.
    
    Args:
        message: Message dictionary
        license_id: License ID for custom filter rules
        recent_messages: List of recent messages for duplicate detection
        
    Returns:
        Tuple of (should_process, reason_if_rejected)
    """
    filter_manager = FilterManager(license_id)
    
    # Load custom filters from database if needed
    # blocked_senders = filter_manager.get_blocked_senders()
    # if blocked_senders:
    #     filter_manager.add_custom_rule(
    #         lambda msg: filter_blocked_senders(msg, blocked_senders)
    #     )
    
    return filter_manager.should_process(message, recent_messages)

