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

