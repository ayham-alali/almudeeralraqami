"""
Al-Mudeer Message Filter Tests
Unit tests for message filtering, especially automated email detection
"""

import pytest
from message_filters import (
    filter_automated_messages,
    filter_spam,
    filter_empty,
    apply_filters,
    FilterManager
)


class TestFilterAutomatedMessages:
    """Tests for the enhanced filter_automated_messages function"""

    # ============ SENDER-BASED FILTERING ============
    
    def test_blocks_noreply_sender(self):
        """Test that noreply@ senders are blocked"""
        message = {
            "body": "Hello, just checking in.",
            "sender_contact": "noreply@company.com",
            "sender_name": "Company",
            "subject": "Hello"
        }
        should_process, reason = filter_automated_messages(message)
        assert should_process is False
        assert "Sender pattern" in reason

    def test_blocks_newsletter_sender(self):
        """Test that newsletter@ senders are blocked"""
        message = {
            "body": "Some content here",
            "sender_contact": "newsletter@example.com",
            "sender_name": "Example",
            "subject": "Updates"
        }
        should_process, reason = filter_automated_messages(message)
        assert should_process is False
        assert "Sender pattern" in reason

    def test_blocks_marketing_sender(self):
        """Test that marketing@ senders are blocked"""
        message = {
            "body": "Content",
            "sender_contact": "marketing@store.com",
            "sender_name": "Store",
            "subject": "Hello"
        }
        should_process, reason = filter_automated_messages(message)
        assert should_process is False
        assert "Sender pattern" in reason

    def test_blocks_notifications_sender(self):
        """Test that notifications@ senders are blocked"""
        message = {
            "body": "You have new activity",
            "sender_contact": "notifications@app.com",
            "sender_name": "App",
            "subject": "Activity"
        }
        should_process, reason = filter_automated_messages(message)
        assert should_process is False
        assert "Sender pattern" in reason

    # ============ OTP / VERIFICATION CODES ============
    
    def test_blocks_otp_message(self):
        """Test that OTP messages are blocked"""
        message = {
            "body": "Your verification code is 123456",
            "sender_contact": "random@bank.com",
            "sender_name": "Bank",
            "subject": "Security Code"
        }
        should_process, reason = filter_automated_messages(message)
        assert should_process is False
        assert "OTP" in reason

    def test_blocks_arabic_otp(self):
        """Test that Arabic OTP messages are blocked"""
        message = {
            "body": "رمز التحقق الخاص بك هو 5678",
            "sender_contact": "info@service.com",
            "sender_name": "Service",
            "subject": "كود التفعيل"
        }
        should_process, reason = filter_automated_messages(message)
        assert should_process is False
        assert "OTP" in reason

    # ============ MARKETING / ADS / OFFERS ============
    
    def test_blocks_unsubscribe_message(self):
        """Test that messages with unsubscribe are blocked"""
        message = {
            "body": "Great deals! Click here to unsubscribe if not interested.",
            "sender_contact": "deals@shop.com",
            "sender_name": "Shop",
            "subject": "Special Deals"
        }
        should_process, reason = filter_automated_messages(message)
        assert should_process is False
        assert "Marketing" in reason

    def test_blocks_discount_message(self):
        """Test that discount/sale messages are blocked"""
        message = {
            "body": "Flash sale! 50% off everything today only!",
            "sender_contact": "sales@store.com",
            "sender_name": "Store",
            "subject": "Sale"
        }
        should_process, reason = filter_automated_messages(message)
        assert should_process is False
        assert "Marketing" in reason

    def test_blocks_arabic_marketing(self):
        """Test that Arabic marketing messages are blocked"""
        message = {
            "body": "عرض خاص لفترة محدودة! احصل على خصم حصري",
            "sender_contact": "offers@shop.sa",
            "sender_name": "متجر",
            "subject": "عرض اليوم"
        }
        should_process, reason = filter_automated_messages(message)
        assert should_process is False
        assert "Marketing" in reason

    # ============ SYSTEM / TRANSACTIONAL ============
    
    def test_blocks_order_confirmation(self):
        """Test that order confirmations are blocked"""
        message = {
            "body": "Your order has been confirmed. Tracking number: 12345",
            "sender_contact": "orders@shop.com",
            "sender_name": "Shop",
            "subject": "Order Confirmation"
        }
        should_process, reason = filter_automated_messages(message)
        assert should_process is False
        assert "Transactional" in reason

    def test_blocks_do_not_reply(self):
        """Test that do-not-reply messages are blocked"""
        message = {
            "body": "This is an automated message. Please do not reply.",
            "sender_contact": "system@company.com",
            "sender_name": "Company",
            "subject": "Notification"
        }
        should_process, reason = filter_automated_messages(message)
        assert should_process is False
        assert "Transactional" in reason

    # ============ ACCOUNT NOTIFICATIONS ============
    
    def test_blocks_password_reset(self):
        """Test that password reset emails are blocked"""
        message = {
            "body": "Click here to reset your password.",
            "sender_contact": "accounts@service.com",
            "sender_name": "Service",
            "subject": "Password Reset"
        }
        should_process, reason = filter_automated_messages(message)
        assert should_process is False
        assert "Account" in reason

    def test_blocks_new_login(self):
        """Test that new login notifications are blocked"""
        message = {
            "body": "We detected a new login to your account from a new device.",
            "sender_contact": "security@app.com",
            "sender_name": "App",
            "subject": "New Login Detected"
        }
        should_process, reason = filter_automated_messages(message)
        assert should_process is False
        assert "Account" in reason or "Security" in reason

    # ============ SECURITY WARNINGS ============
    
    def test_blocks_security_alert(self):
        """Test that security alerts are blocked"""
        message = {
            "body": "Security alert: We detected suspicious activity on your account.",
            "sender_contact": "alerts@bank.com",
            "sender_name": "Bank",
            "subject": "Security Alert"
        }
        should_process, reason = filter_automated_messages(message)
        assert should_process is False
        assert "Security" in reason

    def test_blocks_fraud_alert(self):
        """Test that fraud alerts are blocked"""
        message = {
            "body": "Fraud alert: Unauthorized access attempt blocked.",
            "sender_contact": "security@provider.com",
            "sender_name": "Provider",
            "subject": "Fraud Alert"
        }
        should_process, reason = filter_automated_messages(message)
        assert should_process is False
        assert "Security" in reason

    # ============ NEWSLETTERS ============
    
    def test_blocks_newsletter(self):
        """Test that newsletters are blocked"""
        message = {
            "body": "This week's newsletter: Top stories and news roundup",
            "sender_contact": "news@publication.com",
            "sender_name": "Publication",
            "subject": "Weekly Newsletter"
        }
        should_process, reason = filter_automated_messages(message)
        assert should_process is False
        assert "Newsletter" in reason

    def test_blocks_digest(self):
        """Test that digest emails are blocked"""
        message = {
            "body": "Your weekly digest: Here's what you missed",
            "sender_contact": "updates@platform.com",
            "sender_name": "Platform",
            "subject": "Weekly Digest"
        }
        should_process, reason = filter_automated_messages(message)
        assert should_process is False
        assert "Newsletter" in reason

    # ============ LEGITIMATE MESSAGES (SHOULD PASS) ============
    
    def test_allows_customer_inquiry(self):
        """Test that customer inquiries are allowed"""
        message = {
            "body": "مرحباً، أريد الاستفسار عن منتجاتكم وأسعارها",
            "sender_contact": "customer@gmail.com",
            "sender_name": "أحمد محمد",
            "subject": "استفسار"
        }
        should_process, reason = filter_automated_messages(message)
        assert should_process is True
        assert reason is None

    def test_allows_support_request(self):
        """Test that support requests are allowed"""
        message = {
            "body": "Hi, I'm having an issue with my order and need assistance.",
            "sender_contact": "john.doe@gmail.com",
            "sender_name": "John Doe",
            "subject": "Need Help"
        }
        should_process, reason = filter_automated_messages(message)
        assert should_process is True
        assert reason is None

    def test_allows_business_inquiry(self):
        """Test that business inquiries are allowed"""
        message = {
            "body": "I'm interested in your services for our company. Can we schedule a call?",
            "sender_contact": "manager@business.com",
            "sender_name": "Jane Smith",
            "subject": "Business Inquiry"
        }
        should_process, reason = filter_automated_messages(message)
        assert should_process is True
        assert reason is None


class TestFilterSpam:
    """Tests for spam filtering"""

    def test_blocks_spam_with_multiple_links(self):
        """Test that messages with many links are blocked"""
        message = {
            "body": "Click http://spam1.com http://spam2.com http://spam3.com http://spam4.com"
        }
        should_process, reason = filter_spam(message)
        assert should_process is False
        assert "Spam" in reason

    def test_allows_normal_message(self):
        """Test that normal messages pass spam filter"""
        message = {
            "body": "Hello, I have a question about your services."
        }
        should_process, reason = filter_spam(message)
        assert should_process is True


class TestFilterEmpty:
    """Tests for empty message filtering"""

    def test_blocks_empty_message(self):
        """Test that empty messages are blocked"""
        message = {"body": ""}
        should_process, reason = filter_empty(message)
        assert should_process is False

    def test_blocks_short_message(self):
        """Test that very short messages are blocked"""
        message = {"body": "hi"}
        should_process, reason = filter_empty(message)
        assert should_process is False

    def test_allows_normal_message(self):
        """Test that normal messages pass empty filter"""
        message = {"body": "Hello there, I need help with something."}
        should_process, reason = filter_empty(message)
        assert should_process is True


class TestFilterManager:
    """Tests for the FilterManager class"""

    def test_default_filters_applied(self):
        """Test that default filters are applied"""
        manager = FilterManager(license_id=1)
        
        # Test automated message is blocked
        message = {"body": "Your verification code is 123456"}
        should_process, reason = manager.should_process(message)
        assert should_process is False

    def test_allows_legitimate_message(self):
        """Test that legitimate messages pass all filters"""
        manager = FilterManager(license_id=1)
        
        message = {
            "body": "السلام عليكم، أريد معرفة المزيد عن خدماتكم",
            "sender_contact": "real.customer@gmail.com",
            "sender_name": "عميل حقيقي",
            "subject": "استفسار عن الخدمات"
        }
        should_process, reason = manager.should_process(message)
        assert should_process is True
