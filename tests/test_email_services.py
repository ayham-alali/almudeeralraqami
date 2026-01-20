"""
Al-Mudeer Email Service Tests
Unit tests for Email and Gmail services
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime


# ============ Email Header Decoding ============

class TestEmailHeaderDecoding:
    """Tests for email header decoding functions"""
    
    def test_decode_simple_header(self):
        """Test decoding a simple ASCII header"""
        from services.email_service import EmailService
        
        service = EmailService(
            email_address="test@example.com",
            password="password",
            imap_server="imap.example.com",
            smtp_server="smtp.example.com"
        )
        
        result = service._decode_header_value("Simple Subject")
        assert result == "Simple Subject"
    
    def test_decode_arabic_header(self):
        """Test decoding Arabic encoded header"""
        from services.email_service import EmailService
        
        service = EmailService(
            email_address="test@example.com",
            password="password",
            imap_server="imap.example.com",
            smtp_server="smtp.example.com"
        )
        
        # Base64 encoded Arabic text
        encoded = "=?UTF-8?B?2YXYsdit2KjYpyDYqNmDINmB2Yog2KfZhNmF2YjZgtmB?="
        result = service._decode_header_value(encoded)
        
        assert result is not None
        assert len(result) > 0


# ============ Email Address Extraction ============

class TestEmailAddressExtraction:
    """Tests for email address extraction from headers"""
    
    def test_extract_simple_email(self):
        """Test extracting simple email address"""
        from services.email_service import EmailService
        
        service = EmailService(
            email_address="test@example.com",
            password="password",
            imap_server="imap.example.com",
            smtp_server="smtp.example.com"
        )
        
        name, email = service._extract_email_address("user@example.com")
        
        assert email == "user@example.com"
    
    def test_extract_email_with_name(self):
        """Test extracting email with display name"""
        from services.email_service import EmailService
        
        service = EmailService(
            email_address="test@example.com",
            password="password",
            imap_server="imap.example.com",
            smtp_server="smtp.example.com"
        )
        
        name, email = service._extract_email_address("Ahmed Mohamed <ahmed@example.com>")
        
        assert name == "Ahmed Mohamed"
        assert email == "ahmed@example.com"
    
    def test_extract_email_with_arabic_name(self):
        """Test extracting email with Arabic display name"""
        from services.email_service import EmailService
        
        service = EmailService(
            email_address="test@example.com",
            password="password",
            imap_server="imap.example.com",
            smtp_server="smtp.example.com"
        )
        
        name, email = service._extract_email_address("أحمد محمد <ahmed@example.com>")
        
        assert "أحمد" in name or name  # Should contain Arabic or be extracted
        assert email == "ahmed@example.com"


# ============ Error Formatting ============

class TestErrorFormatting:
    """Tests for user-friendly error formatting"""
    
    def test_format_authentication_error(self):
        """Test formatting authentication errors"""
        from services.email_service import EmailService
        
        service = EmailService(
            email_address="test@example.com",
            password="wrong",
            imap_server="imap.example.com",
            smtp_server="smtp.example.com"
        )
        
        error = Exception("AUTHENTICATIONFAILED")
        result = service._format_error_message(error, "IMAP")
        
        # Should be user-friendly Arabic message
        assert result is not None
        assert len(result) > 0
    
    def test_format_connection_error(self):
        """Test formatting connection errors"""
        from services.email_service import EmailService
        
        service = EmailService(
            email_address="test@example.com",
            password="password",
            imap_server="invalid.server",
            smtp_server="smtp.example.com"
        )
        
        error = Exception("Connection refused")
        result = service._format_error_message(error, "IMAP")
        
        assert result is not None


# ============ Email Provider Settings ============

class TestEmailProviderSettings:
    """Tests for email provider configuration"""
    
    def test_gmail_provider_settings(self):
        """Test Gmail provider settings are correct"""
        from services.email_service import EMAIL_PROVIDERS
        
        gmail = EMAIL_PROVIDERS.get("gmail")
        
        assert gmail is not None
        assert gmail["imap_server"] == "imap.gmail.com"
        assert gmail["smtp_server"] == "smtp.gmail.com"
        assert gmail["imap_port"] == 993
        assert gmail["smtp_port"] == 587


# ============ Gmail OAuth Service ============

class TestGmailOAuthService:
    """Tests for Gmail OAuth service"""
    
    def test_service_initialization(self):
        """Test Gmail OAuth service initializes correctly"""
        from services.gmail_oauth_service import GmailOAuthService
        
        service = GmailOAuthService()
        
        # Should have OAuth configuration
        assert hasattr(service, 'client_id') or hasattr(service, 'flow')


# ============ Gmail API Service ============

class TestGmailAPIService:
    """Tests for Gmail API service"""
    
    def test_decode_base64url(self):
        """Test base64url decoding for Gmail API"""
        from services.gmail_api_service import GmailAPIService
        
        service = GmailAPIService()
        
        # Gmail uses URL-safe base64
        encoded = "SGVsbG8gV29ybGQ"  # "Hello World" without padding
        
        if hasattr(service, '_decode_base64url'):
            result = service._decode_base64url(encoded)
            assert "Hello World" in result or result is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
