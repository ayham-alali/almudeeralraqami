"""
Al-Mudeer API Tests
Basic tests for health endpoints and security utilities
"""

import pytest
from httpx import AsyncClient, ASGITransport

# Import the app - we'll test actual endpoints
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestSecurityModule:
    """Test security utility functions"""
    
    def test_sanitize_string_basic(self):
        """Test basic string sanitization"""
        from security import sanitize_string
        
        # Normal string should pass through
        assert sanitize_string("Hello World") == "Hello World"
        
        # HTML should be escaped
        result = sanitize_string("<script>alert('xss')</script>")
        assert "<script>" not in result
        assert "&lt;script&gt;" in result
        
        # Empty string
        assert sanitize_string("") == ""
        assert sanitize_string(None) == ""
    
    def test_sanitize_string_max_length(self):
        """Test string truncation"""
        from security import sanitize_string
        
        long_string = "a" * 1000
        result = sanitize_string(long_string, max_length=100)
        assert len(result) == 100
    
    def test_sanitize_email_valid(self):
        """Test valid email validation"""
        from security import sanitize_email
        
        assert sanitize_email("test@example.com") == "test@example.com"
        assert sanitize_email("USER@EXAMPLE.COM") == "user@example.com"
        assert sanitize_email("  user@example.com  ") == "user@example.com"
    
    def test_sanitize_email_invalid(self):
        """Test invalid email rejection"""
        from security import sanitize_email
        
        assert sanitize_email("") is None
        assert sanitize_email("notanemail") is None
        assert sanitize_email("missing@domain") is None
        assert sanitize_email("@example.com") is None
    
    def test_sanitize_phone_valid(self):
        """Test valid phone sanitization"""
        from security import sanitize_phone
        
        assert sanitize_phone("+1234567890") == "+1234567890"
        assert sanitize_phone("123-456-7890") == "1234567890"
        assert sanitize_phone("+1 (234) 567-890") == "+1234567890"
    
    def test_sanitize_phone_invalid(self):
        """Test invalid phone rejection"""
        from security import sanitize_phone
        
        assert sanitize_phone("") is None
        assert sanitize_phone("123") is None  # Too short
        assert sanitize_phone("1" * 20) is None  # Too long
    
    def test_sanitize_message(self):
        """Test message sanitization"""
        from security import sanitize_message
        
        # Normal message
        assert sanitize_message("Hello, how are you?") == "Hello, how are you?"
        
        # Message with newlines (should preserve)
        msg = "Line 1\nLine 2"
        result = sanitize_message(msg)
        assert "\n" in result or "&#10;" in result or "Line" in result
        
        # Empty
        assert sanitize_message("") == ""


class TestEncryption:
    """Test encryption/decryption functions"""
    
    def test_encrypt_decrypt_roundtrip(self):
        """Test that encryption and decryption work together"""
        from security import encrypt_sensitive_data, decrypt_sensitive_data
        
        original = "secret_password_123"
        encrypted = encrypt_sensitive_data(original)
        
        # Encrypted should be different from original
        assert encrypted != original
        
        # Decryption should return original
        decrypted = decrypt_sensitive_data(encrypted)
        assert decrypted == original
    
    def test_encrypt_empty_string(self):
        """Test encryption of empty string"""
        from security import encrypt_sensitive_data
        
        assert encrypt_sensitive_data("") == ""
    
    def test_generate_secure_token(self):
        """Test secure token generation"""
        from security import generate_secure_token
        
        token1 = generate_secure_token()
        token2 = generate_secure_token()
        
        # Tokens should be unique
        assert token1 != token2
        
        # Default length should be 64 chars (32 bytes hex-encoded)
        assert len(token1) == 64


class TestModelsImport:
    """Test that models package imports correctly"""
    
    def test_models_import(self):
        """Test importing from models package"""
        from models import (
            init_enhanced_tables,
            save_email_config,
            get_preferences,
            ROLES,
        )
        
        assert callable(init_enhanced_tables)
        assert callable(save_email_config)
        assert callable(get_preferences)
        assert isinstance(ROLES, dict)
        assert "owner" in ROLES
        assert "agent" in ROLES


# Async tests for API endpoints
@pytest.mark.asyncio
class TestHealthEndpoint:
    """Test health check endpoints"""
    
    async def test_root_endpoint(self):
        """Test root endpoint returns OK"""
        # Import here to avoid issues with event loop
        from main import app
        
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/")
            assert response.status_code == 200
            data = response.json()
            assert data.get("status") == "online"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
