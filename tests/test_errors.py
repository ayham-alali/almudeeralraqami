"""
Al-Mudeer Error Tests
Tests for structured error handling
"""

import pytest
from errors import (
    APIError,
    ValidationError,
    NotFoundError,
    AuthenticationError,
    AuthorizationError,
    RateLimitError,
    ExternalServiceError,
    DatabaseError,
)


class TestAPIErrors:
    """Test structured API error classes"""
    
    def test_base_api_error(self):
        """Test base APIError class"""
        error = APIError(
            message="Test error",
            error_code="TEST_ERROR",
            status_code=400,
        )
        
        assert error.message == "Test error"
        assert error.error_code == "TEST_ERROR"
        assert error.status_code == 400
        
        error_dict = error.to_dict()
        assert error_dict["error"] == True
        assert error_dict["error_code"] == "TEST_ERROR"
        assert error_dict["message"] == "Test error"
    
    def test_validation_error(self):
        """Test ValidationError with field info"""
        error = ValidationError(
            message="Invalid email format",
            field="email",
        )
        
        assert error.status_code == 422
        assert error.error_code == "VALIDATION_ERROR"
        assert error.details.get("field") == "email"
        assert "غير صالحة" in error.message_ar
    
    def test_not_found_error(self):
        """Test NotFoundError with resource info"""
        error = NotFoundError(resource="Customer", resource_id="123")
        
        assert error.status_code == 404
        assert error.error_code == "NOT_FOUND"
        assert error.details.get("resource") == "Customer"
        assert error.details.get("id") == "123"
    
    def test_authentication_error(self):
        """Test AuthenticationError"""
        error = AuthenticationError()
        
        assert error.status_code == 401
        assert error.error_code == "AUTH_REQUIRED"
        assert "تسجيل الدخول" in error.message_ar
    
    def test_authorization_error(self):
        """Test AuthorizationError"""
        error = AuthorizationError()
        
        assert error.status_code == 403
        assert error.error_code == "FORBIDDEN"
    
    def test_rate_limit_error(self):
        """Test RateLimitError with retry info"""
        error = RateLimitError(retry_after=120)
        
        assert error.status_code == 429
        assert error.error_code == "RATE_LIMIT_EXCEEDED"
        assert error.details.get("retry_after") == 120
    
    def test_external_service_error(self):
        """Test ExternalServiceError"""
        error = ExternalServiceError(
            service="Telegram",
            message="API timeout",
        )
        
        assert error.status_code == 502
        assert error.error_code == "EXTERNAL_SERVICE_ERROR"
        assert error.details.get("service") == "Telegram"
    
    def test_database_error(self):
        """Test DatabaseError"""
        error = DatabaseError(operation="insert")
        
        assert error.status_code == 500
        assert error.error_code == "DATABASE_ERROR"
        assert error.details.get("operation") == "insert"
    
    def test_error_has_arabic_message(self):
        """Test that all errors have Arabic messages"""
        errors = [
            ValidationError("test"),
            NotFoundError("test"),
            AuthenticationError(),
            AuthorizationError(),
            RateLimitError(),
            ExternalServiceError("test", "test"),
            DatabaseError(),
        ]
        
        for error in errors:
            assert error.message_ar is not None
            assert len(error.message_ar) > 0
