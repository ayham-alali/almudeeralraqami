"""
Al-Mudeer Request Logging Middleware
Structured request/response logging for debugging and audit trails
"""

import time
import uuid
import logging
from typing import Callable
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

logger = logging.getLogger("almudeer.requests")


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """
    Middleware to log HTTP requests and responses with timing information.
    
    Features:
    - Unique request ID for tracing
    - Request method, path, and client IP
    - Response status code and timing
    - Structured JSON-like format for log aggregation
    """
    
    def __init__(self, app: ASGIApp, exclude_paths: list = None):
        super().__init__(app)
        # Paths to exclude from logging (health checks, static files)
        self.exclude_paths = exclude_paths or [
            "/health",
            "/health/live",
            "/health/ready",
            "/docs",
            "/openapi.json",
            "/favicon.ico",
        ]
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Skip logging for excluded paths
        if any(request.url.path.startswith(path) for path in self.exclude_paths):
            return await call_next(request)
        
        # Generate unique request ID
        request_id = str(uuid.uuid4())[:8]
        
        # Add request ID to state for access in handlers
        request.state.request_id = request_id
        
        # Get client info
        client_ip = request.client.host if request.client else "unknown"
        forwarded_for = request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
        if forwarded_for:
            client_ip = forwarded_for
        
        # Start timing
        start_time = time.time()
        
        # Log incoming request
        logger.info(
            f"[{request_id}] --> {request.method} {request.url.path} "
            f"from {client_ip}"
        )
        
        # Process request
        try:
            response = await call_next(request)
            
            # Calculate duration
            duration_ms = (time.time() - start_time) * 1000
            
            # Log response
            log_level = logging.INFO
            if response.status_code >= 500:
                log_level = logging.ERROR
            elif response.status_code >= 400:
                log_level = logging.WARNING
            
            logger.log(
                log_level,
                f"[{request_id}] <-- {response.status_code} "
                f"({duration_ms:.1f}ms)"
            )
            
            # Add request ID to response headers
            response.headers["X-Request-ID"] = request_id
            
            return response
            
        except Exception as e:
            # Log error
            duration_ms = (time.time() - start_time) * 1000
            logger.exception(
                f"[{request_id}] !!! Error after {duration_ms:.1f}ms: {str(e)}"
            )
            raise


class RequestContextMiddleware(BaseHTTPMiddleware):
    """
    Middleware to add request context for logging and tracing.
    Adds license_id and user info to request state when available.
    """
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Extract license key for context
        license_key = request.headers.get("X-License-Key", "")
        if license_key:
            # Mask the license key for logging
            masked = f"{license_key[:10]}...{license_key[-4:]}" if len(license_key) > 14 else "***"
            request.state.license_key_masked = masked
        
        return await call_next(request)


def setup_request_logging(app, exclude_paths: list = None):
    """Configure request logging middleware for the app"""
    app.add_middleware(RequestLoggingMiddleware, exclude_paths=exclude_paths)
    app.add_middleware(RequestContextMiddleware)
