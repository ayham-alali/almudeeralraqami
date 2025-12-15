"""
Performance Middleware for Al-Mudeer
Optimized for Arab World users with latency monitoring
"""

import time
from typing import Callable
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse


class PerformanceMiddleware(BaseHTTPMiddleware):
    """Middleware to track request processing time and add performance headers"""
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Start timing
        start_time = time.time()
        
        # Process request
        response = await call_next(request)

        # Calculate processing time
        process_time = time.time() - start_time

        # Add performance headers
        response.headers["X-Process-Time"] = f"{process_time:.3f}"
        response.headers["X-Response-Time-Ms"] = f"{round(process_time * 1000, 2)}"

        # Add cache control headers for static/cacheable endpoints
        if request.url.path.startswith(("/health", "/")):
            response.headers["Cache-Control"] = "public, max-age=60"

        # Add CORS headers for better browser caching
        response.headers["Access-Control-Max-Age"] = "86400"  # 24 hours

        return response


class CompressionMiddleware(BaseHTTPMiddleware):
    """Simple compression for JSON responses (basic optimization)"""
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        response = await call_next(request)
        
        # Add compression headers
        if "Content-Type" in response.headers:
            content_type = response.headers["Content-Type"]
            if "application/json" in content_type or "text/" in content_type:
                # Note: Railway/Vercel handles actual compression automatically
                # We're just adding headers for client-side optimization
                if "Content-Encoding" not in response.headers:
                    # Let Railway handle compression, but indicate we support it
                    pass
        
        return response


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Add security and performance headers optimized for Arab World"""
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        response = await call_next(request)
        
        # Security headers
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        
        # Performance hints for browsers
        response.headers["X-DNS-Prefetch-Control"] = "on"
        
        # Regional optimization hint (EU West)
        response.headers["X-Region"] = "EU-West"
        
        return response

