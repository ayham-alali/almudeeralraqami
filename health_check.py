"""
Al-Mudeer Health Check Module
Production-ready health and readiness endpoints
"""

import os
import time
from datetime import datetime
from typing import Dict, Any, Optional
from fastapi import APIRouter

router = APIRouter(tags=["Health"])

# Track application start time
_start_time = time.time()


def get_uptime_seconds() -> float:
    """Get application uptime in seconds"""
    return time.time() - _start_time


def format_uptime(seconds: float) -> str:
    """Format uptime as human-readable string"""
    days, remainder = divmod(int(seconds), 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, secs = divmod(remainder, 60)
    
    parts = []
    if days > 0:
        parts.append(f"{days}d")
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    parts.append(f"{secs}s")
    
    return " ".join(parts)


async def check_database_health() -> Dict[str, Any]:
    """Check database connectivity"""
    try:
        from db_helper import get_db, execute_sql
        async with get_db() as db:
            result = await execute_sql(db, "SELECT 1")
            return {"status": "healthy", "latency_ms": 0}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


async def check_redis_health() -> Dict[str, Any]:
    """Check Redis connectivity (if configured)"""
    redis_url = os.getenv("REDIS_URL")
    if not redis_url:
        return {"status": "not_configured"}
    
    try:
        import redis.asyncio as redis
        client = redis.from_url(redis_url)
        await client.ping()
        await client.close()
        return {"status": "healthy"}
    except ImportError:
        return {"status": "not_installed"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


@router.get("/health")
async def health_check():
    """
    Basic health check endpoint for load balancers.
    Returns 200 if the service is running.
    """
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "uptime": format_uptime(get_uptime_seconds()),
    }


@router.get("/health/live")
async def liveness_check():
    """
    Kubernetes liveness probe.
    Returns 200 if the process is alive.
    """
    return {"status": "alive"}


@router.get("/health/ready")
async def readiness_check():
    """
    Kubernetes readiness probe.
    Checks if the service is ready to accept traffic.
    """
    checks = {}
    overall_healthy = True
    
    # Check database
    db_health = await check_database_health()
    checks["database"] = db_health
    if db_health.get("status") != "healthy":
        overall_healthy = False
    
    # Check Redis (optional)
    redis_health = await check_redis_health()
    checks["redis"] = redis_health
    # Redis is optional, don't fail readiness if not configured
    if redis_health.get("status") == "unhealthy":
        overall_healthy = False
    
    return {
        "status": "ready" if overall_healthy else "not_ready",
        "checks": checks,
    }


@router.get("/health/detailed")
async def detailed_health():
    """
    Detailed health check with system information.
    For monitoring dashboards and debugging.
    """
    import platform
    import sys
    
    db_health = await check_database_health()
    redis_health = await check_redis_health()
    
    return {
        "status": "healthy" if db_health.get("status") == "healthy" else "degraded",
        "timestamp": datetime.utcnow().isoformat(),
        "uptime_seconds": get_uptime_seconds(),
        "uptime_formatted": format_uptime(get_uptime_seconds()),
        "version": os.getenv("APP_VERSION", "1.0.0"),
        "environment": os.getenv("ENVIRONMENT", "development"),
        "checks": {
            "database": db_health,
            "redis": redis_health,
        },
        "system": {
            "python_version": sys.version.split()[0],
            "platform": platform.system(),
            "architecture": platform.machine(),
        },
        "config": {
            "db_type": os.getenv("DB_TYPE", "sqlite"),
            "log_level": os.getenv("LOG_LEVEL", "INFO"),
        },
    }
