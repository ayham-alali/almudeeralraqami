"""
Al-Mudeer - Version API Route
Public endpoint for version checking (force update system)

Uses automatic build timestamps - no manual version updates needed.
Any deployment will automatically generate a new build_time.
"""

from fastapi import APIRouter
import os
from datetime import datetime

router = APIRouter(tags=["Version"])

# Build time is set once when the server starts
# This ensures the value changes with each deployment
_BUILD_TIME = datetime.utcnow().isoformat() + "Z"

# Read version from environment or use default
_FRONTEND_VERSION = os.getenv("APP_VERSION", "1.0.0")
_BACKEND_VERSION = os.getenv("APP_VERSION", "1.0.0")


@router.get("/api/version", summary="Get current app version (public)")
async def get_version():
    """
    Public endpoint to check current version.
    Used by frontend for force-update detection.
    No authentication required.
    
    Returns build_time which is unique per deployment.
    Any server restart = new build_time = update popup for users.
    """
    return {
        "frontend": _FRONTEND_VERSION,
        "backend": _BACKEND_VERSION,
        "build_time": _BUILD_TIME,
    }
