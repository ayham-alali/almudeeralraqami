"""
Al-Mudeer - Version API Route
Public endpoint for version checking (force update system)

AUTOMATIC FORCE UPDATE SYSTEM:
- Every backend deployment generates a new min_build_time
- If app's build_time < backend's min_build_time → force update
- No manual version changes needed!

To trigger force update: Just deploy the backend to Railway!
"""

from fastapi import APIRouter
import os
from datetime import datetime

router = APIRouter(tags=["Version"])

# This timestamp is generated when the server starts (each deployment)
# Apps built before this time will be forced to update
_SERVER_BUILD_TIME = datetime.utcnow().isoformat() + "Z"

# Version for display purposes only
_APP_VERSION = os.getenv("APP_VERSION", "1.0.0")
_BACKEND_VERSION = os.getenv("BACKEND_VERSION", "1.0.0")

# APK download URL - your website where users download the app
_APP_DOWNLOAD_URL = os.getenv("APP_DOWNLOAD_URL", "https://almudeer.royaraqamia.com")

# Force update can be disabled in emergencies
_FORCE_UPDATE_ENABLED = os.getenv("FORCE_UPDATE_ENABLED", "true").lower() == "true"


@router.get("/api/version", summary="Get current app version (public)")
async def get_version():
    """
    Public endpoint to check current version.
    Used by frontend for force-update detection.
    No authentication required.
    """
    return {
        "frontend": _APP_VERSION,
        "backend": _BACKEND_VERSION,
        "build_time": _SERVER_BUILD_TIME,
    }


@router.get("/api/app/version-check", summary="Mobile app version check (public)")
async def check_app_version():
    """
    Public endpoint for mobile app AUTOMATIC force update system.
    No authentication required.
    
    HOW IT WORKS:
    - The app sends its build_time (when APK was compiled)
    - Backend returns min_build_time (when backend was deployed)
    - If app_build_time < min_build_time → force update
    
    This means: Deploy backend → All existing apps see force update!
    
    To trigger update: Just deploy to Railway. That's it!
    
    Environment Variables:
    - APP_DOWNLOAD_URL: URL to download the APK from your website
    - FORCE_UPDATE_ENABLED: Set to "false" to temporarily disable force update
    
    Returns:
        - min_build_time: Apps built before this time must update
        - update_url: URL to download the new APK
        - force_update: Whether to force the update
        - message: Message to show users (Arabic)
    """
    return {
        "min_build_time": _SERVER_BUILD_TIME,
        "current_version": _APP_VERSION,
        "update_url": _APP_DOWNLOAD_URL,
        "force_update": _FORCE_UPDATE_ENABLED,
        "message": "يتوفر إصدار جديد من التطبيق يحتوي على تحسينات وميزات جديدة. يرجى التحديث للمتابعة.",
    }


