"""
Al-Mudeer - Version API Route
Public endpoint for version checking (force update system)

AUTOMATIC FORCE UPDATE SYSTEM:
Two triggers for force update:
1. Backend deployment → New server_build_time generated automatically
2. New APK uploaded → Call POST /api/app/trigger-update to update timestamp

To trigger update:
- Deploy backend to Railway (automatic)
- OR call POST /api/app/trigger-update after uploading new APK
"""

from fastapi import APIRouter, Header, HTTPException
import os
from datetime import datetime
from typing import Optional

router = APIRouter(tags=["Version"])

# This timestamp is updated when:
# 1. Server starts (backend deployment)
# 2. Admin calls /api/app/trigger-update (after uploading new APK)
_last_update_trigger_time: str = datetime.utcnow().isoformat() + "Z"

# Version for display purposes only
_APP_VERSION = os.getenv("APP_VERSION", "1.0.0")
_BACKEND_VERSION = os.getenv("BACKEND_VERSION", "1.0.0")

# APK download URL - hosted on this Railway backend
_APP_DOWNLOAD_URL = os.getenv("APP_DOWNLOAD_URL", "https://almudeer.up.railway.app/download/almudeer.apk")

# Force update can be disabled in emergencies
_FORCE_UPDATE_ENABLED = os.getenv("FORCE_UPDATE_ENABLED", "true").lower() == "true"

# Admin key for triggering updates
_ADMIN_KEY = os.getenv("ADMIN_KEY", "")


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
        "build_time": _last_update_trigger_time,
    }


@router.get("/api/app/version-check", summary="Mobile app version check (public)")
async def check_app_version():
    """
    Public endpoint for mobile app AUTOMATIC force update system.
    No authentication required.
    
    HOW IT WORKS:
    - The app stores its install/update time
    - Backend returns the last update trigger time
    - If app_time < trigger_time → force update
    
    Two ways to trigger updates:
    1. Deploy backend to Railway → Automatic new timestamp
    2. Call POST /api/app/trigger-update → Manual trigger after APK upload
    
    Returns:
        - min_build_time: Apps installed before this time must update
        - update_url: URL to download the new APK
        - force_update: Whether to force the update
        - message: Message to show users (Arabic)
    """
    return {
        "min_build_time": _last_update_trigger_time,
        "current_version": _APP_VERSION,
        "update_url": _APP_DOWNLOAD_URL,
        "force_update": _FORCE_UPDATE_ENABLED,
        "message": "يتوفر إصدار جديد من التطبيق يحتوي على تحسينات وميزات جديدة. يرجى التحديث للمتابعة.",
    }


@router.post("/api/app/trigger-update", summary="Trigger force update (admin only)")
async def trigger_force_update(
    x_admin_key: Optional[str] = Header(None, alias="X-Admin-Key")
):
    """
    Trigger a force update for all mobile app users.
    Call this AFTER uploading a new APK to your website.
    
    Requires: X-Admin-Key header
    
    Usage:
    ```bash
    curl -X POST https://almudeer.up.railway.app/api/app/trigger-update \\
         -H "X-Admin-Key: YOUR_ADMIN_KEY"
    ```
    
    After calling this, all users with the old app version will see
    the force update dialog on their next app open.
    """
    # Verify admin key
    if not x_admin_key or x_admin_key != _ADMIN_KEY:
        raise HTTPException(
            status_code=403,
            detail="غير مصرح - مفتاح المسؤول مطلوب"
        )
    
    # Update the trigger timestamp
    global _last_update_trigger_time
    old_time = _last_update_trigger_time
    _last_update_trigger_time = datetime.utcnow().isoformat() + "Z"
    
    return {
        "success": True,
        "message": "تم تفعيل التحديث الإجباري لجميع المستخدمين",
        "previous_trigger_time": old_time,
        "new_trigger_time": _last_update_trigger_time,
    }


@router.delete("/api/app/trigger-update", summary="Reset force update trigger (admin only)")
async def reset_force_update(
    x_admin_key: Optional[str] = Header(None, alias="X-Admin-Key")
):
    """
    Reset the force update trigger (emergency use only).
    This will stop forcing users to update.
    
    Requires: X-Admin-Key header
    """
    # Verify admin key
    if not x_admin_key or x_admin_key != _ADMIN_KEY:
        raise HTTPException(
            status_code=403,
            detail="غير مصرح - مفتاح المسؤول مطلوب"
        )
    
    # Reset to epoch (no one will be forced to update)
    global _last_update_trigger_time
    _last_update_trigger_time = "1970-01-01T00:00:00Z"
    
    return {
        "success": True,
        "message": "تم إلغاء التحديث الإجباري",
        "trigger_time": _last_update_trigger_time,
    }


@router.get("/download/almudeer.apk", summary="Download mobile app APK")
async def download_apk():
    """
    Download the Al-Mudeer mobile app APK.
    Returns the APK file with proper headers for browser download.
    """
    from fastapi.responses import FileResponse
    
    # Path to APK file
    apk_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "static", "download", "almudeer.apk")
    
    if not os.path.exists(apk_path):
        raise HTTPException(
            status_code=404,
            detail="APK file not found. Please contact support."
        )
    
    return FileResponse(
        path=apk_path,
        filename="almudeer.apk",
        media_type="application/vnd.android.package-archive",
        headers={
            "Content-Disposition": "attachment; filename=almudeer.apk"
        }
    )

