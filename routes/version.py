"""
Al-Mudeer - Version API Route
Public endpoint for version checking (force update system)

RELIABLE FORCE UPDATE SYSTEM (Build Number Based):
1. Mobile app has a build number in pubspec.yaml (e.g., version: 1.0.0+2)
2. Backend reads minimum required build number from apk_version.txt
3. If app_build_number < min_build_number → force update

To trigger update:
1. Update pubspec.yaml: version: 1.0.0+2 (increment build number)
2. Build APK
3. Copy APK to backend/static/download/almudeer.apk
4. Update backend/static/download/apk_version.txt to "2"
5. Push to Railway
6. All users with older build see update popup
"""

from fastapi import APIRouter, Header, HTTPException
from fastapi.responses import FileResponse
import os
from typing import Optional

router = APIRouter(tags=["Version"])

# Paths
_STATIC_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "static", "download")
_APK_VERSION_FILE = os.path.join(_STATIC_DIR, "apk_version.txt")
_APK_FILE = os.path.join(_STATIC_DIR, "almudeer.apk")

# Version for display purposes only
_APP_VERSION = os.getenv("APP_VERSION", "1.0.0")
_BACKEND_VERSION = os.getenv("BACKEND_VERSION", "1.0.0")

# APK download URL - hosted on this Railway backend
_APP_DOWNLOAD_URL = os.getenv("APP_DOWNLOAD_URL", "https://almudeer.up.railway.app/download/almudeer.apk")

# Force update can be disabled in emergencies
_FORCE_UPDATE_ENABLED = os.getenv("FORCE_UPDATE_ENABLED", "true").lower() == "true"

# Admin key for manual operations
_ADMIN_KEY = os.getenv("ADMIN_KEY", "")


def _get_min_build_number() -> int:
    """
    Read the minimum required build number from apk_version.txt.
    Falls back to 1 if file doesn't exist or is invalid.
    """
    try:
        if os.path.exists(_APK_VERSION_FILE):
            with open(_APK_VERSION_FILE, "r") as f:
                content = f.read().strip()
                return int(content)
    except (ValueError, IOError):
        pass
    return 1


@router.get("/api/version", summary="Get current app version (public)")
async def get_version():
    """
    Public endpoint to check current version.
    No authentication required.
    """
    return {
        "frontend": _APP_VERSION,
        "backend": _BACKEND_VERSION,
        "min_build_number": _get_min_build_number(),
    }


@router.get("/api/app/version-check", summary="Mobile app version check (public)")
async def check_app_version():
    """
    Public endpoint for mobile app RELIABLE force update system.
    No authentication required.
    
    HOW IT WORKS:
    - The app reads its build number from PackageInfo.buildNumber
    - Backend returns the minimum required build number from apk_version.txt
    - If app_build_number < min_build_number → force update
    
    This is 100% reliable because:
    - Build numbers are deterministic (you set them in pubspec.yaml)
    - No clock skew issues
    - No timing issues
    
    Returns:
        - min_build_number: Apps with build number below this must update
        - update_url: URL to download the new APK
        - force_update: Whether force update is enabled
        - message: Message to show users (Arabic)
    """
    return {
        "min_build_number": _get_min_build_number(),
        "current_version": _APP_VERSION,
        "update_url": _APP_DOWNLOAD_URL,
        "force_update": _FORCE_UPDATE_ENABLED,
        "message": "يتوفر إصدار جديد من التطبيق يحتوي على تحسينات وميزات جديدة. يرجى التحديث للمتابعة.",
    }


@router.post("/api/app/set-min-build", summary="Set minimum build number (admin only)")
async def set_min_build_number(
    build_number: int,
    x_admin_key: Optional[str] = Header(None, alias="X-Admin-Key")
):
    """
    Manually set the minimum required build number.
    Call this AFTER uploading a new APK if you prefer API over file editing.
    
    Requires: X-Admin-Key header
    
    Usage:
    ```bash
    curl -X POST "https://almudeer.up.railway.app/api/app/set-min-build?build_number=2" \
         -H "X-Admin-Key: YOUR_ADMIN_KEY"
    ```
    """
    # Verify admin key
    if not x_admin_key or x_admin_key != _ADMIN_KEY:
        raise HTTPException(
            status_code=403,
            detail="غير مصرح - مفتاح المسؤول مطلوب"
        )
    
    if build_number < 1:
        raise HTTPException(
            status_code=400,
            detail="رقم البناء يجب أن يكون 1 أو أكثر"
        )
    
    # Write to apk_version.txt
    try:
        os.makedirs(_STATIC_DIR, exist_ok=True)
        with open(_APK_VERSION_FILE, "w") as f:
            f.write(str(build_number))
    except IOError as e:
        raise HTTPException(
            status_code=500,
            detail=f"فشل في تحديث رقم الإصدار: {str(e)}"
        )
    
    return {
        "success": True,
        "message": "تم تحديث الحد الأدنى لرقم البناء",
        "min_build_number": build_number,
    }


@router.delete("/api/app/force-update", summary="Disable force update (admin only)")
async def disable_force_update(
    x_admin_key: Optional[str] = Header(None, alias="X-Admin-Key")
):
    """
    Emergency: Reset min build to 0 to stop forcing updates.
    
    Requires: X-Admin-Key header
    """
    # Verify admin key
    if not x_admin_key or x_admin_key != _ADMIN_KEY:
        raise HTTPException(
            status_code=403,
            detail="غير مصرح - مفتاح المسؤول مطلوب"
        )
    
    # Reset to 0 (no one will be forced to update)
    try:
        os.makedirs(_STATIC_DIR, exist_ok=True)
        with open(_APK_VERSION_FILE, "w") as f:
            f.write("0")
    except IOError as e:
        raise HTTPException(
            status_code=500,
            detail=f"فشل في إلغاء التحديث: {str(e)}"
        )
    
    return {
        "success": True,
        "message": "تم إلغاء التحديث الإجباري",
        "min_build_number": 0,
    }


@router.get("/download/almudeer.apk", summary="Download mobile app APK")
async def download_apk():
    """
    Download the Al-Mudeer mobile app APK.
    Returns the APK file with proper headers for browser download.
    """
    if not os.path.exists(_APK_FILE):
        raise HTTPException(
            status_code=404,
            detail="APK file not found. Please contact support."
        )
    
    return FileResponse(
        path=_APK_FILE,
        filename="almudeer.apk",
        media_type="application/vnd.android.package-archive",
        headers={
            "Content-Disposition": "attachment; filename=almudeer.apk"
        }
    )
