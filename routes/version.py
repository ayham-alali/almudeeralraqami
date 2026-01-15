"""
Al-Mudeer - Version API Route
Public endpoint for version checking (force update system)

RELIABLE FORCE UPDATE SYSTEM (Build Number Based):
1. Mobile app has a build number in pubspec.yaml (e.g., version: 1.0.0+2)
2. Backend reads minimum required build number from apk_version.txt
3. If app_build_number < min_build_number → force update

SOFT UPDATE SUPPORT:
- Set is_soft_update=true in update_config.json for optional updates
- Users can dismiss and update later

To trigger update:
1. Update pubspec.yaml: version: 1.0.0+2 (increment build number)
2. Build APK
3. Copy APK to backend/static/download/almudeer.apk
4. Update backend/static/download/apk_version.txt to "2"
5. Update backend/static/download/changelog.json with changes
6. Push to Railway
7. All users with older build see update popup
"""

from fastapi import APIRouter, Header, HTTPException, Query, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Any, Dict, List, Optional
import os
import json
import hashlib
import time
import threading
from datetime import datetime, timezone
import pytz
from database import save_update_event, get_update_events

router = APIRouter(tags=["Version"])

# Paths
_STATIC_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "static", "download")
_APK_VERSION_FILE = os.path.join(_STATIC_DIR, "apk_version.txt")
_APK_FILE = os.path.join(_STATIC_DIR, "almudeer.apk")
_CHANGELOG_FILE = os.path.join(_STATIC_DIR, "changelog.json")
_UPDATE_CONFIG_FILE = os.path.join(_STATIC_DIR, "update_config.json")

# Version for display purposes only
_APP_VERSION = os.getenv("APP_VERSION", "1.0.0")
_BACKEND_VERSION = os.getenv("BACKEND_VERSION", "1.0.0")

# APK download URL - can be CDN URL or Railway backend
# For CDN: Set APK_CDN_URL environment variable
_APK_CDN_URL = os.getenv("APK_CDN_URL", "")
_APP_DOWNLOAD_URL = _APK_CDN_URL if _APK_CDN_URL else os.getenv(
    "APP_DOWNLOAD_URL", "https://almudeer.up.railway.app/download/almudeer.apk"
)

# Force update can be disabled in emergencies
_FORCE_UPDATE_ENABLED = os.getenv("FORCE_UPDATE_ENABLED", "true").lower() == "true"

# Admin key for manual operations
_ADMIN_KEY = os.getenv("ADMIN_KEY", "")

# Update priority levels
UPDATE_PRIORITY_CRITICAL = "critical"
UPDATE_PRIORITY_HIGH = "high"
UPDATE_PRIORITY_NORMAL = "normal"
UPDATE_PRIORITY_LOW = "low"

UPDATE_PRIORITY_LOW = "low"

# Version history file
_VERSION_HISTORY_FILE = os.path.join(_STATIC_DIR, "version_history.json")

# Localization Strings
_MESSAGES = {
    "ar": {
        "rate_limit": "تم تجاوز الحد المسموح من الطلبات. يرجى المحاولة بعد قليل.",
        "admin_required": "غير مصرح - مفتاح المسؤول مطلوب",
        "invalid_build": "رقم البناء يجب أن يكون 1 أو أكثر",
        "invalid_priority": "أولوية التحديث يجب أن تكون: {priorities}",
        "update_failed": "فشل في تحديث رقم الإصدار: {error}",
        "min_build_updated": "تم تحديث الحد الأدنى لرقم البناء",
        "changelog_updated": "تم تحديث سجل التغييرات",
        "changelog_failed": "فشل في تحديث سجل التغييرات: {error}",
        "force_update_disabled": "تم إلغاء التحديث الإجباري",
        "disable_failed": "فشل في إلغاء التحديث: {error}",
        "update_message": "يتوفر إصدار جديد من التطبيق يحتوي على تحسينات وميزات جديدة. يرجى التحديث للمتابعة.",
         "invalid_event": "Invalid event. Must be one of: {events}"
    }
}

# Rate limiting configuration
_RATE_LIMIT_REQUESTS = int(os.getenv("RATE_LIMIT_REQUESTS", "60"))  # requests per window
_RATE_LIMIT_WINDOW = int(os.getenv("RATE_LIMIT_WINDOW", "60"))  # window in seconds


class RateLimiter:
    """
    Simple in-memory rate limiter using sliding window.
    Thread-safe for concurrent requests.
    """
    
    def __init__(self, max_requests: int, window_seconds: int):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._requests: Dict[str, List[float]] = {}
        self._lock = threading.Lock()
    
    def is_allowed(self, identifier: str) -> tuple[bool, int]:
        """
        Check if request is allowed for given identifier.
        
        Returns:
            Tuple of (is_allowed, remaining_requests)
        """
        now = time.time()
        window_start = now - self.window_seconds
        
        with self._lock:
            # Get existing requests for this identifier
            if identifier not in self._requests:
                self._requests[identifier] = []
            
            # Remove expired requests
            self._requests[identifier] = [
                ts for ts in self._requests[identifier] if ts > window_start
            ]
            
            # Check if under limit
            current_count = len(self._requests[identifier])
            remaining = self.max_requests - current_count
            
            if current_count >= self.max_requests:
                return False, 0
            
            # Record this request
            self._requests[identifier].append(now)
            return True, remaining - 1
    
    def cleanup_old_entries(self):
        """Remove entries older than the window. Call periodically."""
        cutoff = time.time() - self.window_seconds
        with self._lock:
            for identifier in list(self._requests.keys()):
                self._requests[identifier] = [
                    ts for ts in self._requests[identifier] if ts > cutoff
                ]
                if not self._requests[identifier]:
                    del self._requests[identifier]


# Global rate limiter instance
_rate_limiter = RateLimiter(_RATE_LIMIT_REQUESTS, _RATE_LIMIT_WINDOW)


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


def _get_changelog() -> dict:
    """
    Read changelog from changelog.json.
    Returns empty dict if file doesn't exist or is invalid.
    """
    try:
        if os.path.exists(_CHANGELOG_FILE):
            with open(_CHANGELOG_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except (json.JSONDecodeError, IOError):
        pass
    return {
        "version": _APP_VERSION,
        "build_number": _get_min_build_number(),
        "changelog_ar": [],
        "changelog_en": [],
        "release_notes_url": ""
    }


def _get_update_config() -> dict:
    """
    Read update configuration from update_config.json.
    Returns default config if file doesn't exist.
    """
    try:
        if os.path.exists(_UPDATE_CONFIG_FILE):
            with open(_UPDATE_CONFIG_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except (json.JSONDecodeError, IOError):
        pass
    return {
        "is_soft_update": False,
        "priority": UPDATE_PRIORITY_NORMAL,
        "min_soft_update_build": 0,
        "rollout_percentage": 100,
        "effective_from": None,
        "effective_until": None,
        "maintenance_hours": None
    }


def _get_apk_sha256() -> Optional[str]:
    """
    Calculate SHA256 hash of APK file for integrity verification.
    Returns None if file doesn't exist.
    """
    try:
        if os.path.exists(_APK_FILE):
            sha256_hash = hashlib.sha256()
            with open(_APK_FILE, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    sha256_hash.update(chunk)
            return sha256_hash.hexdigest()
    except IOError:
        pass
    return None


def _is_update_active(config: dict) -> tuple[bool, str]:
    """
    Check if update is currently active based on scheduling.
    
    Returns:
        Tuple of (is_active, reason)
    """
    now = datetime.now(timezone.utc)
    
    # Check effective_from
    effective_from = config.get("effective_from")
    if effective_from:
        try:
            from_dt = datetime.fromisoformat(effective_from.replace("Z", "+00:00"))
            if now < from_dt:
                return False, f"Update scheduled for {effective_from}"
        except (ValueError, TypeError):
            pass
    
    # Check effective_until
    effective_until = config.get("effective_until")
    if effective_until:
        try:
            until_dt = datetime.fromisoformat(effective_until.replace("Z", "+00:00"))
            if now > until_dt:
                return False, "Update window has expired"
        except (ValueError, TypeError):
            pass
    
    # Check maintenance hours
    maintenance = config.get("maintenance_hours")
    if maintenance:
        try:
            tz_name = maintenance.get("timezone", "UTC")
            try:
                tz = pytz.timezone(tz_name)
            except:
                tz = pytz.UTC
            
            local_now = datetime.now(tz)
            current_time = local_now.strftime("%H:%M")
            start_time = maintenance.get("start", "00:00")
            end_time = maintenance.get("end", "24:00")
            
            if start_time <= current_time <= end_time:
                return False, f"Maintenance window: {start_time} - {end_time}"
        except:
            pass
    
    return True, "Active"


def _get_version_history() -> List[dict]:
    """
    Read version history from version_history.json.
    Returns list of past versions with changelogs.
    """
    try:
        if os.path.exists(_VERSION_HISTORY_FILE):
            with open(_VERSION_HISTORY_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except (json.JSONDecodeError, IOError):
        pass
    return []


def _get_apk_size_mb() -> Optional[float]:
    """
    Get APK file size in megabytes.
    Returns None if file doesn't exist.
    """
    try:
        if os.path.exists(_APK_FILE):
            size_bytes = os.path.getsize(_APK_FILE)
            return round(size_bytes / (1024 * 1024), 1)
    except OSError:
        pass
    return None


def _is_in_rollout(identifier: str, rollout_percentage: int) -> bool:
    """
    Determine if a user is in the rollout based on their identifier.
    Uses consistent hashing so the same user always gets the same result.
    
    Args:
        identifier: User identifier (license key, device ID, etc.)
        rollout_percentage: Percentage of users to include (0-100)
    
    Returns:
        True if user is in the rollout group
    """
    if rollout_percentage >= 100:
        return True
    if rollout_percentage <= 0:
        return False
    
    # Hash the identifier to get a consistent value 0-99
    hash_value = int(hashlib.md5(identifier.encode()).hexdigest(), 16) % 100
    return hash_value < rollout_percentage


def _parse_categorized_changelog(changelog_data: dict) -> dict:
    """
    Parse changelog into categorized format.
    Supports both old format (changelog_ar list) and new format (changes list).
    """
    # Check for new categorized format
    if "changes" in changelog_data:
        return {
            "changes": changelog_data["changes"],
            "changelog_ar": [c.get("text_ar", "") for c in changelog_data["changes"]],
            "changelog_en": [c.get("text_en", "") for c in changelog_data["changes"]],
        }
    
    # Old format - return as-is
    return {
        "changes": [],
        "changelog_ar": changelog_data.get("changelog_ar", []),
        "changelog_en": changelog_data.get("changelog_en", []),
    }


@router.get("/api/version", summary="Get current app version (public)")
async def get_version():
    """
    Public endpoint to check current version.
    No authentication required.
    """
    changelog = _get_changelog()
    return {
        "frontend": _APP_VERSION,
        "backend": _BACKEND_VERSION,
        "min_build_number": _get_min_build_number(),
        "changelog": changelog.get("changelog_ar", []),
    }


@router.get("/api/app/version-check", summary="Mobile app version check (public)")
@router.get("/api/v1/app/version-check", summary="Mobile app version check v1 (public)")
async def check_app_version(request: Request):
    """
    Public endpoint for mobile app RELIABLE force update system.
    No authentication required.
    
    Rate Limited: 60 requests per minute per IP (configurable via RATE_LIMIT_REQUESTS, RATE_LIMIT_WINDOW)
    
    HOW IT WORKS:
    - The app reads its build number from PackageInfo.buildNumber
    - Backend returns the minimum required build number from apk_version.txt
    - If app_build_number < min_build_number → force update (or soft update)
    
    This is 100% reliable because:
    - Build numbers are deterministic (you set them in pubspec.yaml)
    - No clock skew issues
    - No timing issues
    
    SOFT UPDATE:
    - If is_soft_update is true, the dialog can be dismissed
    - min_soft_update_build: Only show soft update for builds >= this
    
    Returns:
        - min_build_number: Apps with build number below this must update
        - update_url: URL to download the new APK (CDN or Railway)
        - force_update: Whether force update is enabled
        - is_soft_update: If true, update is optional (dismissible dialog)
        - priority: Update priority level (critical/high/normal/low)
        - changelog: List of changes in Arabic
        - changelog_en: List of changes in English
        - release_notes_url: URL to full release notes
        - message: Message to show users (Arabic)
    """
    # Rate limiting by client IP
    client_ip = request.client.host if request.client else "unknown"
    is_allowed, remaining = _rate_limiter.is_allowed(client_ip)
    
    if not is_allowed:
        raise HTTPException(
            status_code=429,
            detail=_MESSAGES["ar"]["rate_limit"],
            headers={"Retry-After": str(_RATE_LIMIT_WINDOW)}
        )
    
    changelog_data = _get_changelog()
    update_config = _get_update_config()
    parsed_changelog = _parse_categorized_changelog(changelog_data)
    
    # Check if update is currently active
    is_active, active_reason = _is_update_active(update_config)
    
    return {
        # Core version info
        "min_build_number": _get_min_build_number(),
        "current_version": _APP_VERSION,
        "update_url": _APP_DOWNLOAD_URL,
        
        # Update behavior
        "force_update": _FORCE_UPDATE_ENABLED,
        "is_soft_update": update_config.get("is_soft_update", False),
        "priority": update_config.get("priority", UPDATE_PRIORITY_NORMAL),
        "min_soft_update_build": update_config.get("min_soft_update_build", 0),
        "rollout_percentage": update_config.get("rollout_percentage", 100),
        
        # Scheduling
        "update_active": is_active,
        "update_active_reason": active_reason,
        "effective_from": update_config.get("effective_from"),
        "effective_until": update_config.get("effective_until"),
        
        # Deferral logic
        "max_deferrals": update_config.get("max_deferrals", 0),
        "deferral_expiry_hours": update_config.get("deferral_expiry_hours", 0),
        
        # Changelog (both formats for compatibility)
        "changelog": parsed_changelog["changelog_ar"],
        "changelog_en": parsed_changelog["changelog_en"],
        "changes": parsed_changelog["changes"],  # Categorized format
        "release_notes_url": changelog_data.get("release_notes_url", ""),
        
        # APK info
        "apk_size_mb": _get_apk_size_mb(),
        "apk_sha256": _get_apk_sha256(),
        
        # Rate limit info
        "rate_limit_remaining": remaining,
        
        # User message
        "message": _MESSAGES["ar"]["update_message"],
        
        # iOS
        "ios_store_url": update_config.get("ios_store_url"),
    }


@router.post("/api/app/set-min-build", summary="Set minimum build number (admin only)")
async def set_min_build_number(
    build_number: int,
    is_soft_update: bool = False,
    priority: str = UPDATE_PRIORITY_NORMAL,
    ios_store_url: Optional[str] = None,
    x_admin_key: Optional[str] = Header(None, alias="X-Admin-Key")
):
    """
    Manually set the minimum required build number and update configuration.
    Call this AFTER uploading a new APK if you prefer API over file editing.
    
    Requires: X-Admin-Key header
    
    Args:
        build_number: Minimum required build number
        is_soft_update: If true, update is optional (user can dismiss)
        priority: Update priority (critical, high, normal, low)
        ios_store_url: Optional App Store URL for iOS users
    
    Usage:
    ```bash
    # Force update
    curl -X POST "https://almudeer.up.railway.app/api/app/set-min-build?build_number=2" \
         -H "X-Admin-Key: YOUR_ADMIN_KEY"
    
    # Soft update with high priority
    curl -X POST "https://almudeer.up.railway.app/api/app/set-min-build?build_number=3&is_soft_update=true&priority=high" \
         -H "X-Admin-Key: YOUR_ADMIN_KEY"
    ```
    """
    # Verify admin key
    if not x_admin_key or x_admin_key != _ADMIN_KEY:
        raise HTTPException(
            status_code=403,
            detail=_MESSAGES["ar"]["admin_required"]
        )
    
    if build_number < 1:
        raise HTTPException(
            status_code=400,
            detail=_MESSAGES["ar"]["invalid_build"]
        )
    
    valid_priorities = [UPDATE_PRIORITY_CRITICAL, UPDATE_PRIORITY_HIGH, UPDATE_PRIORITY_NORMAL, UPDATE_PRIORITY_LOW]
    if priority not in valid_priorities:
        raise HTTPException(
            status_code=400,
            detail=_MESSAGES["ar"]["invalid_priority"].format(priorities=', '.join(valid_priorities))
        )
    
    # Write to apk_version.txt
    try:
        os.makedirs(_STATIC_DIR, exist_ok=True)
        with open(_APK_VERSION_FILE, "w") as f:
            f.write(str(build_number))
        
        # Write update config
        update_config = {
            "is_soft_update": is_soft_update,
            "is_soft_update": is_soft_update,
            "priority": priority,
            "min_soft_update_build": 0,
            "ios_store_url": ios_store_url
        }
        with open(_UPDATE_CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(update_config, f, ensure_ascii=False, indent=2)
            
    except IOError as e:
        raise HTTPException(
            status_code=500,
            detail=_MESSAGES["ar"]["update_failed"].format(error=str(e))
        )
    
    return {
        "success": True,
        "message": _MESSAGES["ar"]["min_build_updated"],
        "min_build_number": build_number,
        "is_soft_update": is_soft_update,
        "priority": priority,
        "ios_store_url": ios_store_url,
    }


@router.post("/api/app/set-changelog", summary="Update changelog (admin only)")
async def set_changelog(
    changelog_ar: List[str],
    changelog_en: Optional[List[str]] = None,
    release_notes_url: Optional[str] = None,
    x_admin_key: Optional[str] = Header(None, alias="X-Admin-Key")
):
    """
    Update the changelog for the current version.
    
    Requires: X-Admin-Key header
    
    Args:
        changelog_ar: List of changes in Arabic
        changelog_en: List of changes in English (optional)
        release_notes_url: URL to full release notes (optional)
    
    Usage:
    ```bash
    curl -X POST "https://almudeer.up.railway.app/api/app/set-changelog" \
         -H "X-Admin-Key: YOUR_ADMIN_KEY" \
         -H "Content-Type: application/json" \
         -d '{"changelog_ar": ["تحسين الأداء", "إصلاح الأخطاء"]}'
    ```
    """
    # Verify admin key
    if not x_admin_key or x_admin_key != _ADMIN_KEY:
        raise HTTPException(
            status_code=403,
            detail=_MESSAGES["ar"]["admin_required"]
        )
    
    try:
        os.makedirs(_STATIC_DIR, exist_ok=True)
        
        changelog_data = {
            "version": _APP_VERSION,
            "build_number": _get_min_build_number(),
            "changelog_ar": changelog_ar,
            "changelog_en": changelog_en or [],
            "release_notes_url": release_notes_url or ""
        }
        
        with open(_CHANGELOG_FILE, "w", encoding="utf-8") as f:
            json.dump(changelog_data, f, ensure_ascii=False, indent=2)
            
    except IOError as e:
        raise HTTPException(
            status_code=500,
            detail=_MESSAGES["ar"]["changelog_failed"].format(error=str(e))
        )
    
    return {
        "success": True,
        "message": _MESSAGES["ar"]["changelog_updated"],
        "changelog": changelog_data,
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
            detail=_MESSAGES["ar"]["admin_required"]
        )
    
    # Reset to 0 (no one will be forced to update)
    try:
        os.makedirs(_STATIC_DIR, exist_ok=True)
        with open(_APK_VERSION_FILE, "w") as f:
            f.write("0")
    except IOError as e:
        raise HTTPException(
            status_code=500,
            detail=_MESSAGES["ar"]["disable_failed"].format(error=str(e))
        )
    
    return {
        "success": True,
        "message": _MESSAGES["ar"]["force_update_disabled"],
        "min_build_number": 0,
    }


# ============ Analytics ============

class UpdateEventRequest(BaseModel):
    """Request model for update analytics events"""
    event: str  # viewed, clicked_update, clicked_later, installed
    from_build: int
    to_build: int
    device_id: Optional[str] = None
    device_type: Optional[str] = None  # android, ios, unknown
    license_key: Optional[str] = None


@router.post("/api/app/update-event", summary="Track update event (analytics)")
@router.post("/api/v1/app/update-event", summary="Track update event v1 (analytics)")
async def track_update_event(data: UpdateEventRequest):
    """
    Track update-related events for analytics.
    No authentication required (public endpoint for app usage).
    
    Events:
        - viewed: User saw the update dialog
        - clicked_update: User clicked "Update Now"
        - clicked_later: User clicked "Later" (soft update only)
        - installed: User successfully installed the update
    
    This data helps track:
        - Update adoption rate
        - Time to update
        - Soft update dismissal rate
        - Platform-specific metrics (android vs ios)
    """
    valid_events = ["viewed", "clicked_update", "clicked_later", "installed"]
    if data.event not in valid_events:
        raise HTTPException(
            status_code=400,
            detail=_MESSAGES["ar"]["invalid_event"].format(events=', '.join(valid_events))
        )
    
    # Validate device_type if provided
    valid_device_types = ["android", "ios", "unknown", None]
    if data.device_type and data.device_type not in valid_device_types:
        data.device_type = "unknown"
    
    # Log analytics event
    # Log analytics event
    await save_update_event(
        event=data.event,
        from_build=data.from_build,
        to_build=data.to_build,
        device_id=data.device_id,
        device_type=data.device_type,
        license_key=data.license_key
    )
    
    return {"success": True, "message": "Event tracked"}


@router.get("/api/app/update-analytics", summary="Get update analytics (admin only)")
@router.get("/api/v1/app/update-analytics", summary="Get update analytics v1 (admin only)")
async def get_update_analytics(
    x_admin_key: Optional[str] = Header(None, alias="X-Admin-Key")
):
    """
    Get update analytics summary.
    
    Requires: X-Admin-Key header
    
    Returns:
        - total_views: Total update dialog views
        - total_updates: Total update clicks
        - total_later: Total "Later" clicks
        - adoption_rate: Percentage of views that led to updates
        - by_device_type: Breakdown by platform (android, ios)
        - recent_events: Last 50 events
    """
    # Verify admin key
    # Verify admin key
    if not x_admin_key or x_admin_key != _ADMIN_KEY:
        raise HTTPException(
            status_code=403,
            detail=_MESSAGES["ar"]["admin_required"]
        )
    
    events: List[Dict[str, Any]] = []
    events = await get_update_events(1000)
    
    # Calculate summary
    total_views = sum(1 for e in events if e.get("event") == "viewed")
    total_updates = sum(1 for e in events if e.get("event") == "clicked_update")
    total_later = sum(1 for e in events if e.get("event") == "clicked_later")
    total_installed = sum(1 for e in events if e.get("event") == "installed")
    
    adoption_rate = round((total_updates / total_views * 100), 1) if total_views > 0 else 0
    
    # Device type breakdown
    by_device_type = {
        "android": {
            "views": sum(1 for e in events if e.get("event") == "viewed" and e.get("device_type") == "android"),
            "updates": sum(1 for e in events if e.get("event") == "clicked_update" and e.get("device_type") == "android"),
            "later": sum(1 for e in events if e.get("event") == "clicked_later" and e.get("device_type") == "android"),
            "installed": sum(1 for e in events if e.get("event") == "installed" and e.get("device_type") == "android"),
        },
        "ios": {
            "views": sum(1 for e in events if e.get("event") == "viewed" and e.get("device_type") == "ios"),
            "updates": sum(1 for e in events if e.get("event") == "clicked_update" and e.get("device_type") == "ios"),
            "later": sum(1 for e in events if e.get("event") == "clicked_later" and e.get("device_type") == "ios"),
            "installed": sum(1 for e in events if e.get("event") == "installed" and e.get("device_type") == "ios"),
        },
    }
    
    return {
        "total_views": total_views,
        "total_updates": total_updates,
        "total_later": total_later,
        "total_installed": total_installed,
        "adoption_rate": adoption_rate,
        "by_device_type": by_device_type,
        "recent_events": events[:50],  # Return 50 most recent (already sorted DESC)
    }


# ============ APK Download ============

@router.get("/download/almudeer.apk", summary="Download mobile app APK")
async def download_apk():
    """
    Download the Al-Mudeer mobile app APK.
    Returns the APK file with proper headers for browser download.
    
    Note: If APK_CDN_URL is set, consider redirecting users there instead
    for better download speeds and reduced server load.
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


# ============ Version History ============

@router.get("/api/app/versions", summary="Get version history (public)")
async def get_version_history(
    limit: int = Query(5, ge=1, le=20, description="Number of versions to return")
):
    """
    Get changelog history for multiple versions.
    Useful for users who skipped updates and want to see all changes.
    
    Returns:
        List of versions with their changelogs, newest first.
    """
    # Get version history from file
    history = _get_version_history()
    
    # Add current version at the top
    current_changelog = _get_changelog()
    current = {
        "version": _APP_VERSION,
        "build_number": _get_min_build_number(),
        "release_date": current_changelog.get("release_date", None),
        "changes": current_changelog.get("changes", []),
        "changelog_ar": current_changelog.get("changelog_ar", []),
        "changelog_en": current_changelog.get("changelog_en", []),
    }
    
    all_versions = [current] + history
    
    return {
        "versions": all_versions[:limit],
        "total": len(all_versions),
    }
