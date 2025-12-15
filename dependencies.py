"""
Shared FastAPI dependency helpers for Al-Mudeer.

These functions centralize repeated header-based auth logic without
changing any response shapes that the frontend relies on.
"""

from fastapi import Header, HTTPException
from typing import Dict, Optional

from database import validate_license_key


async def get_license_from_header(
    x_license_key: Optional[str] = Header(None, alias="X-License-Key"),
) -> Dict:
    """
    Resolve and validate a license from the `X-License-Key` header.

    This intentionally preserves the simple string `detail` messages that
    existing frontend error handling already expects.
    """
    if not x_license_key:
        raise HTTPException(status_code=401, detail="مفتاح الاشتراك مطلوب")

    result = await validate_license_key(x_license_key)
    if not result.get("valid"):
        # `result["error"]` is already localized, reuse it as-is
        raise HTTPException(status_code=401, detail=result.get("error", "مفتاح الاشتراك غير صالح"))

    return result


