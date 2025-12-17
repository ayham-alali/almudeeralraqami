"""
Al-Mudeer API Version Router
Provides versioned API endpoints for backward compatibility
"""

from fastapi import APIRouter

# API Version 1 router - all versioned endpoints go through this
v1_router = APIRouter(prefix="/api/v1", tags=["v1"])


def create_versioned_routers():
    """
    Create version-specific routers.
    
    This allows adding new API versions in the future while maintaining
    backward compatibility with existing endpoints.
    
    Returns:
        dict: Dictionary of version routers
    """
    return {
        "v1": v1_router,
    }


# Version constants
CURRENT_API_VERSION = "v1"
SUPPORTED_VERSIONS = ["v1"]
