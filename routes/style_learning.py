"""
Al-Mudeer Style Learning API Routes
API endpoints for the adaptive AI style learning feature
"""

from fastapi import APIRouter, HTTPException, Header, Depends
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
import logging

from style_learning import (
    StyleProfile,
    analyze_messages_for_style,
    save_style_profile,
    get_style_profile,
    create_default_profile,
    init_style_profiles_table,
)
from db_helper import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/style-learning", tags=["Style Learning"])


# ============ Pydantic Models ============

class StyleLearningSettings(BaseModel):
    """User's style learning preferences"""
    enabled: bool = Field(False, description="Whether to use learned style")
    auto_update: bool = Field(True, description="Auto-update profile when sending")


class StyleAnalysisRequest(BaseModel):
    """Request to trigger style analysis"""
    message_limit: int = Field(50, ge=10, le=200, description="Number of messages to analyze")
    channels: Optional[List[str]] = Field(None, description="Specific channels to analyze")


class StyleProfileResponse(BaseModel):
    """Style profile response"""
    profile_id: str
    message_count: int
    formality_level: str
    dialect: str
    uses_emojis: bool
    preferred_length: str
    common_greetings: List[str]
    common_closings: List[str]
    personality_traits: List[str]
    created_at: str
    updated_at: str


class StyleLearningStatusResponse(BaseModel):
    """Full status of style learning feature"""
    enabled: bool
    has_profile: bool
    profile: Optional[StyleProfileResponse]
    messages_analyzed: int


# ============ Helper Functions ============

async def get_license_id(x_license_key: str = Header(...)) -> str:
    """Extract license ID from header"""
    from database import validate_license_key
    
    result = await validate_license_key(x_license_key)
    if not result or not result.get("valid"):
        raise HTTPException(status_code=401, detail="Invalid license key")
    
    return result.get("license_id", x_license_key[:20])


async def get_sent_messages(license_id: str, limit: int = 50, channels: List[str] = None) -> List[Dict]:
    """Fetch user's sent messages from outbox"""
    from db_helper import execute_sql
    
    async with get_db() as db:
        query = """
            SELECT body, channel, sent_at 
            FROM outbox_messages 
            WHERE license_id = ? AND status = 'sent'
        """
        params = [license_id]
        
        if channels:
            placeholders = ','.join(['?' for _ in channels])
            query += f" AND channel IN ({placeholders})"
            params.extend(channels)
        
        query += " ORDER BY sent_at DESC LIMIT ?"
        params.append(limit)
        
        try:
            results = await execute_sql(db, query, tuple(params))
            return [
                {"body": r[0], "channel": r[1], "sent_at": r[2]}
                for r in (results or [])
            ]
        except Exception as e:
            logger.warning(f"Error fetching sent messages: {e}")
            return []


# ============ API Endpoints ============

@router.get("/status", response_model=StyleLearningStatusResponse)
async def get_style_learning_status(license_id: str = Depends(get_license_id)):
    """
    Get the current status of style learning for this user.
    Returns whether it's enabled and the current profile if available.
    """
    async with get_db() as db:
        # Initialize table if needed
        await init_style_profiles_table(db)
        
        # Get current profile
        profile = await get_style_profile(license_id, db)
        
        # Get user preference (enabled/disabled)
        from models import get_preferences
        prefs = await get_preferences(license_id)
        enabled = prefs.get("use_learned_style", False) if prefs else False
        
        if profile:
            return StyleLearningStatusResponse(
                enabled=enabled,
                has_profile=True,
                profile=StyleProfileResponse(
                    profile_id=profile.profile_id,
                    message_count=profile.message_count,
                    formality_level=profile.formality_level,
                    dialect=profile.dialect,
                    uses_emojis=profile.uses_emojis,
                    preferred_length=profile.preferred_length,
                    common_greetings=profile.common_greetings,
                    common_closings=profile.common_closings,
                    personality_traits=profile.personality_traits,
                    created_at=profile.created_at,
                    updated_at=profile.updated_at,
                ),
                messages_analyzed=profile.message_count,
            )
        else:
            return StyleLearningStatusResponse(
                enabled=enabled,
                has_profile=False,
                profile=None,
                messages_analyzed=0,
            )


@router.post("/analyze", response_model=StyleProfileResponse)
async def analyze_writing_style(
    request: StyleAnalysisRequest,
    license_id: str = Depends(get_license_id),
):
    """
    Analyze the user's past sent messages to learn their writing style.
    This creates/updates the StyleProfile for the user.
    """
    # Fetch sent messages
    messages = await get_sent_messages(
        license_id,
        limit=request.message_limit,
        channels=request.channels,
    )
    
    if len(messages) < 3:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "NOT_ENOUGH_DATA",
                "message": "At least 3 sent messages are required for analysis",
                "message_ar": "يجب أن يكون لديك 3 رسائل مرسلة على الأقل للتحليل",
                "current_count": len(messages),
            }
        )
    
    # Analyze messages
    profile = await analyze_messages_for_style(messages, license_id)
    
    # Save to database
    async with get_db() as db:
        await init_style_profiles_table(db)
        await save_style_profile(profile, db)
    
    logger.info(f"Style profile created for {license_id}: {profile.message_count} messages analyzed")
    
    return StyleProfileResponse(
        profile_id=profile.profile_id,
        message_count=profile.message_count,
        formality_level=profile.formality_level,
        dialect=profile.dialect,
        uses_emojis=profile.uses_emojis,
        preferred_length=profile.preferred_length,
        common_greetings=profile.common_greetings,
        common_closings=profile.common_closings,
        personality_traits=profile.personality_traits,
        created_at=profile.created_at,
        updated_at=profile.updated_at,
    )


@router.patch("/settings")
async def update_style_learning_settings(
    settings: StyleLearningSettings,
    license_id: str = Depends(get_license_id),
):
    """
    Enable or disable style learning for this user.
    This is the UI toggle setting.
    """
    from models import update_preferences
    
    await update_preferences(license_id, {
        "use_learned_style": settings.enabled,
        "style_auto_update": settings.auto_update,
    })
    
    return {
        "success": True,
        "message": "Style learning settings updated",
        "message_ar": "تم تحديث إعدادات تعلم الأسلوب",
        "settings": {
            "enabled": settings.enabled,
            "auto_update": settings.auto_update,
        }
    }


@router.delete("/profile")
async def delete_style_profile(license_id: str = Depends(get_license_id)):
    """
    Delete the learned style profile (reset to default AI style).
    """
    from db_helper import execute_sql
    
    async with get_db() as db:
        await execute_sql(db, """
            DELETE FROM style_profiles WHERE license_id = ?
        """, (license_id,))
    
    return {
        "success": True,
        "message": "Style profile deleted",
        "message_ar": "تم حذف ملف الأسلوب المتعلم",
    }


@router.get("/preview")
async def preview_style_response(
    message: str,
    use_learned_style: bool = True,
    license_id: str = Depends(get_license_id),
):
    """
    Preview how a response would look with/without learned style.
    Useful for the UI to show the difference.
    """
    from agent_enhanced import process_message_enhanced
    
    style_profile = None
    if use_learned_style:
        async with get_db() as db:
            profile = await get_style_profile(license_id, db)
            if profile:
                style_profile = profile.to_dict()
    
    result = await process_message_enhanced(
        message=message,
        sender_name="عميل تجريبي",
        use_learned_style=use_learned_style and style_profile is not None,
        style_profile=style_profile,
    )
    
    return {
        "success": result.get("success", False),
        "used_learned_style": use_learned_style and style_profile is not None,
        "draft_response": result.get("data", {}).get("draft_response", ""),
        "quality_score": result.get("data", {}).get("quality_score", 0),
    }
