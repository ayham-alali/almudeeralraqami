"""
Al-Mudeer - Presence and Real-time Indicators Routes
API endpoints for broadcasting typing, recording, and presence status
"""

from datetime import datetime, timezone, timedelta
from fastapi import APIRouter, Depends, Body, HTTPException
from typing import Dict, Any
from pydantic import BaseModel

from db_helper import get_db, fetch_one
from dependencies import get_license_from_header



router = APIRouter(prefix="/api/presence", tags=["Chat Features"])






@router.get("/{sender_contact:path}")
async def get_contact_presence(
    sender_contact: str,
    license: dict = Depends(get_license_from_header)
):
    """
    Get presence status for a contact (customer).
    Inferred from their last message timestamp.
    """
    license_id = license.get("license_id")
    
    # Decoding URI component handled by FastAPI automatically for path params? 
    # Actually for "path" params with slashes it might be tricky but @ symbol is usually fine.
    # sender_contact might be like "@Yamen_Etaki"
    
    async with get_db() as db:
        # Find the latest message from this contact
        query = """
            SELECT created_at 
            FROM inbox_messages 
            WHERE license_key_id = ? AND sender_contact = ?
            ORDER BY created_at DESC 
            LIMIT 1
        """
        row = await fetch_one(db, query, [license_id, sender_contact])
        
        if not row:
            return {
                "is_online": False, 
                "last_seen": None,
                "status_text": "غير متصل"
            }
            
        last_seen_str = row.get("created_at")
        
        # Parse timestamp
        last_seen = None
        if isinstance(last_seen_str, str):
            try:
                last_seen = datetime.fromisoformat(last_seen_str.replace('Z', '+00:00'))
            except:
                pass
        elif isinstance(last_seen_str, datetime):
            last_seen = last_seen_str
            
        if not last_seen:
             return {
                "is_online": False, 
                "last_seen": None,
                "status_text": "غير متصل"
            }

        # Ensure UTC
        if last_seen.tzinfo is None:
            last_seen = last_seen.replace(tzinfo=timezone.utc)
            
        now = datetime.now(timezone.utc)
        diff = now - last_seen
        
        # Logic: Online if message within last 5 minutes
        is_online = diff < timedelta(minutes=5)
        
        # Format status text (Arabic)
        status_text = "غير متصل"
        if is_online:
            status_text = "متصل الآن"
        else:
            # Simple formatting
            if diff < timedelta(hours=1):
                mins = int(diff.total_seconds() / 60)
                status_text = f"آخر ظهور منذ {mins} دقيقة"
            elif diff < timedelta(days=1):
                hours = int(diff.total_seconds() / 3600)
                status_text = f"آخر ظهور منذ {hours} ساعة"
            elif diff < timedelta(days=7):
                days = diff.days
                status_text = f"آخر ظهور منذ {days} يوم"
            else:
                status_text = f"آخر ظهور {last_seen.strftime('%Y-%m-%d')}"

        return {
            "is_online": is_online,
            "last_seen": last_seen.isoformat(),
            "status_text": status_text
        }

