"""
Al-Mudeer - Library API Routes
Handling Notes, Images, Files, Audio, and Video uploads/management
"""

import os
import uuid
import shutil
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form, Query
from pydantic import BaseModel

from dependencies import get_license_from_header
from models.library import (
    get_library_items, 
    get_library_item, 
    add_library_item, 
    update_library_item, 
    delete_library_item, 
    bulk_delete_items,
    get_storage_usage
)
from security import sanitize_string

router = APIRouter(prefix="/api/library", tags=["Library"])

# Ensure upload directory exists
UPLOAD_DIR = os.path.join("static", "uploads", "library")
os.makedirs(UPLOAD_DIR, exist_ok=True)

class NoteCreate(BaseModel):
    customer_id: Optional[int] = None
    title: str
    content: str

class ItemUpdate(BaseModel):
    title: Optional[str] = None
    content: Optional[str] = None
    customer_id: Optional[int] = None

class BulkDeleteRequest(BaseModel):
    item_ids: List[int]

@router.get("/")
async def list_items(
    customer_id: Optional[int] = None,
    type: Optional[str] = None,
    category: Optional[str] = None,
    search: Optional[str] = Query(None, description="Search term for title or content"),
    page: int = 1,
    page_size: int = 50,
    license: dict = Depends(get_license_from_header)
):
    """List library items for the current license."""
    offset = (page - 1) * page_size
    items = await get_library_items(
        license_id=license["license_id"],
        customer_id=customer_id,
        item_type=type,
        category=category,
        search_term=search,
        limit=page_size,
        offset=offset
    )
    
    usage = await get_storage_usage(license["license_id"])
    
    return {
        "success": True,
        "items": items,
        "storage_usage_bytes": usage,
        "page": page,
        "page_size": page_size
    }

@router.post("/notes")
async def create_note(
    data: NoteCreate,
    license: dict = Depends(get_license_from_header)
):
    """Create a new text note."""
    try:
        item = await add_library_item(
            license_id=license["license_id"],
            item_type="note",
            customer_id=data.customer_id,
            title=sanitize_string(data.title),
            content=sanitize_string(data.content, max_length=5000)
        )
        return {"success": True, "item": item}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    customer_id: Optional[int] = Form(None),
    title: Optional[str] = Form(None),
    license: dict = Depends(get_license_from_header)
):
    """Upload a media or file item."""
    # Determine type from mime_type
    content_type = file.content_type or "application/octet-stream"
    item_type = "file"
    if content_type.startswith("image/"):
        item_type = "image"
    elif content_type.startswith("audio/"):
        item_type = "audio"
    elif content_type.startswith("video/"):
        item_type = "video"
        
    # Generate unique filename
    ext = os.path.splitext(file.filename or "")[1]
    filename = f"{uuid.uuid4()}{ext}"
    file_path = os.path.join(UPLOAD_DIR, filename)
    
    # Save file
    try:
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
            
        file_size = os.path.getsize(file_path)
        
        # Add to DB
        item = await add_library_item(
            license_id=license["license_id"],
            item_type=item_type,
            customer_id=customer_id,
            title=sanitize_string(title or file.filename),
            file_path=f"/static/uploads/library/{filename}",
            file_size=file_size,
            mime_type=content_type
        )
        
        return {"success": True, "item": item}
    except ValueError as e:
        # Cleanup file if DB insert fails (e.g. limit reached)
        if os.path.exists(file_path):
            os.remove(file_path)
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        if os.path.exists(file_path):
            os.remove(file_path)
        raise HTTPException(status_code=500, detail=f"حدث خطأ أثناء الرفع: {str(e)}")

@router.patch("/{item_id}")
async def update_item(
    item_id: int,
    data: ItemUpdate,
    license: dict = Depends(get_license_from_header)
):
    """Update item metadata."""
    success = await update_library_item(
        license_id=license["license_id"],
        item_id=item_id,
        **data.dict(exclude_none=True)
    )
    if not success:
        raise HTTPException(status_code=404, detail="العنصر غير موجود")
    return {"success": True}

@router.delete("/{item_id}")
async def delete_item(
    item_id: int,
    license: dict = Depends(get_license_from_header)
):
    """Delete an item."""
    success = await delete_library_item(license["license_id"], item_id)
    if not success:
        raise HTTPException(status_code=404, detail="العنصر غير موجود")
    return {"success": True}

@router.post("/bulk-delete")
async def bulk_delete(
    data: BulkDeleteRequest,
    license: dict = Depends(get_license_from_header)
):
    """Bulk delete items."""
    count = await bulk_delete_items(license["license_id"], data.item_ids)
    return {"success": True, "deleted_count": count}
