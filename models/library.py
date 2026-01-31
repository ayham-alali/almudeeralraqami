"""
Al-Mudeer - Library Models
CRUD operations for notes, images, files, audios, and videos
"""

import os
from datetime import datetime
from typing import List, Optional, Dict, Any

from db_helper import get_db, execute_sql, fetch_all, fetch_one, commit_db, DB_TYPE

# Default limits (can be configured via env)
MAX_STORAGE_PER_LICENSE = int(os.getenv("MAX_STORAGE_PER_LICENSE", 100 * 1024 * 1024))  # 100MB
MAX_FILE_SIZE = int(os.getenv("MAX_FILE_SIZE", 20 * 1024 * 1024))  # 20MB

async def get_library_items(
    license_id: int, 
    customer_id: Optional[int] = None,
    item_type: Optional[str] = None,
    search_term: Optional[str] = None,
    limit: int = 50,
    offset: int = 0
) -> List[dict]:
    """Get library items for a license, optionally filtered by customer or type."""
    query = "SELECT * FROM library_items WHERE license_key_id = ? AND deleted_at IS NULL"
    params = [license_id]
    
    if customer_id is not None:
        query += " AND customer_id = ?"
        params.append(customer_id)
        
    if item_type:
        query += " AND type = ?"
        params.append(item_type)

    if search_term:
        query += " AND (title LIKE ? OR content LIKE ?)"
        search_pattern = f"%{search_term}%"
        params.extend([search_pattern, search_pattern])
        
    query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    
    async with get_db() as db:
        rows = await fetch_all(db, query, params)
        return [dict(row) for row in rows]

async def get_library_item(license_id: int, item_id: int) -> Optional[dict]:
    """Get a specific library item."""
    async with get_db() as db:
        row = await fetch_one(
            db,
            "SELECT * FROM library_items WHERE id = ? AND license_key_id = ? AND deleted_at IS NULL",
            [item_id, license_id]
        )
        return dict(row) if row else None

async def add_library_item(
    license_id: int,
    item_type: str,
    customer_id: Optional[int] = None,
    title: Optional[str] = None,
    content: Optional[str] = None,
    file_path: Optional[str] = None,
    file_size: Optional[int] = 0,
    mime_type: Optional[str] = None
) -> dict:
    """Add a new item to the library."""
    
    # Check storage limit
    current_usage = await get_storage_usage(license_id)
    if current_usage + (file_size or 0) > MAX_STORAGE_PER_LICENSE:
        raise ValueError("تجاوزت حد التخزين المسموح به")

    if file_size and file_size > MAX_FILE_SIZE:
        raise ValueError(f"حجم الملف كبير جداً (الحد الأقصى {MAX_FILE_SIZE / 1024 / 1024}MB)")

    now = datetime.utcnow()
    ts_value = now if DB_TYPE == "postgresql" else now.isoformat()
    
    async with get_db() as db:
        await execute_sql(
            db,
            """
            INSERT INTO library_items 
            (license_key_id, customer_id, type, title, content, file_path, file_size, mime_type, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [license_id, customer_id, item_type, title, content, file_path, file_size, mime_type, ts_value, ts_value]
        )
        await commit_db(db)
        
        # Fetch the created item
        row = await fetch_one(
            db,
            "SELECT * FROM library_items WHERE license_key_id = ? ORDER BY id DESC LIMIT 1",
            [license_id]
        )
        return dict(row)

async def update_library_item(
    license_id: int,
    item_id: int,
    **kwargs
) -> bool:
    """Update library item metadata or content."""
    allowed_fields = ['title', 'content', 'customer_id']
    updates = {k: v for k, v in kwargs.items() if k in allowed_fields}
    
    if not updates:
        return False
    
    now = datetime.utcnow()
    ts_value = now if DB_TYPE == "postgresql" else now.isoformat()
    updates['updated_at'] = ts_value
    
    set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
    values = list(updates.values()) + [item_id, license_id]

    async with get_db() as db:
        await execute_sql(
            db,
            f"UPDATE library_items SET {set_clause} WHERE id = ? AND license_key_id = ?",
            values
        )
        await commit_db(db)
        return True

async def delete_library_item(license_id: int, item_id: int) -> bool:
    """Soft delete a library item."""
    now = datetime.utcnow()
    ts_value = now if DB_TYPE == "postgresql" else now.isoformat()
    
    async with get_db() as db:
        item = await fetch_one(db, "SELECT file_path FROM library_items WHERE id = ? AND license_key_id = ?", [item_id, license_id])
        if not item:
            return False
            
        await execute_sql(
            db,
            "UPDATE library_items SET deleted_at = ? WHERE id = ? AND license_key_id = ?",
            [ts_value, item_id, license_id]
        )
        await commit_db(db)
        return True

async def bulk_delete_items(license_id: int, item_ids: List[int]) -> int:
    """Bulk soft delete library items."""
    if not item_ids:
        return 0
        
    now = datetime.utcnow()
    ts_value = now if DB_TYPE == "postgresql" else now.isoformat()
    
    # SQLite doesn't support multiple ? in IN clause easily, so we build it manually
    id_placeholders = ",".join(["?"] * len(item_ids))
    
    async with get_db() as db:
        await execute_sql(
            db,
            f"UPDATE library_items SET deleted_at = ? WHERE license_key_id = ? AND id IN ({id_placeholders})",
            [ts_value, license_id] + item_ids
        )
        await commit_db(db)
        return len(item_ids)

async def get_storage_usage(license_id: int) -> int:
    """Get total storage usage in bytes for a license."""
    async with get_db() as db:
        row = await fetch_one(
            db,
            "SELECT SUM(file_size) as total FROM library_items WHERE license_key_id = ? AND deleted_at IS NULL",
            [license_id]
        )
        return int(row["total"] or 0)
