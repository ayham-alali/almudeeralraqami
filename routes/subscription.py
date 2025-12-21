"""
Al-Mudeer - Subscription Key Management Routes
Easy subscription key generation and management for clients
"""

import os
from fastapi import APIRouter, HTTPException, Depends, Header
from pydantic import BaseModel, Field, EmailStr
from typing import Optional, List
from datetime import datetime, timedelta
from dotenv import load_dotenv

from database import generate_license_key, validate_license_key
from security import validate_license_key_format

# Load environment variables
load_dotenv()

router = APIRouter(prefix="/api/admin/subscription", tags=["Subscription Management"])

# Admin authentication
ADMIN_KEY = os.getenv("ADMIN_KEY")
if not ADMIN_KEY:
    raise ValueError("ADMIN_KEY environment variable is required")


async def verify_admin(x_admin_key: str = Header(None, alias="X-Admin-Key")):
    """Verify admin key"""
    if not x_admin_key or x_admin_key != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="غير مصرح - Admin key required")


# ============ Schemas ============

class SubscriptionCreate(BaseModel):
    """Request to create a new subscription"""
    company_name: str = Field(..., description="اسم الشركة", min_length=2, max_length=200)
    contact_phone: Optional[str] = Field(None, description="رقم الهاتف")
    days_valid: int = Field(365, description="مدة الصلاحية بالأيام", ge=1, le=3650)
    max_requests_per_day: int = Field(50, description="الحد الأقصى للطلبات اليومية")


class SubscriptionResponse(BaseModel):
    """Response with subscription details"""
    success: bool
    subscription_key: str
    company_name: str
    expires_at: str
    max_requests_per_day: int
    message: str


class SubscriptionListResponse(BaseModel):
    """Response for subscription list"""
    subscriptions: List[dict]
    total: int


class SubscriptionUpdate(BaseModel):
    """Request to update subscription"""
    is_active: Optional[bool] = None
    max_requests_per_day: Optional[int] = Field(None, ge=10, le=100000)
    days_valid_extension: Optional[int] = Field(None, description="إضافة أيام للصلاحية", ge=0, le=3650)
    notes: Optional[str] = Field(None, max_length=500)


# ============ Routes ============

@router.post("/create", response_model=SubscriptionResponse)
async def create_subscription(
    subscription: SubscriptionCreate,
    _: None = Depends(verify_admin)
):
    """
    Create a new subscription key for a client.
    
    This endpoint allows easy generation of subscription keys with customizable
    validity period and request limits.
    """
    import os
    from logging_config import get_logger
    
    logger = get_logger(__name__)
    
    try:
        # Generate the subscription key
        key = await generate_license_key(
            company_name=subscription.company_name,
            days_valid=subscription.days_valid,
            max_requests=subscription.max_requests_per_day
        )
        
        # Calculate expiration date
        expires_at = datetime.now() + timedelta(days=subscription.days_valid)
        
        # Save additional metadata if needed (contact_phone, notes)
        # This could be stored in a separate table or as JSON in license_keys
        
        logger.info(f"Created subscription for {subscription.company_name}: {key[:20]}...")
        
        return SubscriptionResponse(
            success=True,
            subscription_key=key,
            company_name=subscription.company_name,
            expires_at=expires_at.isoformat(),
            max_requests_per_day=subscription.max_requests_per_day,
            message=f"تم إنشاء اشتراك بنجاح لـ {subscription.company_name}"
        )
    
    except Exception as e:
        logger.error(f"Error creating subscription: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"حدث خطأ أثناء إنشاء الاشتراك: {str(e)}"
        )


@router.get("/list", response_model=SubscriptionListResponse)
async def list_subscriptions(
    active_only: bool = False,
    limit: int = 100,
    _: None = Depends(verify_admin)
):
    """
    List all subscriptions with filtering options.
    """
    import os
    from database import DB_TYPE, DATABASE_PATH, DATABASE_URL, POSTGRES_AVAILABLE
    
    try:
        subscriptions = []
        
        if DB_TYPE == "postgresql" and POSTGRES_AVAILABLE:
            import asyncpg
            if not DATABASE_URL:
                raise ValueError("DATABASE_URL is required for PostgreSQL")
            conn = await asyncpg.connect(DATABASE_URL)
            try:
                query = "SELECT id, company_name, contact_email, is_active, created_at, expires_at, max_requests_per_day, requests_today, last_request_date FROM license_keys"
                params = []
                
                if active_only:
                    query += " WHERE is_active = TRUE"
                
                query += " ORDER BY created_at DESC LIMIT $1"
                params.append(limit)
                
                rows = await conn.fetch(query, *params)
                
                for row in rows:
                    row_dict = dict(row)
                    # Calculate days remaining
                    if row_dict.get("expires_at"):
                        if isinstance(row_dict["expires_at"], str):
                            expires = datetime.fromisoformat(row_dict["expires_at"])
                        else:
                            expires = row_dict["expires_at"]
                        days_remaining = (expires - datetime.now()).days
                        row_dict["days_remaining"] = max(0, days_remaining)
                    else:
                        row_dict["days_remaining"] = None
                    
                    subscriptions.append(row_dict)
            finally:
                await conn.close()
        else:
            import aiosqlite
            async with aiosqlite.connect(DATABASE_PATH) as db:
                db.row_factory = aiosqlite.Row
                
                query = "SELECT id, company_name, contact_email, is_active, created_at, expires_at, max_requests_per_day, requests_today, last_request_date FROM license_keys"
                params = []
                
                if active_only:
                    query += " WHERE is_active = 1"
                
                query += " ORDER BY created_at DESC LIMIT ?"
                params.append(limit)
                
                async with db.execute(query, params) as cursor:
                    rows = await cursor.fetchall()
                    
                    for row in rows:
                        row_dict = dict(row)
                        # Calculate days remaining
                        if row_dict.get("expires_at"):
                            expires = datetime.fromisoformat(row_dict["expires_at"])
                            days_remaining = (expires - datetime.now()).days
                            row_dict["days_remaining"] = max(0, days_remaining)
                        else:
                            row_dict["days_remaining"] = None
                        
                        subscriptions.append(row_dict)
        
        return SubscriptionListResponse(
            subscriptions=subscriptions,
            total=len(subscriptions)
        )
    
    except Exception as e:
        from logging_config import get_logger
        logger = get_logger(__name__)
        logger.error(f"Error listing subscriptions: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="حدث خطأ أثناء جلب الاشتراكات")


@router.get("/{license_id}")
async def get_subscription(
    license_id: int,
    _: None = Depends(verify_admin)
):
    """Get details of a specific subscription"""
    from database import get_license_key_by_id, DB_TYPE
    from db_helper import get_db, fetch_one
    
    try:
        async with get_db() as db:
            # Use fetch_one which handles SQL conversion automatically
            row = await fetch_one(db, "SELECT * FROM license_keys WHERE id = ?", [license_id])
            
            if not row:
                raise HTTPException(status_code=404, detail="الاشتراك غير موجود")
            
            subscription = dict(row)
            
            # Get the original license key (decrypted)
            try:
                license_key = await get_license_key_by_id(license_id)
                subscription["license_key"] = license_key
                if not license_key:
                    # Log why key is not available
                    from logging_config import get_logger
                    logger = get_logger(__name__)
                    logger.warning(f"License key not found for subscription {license_id} - may be an old subscription created before encryption was added")
            except Exception as e:
                # If key retrieval fails, set to None and log
                from logging_config import get_logger
                logger = get_logger(__name__)
                logger.error(f"Error retrieving license key for subscription {license_id}: {e}", exc_info=True)
                subscription["license_key"] = None
            
            # Calculate days remaining
            if subscription.get("expires_at"):
                if isinstance(subscription["expires_at"], str):
                    expires = datetime.fromisoformat(subscription["expires_at"])
                else:
                    expires = subscription["expires_at"]
                days_remaining = (expires - datetime.now()).days
                subscription["days_remaining"] = max(0, days_remaining)
            else:
                subscription["days_remaining"] = None
            
            # Calculate usage statistics
            today = datetime.now().date()
            last_request_date = subscription.get("last_request_date")
            if isinstance(last_request_date, str):
                last_request_date = datetime.fromisoformat(last_request_date).date()
            elif last_request_date:
                if hasattr(last_request_date, 'date'):
                    last_request_date = last_request_date.date()
                elif isinstance(last_request_date, datetime):
                    last_request_date = last_request_date.date()
            
            if last_request_date == today:
                subscription["requests_today"] = subscription.get("requests_today", 0)
            else:
                subscription["requests_today"] = 0
            
            return {"subscription": subscription}
    
    except HTTPException:
        raise
    except Exception as e:
        from logging_config import get_logger
        logger = get_logger(__name__)
        logger.error(f"Error getting subscription: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="حدث خطأ أثناء جلب الاشتراك")


@router.patch("/{license_id}")
async def update_subscription(
    license_id: int,
    update: SubscriptionUpdate,
    _: None = Depends(verify_admin)
):
    """Update subscription settings"""
    from database import DB_TYPE, DATABASE_PATH, DATABASE_URL, POSTGRES_AVAILABLE
    from db_helper import get_db, fetch_one, execute_sql, commit_db
    from logging_config import get_logger
    
    logger = get_logger(__name__)
    
    try:
        async with get_db() as db:
            # Get current subscription
            row = await fetch_one(db, "SELECT * FROM license_keys WHERE id = ?", [license_id])
            
            if not row:
                raise HTTPException(status_code=404, detail="الاشتراك غير موجود")
            
            current = dict(row)
            
            # Build update query
            updates = []
            params = []
            param_index = 1
            
            if update.is_active is not None:
                if DB_TYPE == "postgresql":
                    updates.append(f"is_active = ${param_index}")
                else:
                    updates.append("is_active = ?")
                params.append(update.is_active)
                param_index += 1
            
            if update.max_requests_per_day is not None:
                if DB_TYPE == "postgresql":
                    updates.append(f"max_requests_per_day = ${param_index}")
                else:
                    updates.append("max_requests_per_day = ?")
                params.append(update.max_requests_per_day)
                param_index += 1
            
            if update.days_valid_extension is not None and update.days_valid_extension > 0:
                # Extend expiration date
                if current.get("expires_at"):
                    if isinstance(current["expires_at"], str):
                        current_expires = datetime.fromisoformat(current["expires_at"])
                    else:
                        current_expires = current["expires_at"]
                else:
                    current_expires = datetime.now()
                
                new_expires = current_expires + timedelta(days=update.days_valid_extension)
                
                if DB_TYPE == "postgresql":
                    updates.append(f"expires_at = ${param_index}")
                    params.append(new_expires)
                else:
                    updates.append("expires_at = ?")
                    params.append(new_expires.isoformat())
                param_index += 1
            
            if not updates:
                raise HTTPException(status_code=400, detail="لا توجد تحديثات لتطبيقها")
            
            # Execute update
            if DB_TYPE == "postgresql":
                query = f"UPDATE license_keys SET {', '.join(updates)} WHERE id = ${param_index}"
            else:
                query = f"UPDATE license_keys SET {', '.join(updates)} WHERE id = ?"
            params.append(license_id)
            
            await execute_sql(db, query, params)
            await commit_db(db)
            
            logger.info(f"Updated subscription {license_id}")
            
            return {
                "success": True,
                "message": "تم تحديث الاشتراك بنجاح"
            }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating subscription: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="حدث خطأ أثناء تحديث الاشتراك")


@router.post("/{license_id}/regenerate-key")
async def regenerate_subscription_key(
    license_id: int,
    _: None = Depends(verify_admin)
):
    """Regenerate and save license key for old subscriptions that don't have encrypted key"""
    from database import DB_TYPE, hash_license_key
    from db_helper import get_db, fetch_one, execute_sql, commit_db
    from security import encrypt_sensitive_data
    from logging_config import get_logger
    import secrets
    
    logger = get_logger(__name__)
    
    try:
        async with get_db() as db:
            # Check if subscription exists
            row = await fetch_one(db, "SELECT * FROM license_keys WHERE id = ?", [license_id])
            
            if not row:
                raise HTTPException(status_code=404, detail="الاشتراك غير موجود")
            
            subscription = dict(row)
            
            # Check if key already exists
            if subscription.get('license_key_encrypted'):
                raise HTTPException(
                    status_code=400, 
                    detail="هذا الاشتراك يحتوي بالفعل على مفتاح مشفر. لا يمكن إعادة إنشاء المفتاح."
                )
            
            # Generate new key with same format
            raw_key = f"MUDEER-{secrets.token_hex(2).upper()}-{secrets.token_hex(2).upper()}-{secrets.token_hex(2).upper()}"
            key_hash = hash_license_key(raw_key)
            encrypted_key = encrypt_sensitive_data(raw_key)
            
            # Update the subscription with new key hash and encrypted key
            if DB_TYPE == "postgresql":
                await execute_sql(db, """
                    UPDATE license_keys 
                    SET key_hash = $1, license_key_encrypted = $2 
                    WHERE id = $3
                """, [key_hash, encrypted_key, license_id])
            else:
                await execute_sql(db, """
                    UPDATE license_keys 
                    SET key_hash = ?, license_key_encrypted = ? 
                    WHERE id = ?
                """, [key_hash, encrypted_key, license_id])
            
            await commit_db(db)
            
            logger.info(f"Regenerated license key for subscription {license_id}")
            
            return {
                "success": True,
                "license_key": raw_key,
                "message": "تم إعادة إنشاء المفتاح بنجاح"
            }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error regenerating license key: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="حدث خطأ أثناء إعادة إنشاء المفتاح")


@router.delete("/{license_id}")
async def delete_subscription(
    license_id: int,
    _: None = Depends(verify_admin)
):
    """Delete a subscription permanently (hard delete)"""
    from db_helper import get_db, fetch_one, execute_sql, commit_db
    from database import DB_TYPE
    from logging_config import get_logger
    
    logger = get_logger(__name__)
    
    try:
        async with get_db() as db:
            # Check if subscription exists
            row = await fetch_one(db, "SELECT * FROM license_keys WHERE id = ?", [license_id])
            
            if not row:
                raise HTTPException(status_code=404, detail="الاشتراك غير موجود")
            
            # Hard delete: Delete related records first (CASCADE should handle this, but being explicit)
            # Delete usage logs
            if DB_TYPE == "postgresql":
                await execute_sql(db, "DELETE FROM usage_logs WHERE license_key_id = $1", [license_id])
                # Delete CRM entries
                await execute_sql(db, "DELETE FROM crm_entries WHERE license_key_id = $1", [license_id])
                # Delete email configs
                await execute_sql(db, "DELETE FROM email_configs WHERE license_key_id = $1", [license_id])
                # Delete telegram configs
                await execute_sql(db, "DELETE FROM telegram_configs WHERE license_key_id = $1", [license_id])
                # Delete the subscription
                await execute_sql(db, "DELETE FROM license_keys WHERE id = $1", [license_id])
            else:
                await execute_sql(db, "DELETE FROM usage_logs WHERE license_key_id = ?", [license_id])
                await execute_sql(db, "DELETE FROM crm_entries WHERE license_key_id = ?", [license_id])
                await execute_sql(db, "DELETE FROM email_configs WHERE license_key_id = ?", [license_id])
                await execute_sql(db, "DELETE FROM telegram_configs WHERE license_key_id = ?", [license_id])
                await execute_sql(db, "DELETE FROM license_keys WHERE id = ?", [license_id])
            
            await commit_db(db)
            
            logger.info(f"Permanently deleted subscription {license_id}")
            
            return {
                "success": True,
                "message": "تم حذف الاشتراك نهائياً"
            }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting subscription: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="حدث خطأ أثناء حذف الاشتراك")


class ValidateKeyRequest(BaseModel):
    """Request to validate a subscription key"""
    key: str = Field(..., description="Subscription key to validate")


@router.post("/validate-key")
async def validate_subscription_key(
    request: ValidateKeyRequest
):
    """
    Validate a subscription key (public endpoint, no admin required).
    Useful for clients to check their key status.
    """
    if not validate_license_key_format(request.key):
        return {
            "valid": False,
            "error": "تنسيق المفتاح غير صحيح"
        }
    
    result = await validate_license_key(request.key)
    return result


@router.get("/usage/{license_id}")
async def get_subscription_usage(
    license_id: int,
    days: int = 30,
    _: None = Depends(verify_admin)
):
    """Get usage statistics for a subscription"""
    from database import DB_TYPE
    from db_helper import get_db, fetch_all, fetch_one
    
    try:
        async with get_db() as db:
            if DB_TYPE == "postgresql":
                # PostgreSQL query - use parameterized queries with proper INTERVAL syntax
                usage_query = f"""
                    SELECT 
                        DATE(created_at) as date,
                        action_type,
                        COUNT(*) as count
                    FROM usage_logs
                    WHERE license_key_id = $1 
                    AND created_at >= NOW() - INTERVAL '{days} days'
                    GROUP BY DATE(created_at), action_type
                    ORDER BY date DESC
                """
                
                totals_query = f"""
                    SELECT 
                        COUNT(*) as total_requests,
                        COUNT(DISTINCT DATE(created_at)) as active_days
                    FROM usage_logs
                    WHERE license_key_id = $1 
                    AND created_at >= NOW() - INTERVAL '{days} days'
                """
                
                usage_stats = await fetch_all(db, usage_query, [license_id])
                totals_row = await fetch_one(db, totals_query, [license_id])
                totals = totals_row if totals_row else {"total_requests": 0, "active_days": 0}
            else:
                # SQLite query
                usage_query = """
                    SELECT 
                        DATE(created_at) as date,
                        action_type,
                        COUNT(*) as count
                    FROM usage_logs
                    WHERE license_key_id = ? 
                    AND created_at >= datetime('now', '-' || ? || ' days')
                    GROUP BY DATE(created_at), action_type
                    ORDER BY date DESC
                """
                
                totals_query = """
                    SELECT 
                        COUNT(*) as total_requests,
                        COUNT(DISTINCT DATE(created_at)) as active_days
                    FROM usage_logs
                    WHERE license_key_id = ? 
                    AND created_at >= datetime('now', '-' || ? || ' days')
                """
                
                usage_stats = await fetch_all(db, usage_query, [license_id, days])
                totals_row = await fetch_one(db, totals_query, [license_id, days])
                totals = totals_row if totals_row else {"total_requests": 0, "active_days": 0}
            
            return {
                "license_id": license_id,
                "period_days": days,
                "usage_stats": usage_stats,
                "totals": totals
            }
    
    except Exception as e:
        from logging_config import get_logger
        logger = get_logger(__name__)
        logger.error(f"Error getting usage stats: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="حدث خطأ أثناء جلب إحصائيات الاستخدام")

