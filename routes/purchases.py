"""
Al-Mudeer - Purchases API Routes
REST API for managing customer purchases
"""

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime

from models.purchases import (
    create_purchase,
    get_customer_purchases,
    get_purchase,
    update_purchase,
    delete_purchase,
    get_customer_lifetime_value
)
from dependencies import get_license_from_header
from security import sanitize_string

router = APIRouter(prefix="/api", tags=["Purchases"])


# ============ Schemas ============

class PurchaseCreate(BaseModel):
    product_name: str = Field(..., min_length=1, max_length=200)
    amount: float = Field(..., gt=0)
    currency: str = Field(default="SYP", max_length=10)
    status: str = Field(default="completed")
    notes: Optional[str] = Field(default=None, max_length=500)
    purchase_date: Optional[datetime] = None
    payment_type: str = Field(default="spot", description="spot or deferred")
    qard_status: Optional[str] = Field(default=None, description="active, repaid, waived")
    is_interest_free: bool = Field(default=True)


class PurchaseUpdate(BaseModel):
    product_name: Optional[str] = Field(default=None, max_length=200)
    amount: Optional[float] = Field(default=None, gt=0)
    currency: Optional[str] = Field(default=None, max_length=10)
    status: Optional[str] = None
    notes: Optional[str] = Field(default=None, max_length=500)
    purchase_date: Optional[datetime] = None
    payment_type: Optional[str] = None
    qard_status: Optional[str] = None
    is_interest_free: Optional[bool] = None


# ============ Routes ============

@router.get("/customers/{customer_id}/purchases")
async def list_customer_purchases(
    customer_id: int,
    limit: int = 50,
    license: dict = Depends(get_license_from_header)
):
    """Get all purchases for a customer."""
    purchases = await get_customer_purchases(
        license["license_id"],
        customer_id,
        limit
    )
    
    # Calculate lifetime value
    lifetime_value = await get_customer_lifetime_value(
        license["license_id"],
        customer_id
    )
    
    return {
        "purchases": purchases,
        "total": len(purchases),
        "lifetime_value": lifetime_value
    }


@router.post("/customers/{customer_id}/purchases")
async def add_customer_purchase(
    customer_id: int,
    data: PurchaseCreate,
    license: dict = Depends(get_license_from_header)
):
    """Add a new purchase for a customer."""
    # Sanitize text fields
    product_name = sanitize_string(data.product_name, max_length=200)
    notes = sanitize_string(data.notes, max_length=500) if data.notes else None
    
    if not product_name:
        raise HTTPException(status_code=400, detail="اسم المنتج مطلوب")
    
    purchase = await create_purchase(
        license_id=license["license_id"],
        customer_id=customer_id,
        product_name=product_name,
        amount=data.amount,
        currency=data.currency,
        status=data.status,
        notes=notes,
        purchase_date=data.purchase_date,
        payment_type=data.payment_type,
        qard_status=data.qard_status,
        is_interest_free=data.is_interest_free
    )
    
    return {
        "success": True,
        "purchase": purchase,
        "message": "تم إضافة عملية الشراء بنجاح"
    }


@router.get("/purchases/{purchase_id}")
async def get_purchase_detail(
    purchase_id: int,
    license: dict = Depends(get_license_from_header)
):
    """Get a specific purchase by ID."""
    purchase = await get_purchase(license["license_id"], purchase_id)
    
    if not purchase:
        raise HTTPException(status_code=404, detail="عملية الشراء غير موجودة")
    
    return {"purchase": purchase}


@router.patch("/purchases/{purchase_id}")
async def update_purchase_detail(
    purchase_id: int,
    data: PurchaseUpdate,
    license: dict = Depends(get_license_from_header)
):
    """Update a purchase."""
    raw_data = data.dict(exclude_none=True)
    
    # Sanitize text fields
    if "product_name" in raw_data:
        raw_data["product_name"] = sanitize_string(raw_data["product_name"], max_length=200)
    if "notes" in raw_data:
        raw_data["notes"] = sanitize_string(raw_data["notes"], max_length=500)
    
    success = await update_purchase(
        license["license_id"],
        purchase_id,
        **raw_data
    )
    
    if not success:
        raise HTTPException(status_code=400, detail="فشل تحديث عملية الشراء")
    
    return {"success": True, "message": "تم تحديث عملية الشراء بنجاح"}


@router.delete("/purchases/{purchase_id}")
async def delete_purchase_record(
    purchase_id: int,
    license: dict = Depends(get_license_from_header)
):
    """Delete a purchase."""
    success = await delete_purchase(license["license_id"], purchase_id)
    
    if not success:
        raise HTTPException(status_code=404, detail="عملية الشراء غير موجودة")
    
    return {"success": True, "message": "تم حذف عملية الشراء بنجاح"}



