"""
Al-Mudeer - Purchases Model
CRUD operations for customer purchases/transactions
"""

from datetime import datetime
from typing import Optional, List
from decimal import Decimal

from db_helper import get_db, execute_sql, fetch_all, fetch_one, commit_db, DB_TYPE


async def init_ifc_ledger():
    """Ensure the purchases table has Sharia-compliant financial tracking fields."""
    from db_helper import get_db, execute_sql, commit_db, DB_TYPE
    from logging_config import get_logger
    
    logger = get_logger(__name__)
    logger.info("Initializing IFC ledger fields...")
    
    async with get_db() as db:
        # 1. Add payment_type (spot/deferred)
        try:
            await execute_sql(db, "ALTER TABLE purchases ADD COLUMN payment_type TEXT DEFAULT 'spot'")
        except Exception:
            pass
            
        # 2. Add qard_status (active/repaid/waived)
        try:
            await execute_sql(db, "ALTER TABLE purchases ADD COLUMN qard_status TEXT")
        except Exception:
            pass

        # 3. Add is_interest_free (always True for IFC, but good for transparency)
        try:
            if DB_TYPE == "postgresql":
                await execute_sql(db, "ALTER TABLE purchases ADD COLUMN is_interest_free BOOLEAN DEFAULT TRUE")
            else:
                await execute_sql(db, "ALTER TABLE purchases ADD COLUMN is_interest_free INTEGER DEFAULT 1")
        except Exception:
            pass
            
        await commit_db(db)
        logger.info("✅ IFC ledger fields initialized successfully")


async def create_purchase(
    license_id: int,
    customer_id: int,
    product_name: str,
    amount: float,
    currency: str = "SYP",
    status: str = "completed",
    notes: str = None,
    purchase_date: datetime = None,
    payment_type: str = "spot",
    qard_status: str = None,
    is_interest_free: bool = True
) -> dict:
    """Create a new purchase record for a customer with IFC support."""
    if purchase_date is None:
        purchase_date = datetime.utcnow()
    
    async with get_db() as db:
        await execute_sql(
            db,
            """
            INSERT INTO purchases (
                license_key_id, customer_id, product_name, amount, currency, 
                status, notes, purchase_date, payment_type, qard_status, is_interest_free
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                license_id, customer_id, product_name, amount, currency, 
                status, notes, purchase_date, payment_type, qard_status, 
                1 if is_interest_free else 0
            ]
        )
        await commit_db(db)
        
        # Fetch the created purchase
        row = await fetch_one(
            db,
            """
            SELECT * FROM purchases 
            WHERE license_key_id = ? AND customer_id = ?
            ORDER BY id DESC LIMIT 1
            """,
            [license_id, customer_id]
        )
        
        # Update customer lifetime value
        await update_customer_lifetime_value(license_id, customer_id)
        
        return dict(row) if row else {}


async def get_customer_purchases(
    license_id: int,
    customer_id: int,
    limit: int = 50
) -> List[dict]:
    """Get all purchases for a specific customer."""
    async with get_db() as db:
        rows = await fetch_all(
            db,
            """
            SELECT * FROM purchases 
            WHERE license_key_id = ? AND customer_id = ?
            ORDER BY purchase_date DESC
            LIMIT ?
            """,
            [license_id, customer_id, limit]
        )
        return rows


async def get_purchase(
    license_id: int,
    purchase_id: int
) -> Optional[dict]:
    """Get a specific purchase by ID."""
    async with get_db() as db:
        row = await fetch_one(
            db,
            "SELECT * FROM purchases WHERE id = ? AND license_key_id = ?",
            [purchase_id, license_id]
        )
        return dict(row) if row else None


async def update_purchase(
    license_id: int,
    purchase_id: int,
    **kwargs
) -> bool:
    """Update a purchase record."""
    allowed_fields = [
        'product_name', 'amount', 'currency', 'status', 'notes', 
        'purchase_date', 'payment_type', 'qard_status', 'is_interest_free'
    ]
    updates = {k: v for k, v in kwargs.items() if k in allowed_fields and v is not None}
    
    if not updates:
        return False
    
    set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
    values = list(updates.values()) + [purchase_id, license_id]
    
    async with get_db() as db:
        await execute_sql(
            db,
            f"UPDATE purchases SET {set_clause} WHERE id = ? AND license_key_id = ?",
            values
        )
        await commit_db(db)
        
        # Get customer_id to update lifetime value
        purchase = await fetch_one(
            db,
            "SELECT customer_id FROM purchases WHERE id = ? AND license_key_id = ?",
            [purchase_id, license_id]
        )
        if purchase:
            await update_customer_lifetime_value(license_id, purchase["customer_id"])
        
        return True


async def delete_purchase(
    license_id: int,
    purchase_id: int
) -> bool:
    """Delete a purchase record."""
    async with get_db() as db:
        # Get customer_id before deleting
        purchase = await fetch_one(
            db,
            "SELECT customer_id FROM purchases WHERE id = ? AND license_key_id = ?",
            [purchase_id, license_id]
        )
        
        if not purchase:
            return False
        
        customer_id = purchase["customer_id"]
        
        await execute_sql(
            db,
            "DELETE FROM purchases WHERE id = ? AND license_key_id = ?",
            [purchase_id, license_id]
        )
        await commit_db(db)
        
        # Update customer lifetime value
        await update_customer_lifetime_value(license_id, customer_id)
        
        return True


async def update_customer_lifetime_value(
    license_id: int,
    customer_id: int
) -> float:
    """
    Calculate and update the lifetime value for a customer.
    Lifetime value = sum of all completed purchases.
    """
    async with get_db() as db:
        row = await fetch_one(
            db,
            """
            SELECT COALESCE(SUM(amount), 0) as total
            FROM purchases 
            WHERE license_key_id = ? AND customer_id = ? AND status = 'completed'
            """,
            [license_id, customer_id]
        )
        
        lifetime_value = float(row["total"]) if row else 0.0
        
        await execute_sql(
            db,
            "UPDATE customers SET lifetime_value = ? WHERE id = ? AND license_key_id = ?",
            [lifetime_value, customer_id, license_id]
        )
        await commit_db(db)
        
        return lifetime_value


async def get_customer_lifetime_value(
    license_id: int,
    customer_id: int
) -> float:
    """Get the lifetime value for a customer."""
    async with get_db() as db:
        row = await fetch_one(
            db,
            "SELECT lifetime_value FROM customers WHERE id = ? AND license_key_id = ?",
            [customer_id, license_id]
        )
        return float(row["lifetime_value"]) if row and row.get("lifetime_value") else 0.0
