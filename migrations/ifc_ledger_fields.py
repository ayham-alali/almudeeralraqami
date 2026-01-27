"""
Al-Mudeer - IFC Ledger Fields Migration
Adds Sharia-compliant financial tracking fields to the purchases table.
"""

from logging_config import get_logger

logger = get_logger(__name__)

async def migrate_ifc_fields():
    """
    Adds payment_type, qard_status, and is_interest_free to the purchases table.
    """
    from db_helper import get_db, execute_sql, commit_db, DB_TYPE
    
    logger.info("Adding IFC ledger fields to purchases table...")
    
    async with get_db() as db:
        # Add payment_type
        try:
            await execute_sql(db, "ALTER TABLE purchases ADD COLUMN payment_type TEXT DEFAULT 'spot'")
        except Exception:
            pass # Already exists
            
        # Add qard_status
        try:
            await execute_sql(db, "ALTER TABLE purchases ADD COLUMN qard_status TEXT")
        except Exception:
            pass # Already exists

        # Add is_interest_free
        try:
            if DB_TYPE == "postgresql":
                await execute_sql(db, "ALTER TABLE purchases ADD COLUMN is_interest_free BOOLEAN DEFAULT TRUE")
            else:
                await execute_sql(db, "ALTER TABLE purchases ADD COLUMN is_interest_free INTEGER DEFAULT 1")
        except Exception:
            pass # Already exists
            
        await commit_db(db)
        logger.info("✅ IFC ledger fields added to purchases table!")
