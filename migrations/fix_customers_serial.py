"""
Al-Mudeer - Fix Customers Table Auto-Increment
This migration fixes the customers table id column to use SERIAL (auto-increment)
"""

import os
from logging_config import get_logger

logger = get_logger(__name__)


async def fix_customers_serial():
    """
    Fix the customers table to use auto-increment for the id column.
    This is needed when the table was originally created without SERIAL.
    """
    from db_helper import get_db, execute_sql, commit_db, DB_TYPE
    
    if DB_TYPE != "postgresql":
        logger.info("SQLite uses AUTOINCREMENT by default, no fix needed")
        return
    
    logger.info("Fixing customers table serial sequence...")
    
    async with get_db() as db:
        try:
            # Create sequence if not exists
            await execute_sql(db, """
                DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM pg_sequences WHERE schemaname = 'public' AND sequencename = 'customers_id_seq') THEN
                        CREATE SEQUENCE customers_id_seq;
                    END IF;
                END $$;
            """)
            
            # Set the sequence to the max id + 1
            await execute_sql(db, """
                SELECT setval('customers_id_seq', COALESCE((SELECT MAX(id) FROM customers), 0) + 1, false);
            """)
            
            # Alter column to use the sequence as default
            await execute_sql(db, """
                ALTER TABLE customers 
                ALTER COLUMN id SET DEFAULT nextval('customers_id_seq');
            """)
            
            # Make the sequence owned by the column
            await execute_sql(db, """
                ALTER SEQUENCE customers_id_seq OWNED BY customers.id;
            """)
            
            await commit_db(db)
            logger.info("✅ Customers table serial sequence fixed!")
            
        except Exception as e:
            logger.warning(f"Customers serial fix note: {e}")
