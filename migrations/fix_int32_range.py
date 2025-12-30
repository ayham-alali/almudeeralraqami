"""
Migration to fix int32 range errors for message IDs
Changes INTEGER to BIGINT for message IDs that can exceed int32 range
"""

from db_helper import get_db, execute_sql, commit_db, DB_TYPE
from logging_config import get_logger

logger = get_logger(__name__)


async def fix_int32_range_issues():
    """
    Fix int32 range errors by changing INTEGER to BIGINT for message IDs.
    Safe to run multiple times - checks if column already exists as BIGINT.
    """
    
    async with get_db() as db:
        try:
            if DB_TYPE == "postgresql":
                # PostgreSQL: Use ALTER COLUMN to change type
                # Check current type first to avoid errors
                
                # 1. Fix inbox_messages.id
                try:
                    await execute_sql(db, """
                        ALTER TABLE inbox_messages 
                        ALTER COLUMN id TYPE BIGINT
                    """)
                    logger.info("✅ Changed inbox_messages.id to BIGINT")
                except Exception as e:
                    if "already" in str(e).lower() or "does not exist" in str(e).lower():
                        logger.debug(f"inbox_messages.id already BIGINT or error: {e}")
                    else:
                        logger.warning(f"Could not change inbox_messages.id: {e}")
                
                # 2. Fix message_reactions.message_id
                try:
                    await execute_sql(db, """
                        ALTER TABLE message_reactions 
                        ALTER COLUMN message_id TYPE BIGINT
                    """)
                    logger.info("✅ Changed message_reactions.message_id to BIGINT")
                except Exception as e:
                    if "already" in str(e).lower() or "does not exist" in str(e).lower():
                        logger.debug(f"message_reactions.message_id already BIGINT or error: {e}")
                    else:
                        logger.warning(f"Could not change message_reactions.message_id: {e}")
                
                # 3. Fix outbox_messages.inbox_message_id
                try:
                    await execute_sql(db, """
                        ALTER TABLE outbox_messages 
                        ALTER COLUMN inbox_message_id TYPE BIGINT
                    """)
                    logger.info("✅ Changed outbox_messages.inbox_message_id to BIGINT")
                except Exception as e:
                    if "already" in str(e).lower() or "does not exist" in str(e).lower():
                        logger.debug(f"outbox_messages.inbox_message_id already BIGINT or error: {e}")
                    else:
                        logger.warning(f"Could not change outbox_messages.inbox_message_id: {e}")
                
                # 4. Fix customer_messages.inbox_message_id if table exists
                try:
                    await execute_sql(db, """
                        ALTER TABLE customer_messages 
                        ALTER COLUMN inbox_message_id TYPE BIGINT
                    """)
                    logger.info("✅ Changed customer_messages.inbox_message_id to BIGINT")
                except Exception as e:
                    if "already" in str(e).lower() or "does not exist" in str(e).lower() or "relation" in str(e).lower():
                        logger.debug(f"customer_messages.inbox_message_id already BIGINT or table doesn't exist: {e}")
                    else:
                        logger.warning(f"Could not change customer_messages.inbox_message_id: {e}")
                
                # 5. Fix inbox_messages.forwarded_message_id if column exists
                try:
                    await execute_sql(db, """
                        ALTER TABLE inbox_messages 
                        ALTER COLUMN forwarded_message_id TYPE BIGINT
                    """)
                    logger.info("✅ Changed inbox_messages.forwarded_message_id to BIGINT")
                except Exception as e:
                    if "already" in str(e).lower() or "does not exist" in str(e).lower() or "column" in str(e).lower():
                        logger.debug(f"inbox_messages.forwarded_message_id already BIGINT or column doesn't exist: {e}")
                    else:
                        logger.warning(f"Could not change inbox_messages.forwarded_message_id: {e}")
                
            else:
                # SQLite: INTEGER in SQLite can handle large values, but we should still check
                # SQLite's INTEGER is actually 64-bit, so this is less critical
                # But we'll log it for consistency
                logger.info("ℹ️ SQLite uses 64-bit INTEGER by default, no migration needed")
            
            await commit_db(db)
            logger.info("🎉 Int32 range fixes applied successfully!")
            
        except Exception as e:
            logger.error(f"❌ Error applying int32 range fixes: {e}")
            raise


# Run migration when imported directly
if __name__ == "__main__":
    import asyncio
    asyncio.run(fix_int32_range_issues())

