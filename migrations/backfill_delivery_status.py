"""
Backfill Delivery Status for Old Outbox Messages

This migration updates old sent messages that don't have delivery_status set.
It marks them as 'sent' so they display with a single checkmark instead of clock icon.

Run: python -m migrations.backfill_delivery_status
"""

import asyncio
from db_helper import get_db, execute_sql, fetch_one, fetch_all, commit_db, DB_TYPE
from logging_config import get_logger

logger = get_logger(__name__)


async def backfill_delivery_status(dry_run: bool = False) -> dict:
    """
    Backfill delivery_status='sent' for old outbox messages that were sent
    before delivery tracking was implemented.
    
    Args:
        dry_run: If True, only count affected rows without updating
        
    Returns:
        dict with 'count' of affected rows and 'success' status
    """
    try:
        async with get_db() as db:
            # Count messages that need updating
            count_query = """
                SELECT COUNT(*) as count FROM outbox_messages 
                WHERE status = 'sent' 
                AND (delivery_status IS NULL OR delivery_status = '')
            """
            row = await fetch_one(db, count_query, [])
            count = row["count"] if row else 0
            
            logger.info(f"Found {count} outbox messages with NULL delivery_status")
            
            if dry_run:
                logger.info("DRY RUN - No changes made")
                return {"success": True, "count": count, "dry_run": True}
            
            if count == 0:
                logger.info("No messages need updating")
                return {"success": True, "count": 0}
            
            # Update all sent messages without delivery_status
            update_query = """
                UPDATE outbox_messages 
                SET delivery_status = 'sent'
                WHERE status = 'sent' 
                AND (delivery_status IS NULL OR delivery_status = '')
            """
            await execute_sql(db, update_query, [])
            await commit_db(db)
            
            logger.info(f"✅ Updated {count} messages with delivery_status='sent'")
            return {"success": True, "count": count}
            
    except Exception as e:
        logger.error(f"Failed to backfill delivery status: {e}")
        return {"success": False, "error": str(e), "count": 0}


async def verify_backfill() -> dict:
    """
    Verify the backfill was successful by checking remaining NULL delivery_status.
    """
    try:
        async with get_db() as db:
            # Count remaining NULL
            count_query = """
                SELECT COUNT(*) as count FROM outbox_messages 
                WHERE status = 'sent' 
                AND (delivery_status IS NULL OR delivery_status = '')
            """
            row = await fetch_one(db, count_query, [])
            remaining = row["count"] if row else 0
            
            # Count total with delivery_status set
            set_query = """
                SELECT delivery_status, COUNT(*) as count 
                FROM outbox_messages 
                WHERE status = 'sent'
                GROUP BY delivery_status
            """
            rows = await fetch_all(db, set_query, [])
            
            stats = {r["delivery_status"] or "NULL": r["count"] for r in rows}
            
            return {
                "remaining_null": remaining,
                "status_counts": stats,
                "success": remaining == 0
            }
            
    except Exception as e:
        logger.error(f"Failed to verify backfill: {e}")
        return {"success": False, "error": str(e)}


# CLI entry point
if __name__ == "__main__":
    import sys
    
    dry_run = "--dry-run" in sys.argv
    verify = "--verify" in sys.argv
    
    if verify:
        print("Verifying backfill status...")
        result = asyncio.run(verify_backfill())
        print(f"Result: {result}")
    else:
        print(f"Running delivery status backfill (dry_run={dry_run})...")
        result = asyncio.run(backfill_delivery_status(dry_run=dry_run))
        print(f"Result: {result}")
        
        if result["success"] and not dry_run:
            print("\nVerifying...")
            verify_result = asyncio.run(verify_backfill())
            print(f"Verification: {verify_result}")
