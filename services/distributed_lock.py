import os
import asyncio
from logging_config import get_logger
from db_helper import get_db, execute_sql, fetch_one, DB_TYPE

logger = get_logger(__name__)

class DistributedLock:
    """
    Distributed lock mechanism using Database.
    - Postgres: Uses Advisory Locks (session-level).
    - SQLite: Uses a simple lock table with heartbeats.
    """
    
    def __init__(self, lock_id: int, lock_name: str = "telegram_listener"):
        self.lock_id = lock_id # Integer ID for Postgres Advisory Lock
        self.lock_name = lock_name
        self.locked = False
        self._keepalive_task = None
        
    async def acquire(self) -> bool:
        """Try to acquire the distributed lock"""
        try:
            async with get_db() as db:
                if DB_TYPE == "postgresql":
                    # key must be 64-bit int. We use lock_id.
                    # pg_try_advisory_lock(key) returns boolean immediately
                    row = await fetch_one(db, "SELECT pg_try_advisory_lock($1) as locked", [self.lock_id])
                    if row and row['locked']:
                        self.locked = True
                        logger.info(f"Acquired distributed lock (Postgres ID {self.lock_id})")
                        return True
                    else:
                        return False
                else:
                    # SQLite Fallback: Use table 'system_locks'
                    # Create table if not exists
                    await self._ensure_sqlite_table(db)
                    
                    # Try to insert or update if expired
                    import time
                    now = int(time.time())
                    # Expire after 30 seconds
                    
                    # 1. Clear expired locks
                    await execute_sql(db, "DELETE FROM system_locks WHERE lock_name = ? AND expires_at < ?", [self.lock_name, now])
                    
                    # 2. Try insert
                    try:
                        await execute_sql(db, "INSERT INTO system_locks (lock_name, expires_at) VALUES (?, ?)", [self.lock_name, now + 30])
                        self.locked = True
                        self._start_keepalive()
                        logger.info(f"Acquired distributed lock (SQLite {self.lock_name})")
                        return True
                    except Exception:
                        # Already exists
                        return False
                        
        except Exception as e:
            logger.error(f"Error acquiring distributed lock: {e}")
            return False

    async def release(self):
        """Release the lock"""
        if not self.locked:
            return

        if self._keepalive_task:
            self._keepalive_task.cancel()
            self._keepalive_task = None

        try:
            async with get_db() as db:
                if DB_TYPE == "postgresql":
                    await execute_sql(db, "SELECT pg_advisory_unlock($1)", [self.lock_id])
                else:
                    await execute_sql(db, "DELETE FROM system_locks WHERE lock_name = ?", [self.lock_name])
            
            logger.info("Released distributed lock")
            self.locked = False

        except Exception as e:
            logger.error(f"Error releasing lock: {e}")

    async def _ensure_sqlite_table(self, db):
        await execute_sql(db, """
            CREATE TABLE IF NOT EXISTS system_locks (
                lock_name TEXT PRIMARY KEY,
                expires_at INTEGER 
            )
        """)

    def _start_keepalive(self):
        """Start background task to refresh lock (SQLite only)"""
        async def keepalive():
            import time
            while self.locked:
                await asyncio.sleep(10)
                try:
                    async with get_db() as db:
                        now = int(time.time())
                        await execute_sql(db, "UPDATE system_locks SET expires_at = ? WHERE lock_name = ?", [now + 30, self.lock_name])
                except Exception as e:
                    logger.error(f"Error refreshing lock: {e}")
        
        self._keepalive_task = asyncio.create_task(keepalive())
