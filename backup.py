"""
Al-Mudeer Backup Utilities
Database backup and restore functionality
"""

import os
import shutil
import gzip
from datetime import datetime
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class BackupManager:
    """
    Simple backup manager for SQLite databases.
    For PostgreSQL, use pg_dump/pg_restore.
    """
    
    def __init__(self, backup_dir: str = "backups"):
        self.backup_dir = backup_dir
        os.makedirs(backup_dir, exist_ok=True)
    
    def create_backup(
        self,
        db_path: str,
        compress: bool = True,
        prefix: str = "backup"
    ) -> Optional[str]:
        """
        Create a backup of the SQLite database.
        
        Args:
            db_path: Path to the SQLite database file
            compress: Whether to gzip the backup
            prefix: Prefix for the backup filename
            
        Returns:
            Path to the backup file, or None if failed
        """
        if not os.path.exists(db_path):
            logger.error(f"Database file not found: {db_path}")
            return None
        
        # Generate backup filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"{prefix}_{timestamp}.db"
        if compress:
            backup_name += ".gz"
        
        backup_path = os.path.join(self.backup_dir, backup_name)
        
        try:
            if compress:
                # Create compressed backup
                with open(db_path, 'rb') as f_in:
                    with gzip.open(backup_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
            else:
                # Simple copy
                shutil.copy2(db_path, backup_path)
            
            logger.info(f"Backup created: {backup_path}")
            return backup_path
            
        except Exception as e:
            logger.error(f"Backup failed: {e}")
            return None
    
    def restore_backup(
        self,
        backup_path: str,
        db_path: str,
        create_current_backup: bool = True
    ) -> bool:
        """
        Restore a backup to the database path.
        
        Args:
            backup_path: Path to the backup file
            db_path: Path to restore to
            create_current_backup: Create a backup of current DB before restore
            
        Returns:
            True if successful, False otherwise
        """
        if not os.path.exists(backup_path):
            logger.error(f"Backup file not found: {backup_path}")
            return False
        
        try:
            # Optionally backup current database first
            if create_current_backup and os.path.exists(db_path):
                self.create_backup(db_path, prefix="pre_restore")
            
            # Restore based on file type
            if backup_path.endswith('.gz'):
                with gzip.open(backup_path, 'rb') as f_in:
                    with open(db_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
            else:
                shutil.copy2(backup_path, db_path)
            
            logger.info(f"Backup restored from: {backup_path}")
            return True
            
        except Exception as e:
            logger.error(f"Restore failed: {e}")
            return False
    
    def list_backups(self) -> list:
        """List all available backups sorted by date (newest first)"""
        backups = []
        for filename in os.listdir(self.backup_dir):
            if filename.startswith("backup") and (filename.endswith(".db") or filename.endswith(".db.gz")):
                path = os.path.join(self.backup_dir, filename)
                stat = os.stat(path)
                backups.append({
                    "filename": filename,
                    "path": path,
                    "size_mb": round(stat.st_size / (1024 * 1024), 2),
                    "created_at": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                })
        
        # Sort by created_at descending
        backups.sort(key=lambda x: x["created_at"], reverse=True)
        return backups
    
    def cleanup_old_backups(self, keep_count: int = 10) -> int:
        """Remove old backups, keeping only the most recent ones"""
        backups = self.list_backups()
        removed = 0
        
        for backup in backups[keep_count:]:
            try:
                os.remove(backup["path"])
                removed += 1
                logger.info(f"Removed old backup: {backup['filename']}")
            except Exception as e:
                logger.error(f"Failed to remove backup: {e}")
        
        return removed


# Global backup manager instance
backup_manager = BackupManager()


# Convenience functions
def create_backup(db_path: str = None) -> Optional[str]:
    """Create a backup using default settings"""
    db_path = db_path or os.getenv("DATABASE_PATH", "almudeer.db")
    return backup_manager.create_backup(db_path)


def list_backups() -> list:
    """List all available backups"""
    return backup_manager.list_backups()
