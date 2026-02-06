"""
Al-Mudeer - File Storage Service
Handles saving media files to the local filesystem and generating accessible URLs.
"""

import os
import uuid
import logging
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

# Base directory for uploads (configurable for persistence, e.g. Railway volume)
UPLOAD_DIR = os.getenv("UPLOAD_DIR", os.path.join(os.getcwd(), "static", "uploads"))

# Base URL prefix for accessing files
UPLOAD_URL_PREFIX = os.getenv("UPLOAD_URL_PREFIX", "/static/uploads")

class FileStorageService:
    """Service for managing media file storage"""
    
    def __init__(self, upload_dir: str = UPLOAD_DIR):
        self.upload_dir = upload_dir
        self.url_prefix = UPLOAD_URL_PREFIX.rstrip("/")
        
        # Ensure upload directory exists
        if not os.path.exists(self.upload_dir):
            os.makedirs(self.upload_dir, exist_ok=True)
            logger.info(f"Created upload directory: {self.upload_dir}")
            
    def save_file(self, content: bytes, filename: str, mime_type: str, subfolder: str = None) -> Tuple[str, str]:
        """
        Save bytes to a file and return (relative_path, accessible_url)
        
        Args:
            content: Raw file bytes
            filename: Original filename
            mime_type: MIME type of the file
            subfolder: Optional subfolder (e.g. 'library', 'voice')
            
        Returns:
            Tuple of (relative_file_path, public_url)
        """
        try:
            # Determine subfolder if not provided
            if not subfolder:
                if mime_type.startswith("image/"):
                    subfolder = "images"
                elif mime_type.startswith("audio/"):
                    subfolder = "audio"
                elif mime_type.startswith("video/"):
                    subfolder = "video"
                else:
                    subfolder = "docs"
            
            # Create subfolder inside upload_dir
            target_dir = os.path.join(self.upload_dir, subfolder)
            os.makedirs(target_dir, exist_ok=True)
            
            # Unique filename to avoid collisions
            unique_id = uuid.uuid4().hex
            ext = os.path.splitext(filename)[1] or ".bin"
            unique_filename = f"{unique_id}{ext}"
            
            # Full path for saving
            file_path = os.path.join(target_dir, unique_filename)
            
            with open(file_path, "wb") as f:
                f.write(content)
                
            # Relative path for standard serving (forward slashes)
            relative_path = os.path.join(subfolder, unique_filename).replace("\\", "/")
            public_url = f"{self.url_prefix}/{relative_path}"
            
            logger.info(f"Saved file: {relative_path} (URL: {public_url})")
            return relative_path, public_url
            
        except Exception as e:
            logger.error(f"Failed to save file: {e}")
            raise

# Singleton instance
_instance = None

def get_file_storage() -> FileStorageService:
    global _instance
    if _instance is None:
        _instance = FileStorageService()
    return _instance
