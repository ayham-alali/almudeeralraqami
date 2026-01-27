"""
Al-Mudeer - File Storage Service
Handles saving media files to the local filesystem and generating accessible URLs.
"""

import os
import uuid
import mimetypes
import logging
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

# Base directory for uploads
# Should be in a location served by FastAPI
UPLOAD_DIR = os.getenv("UPLOAD_DIR", "static/uploads")

# Base URL for accessing files
# This will be used to construct the 'url' field in attachments
# In production, this should be the public URL
BASE_URL = os.getenv("BASE_URL", "").rstrip("/")

class FileStorageService:
    """Service for managing media file storage"""
    
    def __init__(self, upload_dir: str = UPLOAD_DIR):
        self.upload_dir = upload_dir
        # Ensure upload directory exists
        if not os.path.exists(self.upload_dir):
            os.makedirs(self.upload_dir, exist_ok=True)
            logger.info(f"Created upload directory: {self.upload_dir}")
            
    def save_file(self, content: bytes, filename: Optional[str] = None, mime_type: Optional[str] = None) -> Tuple[str, str]:
        """
        Save bytes to a file and return (relative_path, accessible_url)
        
        Args:
            content: Raw file bytes
            filename: Original filename (if available)
            mime_type: MIME type of the file
            
        Returns:
            Tuple of (relative_file_path, public_url)
        """
        try:
            # 1. Determine extension
            ext = ""
            if mime_type:
                ext = mimetypes.guess_extension(mime_type) or ""
            elif filename:
                _, ext = os.path.splitext(filename)
                
            # 2. Generate unique filename to avoid collisions
            unique_filename = f"{uuid.uuid4().hex}{ext}"
            
            # 3. Determine subfolder based on mime_type (premium organization)
            subfolder = "other"
            if mime_type:
                if mime_type.startswith("image/"):
                    subfolder = "images"
                elif mime_type.startswith("video/"):
                    subfolder = "videos"
                elif mime_type.startswith("audio/"):
                    subfolder = "audio"
                elif "pdf" in mime_type:
                    subfolder = "docs"
            
            # Create subfolder if missing
            full_subfolder_path = os.path.join(self.upload_dir, subfolder)
            if not os.path.exists(full_subfolder_path):
                os.makedirs(full_subfolder_path, exist_ok=True)
                
            # 4. Save to disk
            relative_path = os.path.join(subfolder, unique_filename).replace("\\", "/")
            full_path = os.path.join(self.upload_dir, relative_path)
            
            with open(full_path, "wb") as f:
                f.write(content)
            
            # 5. Construct URL
            # The static/ directory is usually mounted at /static
            # relative_path is something like 'images/abc.png'
            # Full URL: /static/uploads/images/abc.png
            url_path = f"/static/uploads/{relative_path}"
            
            # If BASE_URL is provided, prepend it
            if BASE_URL:
                final_url = f"{BASE_URL}{url_path}"
            else:
                final_url = url_path
                
            logger.info(f"Saved file: {relative_path} (URL: {final_url})")
            return relative_path, final_url
            
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
