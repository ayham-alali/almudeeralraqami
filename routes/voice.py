"""
Al-Mudeer - Voice Message Routes
Handles voice message upload, transcription, and retrieval
Uses S3 for production storage, local for development
"""

import os
import uuid
import tempfile
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, UploadFile, File, HTTPException, Depends
from pydantic import BaseModel

from dependencies import get_license_from_header
from services.voice_service import transcribe_voice_message
from services.file_storage_service import get_file_storage
from logging_config import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/voice", tags=["Voice Messages"])

# File storage service instance
file_storage = get_file_storage()


class VoiceUploadResponse(BaseModel):
    success: bool
    audio_url: Optional[str] = None
    duration: Optional[int] = None  # seconds
    transcript: Optional[str] = None
    error: Optional[str] = None


# S3 and local save functions are now handled by FileStorageService
# (Keeping internal logic if needed, but the service provides a better abstraction)


async def get_audio_duration(file_data: bytes, filename: str) -> int:
    """
    Get audio duration in seconds.
    Uses mutagen library if available, otherwise estimates.
    """
    try:
        from mutagen import File as MutagenFile
        from mutagen.ogg import OggFileType
        from mutagen.mp3 import MP3
        from mutagen.mp4 import MP4
        
        # Save to temp file for mutagen
        suffix = os.path.splitext(filename)[1] or '.ogg'
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
            tmp.write(file_data)
            tmp_path = tmp.name
        
        try:
            audio = MutagenFile(tmp_path)
            if audio and audio.info:
                return int(audio.info.length)
        finally:
            os.unlink(tmp_path)
            
    except ImportError:
        logger.debug("mutagen not installed, estimating duration")
    except Exception as e:
        logger.debug(f"Could not get exact duration: {e}")
    
    # Estimate duration from file size (rough: ~16KB per second for OGG)
    size_kb = len(file_data) / 1024
    estimated_duration = max(1, int(size_kb / 16))
    return estimated_duration


@router.post("/upload", response_model=VoiceUploadResponse)
async def upload_voice_message(
    file: UploadFile = File(...),
    current_user: dict = Depends(get_license_from_header)
):
    """
    Upload a voice message, transcribe it, and return the URL.
    
    - Accepts: audio/ogg, audio/webm, audio/mp3, audio/wav, audio/m4a
    - Transcribes using OpenAI Whisper
    - Stores in S3 (production) or local filesystem (development)
    
    Returns:
        - audio_url: URL to access the audio file
        - duration: Audio duration in seconds
        - transcript: Transcribed text (Arabic)
    """
    # Validate file type
    allowed_types = ['audio/ogg', 'audio/webm', 'audio/mp3', 'audio/mpeg', 'audio/wav', 'audio/x-m4a', 'audio/mp4']
    
    if file.content_type not in allowed_types:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid file type. Allowed: {', '.join(allowed_types)}"
        )
    
    try:
        # Read file data
        file_data = await file.read()
        
        if len(file_data) == 0:
            raise HTTPException(status_code=400, detail="Empty file")
        
        # Generate unique filename
        ext = os.path.splitext(file.filename)[1] or '.ogg'
        unique_filename = f"{uuid.uuid4().hex}{ext}"
        
        # Save file using storage service
        _, audio_url = file_storage.save_file(
            content=file_data,
            filename=file.filename,
            mime_type=file.content_type,
            subfolder="voice"
        )
        
        # Get duration
        duration = await get_audio_duration(file_data, unique_filename)
        
        # Transcribe
        transcript_result = await transcribe_voice_message(file_data, file.filename or "audio.ogg")
        transcript = transcript_result.get("text", "") if transcript_result.get("success") else ""
        
        logger.info(f"Voice upload complete: {audio_url}, duration={duration}s, transcript_len={len(transcript)}")
        
        return VoiceUploadResponse(
            success=True,
            audio_url=audio_url,
            duration=duration,
            transcript=transcript
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Voice upload failed: {e}")
        return VoiceUploadResponse(
            success=False,
            error=f"Upload failed: {str(e)}"
        )


@router.post("/transcribe")
async def transcribe_audio(
    file: UploadFile = File(...),
    current_user: dict = Depends(get_license_from_header)
):
    """
    Transcribe an audio file without storing it.
    Useful for previewing transcription before sending.
    """
    try:
        file_data = await file.read()
        
        if len(file_data) == 0:
            raise HTTPException(status_code=400, detail="Empty file")
        
        result = await transcribe_voice_message(file_data, file.filename or "audio.ogg")
        
        if result.get("success"):
            return {
                "success": True,
                "text": result.get("text", ""),
                "language": result.get("language", "ar"),
                "duration": result.get("duration", 0)
            }
        else:
            return {
                "success": False,
                "error": result.get("error", "Transcription failed")
            }
            
    except Exception as e:
        logger.error(f"Transcription failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
