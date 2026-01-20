"""
Al-Mudeer Voice Service Tests
Tests for voice transcription and handling logic
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from services.voice_service import (
    transcribe_audio_openai,
    transcribe_audio_local,
    transcribe_voice_message,
    transcribe_from_base64,
    transcribe_from_url
)
import sys

class TestVoiceService:
    
    @pytest.mark.asyncio
    async def test_transcribe_audio_openai_success(self):
        """Test OpenAI transcription success"""
        with patch("services.voice_service.OPENAI_API_KEY", "test-key"), \
             patch("httpx.AsyncClient") as mock_client:
            
            mock_post = AsyncMock()
            mock_post.return_value.status_code = 200
            mock_post.return_value.json.return_value = {
                "text": "مرحبا",
                "language": "ar",
                "duration": 5.5,
                "segments": []
            }
            mock_client.return_value.__aenter__.return_value.post = mock_post
            
            result = await transcribe_audio_openai(b"fake-audio")
            
            assert result["success"] is True
            assert result["text"] == "مرحبا"
            assert result["language"] == "ar"

    @pytest.mark.asyncio
    async def test_transcribe_audio_openai_no_key(self):
        """Test OpenAI transcription fails without key"""
        with patch("services.voice_service.OPENAI_API_KEY", ""):
            result = await transcribe_audio_openai(b"fake-audio")
            assert result["success"] is False
            assert "API key not configured" in result["error"]

    @pytest.mark.asyncio
    async def test_transcribe_audio_local_fails_without_whisper(self):
        """Test Local Whisper fails gracefully when not installed"""
        # Ensure whisper is NOT in sys.modules
        with patch.dict("sys.modules"):
            if "whisper" in sys.modules:
                del sys.modules["whisper"]
            
            # We enforce ImportError by patching the function's internal import if possible, 
            # OR we just rely on the fact that whisper is likely not installed in this environment.
            # But to be safe, we can patch __import__ but that's dangerous.
            
            # Let's just check the result. If whisper IS installed, this test might fail (expecting success), 
            # so we should handle both or mock it to fail.
            
            with patch("builtins.__import__", side_effect=ImportError("No module named 'whisper'")):
                 result = await transcribe_audio_local(b"data")
                 assert result["success"] is False
                 assert "Run: pip install openai-whisper" in result["error"]
                
    @pytest.mark.asyncio
    async def test_transcribe_from_base64(self):
        """Test base64 decoding"""
        with patch("services.voice_service.transcribe_voice_message", new_callable=AsyncMock) as mock_transcribe:
            mock_transcribe.return_value = {"success": True}
            
            # "SGVsbG8=" is "Hello"
            result = await transcribe_from_base64("data:audio/ogg;base64,SGVsbG8=")
            
            assert result["success"] is True
            mock_transcribe.assert_called_once()
            call_args = mock_transcribe.call_args[0][0]
            assert call_args == b"Hello"

    @pytest.mark.asyncio
    async def test_transcribe_from_url_success(self):
        """Test fetching and transcribing from URL"""
        with patch("httpx.AsyncClient") as mock_client, \
             patch("services.voice_service.transcribe_voice_message", new_callable=AsyncMock) as mock_transcribe:
            
            mock_get = AsyncMock()
            mock_get.return_value.status_code = 200
            mock_get.return_value.content = b"audio-data"
            mock_client.return_value.__aenter__.return_value.get = mock_get
            
            mock_transcribe.return_value = {"success": True}
            
            result = await transcribe_from_url("http://example.com/audio.ogg")
            
            assert result["success"] is True
            mock_transcribe.assert_called_with(b"audio-data", "audio.ogg")
