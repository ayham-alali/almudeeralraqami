"""
Al-Mudeer - Multi-Provider LLM Service
Production-grade LLM infrastructure with automatic failover and caching

Supports:
- OpenAI (primary)
- Google Gemini (fallback)
- Rule-based (guaranteed fallback)
"""

import os
import random
import asyncio
import hashlib
import json
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any, List
from collections import OrderedDict

import httpx

from logging_config import get_logger

logger = get_logger(__name__)


# ============ Configuration ============

@dataclass
class LLMConfig:
    """LLM service configuration - Free Production Setup"""
    # OpenAI (DISABLED - kept for code compatibility, empty by default)
    openai_api_key: str = field(default_factory=lambda: os.getenv("OPENAI_API_KEY", ""))
    openai_base_url: str = field(default_factory=lambda: os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1"))
    openai_model: str = field(default_factory=lambda: os.getenv("OPENAI_MODEL", "gpt-4o"))
    
    # Provider 1: Google Gemini (PRIMARY - Free tier)
    google_api_key: str = field(default_factory=lambda: os.getenv("GOOGLE_API_KEY", ""))
    google_model: str = field(default_factory=lambda: os.getenv("GOOGLE_MODEL", "gemini-2.5-flash"))
    
    # Provider 2: OpenRouter (BACKUP - Free models)
    openrouter_api_key: str = field(default_factory=lambda: os.getenv("OPENROUTER_API_KEY", ""))
    openrouter_model: str = field(default_factory=lambda: os.getenv("OPENROUTER_MODEL", "google/gemini-2.0-flash-exp:free"))
    
    # Failover: OpenAI (if key set) → Gemini → OpenRouter → Rule-based
    
    # Retry settings - aggressive for rate limit handling
    # Gemini free tier has strict limits (15 RPM), so we wait MUCH longer between retries
    max_retries: int = 5  # Increased from 3 for better recovery
    base_delay: float = 20.0  # Aggressive delay: 20-40-80-160-320s exponential backoff
    
    # Cache settings
    cache_enabled: bool = field(default_factory=lambda: os.getenv("LLM_CACHE_ENABLED", "true").lower() == "true")
    cache_ttl_seconds: int = field(default_factory=lambda: int(os.getenv("LLM_CACHE_TTL_HOURS", "24")) * 3600)
    cache_max_size: int = 1000
    
    # Concurrency control - CRITICAL for preventing rate limits
    # Default to 1 for free tier API to avoid rate limits
    max_concurrent_requests: int = field(default_factory=lambda: int(os.getenv("LLM_MAX_CONCURRENT", "1")))
    
    # Post-request delay in seconds - adds breathing room between requests
    # Gemini free tier: 15 requests/minute = 4s minimum between requests
    # Using 6 seconds to stay safely under 10 RPM (vs 15 RPM limit)
    post_request_delay: float = field(default_factory=lambda: float(os.getenv("LLM_REQUEST_DELAY", "6.0")))


# ============ Global Concurrency Control ============

# Global semaphore to limit concurrent LLM API calls
# This prevents burst requests when multiple messages are processed simultaneously
_llm_semaphore: Optional[asyncio.Semaphore] = None


def get_llm_semaphore(max_concurrent: int = 3) -> asyncio.Semaphore:
    """Get or create global semaphore for LLM concurrency control"""
    global _llm_semaphore
    if _llm_semaphore is None:
        _llm_semaphore = asyncio.Semaphore(max_concurrent)
        logger.info(f"LLM concurrency limiter initialized: max {max_concurrent} concurrent requests")
    return _llm_semaphore


# ============ Global Rate Limiter ============

class GlobalRateLimiter:
    """
    Enforces a minimum time interval between ALL Gemini API requests.
    
    CRITICAL FEATURES:
    1. Starts with current time - forces wait on first request after startup
    2. Reports 429 errors to extend cooldown period by 60s
    3. Uses 15s minimum interval = 4 RPM max (very safe under 15 RPM limit)
    
    This prevents rate limits even when the app restarts mid-request-burst.
    """
    
    # Minimum seconds between requests (15s = max 4 requests/minute)
    # Very conservative to stay well under Gemini's 15 RPM limit
    MIN_INTERVAL = float(os.getenv("LLM_MIN_REQUEST_INTERVAL", "15.0"))
    
    # Extra cooldown added when we receive a 429 response
    RATE_LIMIT_COOLDOWN = 60.0
    
    _instance = None
    _lock = None
    # CRITICAL: Initialize with current time so first request after startup waits
    _last_request_time = None  # Will be set to time.time() on first get_instance()
    _cooldown_until = 0.0  # Extended cooldown after 429 errors
    
    @classmethod
    def get_instance(cls) -> "GlobalRateLimiter":
        if cls._instance is None:
            cls._instance = cls()
            cls._lock = asyncio.Lock()
            # CRITICAL: Set to current time so first request must wait MIN_INTERVAL
            cls._last_request_time = time.time()
            logger.info(f"Global rate limiter initialized: min {cls.MIN_INTERVAL}s between requests, first request will wait")
        return cls._instance
    
    async def wait_for_capacity(self) -> None:
        """
        Wait until enough time has passed since the last request.
        Also respects any extended cooldown from 429 errors.
        """
        async with self._lock:
            now = time.time()
            
            # Check if we're in an extended cooldown from a 429 error
            if now < self._cooldown_until:
                cooldown_wait = self._cooldown_until - now
                logger.warning(f"Rate limiter: in 429 cooldown, waiting {cooldown_wait:.1f}s")
                await asyncio.sleep(cooldown_wait)
                now = time.time()
            
            # Normal rate limiting - ensure MIN_INTERVAL between requests
            time_since_last = now - self._last_request_time
            
            if time_since_last < self.MIN_INTERVAL:
                wait_time = self.MIN_INTERVAL - time_since_last
                logger.info(f"Rate limiter: waiting {wait_time:.1f}s before next request")
                await asyncio.sleep(wait_time)
            
            # Update last request time BEFORE making the request
            self._last_request_time = time.time()
    
    def report_rate_limit_hit(self) -> None:
        """
        Call this when a 429 response is received.
        Extends the cooldown period to prevent further requests.
        """
        self._cooldown_until = time.time() + self.RATE_LIMIT_COOLDOWN
        logger.warning(f"Rate limiter: 429 received, extending cooldown by {self.RATE_LIMIT_COOLDOWN}s")


def get_rate_limiter() -> GlobalRateLimiter:
    """Get the global rate limiter instance"""
    return GlobalRateLimiter.get_instance()


# ============ Response Caching ============

class LRUCache:
    """Thread-safe LRU Cache for LLM responses"""
    
    def __init__(self, max_size: int = 1000, ttl_seconds: int = 86400):
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self.cache: OrderedDict = OrderedDict()
        self._lock = asyncio.Lock()
    
    def _make_key(self, prompt: str, system: Optional[str] = None) -> str:
        """Create cache key from prompt and system message"""
        content = f"{system or ''}||{prompt}"
        return hashlib.sha256(content.encode()).hexdigest()[:32]
    
    async def get(self, prompt: str, system: Optional[str] = None) -> Optional[str]:
        """Get cached response if exists and not expired"""
        key = self._make_key(prompt, system)
        async with self._lock:
            if key not in self.cache:
                return None
            
            entry = self.cache[key]
            if time.time() - entry["timestamp"] > self.ttl_seconds:
                # Expired
                del self.cache[key]
                return None
            
            # Move to end (most recently used)
            self.cache.move_to_end(key)
            logger.debug(f"Cache hit for key {key[:8]}...")
            return entry["response"]
    
    async def set(self, prompt: str, response: str, system: Optional[str] = None):
        """Cache a response"""
        key = self._make_key(prompt, system)
        async with self._lock:
            # Remove oldest if at capacity
            while len(self.cache) >= self.max_size:
                self.cache.popitem(last=False)
            
            self.cache[key] = {
                "response": response,
                "timestamp": time.time()
            }
    
    async def clear(self):
        """Clear all cached entries"""
        async with self._lock:
            self.cache.clear()
    
    @property
    def size(self) -> int:
        return len(self.cache)


# ============ Provider Abstraction ============

@dataclass
class LLMResponse:
    """Standardized LLM response"""
    content: str
    provider: str
    model: str
    cached: bool = False
    tokens_used: int = 0
    latency_ms: int = 0


class LLMProvider(ABC):
    """Abstract base class for LLM providers"""
    
    @property
    @abstractmethod
    def name(self) -> str:
        pass
    
    @property
    @abstractmethod
    def is_available(self) -> bool:
        pass
    
    @abstractmethod
    async def generate(
        self,
        prompt: str,
        system: Optional[str] = None,
        json_mode: bool = False,
        max_tokens: int = 600,
        temperature: float = 0.3,
        attachments: Optional[List[Dict[str, Any]]] = None
    ) -> Optional[LLMResponse]:
        pass


class OpenAIProvider(LLMProvider):
    """OpenAI API Provider"""
    
    def __init__(self, config: LLMConfig):
        self.config = config
        self._last_error_time: Optional[float] = None
        self._error_count = 0
    
    @property
    def name(self) -> str:
        return "openai"
    
    @property
    def is_available(self) -> bool:
        if not self.config.openai_api_key:
            return False
        # Circuit breaker: if too many recent errors, temporarily unavailable
        if self._error_count >= 5 and self._last_error_time:
            if time.time() - self._last_error_time < 60:  # 1 minute cooldown
                return False
            # Reset after cooldown
            self._error_count = 0
        return True
    
    async def generate(
        self,
        prompt: str,
        system: Optional[str] = None,
        json_mode: bool = False,
        max_tokens: int = 600,
        temperature: float = 0.3
    ) -> Optional[LLMResponse]:
        if not self.is_available:
            return None
        
        start_time = time.time()
        
        for attempt in range(self.config.max_retries):
            try:
                async with httpx.AsyncClient(timeout=60.0) as client:
                    body: Dict[str, Any] = {
                        "model": self.config.openai_model,
                        "messages": [
                            {"role": "system", "content": system or "You are a helpful assistant."},
                            {"role": "user", "content": prompt},
                        ],
                        "temperature": temperature,
                        "max_tokens": max_tokens,
                    }

                    # Add attachments for OpenAI (GPT-4 Vision)
                    if attachments:
                        content_parts = [{"type": "text", "text": prompt}]
                        for att in attachments:
                            if att.get("type", "").startswith("image/"):
                                # OpenAI expects base64 or URL
                                if "url" in att:
                                    content_parts.append({
                                        "type": "image_url",
                                        "image_url": {"url": att["url"]}
                                    })
                        body["messages"][1]["content"] = content_parts
                    
                    if json_mode:
                        body["response_format"] = {"type": "json_object"}
                    
                    response = await client.post(
                        f"{self.config.openai_base_url}/chat/completions",
                        headers={
                            "Authorization": f"Bearer {self.config.openai_api_key}",
                            "Content-Type": "application/json",
                        },
                        json=body,
                    )
                    
                    if response.status_code == 429:
                        # Rate limited
                        if attempt < self.config.max_retries - 1:
                            delay = self.config.base_delay * (2 ** attempt)
                            logger.warning(f"OpenAI rate limited, retry {attempt + 1}/{self.config.max_retries} in {delay}s")
                            await asyncio.sleep(delay)
                            continue
                        else:
                            self._record_error()
                            logger.error("OpenAI rate limit exceeded after retries")
                            return None
                    
                    response.raise_for_status()
                    data = response.json()
                    
                    content = (
                        data.get("choices", [{}])[0]
                        .get("message", {})
                        .get("content", "")
                    )
                    
                    tokens = data.get("usage", {}).get("total_tokens", 0)
                    latency = int((time.time() - start_time) * 1000)
                    
                    # Reset error count on success
                    self._error_count = 0
                    
                    return LLMResponse(
                        content=content.strip(),
                        provider=self.name,
                        model=self.config.openai_model,
                        tokens_used=tokens,
                        latency_ms=latency
                    )
                    
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429 and attempt < self.config.max_retries - 1:
                    delay = self.config.base_delay * (2 ** attempt)
                    await asyncio.sleep(delay)
                    continue
                self._record_error()
                logger.error(f"OpenAI HTTP error: {e}")
                return None
            except Exception as e:
                self._record_error()
                logger.error(f"OpenAI error: {e}")
                return None
        
        return None
    
    def _record_error(self):
        self._error_count += 1
        self._last_error_time = time.time()


class GeminiProvider(LLMProvider):
    """Google Gemini API Provider"""
    
    GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models"
    
    def __init__(self, config: LLMConfig):
        self.config = config
        self._last_error_time: Optional[float] = None
        self._error_count = 0
    
    @property
    def name(self) -> str:
        return "gemini"
    
    @property
    def is_available(self) -> bool:
        if not self.config.google_api_key:
            return False
        if self._error_count >= 5 and self._last_error_time:
            if time.time() - self._last_error_time < 60:
                return False
            self._error_count = 0
        return True
    
    async def generate(
        self,
        prompt: str,
        system: Optional[str] = None,
        json_mode: bool = False,
        max_tokens: int = 1024,  # Increased from 600 for fuller Arabic responses
        temperature: float = 0.3,
        attachments: Optional[List[Dict[str, Any]]] = None
    ) -> Optional[LLMResponse]:
        if not self.is_available:
            return None
        
        start_time = time.time()
        
        for attempt in range(self.config.max_retries):
            try:
                async with httpx.AsyncClient(timeout=120.0) as client:  # Increased from 60s for complete generation
                    # Gemini uses different structure
                    full_prompt = f"{system}\n\n{prompt}" if system else prompt
                    
                    parts = [{"text": full_prompt}]
                    
                    # Add attachments for Gemini
                    if attachments:
                        for att in attachments:
                            mime_type = att.get("type", "")
                            # Support image and audio
                            if mime_type.startswith("image/") or mime_type.startswith("audio/"):
                                # If we have direct file data (base64)
                                if "base64" in att:
                                    parts.append({
                                        "inlineData": {
                                            "mimeType": mime_type,
                                            "data": att["base64"] 
                                        }
                                    })
                                # If we have a public URL, Gemini might need it downloaded first
                                # For now, we assume the caller handles downloading or provides base64
                                # Alternatively, fileData can be used if uploaded to File API, but that requires extra steps.
                    
                    body = {
                        "contents": [
                            {
                                "parts": parts
                            }
                        ],
                        "generationConfig": {
                            "temperature": temperature,
                            "maxOutputTokens": max_tokens,
                        }
                    }
                    
                    if json_mode:
                        body["generationConfig"]["responseMimeType"] = "application/json"
                    
                    url = f"{self.GEMINI_API_URL}/{self.config.google_model}:generateContent?key={self.config.google_api_key}"
                    
                    response = await client.post(
                        url,
                        headers={"Content-Type": "application/json"},
                        json=body,
                    )
                    
                    if response.status_code == 429:
                        # CRITICAL: Report 429 to global rate limiter to extend cooldown
                        get_rate_limiter().report_rate_limit_hit()
                        
                        # DON'T RETRY INTERNALLY - return None immediately
                        # The worker's retry mechanism will retry the whole message later,
                        # going through wait_for_capacity() which respects the cooldown
                        logger.warning(f"Gemini rate limited (429), cooldown set, will retry via worker")
                        self._record_error()
                        return None
                    
                    response.raise_for_status()
                    data = response.json()
                    
                    # Extract content from Gemini response
                    candidates = data.get("candidates", [])
                    if not candidates:
                        return None
                    
                    content = candidates[0].get("content", {}).get("parts", [{}])[0].get("text", "")
                    
                    latency = int((time.time() - start_time) * 1000)
                    
                    self._error_count = 0
                    
                    return LLMResponse(
                        content=content.strip(),
                        provider=self.name,
                        model=self.config.google_model,
                        latency_ms=latency
                    )
                    
            except Exception as e:
                self._record_error()
                logger.error(f"Gemini error: {e}")
                if attempt < self.config.max_retries - 1:
                    # Exponential backoff with jitter
                    delay = self.config.base_delay * (2 ** attempt) + random.uniform(0, 5)
                    await asyncio.sleep(delay)
                    continue
                return None
        
        return None
    
    def _record_error(self):
        self._error_count += 1
        self._last_error_time = time.time()


class OpenRouterProvider(LLMProvider):
    """OpenRouter API Provider - aggregates many LLM providers with free tier"""
    
    OPENROUTER_API_URL = "https://openrouter.ai/api/v1/chat/completions"
    
    def __init__(self, config: LLMConfig):
        self.config = config
        self._last_error_time: Optional[float] = None
        self._error_count = 0
    
    @property
    def name(self) -> str:
        return "openrouter"
    
    @property
    def is_available(self) -> bool:
        if not self.config.openrouter_api_key:
            return False
        if self._error_count >= 5 and self._last_error_time:
            if time.time() - self._last_error_time < 60:
                return False
            self._error_count = 0
        return True
    
    async def generate(
        self,
        prompt: str,
        system: Optional[str] = None,
        json_mode: bool = False,
        max_tokens: int = 600,
        temperature: float = 0.3,
        attachments: Optional[List[Dict[str, Any]]] = None
    ) -> Optional[LLMResponse]:
        if not self.is_available:
            return None
        
        start_time = time.time()
        
        for attempt in range(self.config.max_retries):
            try:
                async with httpx.AsyncClient(timeout=60.0) as client:
                    messages = []
                    if system:
                        messages.append({"role": "system", "content": system})
                    messages.append({"role": "user", "content": prompt})
                    
                    body = {
                        "model": self.config.openrouter_model,
                        "messages": messages,
                        "max_tokens": max_tokens,
                        "temperature": temperature,
                    }
                    
                    response = await client.post(
                        self.OPENROUTER_API_URL,
                        headers={
                            "Authorization": f"Bearer {self.config.openrouter_api_key}",
                            "Content-Type": "application/json",
                            "HTTP-Referer": "https://almudeer.app",
                            "X-Title": "Al-Mudeer",
                        },
                        json=body,
                    )
                    
                    if response.status_code == 429:
                        if attempt < self.config.max_retries - 1:
                            delay = self.config.base_delay * (2 ** attempt)
                            logger.warning(f"OpenRouter rate limited, retry {attempt + 1}/{self.config.max_retries}")
                            await asyncio.sleep(delay)
                            continue
                        else:
                            self._record_error()
                            return None
                    
                    response.raise_for_status()
                    data = response.json()
                    
                    content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                    latency = int((time.time() - start_time) * 1000)
                    
                    self._error_count = 0
                    
                    return LLMResponse(
                        content=content.strip(),
                        provider=self.name,
                        model=self.config.openrouter_model,
                        latency_ms=latency
                    )
                    
            except Exception as e:
                self._record_error()
                logger.error(f"OpenRouter error: {e}")
                if attempt < self.config.max_retries - 1:
                    await asyncio.sleep(self.config.base_delay * (2 ** attempt))
                    continue
                return None
        
        return None
    
    def _record_error(self):
        self._error_count += 1
        self._last_error_time = time.time()


# ============ Main Service ============

class LLMService:
    """
    Production-grade LLM service with multi-provider failover.
    
    Features:
    - Automatic failover: OpenAI -> Gemini -> Rule-based
    - Response caching for identical prompts
    - Circuit breaker pattern for failing providers
    - Retry with exponential backoff
    """
    
    def __init__(self, config: Optional[LLMConfig] = None):
        self.config = config or LLMConfig()
        self.cache = LRUCache(
            max_size=self.config.cache_max_size,
            ttl_seconds=self.config.cache_ttl_seconds
        ) if self.config.cache_enabled else None
        
        # Initialize providers in priority order
        # GEMINI-ONLY MODE: Only Gemini is active by default
        # OpenAI is kept as fallback if key is provided
        # OpenRouter is DISABLED to ensure Gemini-quality responses
        # Set ENABLE_OPENROUTER=true to re-enable OpenRouter as backup
        enable_openrouter = os.getenv("ENABLE_OPENROUTER", "false").lower() == "true"
        
        self.providers: List[LLMProvider] = [
            OpenAIProvider(self.config),  # Only used if OPENAI_API_KEY is set
            GeminiProvider(self.config),   # PRIMARY - always active
        ]
        
        # Only add OpenRouter if explicitly enabled (disabled by default for quality)
        if enable_openrouter:
            self.providers.append(OpenRouterProvider(self.config))
            logger.info("OpenRouter backup provider ENABLED")
        else:
            logger.info("OpenRouter backup provider DISABLED (Gemini-only mode)")
        
        # Track statistics
        self.stats = {
            "total_requests": 0,
            "cache_hits": 0,
            "provider_calls": {},
            "failures": 0,
        }
        
        logger.info(f"LLM Service initialized with {len([p for p in self.providers if p.is_available])} available providers")
    
    async def generate(
        self,
        prompt: str,
        system: Optional[str] = None,
        json_mode: bool = False,
        max_tokens: int = 2048,  # Increased for Arabic responses
        temperature: float = 0.3,
        use_cache: bool = True,
        attachments: Optional[List[Dict[str, Any]]] = None
    ) -> Optional[LLMResponse]:
        """
        Generate a response using available providers with automatic failover.
        
        Args:
            prompt: User prompt
            system: System prompt (optional)
            json_mode: Request JSON response format
            max_tokens: Maximum response tokens
            temperature: Response creativity (0.0-1.0)
            use_cache: Whether to check/store in cache
        
        Returns:
            LLMResponse or None if all providers fail
        """
        self.stats["total_requests"] += 1
        
        # Check cache first
        # Disable cache if attachments are present (content might differ even if prompt is same)
        if self.cache and use_cache and not attachments:
            cached = await self.cache.get(prompt, system)
            if cached:
                self.stats["cache_hits"] += 1
                return LLMResponse(
                    content=cached,
                    provider="cache",
                    model="cached",
                    cached=True
                )
        
        # Try each provider in order
        for provider in self.providers:
            if not provider.is_available:
                logger.debug(f"Provider {provider.name} not available, skipping")
                continue
            
            logger.debug(f"Trying provider: {provider.name}")
            
            response = await provider.generate(
                prompt=prompt,
                system=system,
                json_mode=json_mode,
                max_tokens=max_tokens,
                temperature=temperature,
                attachments=attachments
            )
            
            if response and response.content:
                # Track usage
                self.stats["provider_calls"][provider.name] = \
                    self.stats["provider_calls"].get(provider.name, 0) + 1
                
                # Cache successful response
                if self.cache and use_cache:
                    await self.cache.set(prompt, response.content, system)
                
                logger.info(f"LLM response from {provider.name} ({response.latency_ms}ms)")
                return response
        
        # All providers failed
        self.stats["failures"] += 1
        logger.warning("All LLM providers failed, returning None (will be retried later)")
        return None
    
    def get_stats(self) -> Dict[str, Any]:
        """Get service statistics"""
        return {
            **self.stats,
            "cache_size": self.cache.size if self.cache else 0,
            "available_providers": [p.name for p in self.providers if p.is_available],
        }
    
    async def health_check(self) -> Dict[str, Any]:
        """Check health of all providers"""
        results = {}
        for provider in self.providers:
            results[provider.name] = {
                "available": provider.is_available,
                "configured": bool(
                    (provider.name == "openai" and self.config.openai_api_key) or
                    (provider.name == "gemini" and self.config.google_api_key)
                )
            }
        return results


# ============ Global Instance ============

_llm_service: Optional[LLMService] = None


def get_llm_service() -> LLMService:
    """Get or create the global LLM service instance"""
    global _llm_service
    if _llm_service is None:
        _llm_service = LLMService()
    return _llm_service


async def llm_generate(
    prompt: str,
    system: Optional[str] = None,
    json_mode: bool = False,
    max_tokens: int = 2048,  # Increased for Arabic responses
    temperature: float = 0.3,
    attachments: Optional[List[Dict[str, Any]]] = None
) -> Optional[str]:
    """
    Convenience function for generating LLM responses.
    
    Uses global rate limiter + semaphore to prevent rate limiting.
    Rate limiter enforces minimum 10s between requests (6 RPM max).
    Returns just the content string (or None) for backward compatibility.
    """
    service = get_llm_service()
    semaphore = get_llm_semaphore(service.config.max_concurrent_requests)
    rate_limiter = get_rate_limiter()
    
    # Wait for semaphore - limits to N concurrent LLM requests
    async with semaphore:
        # CRITICAL: Wait for rate limiter BEFORE making request
        # This ensures minimum 10s gap between ALL requests globally
        await rate_limiter.wait_for_capacity()
        
        logger.debug(f"Acquired LLM semaphore + rate limit capacity, processing request...")
        response = await service.generate(
            prompt=prompt,
            system=system,
            json_mode=json_mode,
            max_tokens=max_tokens,
            temperature=temperature,
            attachments=attachments
        )
        
        # Add post-request delay for extra safety margin
        if service.config.post_request_delay > 0:
            await asyncio.sleep(service.config.post_request_delay)
        
        return response.content if response else None

