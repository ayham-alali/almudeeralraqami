"""
Al-Mudeer - Multi-Provider LLM Service
Production-grade LLM infrastructure with automatic failover and caching

Supports:
- OpenAI (primary)
- Google Gemini (fallback)
- Rule-based (guaranteed fallback)
"""

import os
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
    """LLM service configuration"""
    # Provider API keys
    openai_api_key: str = field(default_factory=lambda: os.getenv("OPENAI_API_KEY", ""))
    openai_base_url: str = field(default_factory=lambda: os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1"))
    openai_model: str = field(default_factory=lambda: os.getenv("OPENAI_MODEL", "gpt-4o"))
    
    google_api_key: str = field(default_factory=lambda: os.getenv("GOOGLE_API_KEY", ""))
    google_model: str = field(default_factory=lambda: os.getenv("GOOGLE_MODEL", "gemini-2.0-flash"))
    
    # Retry settings - more aggressive for rate limit handling
    max_retries: int = 3
    base_delay: float = 2.0  # Increased from 1.0 for better rate limit recovery
    
    # Cache settings
    cache_enabled: bool = field(default_factory=lambda: os.getenv("LLM_CACHE_ENABLED", "true").lower() == "true")
    cache_ttl_seconds: int = field(default_factory=lambda: int(os.getenv("LLM_CACHE_TTL_HOURS", "24")) * 3600)
    cache_max_size: int = 1000
    
    # Concurrency control - CRITICAL for preventing rate limits
    # Default to 3 concurrent requests
    max_concurrent_requests: int = field(default_factory=lambda: int(os.getenv("LLM_MAX_CONCURRENT", "3")))
    
    # Post-request delay in seconds - adds breathing room between requests
    post_request_delay: float = field(default_factory=lambda: float(os.getenv("LLM_REQUEST_DELAY", "0.5")))


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
        temperature: float = 0.3
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
        max_tokens: int = 600,
        temperature: float = 0.3
    ) -> Optional[LLMResponse]:
        if not self.is_available:
            return None
        
        start_time = time.time()
        
        for attempt in range(self.config.max_retries):
            try:
                async with httpx.AsyncClient(timeout=60.0) as client:
                    # Gemini uses different structure
                    full_prompt = f"{system}\n\n{prompt}" if system else prompt
                    
                    body = {
                        "contents": [
                            {
                                "parts": [
                                    {"text": full_prompt}
                                ]
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
                        if attempt < self.config.max_retries - 1:
                            delay = self.config.base_delay * (2 ** attempt)
                            logger.warning(f"Gemini rate limited, retry {attempt + 1}/{self.config.max_retries}")
                            await asyncio.sleep(delay)
                            continue
                        else:
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
        self.providers: List[LLMProvider] = [
            OpenAIProvider(self.config),
            GeminiProvider(self.config),
        ]
        
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
        max_tokens: int = 600,
        temperature: float = 0.3,
        use_cache: bool = True
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
        if self.cache and use_cache:
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
                temperature=temperature
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
        logger.warning("All LLM providers failed, returning None for rule-based fallback")
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
    max_tokens: int = 600,
    temperature: float = 0.3
) -> Optional[str]:
    """
    Convenience function for generating LLM responses.
    
    Uses global semaphore to limit concurrent requests and prevent rate limiting.
    Returns just the content string (or None) for backward compatibility.
    """
    service = get_llm_service()
    semaphore = get_llm_semaphore(service.config.max_concurrent_requests)
    
    # Wait for semaphore - limits to N concurrent LLM requests
    async with semaphore:
        logger.debug(f"Acquired LLM semaphore, processing request...")
        response = await service.generate(
            prompt=prompt,
            system=system,
            json_mode=json_mode,
            max_tokens=max_tokens,
            temperature=temperature
        )
        
        # Add post-request delay to prevent burst rate limits
        # This gives the API time to recover between sequential requests
        if service.config.post_request_delay > 0:
            await asyncio.sleep(service.config.post_request_delay)
        
        return response.content if response else None
