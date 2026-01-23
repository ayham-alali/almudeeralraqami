"""
Al-Mudeer - Multi-Provider LLM Service
Production-grade LLM infrastructure with automatic failover and caching

Supports:
- OpenAI (primary)
- Google Gemini via Vertex AI (fallback)
- Rule-based (guaranteed fallback)
"""

import os
import random
import asyncio
import hashlib
import json
import time
import tempfile
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any, List
from collections import OrderedDict

import httpx

# Google GenAI imports (lazy loaded for startup performance)
# Using the lightweight google-genai SDK instead of heavy google-cloud-aiplatform
genai_client = None

def _init_genai(project_id: str, location: str):
    """Lazy initialize Google GenAI SDK with Vertex AI backend"""
    global genai_client
    if genai_client is None:
        try:
            from google import genai
            from google.genai import types
            
            # Initialize with Vertex AI backend
            genai_client = genai.Client(
                vertexai=True,
                project=project_id,
                location=location,
            )
            return genai_client
        except ImportError as e:
            return None
        except Exception as e:
            return None
    return genai_client

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
    
    # Provider 1: Google Gemini via Vertex AI (PRIMARY)
    # GCP Project and Location for Vertex AI
    gcp_project_id: str = field(default_factory=lambda: os.getenv("GCP_PROJECT_ID", "gen-lang-client-0624316154"))
    gcp_location: str = field(default_factory=lambda: os.getenv("GCP_LOCATION", "us-central1"))
    google_model: str = field(default_factory=lambda: os.getenv("GOOGLE_MODEL", "gemini-2.5-flash"))
    # Legacy API key (deprecated, kept for fallback)
    google_api_key: str = field(default_factory=lambda: os.getenv("GOOGLE_API_KEY", ""))
    
    # Provider 2: OpenRouter (BACKUP - Free models)
    openrouter_api_key: str = field(default_factory=lambda: os.getenv("OPENROUTER_API_KEY", ""))
    openrouter_model: str = field(default_factory=lambda: os.getenv("OPENROUTER_MODEL", "google/gemini-2.0-flash-exp:free"))
    
    # Failover: OpenAI (if key set) → Gemini (Vertex AI) → OpenRouter → Rule-based
    
    # Retry settings - aggressive for rate limit handling
    # Vertex AI has better rate limits than AI Studio, but we keep conservative settings
    max_retries: int = 20  # increased to 20 for "Patient Retry"
    base_delay: float = 30.0  # Increased delay: 30s+ to wait out congestion
    
    # Cache settings
    cache_enabled: bool = field(default_factory=lambda: os.getenv("LLM_CACHE_ENABLED", "true").lower() == "true")
    cache_ttl_seconds: int = field(default_factory=lambda: int(os.getenv("LLM_CACHE_TTL_HOURS", "24")) * 3600)
    cache_max_size: int = 1000
    
    # Concurrency control - CRITICAL for preventing rate limits
    # Default to 3 for better performance on Vertex AI
    max_concurrent_requests: int = field(default_factory=lambda: int(os.getenv("LLM_MAX_CONCURRENT", "3")))
    
    # Post-request delay in seconds - adds breathing room between requests
    # Vertex AI has better quotas, reduced for better UX
    post_request_delay: float = field(default_factory=lambda: float(os.getenv("LLM_REQUEST_DELAY", "2.0")))


# ============ Backoff with Jitter ============

def calculate_backoff_delay(
    base_delay: float,
    attempt: int,
    max_delay: float = 120.0,
    jitter_percentage: float = 0.2
) -> float:
    """
    Calculate exponential backoff delay with jitter.
    
    Jitter helps prevent the "thundering herd" problem where multiple
    clients retry simultaneously after a rate limit, causing another
    spike in requests.
    
    Args:
        base_delay: Base delay in seconds
        attempt: Current attempt number (0-indexed)
        max_delay: Maximum delay cap in seconds
        jitter_percentage: Random variation (0.2 = ±20%)
        
    Returns:
        Delay in seconds with jitter applied
    """
    # Exponential backoff: base_delay * (2 ^ attempt)
    delay = base_delay * (2 ** attempt)
    
    # Cap at maximum
    delay = min(delay, max_delay)
    
    # Apply jitter (±jitter_percentage)
    jitter_range = delay * jitter_percentage
    jitter = random.uniform(-jitter_range, jitter_range)
    
    return max(0.1, delay + jitter)  # Ensure minimum of 100ms

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
    2. Reports 429 errors to extend cooldown period
    3. Uses 15s minimum interval = 4 RPM max (very safe under 15 RPM limit)
    4. Exponential cooldown after consecutive 429s (handles daily limit)
    
    This prevents rate limits even when the app restarts mid-request-burst.
    """
    
    # Minimum seconds between requests (4s = max 15 requests/minute)
    # Optimized for Vertex AI default quotas while staying safe
    MIN_INTERVAL = float(os.getenv("LLM_MIN_REQUEST_INTERVAL", "4.0"))
    
    # Base cooldown after 429 (5 minutes)
    BASE_COOLDOWN = 300.0
    
    # Maximum cooldown (1 hour) - for when daily limit is hit
    MAX_COOLDOWN = 3600.0
    
    _instance = None
    _lock = None
    _last_request_time = None
    _cooldown_until = 0.0
    _consecutive_429s = 0  # Track consecutive 429 errors
    
    @classmethod
    def get_instance(cls) -> "GlobalRateLimiter":
        if cls._instance is None:
            cls._instance = cls()
            cls._lock = asyncio.Lock()
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
        Uses exponential backoff for consecutive 429s.
        """
        self._consecutive_429s += 1
        
        # Exponential backoff: 5min, 10min, 20min, 40min, max 1 hour
        cooldown = min(self.BASE_COOLDOWN * (2 ** (self._consecutive_429s - 1)), self.MAX_COOLDOWN)
        self._cooldown_until = time.time() + cooldown
        
        logger.warning(f"Rate limiter: 429 #{self._consecutive_429s}, cooldown {cooldown/60:.1f} minutes")
        
        if self._consecutive_429s >= 3:
            logger.error(f"Rate limiter: {self._consecutive_429s} consecutive 429s - may have hit daily limit!")
    
    def report_success(self) -> None:
        """Call this when a request succeeds to reset the 429 counter."""
        if self._consecutive_429s > 0:
            logger.info(f"Rate limiter: request succeeded, resetting 429 counter from {self._consecutive_429s}")
            self._consecutive_429s = 0
    
    def is_in_cooldown(self) -> bool:
        """
        Check if we're currently in a rate limit cooldown period.
        Workers should check this BEFORE queuing any AI requests.
        """
        return time.time() < self._cooldown_until
    
    def get_cooldown_remaining(self) -> float:
        """Get seconds remaining in cooldown, or 0 if not in cooldown."""
        remaining = self._cooldown_until - time.time()
        return max(0, remaining)


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
    tool_calls: Optional[List[Dict[str, Any]]] = None


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
    """Google Gemini via Vertex AI Provider (using lightweight google-genai SDK)"""
    
    _initialized = False
    _client = None
    _temp_cred_file = None
    
    def __init__(self, config: LLMConfig):
        self.config = config
        self._last_error_time: Optional[float] = None
        self._error_count = 0
        
        # Initialize on first use
        self._ensure_initialized()
    
    def _ensure_initialized(self) -> bool:
        """Initialize Google GenAI SDK with Vertex AI backend"""
        if GeminiProvider._initialized and GeminiProvider._client is not None:
            return True
        
        try:
            # Handle service account credentials for Railway deployment
            # Railway sets GCP_SERVICE_ACCOUNT_KEY as JSON string
            gcp_sa_key = os.getenv("GCP_SERVICE_ACCOUNT_KEY")
            if gcp_sa_key:
                # Write to temp file for Google SDK to use
                if GeminiProvider._temp_cred_file is None:
                    # Create temp file that persists for the lifetime of the app
                    fd, path = tempfile.mkstemp(suffix=".json", prefix="gcp_creds_")
                    with os.fdopen(fd, 'w') as f:
                        f.write(gcp_sa_key)
                    GeminiProvider._temp_cred_file = path
                    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path
                    logger.info(f"Set GOOGLE_APPLICATION_CREDENTIALS from GCP_SERVICE_ACCOUNT_KEY")
            
            # Initialize the GenAI client with Vertex AI backend
            client = _init_genai(
                project_id=self.config.gcp_project_id,
                location=self.config.gcp_location
            )
            
            if client is None:
                logger.warning("Google GenAI SDK not available, Gemini provider unavailable")
                return False
            
            GeminiProvider._client = client
            GeminiProvider._initialized = True
            logger.info(f"Google GenAI initialized: project={self.config.gcp_project_id}, location={self.config.gcp_location}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize Google GenAI: {e}")
            return False
    
    @property
    def name(self) -> str:
        return "gemini"
    
    @property
    def is_available(self) -> bool:
        # Check if GenAI is properly initialized
        if not self._ensure_initialized():
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
        max_tokens: int = 1024,
        temperature: float = 0.3,
        attachments: Optional[List[Dict[str, Any]]] = None,
        tools: Optional[List[Dict[str, Any]]] = None,
    ) -> Optional[LLMResponse]:
        if not self.is_available:
            return None
        
        client = GeminiProvider._client
        if client is None:
            logger.warning("Gemini client not available")
            return None
        
        # CRITICAL: Enforce global rate limit HERE for Gemini only
        await get_rate_limiter().wait_for_capacity()
        
        start_time = time.time()
        
        # Build the prompt content
        full_prompt = f"{system}\n\n{prompt}" if system else prompt
        
        # Build content parts
        from google.genai import types
        
        content_parts = [full_prompt]
        
        # Add attachments (images, audio)
        if attachments:
            for att in attachments:
                mime_type = att.get("type", "")
                if mime_type.startswith("image/") or mime_type.startswith("audio/") or mime_type == "application/pdf":
                    if "base64" in att:
                        import base64
                        data = base64.b64decode(att["base64"])
                        content_parts.append(types.Part.from_bytes(data=data, mime_type=mime_type))
        
        # Configure generation
        # If tools provided, use them
        if tools:
            # We assume tools list matches Gemini "tools" format (list of Tool objects or dicts)
            config = types.GenerateContentConfig(
                temperature=temperature,
                max_output_tokens=max_tokens,
                tools=tools,
            )
        elif json_mode:
            config = types.GenerateContentConfig(
                temperature=temperature,
                max_output_tokens=max_tokens,
                response_mime_type="application/json",
            )
        else:
            config = types.GenerateContentConfig(
                temperature=temperature,
                max_output_tokens=max_tokens,
            )
        
        for attempt in range(self.config.max_retries):
            try:
                # Use async generation via executor
                response = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: client.models.generate_content(
                        model=self.config.google_model,
                        contents=content_parts,
                        config=config,
                    )
                )
                
                # Handling function calls
                function_calls = []
                content = ""
                
                # Check for function calls in candidates
                # Check for function calls in candidates
                if hasattr(response, 'candidates') and response.candidates:
                    first_candidate = response.candidates[0]
                    
                    # Method 1: Check parts (Standard Gemini)
                    if hasattr(first_candidate, 'content') and first_candidate.content.parts:
                        for part in first_candidate.content.parts:
                            if hasattr(part, 'function_call') and part.function_call:
                                # It's a function call
                                function_calls.append({
                                    "name": part.function_call.name,
                                    "args": dict(part.function_call.args)
                                })
                            elif hasattr(part, 'text') and part.text:
                                content += part.text
                    
                    # Method 2: Check candidate level (Additional Vertex AI check)
                    if not function_calls and hasattr(first_candidate, 'function_calls') and first_candidate.function_calls:
                         for fc in first_candidate.function_calls:
                             function_calls.append({
                                 "name": fc.name,
                                 "args": dict(fc.args)
                             })

                # Use text if no function call processing needed here (we return extracted calls separately if architecture allows, 
                # but since LLMResponse is simple, we might need to update it or return raw response to agent)
                # For now, let's assume we return the raw response object if it has tools?
                # Or better: Update LLMResponse to include tool_calls
                
                if not content and hasattr(response, 'text') and response.text:
                   content = response.text
                
                latency = int((time.time() - start_time) * 1000)
                self._error_count = 0
                get_rate_limiter().report_success()
                
                return LLMResponse(
                    content=content.strip(),
                    provider=self.name,
                    model=self.config.google_model,
                    latency_ms=latency,
                    tool_calls=function_calls if function_calls else None
                )
                
            except Exception as e:
                error_str = str(e).lower()
                
                # Check for rate limit errors (429)
                if "429" in str(e) or "resource_exhausted" in error_str or "quota" in error_str:
                    # CRITICAL: Report 429 to global rate limiter
                    get_rate_limiter().report_rate_limit_hit()
                    
                    if attempt < self.config.max_retries - 1:
                        # Patient Retry: Wait it out!
                        global_remaining = get_rate_limiter().get_cooldown_remaining()
                        backoff_delay = self.config.base_delay * (1.5 ** attempt)
                        
                        delay = max(backoff_delay, global_remaining)
                        
                        if global_remaining > backoff_delay:
                            logger.warning(f"Gemini 429: Global cooldown ({global_remaining:.1f}s) > Backoff ({backoff_delay:.1f}s). Waiting {delay:.1f}s...")
                        else:
                            logger.warning(f"Gemini 429 (Patient Retry {attempt+1}/{self.config.max_retries}), waiting {delay:.1f}s...")
                        
                        await asyncio.sleep(delay)
                        continue
                    else:
                        logger.error("Gemini failed after maximum patient retries")
                        self._record_error()
                        return None
                
                # Other errors
                self._record_error()
                logger.error(f"Gemini error: {e}")
                if attempt < self.config.max_retries - 1:
                    delay = self.config.base_delay * (2 ** attempt) + random.uniform(0, 5)
                    await asyncio.sleep(delay)
                    continue
                return None
        
        return None
    
    def _record_error(self):
        self._error_count += 1
        self._last_error_time = time.time()

    async def embed_text(self, text: str) -> Optional[List[float]]:
        """
        Generate embeddings for text using Gemini text-embedding-004 model.
        Returns a list of floats (the vector).
        """
        if not self.is_available:
            return None

        client = GeminiProvider._client
        if client is None:
            return None

        try:
            # Rate limit check
            await get_rate_limiter().wait_for_capacity()
            
            # Run in executor because google-genai might be sync or we want to be safe
            response = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: client.models.embed_content(
                    model="text-embedding-004",
                    contents=text,
                    config=None # Optional config
                )
            )
            
            # Handle response structure
            if hasattr(response, 'embeddings') and response.embeddings:
                return response.embeddings[0].values
            return None
            
        except Exception as e:
            logger.error(f"Gemini embedding error: {e}")
            return None


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
        
        # fallback models if primary rate limits (Free tier strategy)
        models_to_try = [
            self.config.openrouter_model,  # Primary from config
            "google/gemini-2.0-flash-exp:free", # User preferred
            "google/gemini-2.0-pro-exp-02-05:free", # Pro fallback
        ]
        
        # Max global timeout for all attempts
        # Outer loop for Patient Retry of the ENTIRE model list
        for loop_attempt in range(self.config.max_retries):
            # Try each model in the list
            for model in models_to_try:
                # Try each model up to 2 times (short retry)
                for attempt in range(2):
                    try:
                        async with httpx.AsyncClient(timeout=45.0) as client: # Reduced timeout per attempt
                            messages = []
                            if system:
                                messages.append({"role": "system", "content": system})
                            messages.append({"role": "user", "content": prompt})
                            
                            body = {
                                "model": model,
                                "messages": messages,
                                "max_tokens": max_tokens,
                                "temperature": temperature,
                            }
                            
                            # Add HTTP referer for OpenRouter rankings
                            headers = {
                                "Authorization": f"Bearer {self.config.openrouter_api_key}",
                                "Content-Type": "application/json",
                                "HTTP-Referer": "https://almudeer.royaraqamia.com",
                                "X-Title": "Al-Mudeer",
                            }
                            
                            response = await client.post(
                                self.OPENROUTER_API_URL,
                                headers=headers,
                                json=body,
                            )
                            
                            if response.status_code == 429:
                                # If rate limited, try next model immediately
                                logger.warning(f"OpenRouter 429 on {model}. Switching model...")
                                break # Break inner loop to try next model
                            
                            response.raise_for_status()
                            data = response.json()
                            
                            content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                            latency = int((time.time() - start_time) * 1000)
                            
                            self._error_count = 0
                            
                            return LLMResponse(
                                content=content.strip(),
                                provider=self.name,
                                model=model,
                                latency_ms=latency
                            )
                            
                    except httpx.HTTPStatusError as e:
                        if e.response.status_code == 429:
                            logger.warning(f"OpenRouter 429 on {model}. Switching model...")
                            break 
                        
                        if e.response.status_code == 400 and json_mode:
                            logger.warning(f"OpenRouter 400 on {model} with json_mode=True. Retrying without json_mode...")
                            try:
                                if "response_format" in body:
                                    del body["response_format"]
                                
                                # CRITICAL FIX: Use a FRESH client for the retry
                                # The previous client might be in a bad state after the 400 error
                                async with httpx.AsyncClient(timeout=45.0) as retry_client:
                                    response = await retry_client.post(
                                        self.OPENROUTER_API_URL,
                                        headers=headers,
                                        json=body,
                                    )
                                    response.raise_for_status()
                                    data = response.json()
                                    content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                                    latency = int((time.time() - start_time) * 1000)
                                    self._error_count = 0
                                    return LLMResponse(
                                        content=content.strip(),
                                        provider=self.name,
                                        model=model,
                                        latency_ms=latency
                                    )
                            except Exception as retry_e:
                                logger.error(f"Smart Retry failed on {model}: {retry_e}")
                                break 

                        logger.warning(f"OpenRouter HTTP {e.response.status_code} on {model}: {e}")
                        break 

                    except Exception as e:
                        logger.warning(f"OpenRouter error on {model}: {e}")
                        if attempt == 0:
                            await asyncio.sleep(1)
                            continue
                        else:
                            break 
            
            # If we're here, ALL models failed for this loop_attempt
            # Wait and retry the whole list (Patient Retry)
            if loop_attempt < self.config.max_retries - 1:
                delay = self.config.base_delay * (1.5 ** loop_attempt)
                logger.warning(f"OpenRouter: ALL models failed/busy. Patient Retry {loop_attempt+1}/{self.config.max_retries} in {delay:.1f}s...")
                await asyncio.sleep(delay)
                continue
            else:
                self._record_error()
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
        # OpenRouter is ENABLED by default as high-quality backup (Gemini 2.0 Flash)
        # providing failover when Gemini API hits daily rate limits
        enable_openrouter = os.getenv("ENABLE_OPENROUTER", "true").lower() == "true"
        
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
        attachments: Optional[List[Dict[str, Any]]] = None,
        tools: Optional[List[Dict[str, Any]]] = None
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
            tools: Optional list of tool definitions for function calling
        
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
            # Smart Failover: Skip Gemini if global rate limiter is in cooldown
            # This allows immediate failover to OpenRouter without waiting
            if provider.name == "gemini":
                rate_limiter = get_rate_limiter()
                if rate_limiter.is_in_cooldown():
                    remaining = rate_limiter.get_cooldown_remaining()
                    logger.warning(f"Skipping Gemini due to active rate limit cooldown ({remaining:.1f}s), failing over to next provider")
                    continue

            if not provider.is_available:
                logger.debug(f"Provider {provider.name} not available, skipping")
                continue
            
            logger.debug(f"Trying provider: {provider.name}")
            
            # Only pass tools to providers that support it (Gemini)
            if provider.name == "gemini" and tools:
                response = await provider.generate(
                    prompt=prompt,
                    system=system,
                    json_mode=json_mode,
                    max_tokens=max_tokens,
                    temperature=temperature,
                    attachments=attachments,
                    tools=tools
                )
            else:
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
    attachments: Optional[List[Dict[str, Any]]] = None,
    tools: Optional[List[Dict[str, Any]]] = None,
) -> Any:
    """
    Convenience function for generating LLM responses.
    
    Uses global rate limiter + semaphore to prevent rate limiting.
    Rate limiter enforces minimum 10s between requests (6 RPM max).
    Returns content string (legacy) or LLMResponse object (if tools used).
    """
    service = get_llm_service()
    semaphore = get_llm_semaphore(service.config.max_concurrent_requests)
    
    # Wait for semaphore - limits to N concurrent LLM requests
    async with semaphore:
        # Note: We removed the global rate_limiter.wait_for_capacity() call here.
        # It is now handled inside GeminiProvider.generate() to allow failover to OpenRouter.
        
        logger.debug(f"Acquired LLM semaphore, processing request...")
        response = await service.generate(
            prompt=prompt,
            system=system,
            json_mode=json_mode,
            max_tokens=max_tokens,
            temperature=temperature,
            attachments=attachments,
            tools=tools
        )
        
        # Add post-request delay for extra safety margin
        if service.config.post_request_delay > 0:
            await asyncio.sleep(service.config.post_request_delay)
        
        # Standardized Output Logic:
        # If tools are requested, we return the full LLMResponse object to allow tool call inspection.
        # This prevents the "AttributeError: 'str' object has no attribute 'tool_calls'" crash.
        if tools and response:
            return response
            
        return response.content if response else None

