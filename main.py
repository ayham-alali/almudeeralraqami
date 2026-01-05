"""
Al-Mudeer (المدير) - FastAPI Backend
B2B AI Agent for Syrian and Arab Market
"""

import os
import warnings

# Disable ChromaDB telemetry BEFORE any imports that might use it
# This fixes PostHog compatibility errors
os.environ["ANONYMIZED_TELEMETRY"] = "False"

# Suppress harmless Pydantic field shadowing warnings from ChromaDB
warnings.filterwarnings("ignore", message="Field name .* shadows an attribute in parent")

import json
import asyncio
from contextlib import asynccontextmanager
from typing import Optional
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

from fastapi import FastAPI, HTTPException, Depends, Header, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

# Performance middleware
from middleware import PerformanceMiddleware, SecurityHeadersMiddleware

# Logging
from logging_config import setup_logging, get_logger

# Setup logging
setup_logging(os.getenv("LOG_LEVEL", "INFO"))
logger = get_logger(__name__)
DEBUG_ERRORS = os.getenv("DEBUG_ERRORS", "0") == "1"

from database import (
    init_database,
    create_demo_license,
    validate_license_key,
    increment_usage,
    save_crm_entry,
    get_crm_entries,
    get_entry_by_id,
    generate_license_key,
)
from schemas import (
    LicenseKeyValidation,
    LicenseKeyResponse,
    LicenseKeyCreate,
    MessageInput,
    ProcessingResponse,
    AnalysisResult,
    CRMEntryCreate,
    CRMEntry,
    CRMListResponse,
    HealthCheck
)
from agent import process_message
from models import (
    init_enhanced_tables,
    init_customers_and_analytics,
    get_preferences,
    get_recent_conversation,
)
# Debug logging for imports
import logging
logger = logging.getLogger("startup")
try:
    from routes import integrations_router, features_router, whatsapp_router, export_router, notifications_router, purchases_router, knowledge_router
    logger.info("Successfully imported integration routes")
except ImportError as e:
    logger.error(f"Failed to import routes: {e}")
    raise e
from routes.subscription import router as subscription_router
from security import sanitize_message, sanitize_string
from workers import start_message_polling, stop_message_polling, start_subscription_reminders, stop_subscription_reminders
from db_pool import db_pool
from services.task_queue import get_task_queue, enqueue_ai_task, get_ai_task_status
from services.websocket_manager import get_websocket_manager, broadcast_new_message
from services.pagination import paginate_inbox, paginate_crm, paginate_customers, PaginationParams
from services.request_batcher import get_request_batcher, batch_analyze
from services.db_indexes import create_indexes


# ============ App Lifecycle ============

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize database on startup"""
    try:
        logger.info("Initializing Al-Mudeer backend...")

        # Initialize database connection pool (SQLite now, PostgreSQL-ready)
        try:
            await db_pool.initialize()
            logger.info(f"Database pool initialized using DB_TYPE={os.getenv('DB_TYPE', 'sqlite')}")
        except Exception as e:
            logger.warning(f"Database pool initialization failed (fallback to direct connections): {e}")
        
        # Run migrations first
        try:
            from migrations import migration_manager
            await migration_manager.migrate()
            logger.info("Database migrations completed")
        except Exception as e:
            logger.warning(f"Migration check failed (may be first run): {e}")
        
        await init_database()
        try:
            await init_enhanced_tables()  # Email & Telegram tables
        except Exception as e:
            logger.warning(f"Enhanced tables initialization warning (may already exist): {e}")
        try:
            from services.notification_service import init_notification_tables
            await init_notification_tables()  # Notification tables
        except Exception as e:
            logger.warning(f"Notification tables initialization warning (may already exist): {e}")
        try:
            await init_customers_and_analytics()  # Customers, Analytics
        except Exception as e:
            logger.warning(f"Customers/analytics initialization warning (may already exist): {e}")
        
        # Create database indexes for query optimization
        try:
            await create_indexes()
            logger.info("Database indexes created/verified")
        except Exception as e:
            logger.warning(f"Index creation warning: {e}")
        
        # Push Notification Services
        try:
            from services.push_service import log_vapid_status, ensure_push_subscription_table
            log_vapid_status()
            await ensure_push_subscription_table()
        except Exception as e:
            logger.warning(f"Push service initialization warning: {e}")
        
        # Create users table for JWT auth
        try:
            from migrations.users_table import create_users_table
            await create_users_table()
        except Exception as e:
            logger.warning(f"Users table creation note: {e}")
        
        # Fix customers table serial (PostgreSQL auto-increment)
        try:
            from migrations.fix_customers_serial import fix_customers_serial
            await fix_customers_serial()
        except Exception as e:
            logger.warning(f"Customers serial fix note: {e}")


        # Create backfill queue table for historical chat processing
        try:
            from migrations.backfill_queue_table import create_backfill_queue_table
            await create_backfill_queue_table()
        except Exception as e:
            logger.warning(f"Backfill queue table creation note: {e}")

        
        # Create purchases table and analytics columns
        try:
            from migrations.purchases_table import create_purchases_table
            await create_purchases_table()
        except Exception as e:
            logger.warning(f"Purchases table creation note: {e}")

        
        # Ensure language/dialect columns exist in inbox_messages
        try:
            from migrations import ensure_inbox_columns
            await ensure_inbox_columns()
            logger.info("Inbox columns verified (language, dialect)")
        except Exception as e:
            logger.warning(f"Inbox column migration warning: {e}")
        
        # Ensure user_preferences columns exist (tone, business_name, etc.)
        try:
            from migrations import ensure_user_preferences_columns
            await ensure_user_preferences_columns()
        except Exception as e:
            logger.warning(f"User preferences column migration warning: {e}")
        
        # Ensure chat features schema exists (reactions, presence, voice)
        try:
            from migrations.chat_features import ensure_chat_features_schema
            await ensure_chat_features_schema()
            logger.info("Chat features schema verified (reactions, presence, voice)")
        except Exception as e:
            logger.warning(f"Chat features schema migration warning: {e}")
        
        # Fix int32 range issues for message IDs (BIGINT migration)
        try:
            from migrations.fix_int32_range import fix_int32_range_issues
            await fix_int32_range_issues()
            logger.info("Int32 range fixes applied (message IDs now BIGINT)")
        except Exception as e:
            logger.warning(f"Int32 range fix migration warning: {e}")
        
        demo_key = await create_demo_license()
        if demo_key:
            logger.info(f"Demo license key created: {demo_key[:20]}...")
            print(f"\n{'='*50}")
            print(f"Demo License Key: {demo_key}")
            print(f"{'='*50}\n")
        
        # Start background workers for message polling
        try:
            await start_message_polling()
            logger.info("Message polling workers started")
        except Exception as e:
            logger.warning(f"Failed to start message polling workers: {e}")
        
        # Start subscription reminder worker (daily check for expiring subs)
        try:
            await start_subscription_reminders()
            logger.info("Subscription reminder worker started")
        except Exception as e:
            logger.warning(f"Failed to start subscription reminder worker: {e}")
        
        # Initialize task queue for async AI processing
        try:
            task_queue = await get_task_queue()
            
            # Handler for queued AI tasks
            async def handle_ai_task(task_type: str, payload: dict):
                if task_type == "analyze":
                    return await process_message(
                        message=payload.get("message"),
                        message_type=payload.get("message_type"),
                        sender_name=payload.get("sender_name"),
                        sender_contact=payload.get("sender_contact"),
                    )
                return {"success": False, "error": f"Unknown task: {task_type}"}
            
            await task_queue.start_worker(handle_ai_task)
            logger.info("Task queue and worker started")
        except Exception as e:
            logger.warning(f"Task queue initialization warning: {e}")
        
        logger.info("Al-Mudeer backend initialized successfully")
        print("Al-Mudeer Premium Backend Ready!")
        print("Customers & Analytics - All Ready!")
        print("Background workers active for automatic message processing")
    except Exception as e:
        logger.error(f"Failed to initialize backend: {e}", exc_info=True)
        raise
    yield
    # Shutdown
    try:
        await stop_message_polling()
        logger.info("Message polling workers stopped")
    except Exception as e:
        logger.warning(f"Error stopping workers: {e}")
    try:
        await stop_subscription_reminders()
        logger.info("Subscription reminder worker stopped")
    except Exception as e:
        logger.warning(f"Error stopping subscription reminder: {e}")
    try:
        task_queue = await get_task_queue()
        await task_queue.stop_worker()
        logger.info("Task queue worker stopped")
    except Exception as e:
        logger.warning(f"Error stopping task queue: {e}")
    try:
        await db_pool.close()
        logger.info("Database pool closed")
    except Exception as e:
        logger.warning(f"Error closing database pool: {e}")
    logger.info("Shutting down Al-Mudeer backend...")


# ============ Create App ============

app = FastAPI(
    title="Al-Mudeer API - المدير",
    description="""
    واجهة برمجة تطبيقات المدير الذكي للأعمال
    
    ## المميزات
    
    * 📧 تحليل الرسائل تلقائياً
    * 🎯 تصنيف نوايا العملاء
    * 📝 صياغة ردود احترافية
    * 💾 إدارة سجلات العملاء (CRM)
    
    ## المصادقة
    
    جميع الطلبات المحمية تتطلب مفتاح اشتراك في header:
    `X-License-Key: YOUR_LICENSE_KEY`
    """,
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_tags=[
        {"name": "System", "description": "System endpoints (health, etc.)"},
        {"name": "Authentication", "description": "License key validation"},
        {"name": "Admin", "description": "Admin operations (license management)"},
        {"name": "Analysis", "description": "Message analysis and processing"},
        {"name": "CRM", "description": "Customer relationship management"},
    ]
)

# Rate Limiting
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Gzip Compression (reduces bandwidth by 60-80% for JSON responses)
from starlette.middleware.gzip import GZipMiddleware
app.add_middleware(GZipMiddleware, minimum_size=500)  # Compress responses > 500 bytes

# Performance & Security Middleware (Order matters!)
app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(PerformanceMiddleware)

# CORS for frontend (optimized for Arab World)
frontend_urls = [
    "http://localhost:3000",
    "http://localhost:3001",
    "http://localhost:3100",
    "http://127.0.0.1:3000",
    "http://127.0.0.1:3100",
    "https://almudeer.royaraqamia.com",
    "https://www.almudeer.royaraqamia.com",
    "https://almudeer.up.railway.app",
    os.getenv("FRONTEND_URL", "https://almudeer.royaraqamia.com")
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=frontend_urls,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    max_age=86400,  # Cache CORS preflight for 24 hours (better for Arab World latency)
)

# Include routes (legacy /api/ prefix for backward compatibility)
logger.info(f"Including integrations_router with {len(integrations_router.routes)} routes")
app.include_router(integrations_router)    # Email & Telegram
app.include_router(features_router)        # Customers, Analytics
app.include_router(whatsapp_router)        # WhatsApp Business

app.include_router(export_router)          # Export & Reports
app.include_router(notifications_router)   # Smart Notifications & Integrations
app.include_router(purchases_router)       # Customer Purchases
app.include_router(knowledge_router)       # Knowledge Base (RAG)
app.include_router(subscription_router)    # Subscription Key Management

# JWT Authentication routes
from routes.auth import router as auth_router
app.include_router(auth_router)

# Voice message routes
try:
    from routes.voice import router as voice_router
    app.include_router(voice_router)
except Exception as e:
    logger.warning(f"Voice router not loaded: {e}")

# Health check endpoints (no prefix, accessible at root level)
from health_check import router as health_router
app.include_router(health_router)

# Version check endpoint (public, for force-update system)
# Also includes /download/almudeer.apk endpoint for APK downloads
from routes.version import router as version_router
app.include_router(version_router)

@app.get("/debug/routes")
async def list_all_routes(x_admin_key: str = Header(None, alias="X-Admin-Key")):
    """List all registered routes for debugging (development only, or requires admin key)"""
    import logging
    
    # SECURITY: Only allow in development OR with admin key
    is_production = os.getenv("ENVIRONMENT", "development") == "production"
    
    if is_production:
        # In production, require admin key
        if not x_admin_key or x_admin_key != ADMIN_KEY:
            raise HTTPException(
                status_code=403, 
                detail="Debug endpoint not available in production without admin key"
            )
    
    logger = logging.getLogger("debug")
    routes = []
    for route in app.routes:
        routes.append({
            "path": route.path,
            "name": route.name,
            "methods": list(route.methods) if hasattr(route, "methods") else None
        })
    logger.info(f"Listing {len(routes)} routes")
    return {"count": len(routes), "routes": routes}

# Style Learning API (adaptive AI) - optional, may require additional setup
try:
    from routes.style_learning import router as style_learning_router
    app.include_router(style_learning_router)
except Exception as e:
    logger.warning(f"Style learning router not loaded: {e}")

# API Version 1 routes (new /api/v1/ prefix)
# These mirror the legacy routes but with versioned prefix for future compatibility
from fastapi import APIRouter
v1_router = APIRouter(prefix="/api/v1")
v1_router.include_router(integrations_router.router if hasattr(integrations_router, 'router') else integrations_router, prefix="")
v1_router.include_router(features_router.router if hasattr(features_router, 'router') else features_router, prefix="")
v1_router.include_router(whatsapp_router.router if hasattr(whatsapp_router, 'router') else whatsapp_router, prefix="")

v1_router.include_router(export_router.router if hasattr(export_router, 'router') else export_router, prefix="")
v1_router.include_router(notifications_router.router if hasattr(notifications_router, 'router') else notifications_router, prefix="")
v1_router.include_router(subscription_router, prefix="")
# Note: v1_router is prepared but routes already have /api/ prefix
# Future versions can modify prefixes as needed


# ============ License Key Middleware ============

async def verify_license(x_license_key: str = Header(None, alias="X-License-Key")) -> dict:
    """Dependency to verify license key from header"""
    if not x_license_key:
        logger.warning("License key missing in request header")
        raise HTTPException(
            status_code=401,
            detail={"error": "مفتاح الاشتراك مطلوب", "code": "LICENSE_REQUIRED"}
        )
    
    result = await validate_license_key(x_license_key)
    
    if not result["valid"]:
        logger.warning(f"Invalid license key attempt: {x_license_key[:10]}...")
        raise HTTPException(
            status_code=401,
            detail={"error": result["error"], "code": "LICENSE_INVALID"}
        )
    
    logger.debug(f"License validated for company: {result.get('company_name')}")
    return result


# ============ Public Routes ============

@app.get("/", response_model=HealthCheck)
async def root():
    """Health check endpoint"""
    return HealthCheck()


@app.get("/health", response_model=HealthCheck, tags=["System"])
async def health_check():
    """
    Health check endpoint with system status.
    
    Returns:
        - status: Service health status
        - database: Database connection status
        - cache: Cache availability
        - timestamp: Unix timestamp
    """
    import time
    from cache import cache
    
    health_data = {
        "status": "healthy",
        "timestamp": time.time(),
        "database": "connected",
        "cache": "available" if cache.use_redis or cache.memory_cache else "unavailable",
        "version": "1.0.0",
        "service": "Al-Mudeer API"
    }
    
    # Test database connection
    try:
        from database import DATABASE_PATH
        import os
        if os.path.exists(DATABASE_PATH):
            health_data["database"] = "connected"
        else:
            health_data["database"] = "not_found"
            health_data["status"] = "degraded"
    except Exception as e:
        health_data["database"] = f"error: {str(e)}"
        health_data["status"] = "unhealthy"
        logger.error(f"Health check database error: {e}")
    
    return HealthCheck(**health_data)


@app.post("/api/auth/validate", response_model=LicenseKeyResponse, tags=["Authentication"])
@limiter.limit("10/minute")  # Rate limit: 10 validations per minute per IP
async def validate_license(request: Request, data: LicenseKeyValidation):
    """
    Validate a license key and return details.
    
    Args:
        data: License key validation request
        
    Returns:
        License key validation result with company info and remaining requests
    """
    try:
        result = await validate_license_key(data.key)
        logger.info(f"License validation: {'valid' if result['valid'] else 'invalid'}")
    except Exception as e:
        logger.error(f"License validation error: {e}", exc_info=True)
        result = {"valid": False, "error": "حدث خطأ في التحقق من المفتاح"}
    
    return LicenseKeyResponse(
        valid=result["valid"],
        company_name=result.get("company_name"),
        created_at=result.get("created_at"),
        expires_at=result.get("expires_at"),
        requests_remaining=result.get("requests_remaining"),
        error=result.get("error")
    )


# ============ Admin Routes (Protected by Admin Key) ============

ADMIN_KEY = os.getenv("ADMIN_KEY")
if not ADMIN_KEY:
    raise ValueError(
        "ADMIN_KEY environment variable is required. "
        "Please set it in your environment or .env file for security."
    )


async def verify_admin(x_admin_key: str = Header(None, alias="X-Admin-Key")):
    """Verify admin key"""
    if not x_admin_key or x_admin_key != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="غير مصرح")


@app.post("/api/admin/license/create", tags=["Admin"])
async def create_license(data: LicenseKeyCreate, _: None = Depends(verify_admin)):
    """
    Create a new license key (admin only).
    
    Requires: X-Admin-Key header
    
    Args:
        data: License key creation request
        
    Returns:
        Generated license key
    """
    logger.info(f"Creating license for company: {data.company_name}")
    key = await generate_license_key(
        company_name=data.company_name,
        contact_email=data.contact_email,
        days_valid=data.days_valid,
        max_requests=data.max_requests_per_day
    )
    logger.info(f"License created: {key[:20]}...")
    return {"success": True, "license_key": key}


# ============ Protected Routes (Require License Key) ============

@app.post("/api/analyze", response_model=ProcessingResponse, tags=["Analysis"])
@limiter.limit("30/minute")  # Rate limit: 30 requests per minute per IP
async def analyze_message(
    request: Request,
    data: MessageInput,
    license: dict = Depends(verify_license)
):
    """
    Analyze a message - classify intent, extract information.
    
    Requires: X-License-Key header
    
    Args:
        data: Message input with text and metadata
        
    Returns:
        Analysis result with intent, urgency, sentiment, and extracted data
    """
    # Sanitize inputs
    sanitized_message = sanitize_message(data.message)
    sanitized_sender_name = sanitize_string(data.sender_name) if data.sender_name else None
    sanitized_sender_contact = sanitize_string(data.sender_contact) if data.sender_contact else None
    
    license_id = license["license_id"]

    # Increment usage
    await increment_usage(
        license_id,
        "analyze",
        sanitized_message[:100]
    )
    
    # Load workspace preferences for tone & business profile
    prefs = await get_preferences(license_id)

    # Load recent conversation history for this sender (if available)
    conversation_history = ""
    if sanitized_sender_contact:
        # Use new unified history function
        from models.inbox import get_chat_history_for_llm
        conversation_history = await get_chat_history_for_llm(
            license_id=license_id,
            sender_contact=sanitized_sender_contact,
            limit=10,
        )

    # Process the message
    result = await process_message(
        message=sanitized_message,
        message_type=data.message_type,
        sender_name=sanitized_sender_name,
        sender_contact=sanitized_sender_contact,
        preferences=prefs,
        history=conversation_history,  # CORRECTED
    )
    
    if result["success"]:
        return ProcessingResponse(
            success=True,
            data=AnalysisResult(**result["data"])
        )
    else:
        return ProcessingResponse(
            success=False,
            error=result["error"]
        )


# ============ Async Processing (Non-blocking AI) ============

@app.post("/api/analyze/async", tags=["Analysis"])
@limiter.limit("60/minute")  # Higher limit for async requests
async def analyze_message_async(
    request: Request,
    data: MessageInput,
    license: dict = Depends(verify_license)
):
    """
    Queue a message for async AI analysis (non-blocking).
    
    Returns a task_id immediately. Poll /api/task/{task_id} for results.
    Better for high-load scenarios where you don't need immediate results.
    """
    sanitized_message = sanitize_message(data.message)
    
    task_id = await enqueue_ai_task("analyze", {
        "message": sanitized_message,
        "message_type": data.message_type,
        "sender_name": data.sender_name,
        "sender_contact": data.sender_contact,
        "license_id": license["license_id"],
    })
    
    return {
        "success": True,
        "task_id": task_id,
        "status": "queued",
        "message": "Task queued for processing. Poll /api/task/{task_id} for results."
    }


@app.get("/api/task/{task_id}", tags=["Analysis"])
async def get_task_status_endpoint(
    task_id: str,
    license: dict = Depends(verify_license)
):
    """
    Get the status of an async task.
    
    Returns:
        - status: pending, processing, completed, or failed
        - result: The analysis result if completed
        - error: Error message if failed
    """
    task = await get_ai_task_status(task_id)
    
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    return {
        "success": True,
        "task_id": task_id,
        "status": task.get("status"),
        "result": task.get("result"),
        "error": task.get("error"),
        "created_at": task.get("created_at"),
        "completed_at": task.get("completed_at"),
    }


# ============ Batched AI Processing ============

@app.post("/api/analyze/batch", tags=["Analysis"])
@limiter.limit("20/minute")
async def analyze_batch(
    request: Request,
    data: MessageInput,
    license: dict = Depends(verify_license)
):
    """
    Analyze a message using request batching.
    Groups similar requests for more efficient AI processing.
    """
    result = await batch_analyze(
        message=sanitize_message(data.message),
        message_type=data.message_type,
        sender_name=data.sender_name,
        license_id=license["license_id"],
    )
    return ProcessingResponse(
        success=result.get("success", False),
        data=AnalysisResult(**result["data"]) if result.get("success") else None,
        error=result.get("error")
    )


# ============ WebSocket Real-time Updates ============

from fastapi import WebSocket, WebSocketDisconnect

async def handle_websocket_connection(websocket: WebSocket, license_key: str):
    """Shared WebSocket connection handler"""
    # Validate license key
    license_result = await validate_license_key(license_key)
    if not license_result["valid"]:
        await websocket.close(code=4001, reason="Invalid license key")
        return
    
    license_id = license_result["license_id"]
    manager = get_websocket_manager()
    
    await manager.connect(websocket, license_id)
    try:
        while True:
            # Keep connection alive, handle pings
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text('{"event":"pong"}')
    except WebSocketDisconnect:
        await manager.disconnect(websocket, license_id)


@app.websocket("/ws")
async def websocket_endpoint_query(websocket: WebSocket, license: str = Query(None)):
    """
    WebSocket endpoint supporting query parameter: /ws?license=KEY
    """
    if not license:
        await websocket.close(code=4003, reason="License key required")
        return
    await handle_websocket_connection(websocket, license)


@app.websocket("/ws/{license_key}")
async def websocket_endpoint(websocket: WebSocket, license_key: str):
    """
    WebSocket endpoint supporting path parameter: /ws/KEY
    """
    await handle_websocket_connection(websocket, license_key)


# ============ Paginated Endpoints ============

@app.get("/api/inbox/paginated", tags=["CRM"])
async def get_inbox_paginated(
    page: int = 1,
    page_size: int = 20,
    channel: str = None,
    is_read: bool = None,
    license: dict = Depends(verify_license)
):
    """
    Get inbox messages with pagination.
    
    Returns:
        - items: List of messages
        - pagination: {total, page, page_size, total_pages, has_next, has_prev}
    """
    return await paginate_inbox(
        license_id=license["license_id"],
        page=page,
        page_size=page_size,
        channel=channel,
        is_read=is_read,
    )


@app.get("/api/crm/paginated", tags=["CRM"])
async def get_crm_paginated(
    page: int = 1,
    page_size: int = 20,
    license: dict = Depends(verify_license)
):
    """
    Get CRM entries with pagination.
    """
    return await paginate_crm(
        license_id=license["license_id"],
        page=page,
        page_size=page_size,
    )


@app.get("/api/customers/paginated", tags=["CRM"])
async def get_customers_paginated(
    page: int = 1,
    page_size: int = 20,
    search: str = None,
    license: dict = Depends(verify_license)
):
    """
    Get customers with pagination and optional search.
    """
    return await paginate_customers(
        license_id=license["license_id"],
        page=page,
        page_size=page_size,
        search=search,
    )

@app.post("/api/draft", response_model=ProcessingResponse, tags=["Analysis"])
@limiter.limit("30/minute")  # Rate limit: 30 requests per minute per IP
async def draft_response(
    request: Request,
    data: MessageInput,
    license: dict = Depends(verify_license)
):
    """
    Generate a draft response for a message.
    
    Requires: X-License-Key header
    
    Args:
        data: Message input with text and metadata
        
    Returns:
        Draft response with suggested reply text
    """
    # Sanitize inputs
    sanitized_message = sanitize_message(data.message)
    sanitized_sender_name = sanitize_string(data.sender_name) if data.sender_name else None
    sanitized_sender_contact = sanitize_string(data.sender_contact) if data.sender_contact else None
    
    license_id = license["license_id"]

    await increment_usage(
        license_id,
        "draft",
        sanitized_message[:100]
    )
    
    prefs = await get_preferences(license_id)

    # Load recent conversation history for this sender (if available)
    conversation_history = ""
    if sanitized_sender_contact:
        # Use new unified history function
        from models.inbox import get_chat_history_for_llm
        conversation_history = await get_chat_history_for_llm(
            license_id=license_id,
            sender_contact=sanitized_sender_contact,
            limit=10,
        )

    result = await process_message(
        message=sanitized_message,
        message_type=data.message_type,
        sender_name=sanitized_sender_name,
        sender_contact=sanitized_sender_contact,
        preferences=prefs,
        history=conversation_history,  # CORRECTED
    )
    
    if result["success"]:
        return ProcessingResponse(
            success=True,
            data=AnalysisResult(**result["data"])
        )
    else:
        return ProcessingResponse(
            success=False,
            error=result["error"]
        )


@app.post("/api/crm/save", tags=["CRM"])
async def save_to_crm(
    data: CRMEntryCreate,
    license: dict = Depends(verify_license)
):
    """
    Save an entry to CRM.
    
    Requires: X-License-Key header
    
    Args:
        data: CRM entry data
        
    Returns:
        Success status and entry ID
    """
    await increment_usage(
        license["license_id"],
        "crm_save",
        data.original_message[:100] if data.original_message else None
    )
    
    entry_id = await save_crm_entry(
        license_id=license["license_id"],
        sender_name=data.sender_name,
        sender_contact=data.sender_contact,
        message_type=data.message_type,
        intent=data.intent,
        extracted_data=data.extracted_data,
        original_message=data.original_message,
        draft_response=data.draft_response
    )
    
    return {"success": True, "entry_id": entry_id, "message": "تم الحفظ بنجاح"}


@app.get("/api/ai-usage", tags=["Analytics"])
async def get_ai_usage_endpoint(
    license: dict = Depends(verify_license)
):
    """
    Get current AI usage quota for the day.
    
    Returns:
        used: Number of messages processed today
        limit: Daily limit (50)
        remaining: Remaining quota
        percentage: Usage percentage
    """
    from models import get_ai_usage_today
    return await get_ai_usage_today(license["license_id"])


@app.get("/api/crm/entries", response_model=CRMListResponse, tags=["CRM"])
async def list_crm_entries(
    limit: int = 50,
    license: dict = Depends(verify_license)
):
    """
    List CRM entries.
    
    Requires: X-License-Key header
    
    Args:
        limit: Maximum number of entries to return (default: 50)
        
    Returns:
        List of CRM entries
    """
    entries = await get_crm_entries(license["license_id"], limit)
    
    return CRMListResponse(
        entries=[CRMEntry(**e) for e in entries],
        total=len(entries)
    )


@app.get("/api/crm/entries/{entry_id}", tags=["CRM"])
async def get_crm_entry(
    entry_id: int,
    license: dict = Depends(verify_license)
):
    """
    Get a specific CRM entry by ID.
    
    Requires: X-License-Key header
    
    Args:
        entry_id: CRM entry ID
        
    Returns:
        CRM entry details
    """
    entry = await get_entry_by_id(entry_id, license["license_id"])
    
    if not entry:
        raise HTTPException(status_code=404, detail="السجل غير موجود")
    
    return {"success": True, "entry": CRMEntry(**entry)}


@app.get("/api/user/info", tags=["Authentication"])
async def get_user_info(license: dict = Depends(verify_license)):
    """
    Get current user/license information.
    
    Requires: X-License-Key header
    
    Returns:
        License details including company name, expiration, and remaining requests
    """
    return {
        "company_name": license["company_name"],
        "created_at": license.get("created_at"),
        "expires_at": license["expires_at"],
        "requests_remaining": license["requests_remaining"]
    }


# ============ Error Handlers ============

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    logger.warning(
        f"HTTP {exc.status_code} error: {exc.detail}",
        extra={"extra_fields": {"path": request.url.path, "method": request.method}}
    )
    return JSONResponse(
        status_code=exc.status_code,
        content={"success": False, "error": exc.detail}
    )


@app.exception_handler(RateLimitExceeded)
async def rate_limit_handler(request: Request, exc: RateLimitExceeded):
    logger.warning(
        f"Rate limit exceeded for {get_remote_address(request)}",
        extra={"extra_fields": {"path": request.url.path, "ip": get_remote_address(request)}}
    )
    return JSONResponse(
        status_code=429,
        content={"success": False, "error": "تم تجاوز الحد المسموح. يرجى المحاولة لاحقاً"}
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    logger.error(
        f"Unhandled exception: {exc}",
        exc_info=True,
        extra={"extra_fields": {"path": request.url.path, "method": request.method}}
    )
    payload = {"success": False, "error": "حدث خطأ في الخادم"}
    # When DEBUG_ERRORS=1 (set via env variable), include debug info in response
    if DEBUG_ERRORS:
        payload["debug"] = {
            "type": type(exc).__name__,
            "message": str(exc),
        }
    return JSONResponse(status_code=500, content=payload)


# ============ Run Server ============

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )

