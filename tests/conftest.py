"""
Al-Mudeer Test Fixtures
Shared pytest fixtures for backend testing
"""

import os
import sys
import pytest
import asyncio
from typing import AsyncGenerator, Generator

# Set test environment
os.environ["TESTING"] = "1"
os.environ["DB_TYPE"] = "sqlite"
os.environ["DATABASE_PATH"] = ":memory:"
os.environ["ADMIN_KEY"] = "test-admin-key"
os.environ["ENCRYPTION_KEY"] = "test-encryption-key-for-tests"

# Add backend to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture(scope="session")
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    """Create event loop for async tests"""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
async def test_app():
    """Create test FastAPI app instance"""
    from main import app
    yield app


@pytest.fixture
async def test_client(test_app):
    """Create async test client"""
    from httpx import AsyncClient, ASGITransport
    
    transport = ASGITransport(app=test_app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.fixture
def sample_license_key() -> str:
    """Return a sample license key for testing"""
    return "MUDEER-TEST-1234-5678"


@pytest.fixture
def sample_message() -> dict:
    """Return a sample message for testing"""
    return {
        "body": "مرحباً، أريد الاستفسار عن الأسعار",
        "sender_name": "أحمد محمد",
        "sender_contact": "+963912345678",
        "channel": "telegram",
    }


@pytest.fixture
def auth_headers(sample_license_key) -> dict:
    """Return authentication headers"""
    return {"X-License-Key": sample_license_key}


@pytest.fixture
async def db_session():
    """Create a test database session"""
    from db_helper import get_db
    async with get_db() as db:
        yield db
