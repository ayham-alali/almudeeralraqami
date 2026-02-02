"""
Al-Mudeer Multi-Account & Authorization Verification
Tests specifically for:
1. One-Active-Account-Per-Device policy (FCM)
2. Logout token cleanup
3. License switching behavior
"""

import pytest
from services.fcm_mobile_service import save_fcm_token, ensure_fcm_tokens_table
from db_helper import fetch_one, execute_sql, commit_db

# Use existing db_session fixture from conftest.py
"""
Al-Mudeer Multi-Account & Authorization Verification
Tests specifically for:
1. One-Active-Account-Per-Device policy (FCM)
2. Logout token cleanup
3. License switching behavior
"""

import pytest
from unittest.mock import patch, MagicMock
from contextlib import asynccontextmanager
from services.fcm_mobile_service import save_fcm_token, ensure_fcm_tokens_table, remove_fcm_token
from db_helper import fetch_one, execute_sql, commit_db

# Helper to mock get_db behavior using shared session
@asynccontextmanager
async def mock_get_db_context(db):
    yield db

@pytest.mark.asyncio
async def test_fcm_token_migration_same_device(db_session):
    """
    Verify 'One Active Account per Device' policy.
    When a new account logs in on the same device, the FCM token should be re-assigned 
    to the new account to prevent 'leaked' notifications to the old account.
    """
    
    # Patch get_db to return our shared db_session
    # This ensures ensure_fcm_tokens_table and save_fcm_token use the SAME in-memory DB
    with patch("db_helper.get_db", side_effect=lambda: mock_get_db_context(db_session)):
        
        # 1. Setup: Ensure tables and create two licenses
        await ensure_fcm_tokens_table()
        
        # Create License A
        await execute_sql(db_session, 
            "INSERT INTO license_keys (key_hash, company_name, is_active) VALUES (?, ?, ?)",
            ["hash_A", "Company A", 1]
        )
        license_a = await fetch_one(db_session, "SELECT id FROM license_keys WHERE key_hash = ?", ["hash_A"])
        id_a = license_a["id"]
        
        # Create License B
        await execute_sql(db_session, 
            "INSERT INTO license_keys (key_hash, company_name, is_active) VALUES (?, ?, ?)",
            ["hash_B", "Company B", 1]
        )
        license_b = await fetch_one(db_session, "SELECT id FROM license_keys WHERE key_hash = ?", ["hash_B"])
        id_b = license_b["id"]
        await commit_db(db_session)
        
        device_id = "device_123"
        fcm_token = "fcm_token_xyz"
        
        # 2. Register Token for Account A
        await save_fcm_token(id_a, fcm_token, "android", device_id)
        
        # Verify A has the token active
        token_row = await fetch_one(db_session, 
            "SELECT * FROM fcm_tokens WHERE token = ? AND license_key_id = ?", 
            [fcm_token, id_a]
        )
        assert token_row is not None
        assert token_row["is_active"] == 1
        assert token_row["device_id"] == device_id

        # 3. Switch Account: Register SAME Token for Account B on SAME Device
        # This simulates the user logging into Account B on the same phone
        await save_fcm_token(id_b, fcm_token, "android", device_id)
        
        # 4. Verify Migration
        
        # Check Account A: Should NOT have an active token for this device anymore
        token_row_a_new = await fetch_one(db_session, 
            "SELECT * FROM fcm_tokens WHERE token = ? AND license_key_id = ?", 
            [fcm_token, id_a]
        )
        # Depending on implementation, it might be deactivated or removed
        # The current implementation deactivates entries with the same device ID but different license
        # WAIT: save_fcm_token implementation performs migration if token matches
        
        # Specific Check: The token row for A should be gone or deactivated?
        # save_fcm_token updates the existing row with new license_key_id
        if token_row_a_new:
             # If the row ID was key_A, and it was UPDATED to key_B, then select * where key=key_A will verify it's gone
             pass
        
        # Let's verify by ID to be sure what happened
        # We expect the token row associated with id_a is now associated with id_b, OR id_a is inactive
        
        # Verify A does NOT have active token
        count_a = await fetch_one(db_session, 
            "SELECT COUNT(*) as count FROM fcm_tokens WHERE license_key_id = ? AND is_active = 1", 
            [id_a]
        )
        assert count_a["count"] == 0, "Old account should not have active tokens"

        # Check Account B: Should OWN the token now
        token_row_b = await fetch_one(db_session, 
            "SELECT * FROM fcm_tokens WHERE token = ? AND license_key_id = ?", 
            [fcm_token, id_b]
        )
        assert token_row_b is not None
        assert token_row_b["is_active"] == 1
        assert token_row_b["device_id"] == device_id
        
        # 5. Verify Deduplication
        # Registering B again should not duplicate rows
        await save_fcm_token(id_b, fcm_token, "android", device_id)
        
        count_b = await fetch_one(db_session, 
            "SELECT COUNT(*) as count FROM fcm_tokens WHERE license_key_id = ? AND is_active = 1", 
            [id_b]
        )
        assert count_b["count"] == 1, "Should definitely limit to one active token per device"

@pytest.mark.asyncio
async def test_logout_cleanup_simulation(db_session):
    """
    Verify logic for cleaning up tokens on logout.
    """
    # Patch get_db to return our shared db_session
    with patch("db_helper.get_db", side_effect=lambda: mock_get_db_context(db_session)):
        await ensure_fcm_tokens_table()
        
        # Setup License
        await execute_sql(db_session, 
            "INSERT INTO license_keys (key_hash, company_name, is_active) VALUES (?, ?, ?)",
            ["hash_C", "Company C", 1]
        )
        license_c = await fetch_one(db_session, "SELECT id FROM license_keys WHERE key_hash = ?", ["hash_C"])
        id_c = license_c["id"]
        
        fcm_token = "logout_token_123"
        await save_fcm_token(id_c, fcm_token, "ios", "device_ios_999")
        
        # Verify Active
        row = await fetch_one(db_session, "SELECT is_active FROM fcm_tokens WHERE token = ?", [fcm_token])
        assert row is not None
        assert row["is_active"] == 1
        
        # Simulate Logout
        await remove_fcm_token(fcm_token)
        
        # Verify Removed
        row_after = await fetch_one(db_session, "SELECT * FROM fcm_tokens WHERE token = ?", [fcm_token])
        assert row_after is None, "Token should be deleted on logout"
