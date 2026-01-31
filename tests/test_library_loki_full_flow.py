import pytest
from fastapi.testclient import TestClient
import os
import asyncio

# Set DB URL provided by user
os.environ["DATABASE_URL"] = ""
os.environ["DB_TYPE"] = "postgresql"

from db_helper import execute_sql, commit_db, get_db
from main import app
from routes.library import router
from dependencies import get_license_from_header

# Global var to store a valid ID
VALID_LICENSE_ID = 1

async def setup_test_data():
    global VALID_LICENSE_ID
    async with get_db() as db:
        print("Fetching an existing valid license key...")
        try:
            row = await db.fetchrow("SELECT id FROM license_keys WHERE is_active = true LIMIT 1")
            if row:
                VALID_LICENSE_ID = row['id']
                print(f"Using existing License ID: {VALID_LICENSE_ID}")
            else:
                print("No active license found in DB. Test might fail.")
        except Exception as e:
            print(f"Setup error: {e}")

# Run setup
loop = asyncio.new_event_loop()
loop.run_until_complete(setup_test_data())

# Override dependency
async def mock_verify_license():
    # Use the global ID that was fetched from DB
    return {"license_id": VALID_LICENSE_ID, "valid": True, "license_key": "test_key", "plan": "premium"}

app.dependency_overrides[get_license_from_header] = mock_verify_license

client = TestClient(app)

def test_library_full_flow_sync():
    headers = {"X-License-Key": "test_key"}
    print(f"Starting test with License ID: {VALID_LICENSE_ID}")
    
    # 1. Create Note
    note_data = {
        "title": "Loki Test Note",
        "content": "This is a test note from Loki Mode.",
        "customer_id": 123
    }
    resp = client.post("/api/library/notes", json=note_data, headers=headers)
    assert resp.status_code == 200, f"Create Failed: {resp.text}"
    data = resp.json()
    assert data["success"] is True
    note_id = data["item"]["id"]
    print(f"✅ Note Created: {note_id}")

    # 2. Update Note
    update_data = {
        "title": "Loki Test Note Updated",
        "content": "Updated content."
    }
    resp = client.patch(f"/api/library/{note_id}", json=update_data, headers=headers)
    assert resp.status_code == 200, f"Update Failed: {resp.text}"
    print(f"✅ Note Updated: {note_id}")

    # 3. Search for Note
    resp = client.get("/api/library/?search=Loki", headers=headers)
    assert resp.status_code == 200, f"Search Failed: {resp.text}"
    data = resp.json()
    items = data["items"]
    assert len(items) > 0
    assert items[0]["title"] == "Loki Test Note Updated"
    print(f"✅ Search Verified")

    # 4. Pagination (Limit 1)
    resp = client.get("/api/library/?page=1&page_size=1", headers=headers)
    assert resp.status_code == 200, f"Pagination Failed: {resp.text}"
    data = resp.json()
    assert len(data["items"]) == 1
    print(f"✅ Pagination Verified")

    # 5. Delete Note
    resp = client.delete(f"/api/library/{note_id}", headers=headers)
    assert resp.status_code == 200, f"Delete Failed: {resp.text}"
    print(f"✅ Note Deleted")

    # 6. Verify Deletion
    resp = client.get("/api/library/?search=Loki", headers=headers)
    assert resp.status_code == 200
    data = resp.json()
    # Should be empty or not contain the deleted item
    ids = [item['id'] for item in data['items']]
    assert note_id not in ids
    print(f"✅ Deletion Verified")

if __name__ == "__main__":
    test_library_full_flow_sync()
