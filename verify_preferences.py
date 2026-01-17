import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)
import asyncio
import os
import sys

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from models.base import init_models
from models import get_preferences, update_preferences, delete_preferences

# Mock License ID for testing
LICENSE_ID = 99999

async def test_preferences_flow():
    print(f"--- Testing Preferences Flow for License {LICENSE_ID} ---")
    
    # 1. Cleanup
    await delete_preferences(LICENSE_ID)
    print("[x] Cleaned up old preferences")

    # 2. Get Default (Should create them)
    prefs = await get_preferences(LICENSE_ID)
    print(f"[x] Defaults fetched: Tone={prefs.get('tone')}, Langs={prefs.get('preferred_languages')}, Notif={prefs.get('notifications_enabled')}")
    
    assert prefs['tone'] == 'formal'
    assert isinstance(prefs['preferred_languages'], list)
    assert 'ar' in prefs['preferred_languages']

    # 3. Update with List (JSON logic check)
    new_langs = ["en", "fr"]
    success = await update_preferences(
        LICENSE_ID, 
        preferred_languages=new_langs,
        tone="friendly",
        notifications_enabled=False
    )
    print(f"[x] Update success: {success}")
    assert success is True

    # 4. Verify Persistence
    updated_prefs = await get_preferences(LICENSE_ID)
    print(f"[x] Updated fetched: Tone={updated_prefs.get('tone')}, Langs={updated_prefs.get('preferred_languages')}, Notif={updated_prefs.get('notifications_enabled')}")
    
    assert updated_prefs['tone'] == 'friendly'
    assert updated_prefs['notifications_enabled'] == False
    assert updated_prefs['preferred_languages'] == new_langs
    
    # 5. Verify CSV Backward Compatibility (Manually inject CSV)
    from db_helper import get_db, execute_sql, commit_db
    async with get_db() as db:
        await execute_sql(
            db, 
            "UPDATE user_preferences SET preferred_languages = ? WHERE license_key_id = ?",
            ["es,it", LICENSE_ID]
        )
        await commit_db(db)
    
    legacy_prefs = await get_preferences(LICENSE_ID)
    print(f"[x] Legacy CSV fetched as: {legacy_prefs.get('preferred_languages')}")
    
    langs = legacy_prefs['preferred_languages']
    if not isinstance(langs, list):
        print(f"FAILED: Expected list, got {type(langs)}")
    if 'es' not in langs:
        print(f"FAILED: 'es' not in {langs}")
    if 'it' not in langs:
        print(f"FAILED: 'it' not in {langs}")
        
    assert isinstance(langs, list)
    assert 'es' in langs
    assert 'it' in langs

    print("\n[SUCCESS] All preference tests passed!")

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(test_preferences_flow())
