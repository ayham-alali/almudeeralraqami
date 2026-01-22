import asyncio
import os
import sys

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from models.inbox import get_conversation_messages_cursor

async def verify_sorting():
    print("=" * 80)
    print("VERIFYING CHRONOLOGICAL SORTING IN get_conversation_messages_cursor")
    print("=" * 80)
    
    # Fetch newest backwards
    result = await get_conversation_messages_cursor(license_id=5, sender_contact='+963968478904', limit=5)
    
    messages = result['messages']
    print(f"\nFetched {len(messages)} messages (Newest first).")
    
    # We expect 1371 (9:26) to come BEFORE 1372 (9:07) in a DESC list
    print("\nOrder Check (Expected: 1371 then 1372):")
    for m in messages:
        ts = m.get('received_at') or m.get('created_at')
        print(f"  ID: {m['id']} | Time: {ts} | Body: {m['body'][:30]}")
        
    all_ids = [m['id'] for m in messages]
    try:
        idx_1371 = all_ids.index(1371)
        idx_1372 = all_ids.index(1372)
        
        if idx_1371 < idx_1372:
            print(f"\n✅ PASSED: ID 1371 (9:26) correctly comes before ID 1372 (9:07) in DESC list.")
        else:
            print(f"\n❌ FAILED: ID 1372 still comes before ID 1371.")
    except ValueError as e:
        print(f"\n⚠️ Search failed: {e}")
            
    print("\n" + "=" * 80)

if __name__ == "__main__":
    asyncio.run(verify_sorting())
