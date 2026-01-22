import asyncio
import asyncpg
import json

async def inspect_timestamps():
    conn = await asyncpg.connect('')
    
    ids = [1371, 1372]
    print(f"\nINSPECTING TIMESTAMPS FOR MESSAGES {ids}:")
    
    rows = await conn.fetch("""
        SELECT id, body, created_at, received_at, status
        FROM inbox_messages
        WHERE id = ANY($1)
    """, ids)
    
    for r in rows:
        print(f"\nMessage ID: {r['id']}")
        print(f"  Body: {r['body'][:50]}...")
        print(f"  created_at:  {r['created_at']}")
        print(f"  received_at: {r['received_at']}")
        print(f"  status:      {r['status']}")
        
    await conn.close()

asyncio.run(inspect_timestamps())
