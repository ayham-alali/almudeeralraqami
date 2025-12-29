#!/usr/bin/env python3
"""Quick check of data by date."""
import asyncio
import asyncpg

async def check():
    conn = await asyncpg.connect('postgresql://postgres:YmXFyjeYZdrUSpixHuqyRkEWeBHCaaqb@shortline.proxy.rlwy.net:56163/railway')
    
    # Check messages by date
    msgs = await conn.fetch('''
        SELECT DATE(created_at) as msg_date, COUNT(*) as count
        FROM inbox_messages
        WHERE status != 'pending' AND sentiment IS NOT NULL
        GROUP BY DATE(created_at)
        ORDER BY msg_date
    ''')
    print('Messages by date (with sentiment):')
    for m in msgs:
        print(f'  {m["msg_date"]}: {m["count"]} messages')
    
    # Check analytics records
    ana = await conn.fetch('''
        SELECT date, license_key_id, messages_received, positive_sentiment, negative_sentiment, neutral_sentiment
        FROM analytics
        ORDER BY license_key_id, date
    ''')
    print()
    print('Analytics records:')
    for a in ana:
        total_sent = (a["positive_sentiment"] or 0) + (a["negative_sentiment"] or 0) + (a["neutral_sentiment"] or 0)
        print(f'  License {a["license_key_id"]} | {a["date"]}: msgs={a["messages_received"]}, pos={a["positive_sentiment"]}, neg={a["negative_sentiment"]}, neu={a["neutral_sentiment"]}, total_sentiment={total_sent}')
    
    await conn.close()

asyncio.run(check())
