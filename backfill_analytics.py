#!/usr/bin/env python3
"""
Backfill Missing Analytics Records

This script creates analytics records for dates that have inbox_messages
but no corresponding analytics entries. It aggregates sentiment counts
from actual message data.

Usage:
    python backfill_analytics.py --database-url "postgresql://..."
    python backfill_analytics.py --database-url "postgresql://..." --apply
"""

import asyncio
import argparse
from datetime import datetime, date
import asyncpg


async def backfill_analytics(database_url: str, dry_run: bool = True):
    """Backfill missing analytics records from inbox_messages."""
    print("=" * 70)
    print("Analytics Backfill Script")
    print("=" * 70)
    print(f"Mode: {'DRY RUN (checking only)' if dry_run else 'LIVE (will update database)'}")
    print()
    
    conn = await asyncpg.connect(database_url)
    
    try:
        # Get all dates that have messages, grouped by license
        message_dates = await conn.fetch("""
            SELECT 
                license_key_id,
                DATE(created_at) as msg_date,
                COUNT(*) as message_count,
                COUNT(*) FILTER (WHERE sentiment ILIKE '%positive%' OR sentiment ILIKE '%إيجابي%') as positive_count,
                COUNT(*) FILTER (WHERE sentiment ILIKE '%negative%' OR sentiment ILIKE '%سلبي%') as negative_count,
                COUNT(*) FILTER (WHERE sentiment ILIKE '%neutral%' OR sentiment ILIKE '%محايد%') as neutral_count
            FROM inbox_messages
            WHERE status != 'pending'
            GROUP BY license_key_id, DATE(created_at)
            ORDER BY license_key_id, msg_date
        """)
        
        print(f"Found {len(message_dates)} license/date combinations with messages")
        print()
        
        # Get existing analytics records
        existing = await conn.fetch("""
            SELECT license_key_id, date
            FROM analytics
        """)
        existing_set = set((r['license_key_id'], r['date']) for r in existing)
        
        print(f"Found {len(existing_set)} existing analytics records")
        print()
        
        # Find missing records
        missing = []
        updates_needed = []
        
        for row in message_dates:
            license_id = row['license_key_id']
            msg_date = row['msg_date']
            
            if (license_id, msg_date) not in existing_set:
                missing.append({
                    'license_id': license_id,
                    'date': msg_date,
                    'messages': row['message_count'],
                    'positive': row['positive_count'],
                    'negative': row['negative_count'],
                    'neutral': row['neutral_count'],
                })
            else:
                # Check if existing record needs updating
                current = await conn.fetchrow("""
                    SELECT messages_received, positive_sentiment, negative_sentiment, neutral_sentiment
                    FROM analytics
                    WHERE license_key_id = $1 AND date = $2
                """, license_id, msg_date)
                
                if current:
                    if (current['messages_received'] != row['message_count'] or
                        current['positive_sentiment'] != row['positive_count'] or
                        current['negative_sentiment'] != row['negative_count'] or
                        current['neutral_sentiment'] != row['neutral_count']):
                        updates_needed.append({
                            'license_id': license_id,
                            'date': msg_date,
                            'old_messages': current['messages_received'],
                            'new_messages': row['message_count'],
                            'old_positive': current['positive_sentiment'],
                            'new_positive': row['positive_count'],
                            'old_negative': current['negative_sentiment'],
                            'new_negative': row['negative_count'],
                            'old_neutral': current['neutral_sentiment'],
                            'new_neutral': row['neutral_count'],
                        })
        
        # Report missing records
        if missing:
            print(f"MISSING RECORDS ({len(missing)} to create):")
            print("-" * 70)
            for m in missing:
                print(f"  License {m['license_id']} | {m['date']}")
                print(f"    Messages: {m['messages']}")
                print(f"    Sentiment: +{m['positive']} / -{m['negative']} / ~{m['neutral']}")
            print()
        else:
            print("[OK] No missing analytics records")
            print()
        
        # Report updates needed
        if updates_needed:
            print(f"UPDATES NEEDED ({len(updates_needed)} records):")
            print("-" * 70)
            for u in updates_needed:
                print(f"  License {u['license_id']} | {u['date']}")
                print(f"    Messages: {u['old_messages']} -> {u['new_messages']}")
                print(f"    Positive: {u['old_positive']} -> {u['new_positive']}")
                print(f"    Negative: {u['old_negative']} -> {u['new_negative']}")
                print(f"    Neutral: {u['old_neutral']} -> {u['new_neutral']}")
            print()
        else:
            print("[OK] No existing records need updating")
            print()
        
        # Apply changes
        if not dry_run and (missing or updates_needed):
            print("APPLYING CHANGES...")
            print("-" * 70)
            
            # Insert missing records - get max id and increment
            for m in missing:
                # Get next ID
                max_id = await conn.fetchval("SELECT COALESCE(MAX(id), 0) + 1 FROM analytics")
                await conn.execute("""
                    INSERT INTO analytics (
                        id, license_key_id, date, messages_received, messages_replied,
                        auto_replies, positive_sentiment, negative_sentiment, neutral_sentiment,
                        time_saved_seconds
                    ) VALUES ($1, $2, $3, $4, 0, 0, $5, $6, $7, 0)
                """, max_id, m['license_id'], m['date'], m['messages'], 
                    m['positive'], m['negative'], m['neutral'])
            
            # Update existing records
            for u in updates_needed:
                await conn.execute("""
                    UPDATE analytics
                    SET messages_received = $1,
                        positive_sentiment = $2,
                        negative_sentiment = $3,
                        neutral_sentiment = $4
                    WHERE license_key_id = $5 AND date = $6
                """, u['new_messages'], u['new_positive'], u['new_negative'],
                    u['new_neutral'], u['license_id'], u['date'])
            
            print(f"[SUCCESS] Created {len(missing)} new records, updated {len(updates_needed)} existing records!")
        elif missing or updates_needed:
            print("-" * 70)
            print("DRY RUN - No changes made. Run with --apply to apply changes.")
        
        # Show summary
        print()
        print("=" * 70)
        print("AFTER BACKFILL - Expected Totals:")
        
        # Calculate expected totals
        total_positive = sum(m['positive'] for m in missing) + sum(u['new_positive'] for u in updates_needed)
        total_negative = sum(m['negative'] for m in missing) + sum(u['new_negative'] for u in updates_needed)
        total_neutral = sum(m['neutral'] for m in missing) + sum(u['new_neutral'] for u in updates_needed)
        
        # Add unchanged existing records
        unchanged = await conn.fetchrow("""
            SELECT 
                SUM(positive_sentiment) as pos,
                SUM(negative_sentiment) as neg,
                SUM(neutral_sentiment) as neu
            FROM analytics
        """)
        
        if not dry_run:
            # Re-query after changes
            final = await conn.fetchrow("""
                SELECT 
                    SUM(positive_sentiment) as pos,
                    SUM(negative_sentiment) as neg,
                    SUM(neutral_sentiment) as neu,
                    SUM(messages_received) as msgs
                FROM analytics
            """)
            total = (final['pos'] or 0) + (final['neg'] or 0) + (final['neu'] or 0)
            satisfaction = round((final['pos'] or 0) / max(total, 1) * 100)
            
            print(f"  Total Messages: {final['msgs']}")
            print(f"  Positive: {final['pos']}")
            print(f"  Negative: {final['neg']}")
            print(f"  Neutral: {final['neu']}")
            print(f"  Satisfaction Rate: {satisfaction}%")
        
    finally:
        await conn.close()


def main():
    parser = argparse.ArgumentParser(description='Backfill missing analytics records')
    parser.add_argument('--database-url', required=True, help='PostgreSQL database URL')
    parser.add_argument('--apply', action='store_true', help='Apply changes (default is dry-run)')
    
    args = parser.parse_args()
    
    asyncio.run(backfill_analytics(args.database_url, dry_run=not args.apply))


if __name__ == "__main__":
    main()
