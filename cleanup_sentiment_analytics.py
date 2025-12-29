#!/usr/bin/env python3
"""
Cleanup Sentiment Analytics

This script checks and reconciles sentiment counts in the analytics table
with actual message sentiments from inbox_messages table.

Similar to the messages_received cleanup, sentiment counts may be inflated
because they were incremented early in the pipeline but never decremented
when messages were filtered, deleted, or failed processing.

Usage:
    python cleanup_sentiment_analytics.py --database-url "postgresql://..."
    python cleanup_sentiment_analytics.py --database-url "postgresql://..." --apply
"""

import asyncio
import argparse
from datetime import datetime, date
import asyncpg


async def check_and_fix_sentiments(database_url: str, dry_run: bool = True):
    """Check and fix sentiment counts in analytics table."""
    print("=" * 70)
    print("Sentiment Analytics Cleanup Script")
    print("=" * 70)
    print(f"Mode: {'DRY RUN (checking only)' if dry_run else 'LIVE (will update database)'}")
    print()
    
    conn = await asyncpg.connect(database_url)
    
    try:
        # ========== PART 1: CHECK DISCREPANCIES ==========
        print("PART 1: Checking sentiment count discrepancies")
        print("-" * 70)
        
        # Get current analytics sentiment counts
        analytics_rows = await conn.fetch("""
            SELECT 
                id,
                license_key_id,
                date,
                positive_sentiment,
                negative_sentiment,
                neutral_sentiment,
                messages_received
            FROM analytics
            ORDER BY license_key_id, date
        """)
        
        print(f"Found {len(analytics_rows)} analytics records to check")
        print()
        
        # For each analytics record, count actual sentiments from inbox_messages
        discrepancies = []
        total_positive_diff = 0
        total_negative_diff = 0
        total_neutral_diff = 0
        
        for row in analytics_rows:
            license_id = row['license_key_id']
            analytics_date = row['date']
            
            # Convert date for query
            if isinstance(analytics_date, date):
                date_str = analytics_date.isoformat()
            else:
                date_str = str(analytics_date)[:10]
            
            # Count actual sentiments from inbox_messages for this license and date
            actual_counts = await conn.fetchrow("""
                SELECT 
                    COUNT(*) FILTER (WHERE sentiment ILIKE '%positive%' OR sentiment ILIKE '%إيجابي%') as actual_positive,
                    COUNT(*) FILTER (WHERE sentiment ILIKE '%negative%' OR sentiment ILIKE '%سلبي%') as actual_negative,
                    COUNT(*) FILTER (WHERE sentiment ILIKE '%neutral%' OR sentiment ILIKE '%محايد%') as actual_neutral,
                    COUNT(*) as total_with_sentiment
                FROM inbox_messages
                WHERE license_key_id = $1
                  AND DATE(created_at) = $2
                  AND status != 'pending'
                  AND sentiment IS NOT NULL
            """, license_id, analytics_date)
            
            stored_positive = row['positive_sentiment'] or 0
            stored_negative = row['negative_sentiment'] or 0
            stored_neutral = row['neutral_sentiment'] or 0
            
            actual_positive = actual_counts['actual_positive'] or 0
            actual_negative = actual_counts['actual_negative'] or 0
            actual_neutral = actual_counts['actual_neutral'] or 0
            
            positive_diff = stored_positive - actual_positive
            negative_diff = stored_negative - actual_negative
            neutral_diff = stored_neutral - actual_neutral
            
            if positive_diff != 0 or negative_diff != 0 or neutral_diff != 0:
                discrepancies.append({
                    'id': row['id'],
                    'license_id': license_id,
                    'date': date_str,
                    'stored_positive': stored_positive,
                    'actual_positive': actual_positive,
                    'positive_diff': positive_diff,
                    'stored_negative': stored_negative,
                    'actual_negative': actual_negative,
                    'negative_diff': negative_diff,
                    'stored_neutral': stored_neutral,
                    'actual_neutral': actual_neutral,
                    'neutral_diff': neutral_diff,
                })
                total_positive_diff += positive_diff
                total_negative_diff += negative_diff
                total_neutral_diff += neutral_diff
        
        if not discrepancies:
            print("[OK] No sentiment discrepancies found! All counts match.")
            print()
        else:
            print(f"Found {len(discrepancies)} records with sentiment discrepancies:")
            print()
            
            for d in discrepancies:
                print(f"  License {d['license_id']} | Date: {d['date']}")
                if d['positive_diff'] != 0:
                    print(f"    Positive: {d['stored_positive']} stored -> {d['actual_positive']} actual (diff: {d['positive_diff']:+d})")
                if d['negative_diff'] != 0:
                    print(f"    Negative: {d['stored_negative']} stored -> {d['actual_negative']} actual (diff: {d['negative_diff']:+d})")
                if d['neutral_diff'] != 0:
                    print(f"    Neutral: {d['stored_neutral']} stored -> {d['actual_neutral']} actual (diff: {d['neutral_diff']:+d})")
                print()
            
            print("-" * 70)
            print(f"SUMMARY:")
            print(f"  Total Positive inflation: {total_positive_diff:+d}")
            print(f"  Total Negative inflation: {total_negative_diff:+d}")
            print(f"  Total Neutral inflation: {total_neutral_diff:+d}")
            print()
        
        # ========== PART 2: SHOW OVERALL STATS ==========
        print("PART 2: Overall Sentiment Statistics")
        print("-" * 70)
        
        # Current totals from analytics
        analytics_totals = await conn.fetchrow("""
            SELECT 
                SUM(positive_sentiment) as total_positive,
                SUM(negative_sentiment) as total_negative,
                SUM(neutral_sentiment) as total_neutral
            FROM analytics
        """)
        
        # Actual totals from inbox_messages
        actual_totals = await conn.fetchrow("""
            SELECT 
                COUNT(*) FILTER (WHERE sentiment ILIKE '%positive%' OR sentiment ILIKE '%إيجابي%') as actual_positive,
                COUNT(*) FILTER (WHERE sentiment ILIKE '%negative%' OR sentiment ILIKE '%سلبي%') as actual_negative,
                COUNT(*) FILTER (WHERE sentiment ILIKE '%neutral%' OR sentiment ILIKE '%محايد%') as actual_neutral
            FROM inbox_messages
            WHERE status != 'pending'
              AND sentiment IS NOT NULL
        """)
        
        print(f"Analytics table shows:")
        print(f"  Positive: {analytics_totals['total_positive'] or 0}")
        print(f"  Negative: {analytics_totals['total_negative'] or 0}")
        print(f"  Neutral: {analytics_totals['total_neutral'] or 0}")
        print()
        print(f"Actual inbox_messages show:")
        print(f"  Positive: {actual_totals['actual_positive'] or 0}")
        print(f"  Negative: {actual_totals['actual_negative'] or 0}")
        print(f"  Neutral: {actual_totals['actual_neutral'] or 0}")
        print()
        
        # Calculate what the satisfaction rate SHOULD be
        actual_pos = actual_totals['actual_positive'] or 0
        actual_neg = actual_totals['actual_negative'] or 0
        actual_neu = actual_totals['actual_neutral'] or 0
        actual_total = actual_pos + actual_neg + actual_neu
        
        stored_pos = analytics_totals['total_positive'] or 0
        stored_neg = analytics_totals['total_negative'] or 0
        stored_neu = analytics_totals['total_neutral'] or 0
        stored_total = stored_pos + stored_neg + stored_neu
        
        current_satisfaction = round(stored_pos / max(stored_total, 1) * 100)
        correct_satisfaction = round(actual_pos / max(actual_total, 1) * 100)
        
        print(f"Satisfaction Rate (المشاعر الإيجابية):")
        print(f"  Currently showing: {current_satisfaction}%")
        print(f"  Should be: {correct_satisfaction}%")
        print()
        
        # ========== PART 3: APPLY FIXES ==========
        if discrepancies and not dry_run:
            print("PART 3: Applying fixes...")
            print("-" * 70)
            
            for d in discrepancies:
                await conn.execute("""
                    UPDATE analytics 
                    SET positive_sentiment = $1, negative_sentiment = $2, neutral_sentiment = $3
                    WHERE id = $4
                """, d['actual_positive'], d['actual_negative'], d['actual_neutral'], d['id'])
            
            print(f"[SUCCESS] Updated {len(discrepancies)} analytics records!")
            print()
            print(f"New satisfaction rate will be: {correct_satisfaction}%")
        elif discrepancies:
            print("-" * 70)
            print("DRY RUN - No changes made. Run with --apply to fix the data.")
        
    finally:
        await conn.close()


def main():
    parser = argparse.ArgumentParser(description='Cleanup sentiment analytics counts')
    parser.add_argument('--database-url', required=True, help='PostgreSQL database URL')
    parser.add_argument('--apply', action='store_true', help='Apply fixes (default is dry-run)')
    
    args = parser.parse_args()
    
    asyncio.run(check_and_fix_sentiments(args.database_url, dry_run=not args.apply))


if __name__ == "__main__":
    main()
