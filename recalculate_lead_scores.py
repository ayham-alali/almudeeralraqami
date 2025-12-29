#!/usr/bin/env python3
"""
Recalculate Lead Scores for All Customers

This script recalculates lead scores and segments for all existing customers
using the new improved scoring algorithm that includes:
- Purchase history (most important factor)
- Engagement duration
- Rebalanced intent/sentiment weights
- Stricter segment thresholds

Usage:
    python recalculate_lead_scores.py --database-url "postgresql://..."
    python recalculate_lead_scores.py --database-url "postgresql://..." --dry-run
"""

import asyncio
import argparse
from datetime import datetime
import asyncpg


def calculate_lead_score(
    total_messages: int,
    purchase_count: int = 0,
    total_purchase_value: float = 0.0,
    days_since_first_contact: int = None,
    days_since_last_contact: int = None,
    sentiment: str = None,
    intent: str = None
) -> int:
    """New improved lead score calculation."""
    score = 0
    
    # === PURCHASE HISTORY (0-35 points) ===
    if purchase_count > 0:
        if purchase_count >= 5:
            score += 20
        elif purchase_count >= 3:
            score += 15
        elif purchase_count >= 1:
            score += 10
        
        if total_purchase_value >= 1000:
            score += 15
        elif total_purchase_value >= 500:
            score += 10
        elif total_purchase_value >= 100:
            score += 5
    
    # === MESSAGE ENGAGEMENT (0-25 points) ===
    if total_messages == 0:
        score += 0
    elif total_messages <= 2:
        score += 5
    elif total_messages <= 5:
        score += 10
    elif total_messages <= 15:
        score += 15
    elif total_messages <= 30:
        score += 20
    else:
        score += 25
    
    # === ENGAGEMENT DURATION (0-15 points) ===
    if days_since_first_contact is not None:
        if days_since_first_contact >= 90:
            score += 15
        elif days_since_first_contact >= 30:
            score += 10
        elif days_since_first_contact >= 7:
            score += 5
    
    # === INTENT SIGNALS (0-15 points) ===
    if intent:
        intent_lower = intent.lower()
        if any(kw in intent_lower for kw in ["شراء", "طلب"]):
            score += 15
        elif any(kw in intent_lower for kw in ["عرض", "سعر"]):
            score += 10
        elif "استفسار" in intent_lower:
            score += 5
    
    # === SENTIMENT (0-10 points) ===
    if sentiment:
        sentiment_lower = sentiment.lower()
        if any(kw in sentiment_lower for kw in ["إيجابي", "positive"]):
            score += 10
        elif any(kw in sentiment_lower for kw in ["محايد", "neutral"]):
            score += 5
    
    # === RECENCY ADJUSTMENT (-5 to +5 points) ===
    if days_since_last_contact is not None:
        if days_since_last_contact <= 3:
            score += 5
        elif days_since_last_contact <= 14:
            score += 2
        elif days_since_last_contact > 60:
            score -= 5
    
    return max(0, min(100, score))


def determine_segment(lead_score: int, total_messages: int, is_vip: bool, purchase_count: int) -> str:
    """New improved segment determination."""
    if is_vip:
        return "VIP"
    
    if lead_score >= 75 and purchase_count > 0:
        return "High-Value"
    
    if lead_score >= 50 or purchase_count > 0:
        return "Warm Lead"
    
    if lead_score >= 25:
        return "Cold Lead"
    
    if total_messages == 0:
        return "New"
    
    return "Low-Engagement"


async def recalculate_all_scores(database_url: str, dry_run: bool = True):
    """Recalculate lead scores for all customers."""
    print("=" * 60)
    print("Lead Score Recalculation Script")
    print("=" * 60)
    print(f"Mode: {'DRY RUN (no changes)' if dry_run else 'LIVE (will update database)'}")
    print()
    
    conn = await asyncpg.connect(database_url)
    
    try:
        # Get all customers with their data
        customers = await conn.fetch("""
            SELECT 
                c.id,
                c.license_key_id,
                c.name,
                c.total_messages,
                c.lead_score as old_score,
                c.segment as old_segment,
                c.is_vip,
                c.created_at,
                c.last_contact_at,
                (SELECT COUNT(*) FROM purchases p WHERE p.customer_id = c.id) as purchase_count,
                (SELECT COALESCE(SUM(amount), 0) FROM purchases p WHERE p.customer_id = c.id) as total_purchase_value
            FROM customers c
            ORDER BY c.id
        """)
        
        print(f"Found {len(customers)} customers to process")
        print()
        
        changes = []
        now = datetime.utcnow()
        
        for customer in customers:
            # Calculate days since first/last contact
            days_since_first = None
            days_since_last = None
            
            if customer['created_at']:
                created = customer['created_at']
                if hasattr(created, 'replace'):
                    created = created.replace(tzinfo=None)
                days_since_first = (now - created).days
            
            if customer['last_contact_at']:
                last = customer['last_contact_at']
                if hasattr(last, 'replace'):
                    last = last.replace(tzinfo=None)
                days_since_last = (now - last).days
            
            # Calculate new score
            new_score = calculate_lead_score(
                total_messages=customer['total_messages'] or 0,
                purchase_count=customer['purchase_count'] or 0,
                total_purchase_value=float(customer['total_purchase_value'] or 0),
                days_since_first_contact=days_since_first,
                days_since_last_contact=days_since_last
            )
            
            # Determine new segment
            new_segment = determine_segment(
                lead_score=new_score,
                total_messages=customer['total_messages'] or 0,
                is_vip=customer['is_vip'] or False,
                purchase_count=customer['purchase_count'] or 0
            )
            
            old_score = customer['old_score'] or 0
            old_segment = customer['old_segment'] or 'New'
            
            if new_score != old_score or new_segment != old_segment:
                changes.append({
                    'id': customer['id'],
                    'license_id': customer['license_key_id'],
                    'name': customer['name'],
                    'messages': customer['total_messages'] or 0,
                    'purchases': customer['purchase_count'] or 0,
                    'old_score': old_score,
                    'new_score': new_score,
                    'old_segment': old_segment,
                    'new_segment': new_segment
                })
        
        # Print changes
        if not changes:
            print("[OK] No changes needed - all scores are already correct!")
            return
        
        print(f"Found {len(changes)} customers that need updates:")
        print("-" * 80)
        
        for c in changes:
            score_change = c['new_score'] - c['old_score']
            score_arrow = "^" if score_change > 0 else "v" if score_change < 0 else "="
            
            print(f"  Customer: {c['name'] or 'N/A'} (ID: {c['id']})")
            print(f"    Messages: {c['messages']}, Purchases: {c['purchases']}")
            print(f"    Score: {c['old_score']} -> {c['new_score']} ({score_arrow}{abs(score_change)})")
            print(f"    Segment: {c['old_segment']} -> {c['new_segment']}")
            print()
        
        # Apply changes if not dry run
        if not dry_run:
            print("-" * 80)
            print("Applying changes...")
            
            for c in changes:
                await conn.execute(
                    "UPDATE customers SET lead_score = $1, segment = $2 WHERE id = $3 AND license_key_id = $4",
                    c['new_score'], c['new_segment'], c['id'], c['license_id']
                )
            
            print(f"[SUCCESS] Updated {len(changes)} customer records!")
        else:
            print("-" * 80)
            print("DRY RUN - No changes made. Run with --apply to apply changes.")
        
    finally:
        await conn.close()


def main():
    parser = argparse.ArgumentParser(description='Recalculate customer lead scores')
    parser.add_argument('--database-url', required=True, help='PostgreSQL database URL')
    parser.add_argument('--apply', action='store_true', help='Apply changes (default is dry-run)')
    
    args = parser.parse_args()
    
    asyncio.run(recalculate_all_scores(args.database_url, dry_run=not args.apply))


if __name__ == "__main__":
    main()
