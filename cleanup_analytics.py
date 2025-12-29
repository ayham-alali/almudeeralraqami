#!/usr/bin/env python3
"""
Analytics & Customers Cleanup Script
Reconciles:
  1. analytics.messages_received with actual inbox_messages count
  2. customers.total_messages with actual linked inbox_messages count
  3. customers missing phone/email - fills from linked inbox_messages

Usage:
    python cleanup_analytics.py --database-url "postgresql://..."
    
Options:
    --dry-run    Show what would be changed without modifying data
"""

import asyncio
import argparse
from datetime import datetime

try:
    import asyncpg
except ImportError:
    print("Error: asyncpg required. Install with: pip install asyncpg")
    exit(1)


async def run_cleanup(database_url: str, dry_run: bool = False):
    """Run the analytics and customers cleanup process."""
    
    print(f"\n{'='*60}")
    print(f"Analytics & Customers Cleanup Script")
    print(f"{'='*60}")
    print(f"Database: {database_url[:40]}...")
    print(f"Mode: {'DRY RUN (no changes)' if dry_run else 'LIVE (will modify data)'}")
    print(f"Time: {datetime.now().isoformat()}")
    print(f"{'='*60}\n")
    
    # Connect to database
    conn = await asyncpg.connect(database_url)
    print("[OK] Connected to database\n")
    
    try:
        # =============================================
        # PART 1: ANALYTICS CLEANUP
        # =============================================
        print("=" * 40)
        print("PART 1: ANALYTICS CLEANUP")
        print("=" * 40 + "\n")
        
        # Get current analytics totals per license
        print("[1] Fetching current analytics...")
        current_analytics = await conn.fetch("""
            SELECT 
                license_key_id,
                SUM(messages_received) as analytics_total
            FROM analytics
            GROUP BY license_key_id
            ORDER BY license_key_id
        """)
        
        for row in current_analytics:
            print(f"  License {row['license_key_id']}: {row['analytics_total']} messages in analytics")
        
        # Get actual inbox_messages count per license
        print("\n[2] Counting actual inbox messages...")
        actual_counts = await conn.fetch("""
            SELECT 
                license_key_id,
                COUNT(*) as inbox_total
            FROM inbox_messages
            WHERE status != 'pending'
            GROUP BY license_key_id
            ORDER BY license_key_id
        """)
        
        for row in actual_counts:
            print(f"  License {row['license_key_id']}: {row['inbox_total']} actual messages in inbox")
        
        # Get detailed breakdown by date for correction
        print("\n[3] Building correction data...")
        inbox_by_date = await conn.fetch("""
            SELECT 
                license_key_id,
                DATE(created_at) as msg_date,
                COUNT(*) as actual_count
            FROM inbox_messages
            WHERE status != 'pending'
            GROUP BY license_key_id, DATE(created_at)
            ORDER BY license_key_id, msg_date
        """)
        
        # Compare and prepare corrections
        print("\n[4] Calculating corrections...\n")
        
        analytics_corrections = []
        analytics_total_before = 0
        analytics_total_after = 0
        
        analytics_records = await conn.fetch("""
            SELECT id, license_key_id, date, messages_received
            FROM analytics
            ORDER BY license_key_id, date
        """)
        
        actual_lookup = {
            (row['license_key_id'], row['msg_date']): row['actual_count']
            for row in inbox_by_date
        }
        
        for record in analytics_records:
            key = (record['license_key_id'], record['date'])
            current_value = record['messages_received']
            correct_value = actual_lookup.get(key, 0)
            
            analytics_total_before += current_value
            analytics_total_after += correct_value
            
            if current_value != correct_value:
                analytics_corrections.append({
                    'id': record['id'],
                    'license_key_id': record['license_key_id'],
                    'date': record['date'],
                    'old_value': current_value,
                    'new_value': correct_value
                })
                print(f"  [CHANGE] License {record['license_key_id']}, {record['date']}: {current_value} -> {correct_value}")
        
        print(f"\n  Analytics Summary:")
        print(f"    Before: {analytics_total_before}, After: {analytics_total_after}")
        print(f"    Difference: {analytics_total_before - analytics_total_after}")
        print(f"    Records to update: {len(analytics_corrections)}")
        
        # =============================================
        # PART 2: CUSTOMERS MESSAGE COUNT CLEANUP
        # =============================================
        print("\n" + "=" * 40)
        print("PART 2: CUSTOMERS MESSAGE COUNT CLEANUP")
        print("=" * 40 + "\n")
        
        print("[1] Fetching current customer message counts...")
        current_customers = await conn.fetch("""
            SELECT id, name, total_messages
            FROM customers
            WHERE total_messages > 0
            ORDER BY total_messages DESC
        """)
        
        for row in current_customers[:10]:
            name = row['name'] or 'No Name'
            print(f"  Customer {row['id']} ({name}): {row['total_messages']} in total_messages")
        if len(current_customers) > 10:
            print(f"  ... and {len(current_customers) - 10} more customers")
        
        print("\n[2] Counting actual linked inbox messages per customer...")
        actual_customer_counts = await conn.fetch("""
            SELECT 
                c.id,
                c.name,
                c.total_messages as current_count,
                COUNT(cm.inbox_message_id) as actual_count
            FROM customers c
            LEFT JOIN customer_messages cm ON cm.customer_id = c.id
            LEFT JOIN inbox_messages im ON im.id = cm.inbox_message_id AND im.status != 'pending'
            GROUP BY c.id, c.name, c.total_messages
            HAVING c.total_messages != COUNT(cm.inbox_message_id) OR c.total_messages > 0
            ORDER BY c.total_messages DESC
        """)
        
        print("\n[3] Calculating customer count corrections...\n")
        
        customer_count_corrections = []
        customer_total_before = 0
        customer_total_after = 0
        
        for row in actual_customer_counts:
            current_value = row['current_count'] or 0
            correct_value = row['actual_count'] or 0
            
            customer_total_before += current_value
            customer_total_after += correct_value
            
            if current_value != correct_value:
                customer_count_corrections.append({
                    'id': row['id'],
                    'name': row['name'] or 'No Name',
                    'old_value': current_value,
                    'new_value': correct_value
                })
                name = row['name'] or 'No Name'
                print(f"  [CHANGE] Customer {row['id']} ({name}): {current_value} -> {correct_value}")
        
        print(f"\n  Customers Count Summary:")
        print(f"    Before: {customer_total_before}, After: {customer_total_after}")
        print(f"    Records to update: {len(customer_count_corrections)}")
        
        # =============================================
        # PART 3: CUSTOMERS MISSING CONTACT INFO
        # =============================================
        print("\n" + "=" * 40)
        print("PART 3: CUSTOMERS MISSING CONTACT INFO")
        print("=" * 40 + "\n")
        
        print("[1] Finding customers without phone or email...")
        missing_contact = await conn.fetch("""
            SELECT id, name, phone, email
            FROM customers
            WHERE (phone IS NULL OR phone = '') AND (email IS NULL OR email = '')
        """)
        
        print(f"  Found {len(missing_contact)} customers without contact info\n")
        
        print("[2] Looking up contact info from linked inbox messages...")
        
        contact_fixes = []
        
        for customer in missing_contact:
            # Try to find contact from linked inbox messages
            contact_row = await conn.fetchrow("""
                SELECT DISTINCT im.sender_contact, im.channel
                FROM customer_messages cm
                JOIN inbox_messages im ON im.id = cm.inbox_message_id
                WHERE cm.customer_id = $1
                  AND im.sender_contact IS NOT NULL 
                  AND im.sender_contact != ''
                LIMIT 1
            """, customer['id'])
            
            if contact_row and contact_row['sender_contact']:
                sender_contact = contact_row['sender_contact']
                # Determine if it's email or phone
                is_email = '@' in sender_contact
                
                contact_fixes.append({
                    'id': customer['id'],
                    'name': customer['name'] or 'No Name',
                    'field': 'email' if is_email else 'phone',
                    'value': sender_contact
                })
                name = customer['name'] or 'No Name'
                field_name = 'email' if is_email else 'phone'
                print(f"  [FIX] Customer {customer['id']} ({name}): {field_name} = {sender_contact}")
        
        print(f"\n  Contact Info Summary:")
        print(f"    Customers missing contact: {len(missing_contact)}")
        print(f"    Can be fixed from inbox: {len(contact_fixes)}")
        print(f"    Cannot be fixed (no linked messages): {len(missing_contact) - len(contact_fixes)}")
        
        # =============================================
        # APPLY ALL CORRECTIONS
        # =============================================
        print("\n" + "=" * 40)
        print("APPLYING CORRECTIONS")
        print("=" * 40 + "\n")
        
        total_changes = len(analytics_corrections) + len(customer_count_corrections) + len(contact_fixes)
        
        if total_changes == 0:
            print("[DONE] No corrections needed - all data is already accurate!")
            return
        
        if dry_run:
            print("[DRY RUN] No changes made. Run without --dry-run to apply changes.")
            return
        
        print("[5] Applying corrections...")
        
        async with conn.transaction():
            # Analytics corrections
            for correction in analytics_corrections:
                await conn.execute("""
                    UPDATE analytics 
                    SET messages_received = $1
                    WHERE id = $2
                """, correction['new_value'], correction['id'])
            
            # Customer count corrections
            for correction in customer_count_corrections:
                await conn.execute("""
                    UPDATE customers 
                    SET total_messages = $1
                    WHERE id = $2
                """, correction['new_value'], correction['id'])
            
            # Customer contact info fixes
            for fix in contact_fixes:
                if fix['field'] == 'email':
                    await conn.execute("""
                        UPDATE customers 
                        SET email = $1
                        WHERE id = $2
                    """, fix['value'], fix['id'])
                else:
                    await conn.execute("""
                        UPDATE customers 
                        SET phone = $1
                        WHERE id = $2
                    """, fix['value'], fix['id'])
        
        print(f"\n[SUCCESS] Updated {len(analytics_corrections)} analytics records!")
        print(f"[SUCCESS] Updated {len(customer_count_corrections)} customer message counts!")
        print(f"[SUCCESS] Fixed {len(contact_fixes)} customers with missing contact info!")
        
    finally:
        await conn.close()
        print("\n[DONE] Database connection closed.")


def main():
    parser = argparse.ArgumentParser(description='Clean up analytics and customer data')
    parser.add_argument('--database-url', required=True, help='PostgreSQL connection URL')
    parser.add_argument('--dry-run', action='store_true', help='Preview changes without applying')
    
    args = parser.parse_args()
    
    asyncio.run(run_cleanup(args.database_url, args.dry_run))


if __name__ == "__main__":
    main()
