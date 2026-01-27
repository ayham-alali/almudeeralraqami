"""
Al-Mudeer - Zakat Calculation Service
Provides business owners with Zakat estimations based on Sharia rules.
"""

from typing import Dict, Any
from datetime import datetime
from db_helper import get_db, fetch_one, DB_TYPE

# Standard Zakat Rate (2.5%)
ZAKAT_RATE = 0.025

# Approximate Gold Nisab (85g of Gold) in USD/Local Currency
# In a real app, this should be fetched from an API or set by the user.
# For now, we use a constant as a placeholder.
GOLD_NISAB_USD = 5000 

async def calculate_zakat_summary(license_id: int) -> Dict[str, Any]:
    """
    Calculates a Zakat estimation for the business.
    Zakatable Assets = Liquidity + Trade Goods + Receivables
    Liabilities = Debts to be paid
    """
    async with get_db() as db:
        # 1. Calculate Liquidity (Total amount from completed transactions - this is a proxy for cash flow)
        # Note: In a full ERP, this would be the actual Cash/Bank balance.
        liquidity_row = await fetch_one(
            db,
            """
            SELECT SUM(amount) as total
            FROM purchases
            WHERE license_key_id = ? AND status = 'completed'
            """,
            [license_id]
        )
        total_liquidity = float(liquidity_row["total"]) if liquidity_row and liquidity_row["total"] else 0.0
        
        # 2. Calculate Active Receivables (Qard given to customers)
        receivables_row = await fetch_one(
            db,
            """
            SELECT SUM(amount) as total
            FROM purchases
            WHERE license_key_id = ? AND payment_type = 'deferred' AND qard_status = 'active'
            """,
            [license_id]
        )
        total_receivables = float(receivables_row["total"]) if receivables_row and receivables_row["total"] else 0.0
        
        # 3. Calculate Zakat Base
        # Zakat Base = Assets - Liabilities
        # For simplicity, we assume liabilities are 0 unless specifically tracked in an 'expenses' table.
        zakat_base = total_liquidity + total_receivables
        
        # 4. Check against Nisab
        # We assume currency is SYP for Syrian market or generic.
        # In a real scenario, we'd convert Nisab to local currency.
        is_above_nisab = zakat_base > (GOLD_NISAB_USD * 1000) # Simple conversion factor
        
        estimated_zakat = zakat_base * ZAKAT_RATE if is_above_nisab else 0.0
        
        return {
            "zakat_base": zakat_base,
            "estimated_zakat": estimated_zakat,
            "is_above_nisab": is_above_nisab,
            "currency": "SYP", # Default to Syrian Pound
            "rate": ZAKAT_RATE,
            "calculation_date": datetime.utcnow().isoformat()
        }
