
import asyncio
import sys
import os
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add backend to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db_helper import get_db, fetch_all
from services.knowledge_base import get_knowledge_base

async def ingest_business_data():
    """
    Sync business preferences and data to the Knowledge Base (ChromaDB)
    """
    print("🚀 Starting Business Data Ingestion...")
    kb = get_knowledge_base()
    
    async with get_db() as db:
        # 1. Fetch User Preferences (Business Profile)
        preferences = await fetch_all(db, "SELECT * FROM user_preferences")
        
        for pref in preferences:
            license_id = pref['license_key_id']
            business_name = pref.get('business_name', 'Unnamed Business')
            industry = pref.get('industry', 'General')
            products = pref.get('products_services', '')
            tone_rules = pref.get('custom_tone_guidelines', '')
            
            # Construct a comprehensive "Business Fact" document
            fact_text = f"Business Name: {business_name}\nIndustry: {industry}\n"
            if products:
                fact_text += f"Products and Services: {products}\n"
            if tone_rules:
                fact_text += f"Customer Service Guidelines: {tone_rules}\n"
            
            # Add to KB
            print(f"Indexing profile for license {license_id} ({business_name})...")
            await kb.add_document(
                text=fact_text, 
                metadata={"license_id": license_id, "type": "business_profile"}
            )
            
        # 2. Fetch Recent Orders (Optional: for context)
        # We index orders as facts so GPT can answer "What's in order #123?"
        orders = await fetch_all(db, "SELECT * FROM orders ORDER BY created_at DESC LIMIT 100")
        for order in orders:
            order_fact = f"Order #{order['order_ref']} details: Status is {order['status']}. Items: {order['items']}. Total: {order['total_amount']}."
            await kb.add_document(
                text=order_fact,
                metadata={"license_id": 1, "type": "order", "ref": order['order_ref']} # Default to license 1 if not specified
            )

    print("✅ Ingestion complete.")

if __name__ == "__main__":
    asyncio.run(ingest_business_data())
