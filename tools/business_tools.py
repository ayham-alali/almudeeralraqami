
"""
Al-Mudeer - Actionable Business Tools
These functions are callable by the AI agent to interact with live data.
"""

import asyncio
from datetime import datetime
from db_helper import get_db, fetch_one

async def get_business_hours(license_id: int):
    """Get the business hours for the specific license"""
    # In a real app, this would query the DB
    return "9:00 AM - 6:00 PM, Saturday to Thursday. Closed on Friday."

async def check_order_status(order_ref: str):
    """Check the status of a specific order in the database"""
    async with get_db() as db:
        order = await fetch_one(db, "SELECT status, updated_at FROM orders WHERE order_ref = ?", [order_ref])
        if order:
            return f"Order {order_ref} status is '{order['status']}'. Last updated: {order['updated_at']}"
        return f"Order {order_ref} not found."

async def get_product_info(product_name: str):
    """Lookup product price or details"""
    # Simulated product DB
    products = {
        "قمح": "شوال القمح بسعر 50 دولار متوفر حالياً.",
        "ذرة": "الذرة بسعر 40 دولار للشوال.",
        "سماد": "الأسمدة العضوية متوفرة بسعر 25 دولار للكيس."
    }
    for k, v in products.items():
        if k in product_name:
            return v
    return "المنتج غير موجود حالياً، لكن يمكننا توفيره عند الطلب."

# Tool Definitions for LLM (JSON Schema)
BUSINESS_TOOLS = [
    {
        "name": "get_business_hours",
        "description": "Get the opening and closing hours of the store/business.",
        "parameters": {
            "type": "object",
            "properties": {
                "license_id": {"type": "integer", "description": "The business license ID"}
            }
        }
    },
    {
        "name": "check_order_status",
        "description": "Check the delivery or shipping status of a customer order using its reference number (e.g. REF-123).",
        "parameters": {
            "type": "object",
            "properties": {
                "order_ref": {"type": "string", "description": "The order reference number"}
            },
            "required": ["order_ref"]
        }
    },
    {
        "name": "get_product_info",
        "description": "Get pricing and availability for specific products mentioned by the customer.",
        "parameters": {
            "type": "object",
            "properties": {
                "product_name": {"type": "string", "description": "The name of the product"}
            },
            "required": ["product_name"]
        }
    }
]

async def execute_tool(tool_name: str, args: dict):
    """Execute a tool by name with arguments"""
    if tool_name == "get_business_hours":
        return await get_business_hours(args.get("license_id", 1))
    elif tool_name == "check_order_status":
        return await check_order_status(args.get("order_ref"))
    elif tool_name == "get_product_info":
        return await get_product_info(args.get("product_name"))
    return f"Error: Tool {tool_name} not found."
