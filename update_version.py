#!/usr/bin/env python3
"""
Al-Mudeer Version Update Script
Updated to use the Backend API (Database Source of Truth)

Usage:
  python update_version.py --build 5 --force --notes-ar "إصلاحات هامة" --notes-en "Critical bug fixes"

Arguments:
  --build       New build number (integer)
  --force       Enable force update (critical)
  --notes-ar    Changelog in Arabic
  --notes-en    Changelog in English (optional)
  --ios-url     Custom iOS Store URL (optional)
  --url         Backend URL (default: https://almudeer.up.railway.app)
  --key         Admin Key (env: ADMIN_KEY)
"""

import argparse
import os
import sys
import httpx
import asyncio

# Default Configuration
DEFAULT_URL = "https://almudeer.up.railway.app"
ENV_ADMIN_KEY = os.getenv("ADMIN_KEY", "")


async def update_version(args):
    base_url = args.url.rstrip("/")
    admin_key = args.key or ENV_ADMIN_KEY
    
    if not admin_key:
        print("❌ Error: Admin key is required. Set ADMIN_KEY env var or use --key.")
        sys.exit(1)
        
    headers = {
        "X-Admin-Key": admin_key,
        "Content-Type": "application/json"
    }
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        print(f"🔄 Connecting to {base_url}...")
        
        # 1. Set Min Build Number
        print(f"🚀 Setting minimum build number to {args.build} (Force: {args.force})...")
        try:
            priority = "critical" if args.force else "normal"
            payload = {
                "build_number": args.build,
                "is_soft_update": not args.force, # Force = !Soft
                "priority": priority,
                "ios_store_url": args.ios_url
            }
            
            # Note: Query params for scalar, but let's check the API definition
            # The API uses query params for simple types in the signature
            # But httpx.post(params=...) sends query params
            response = await client.post(
                f"{base_url}/api/app/set-min-build",
                headers=headers,
                params=payload 
            )
            response.raise_for_status()
            print(f"✅ Build number updated: {response.json()}")
        except httpx.HTTPStatusError as e:
            print(f"❌ Failed to set build number: {e.response.text}")
            sys.exit(1)
        except Exception as e:
            print(f"❌ Connection error: {str(e)}")
            sys.exit(1)
            
        # 2. Set Changelog
        print(f"📝 Updating changelog...")
        try:
            payload = {
                "changelog_ar": [args.notes_ar],
                "changelog_en": [args.notes_en] if args.notes_en else [],
                "release_notes_url": ""
            }
            
            # The API expects JSON body for lists
            response = await client.post(
                f"{base_url}/api/app/set-changelog",
                headers=headers,
                json=payload
            )
            response.raise_for_status()
            print(f"✅ Changelog updated: {response.json()}")
        except httpx.HTTPStatusError as e:
            print(f"❌ Failed to set changelog: {e.response.text}")
            sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Update Al-Mudeer App Version (via API)")
    
    parser.add_argument("--build", type=int, required=True, help="New build number")
    parser.add_argument("--force", action="store_true", help="Force this update (critical)")
    parser.add_argument("--notes-ar", required=True, help="Changelog details (Arabic)")
    parser.add_argument("--notes-en", help="Changelog details (English)")
    parser.add_argument("--ios-url", help="iOS App Store URL")
    
    parser.add_argument("--url", default=DEFAULT_URL, help=f"Backend base URL (default: {DEFAULT_URL})")
    parser.add_argument("--key", help="Admin API Key")

    args = parser.parse_args()
    
    asyncio.run(update_version(args))


if __name__ == "__main__":
    main()
