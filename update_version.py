
import os
import json
import argparse
import sys
from datetime import datetime

# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static", "download")
APK_VERSION_FILE = os.path.join(STATIC_DIR, "apk_version.txt")
CHANGELOG_FILE = os.path.join(STATIC_DIR, "changelog.json")
UPDATE_CONFIG_FILE = os.path.join(STATIC_DIR, "update_config.json")
VERSION_HISTORY_FILE = os.path.join(STATIC_DIR, "version_history.json")

def update_files(build_number, is_force, notes_ar, notes_en):
    # 1. Update minimum build number (apk_version.txt)
    with open(APK_VERSION_FILE, "w") as f:
        f.write(str(build_number))
    print(f"✅ Updated {APK_VERSION_FILE} to {build_number}")

    # 2. Update update_config.json
    try:
        with open(UPDATE_CONFIG_FILE, "r") as f:
            config = json.load(f)
    except FileNotFoundError:
        config = {}
    
    config["is_soft_update"] = not is_force
    config["priority"] = "critical" if is_force else "normal"
    
    with open(UPDATE_CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=4)
    print(f"✅ Updated {UPDATE_CONFIG_FILE} (Force: {is_force})")

    # 3. Update changelog.json
    changelog_data = {
        "version": f"1.0.0+{build_number}", # Assuming version format
        "build_number": build_number,
        "date": datetime.now().strftime("%Y-%m-%d"),
        "changelog_ar": [notes_ar],
        "changelog_en": [notes_en],
        "release_notes_url": ""
    }
    with open(CHANGELOG_FILE, "w", encoding="utf-8") as f:
        json.dump(changelog_data, f, indent=4, ensure_ascii=False)
    print(f"✅ Updated {CHANGELOG_FILE}")

    # 4. Update version_history.json
    try:
        with open(VERSION_HISTORY_FILE, "r", encoding="utf-8") as f:
            history = json.load(f)
    except FileNotFoundError:
        history = []
    
    # Check if this build already exists in history
    existing = next((item for item in history if item.get("build_number") == build_number), None)
    if not existing:
        history.insert(0, changelog_data) # Add new version at the top
        with open(VERSION_HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(history, f, indent=4, ensure_ascii=False)
        print(f"✅ Added to {VERSION_HISTORY_FILE}")
    else:
        print(f"ℹ️ Build {build_number} already in history, skipping append.")

def main():
    parser = argparse.ArgumentParser(description="Update backend version files for Al-Mudeer.")
    parser.add_argument("--build", type=int, required=True, help="New build number (e.g. 5)")
    parser.add_argument("--force", action="store_true", help="Set as FORCE update (critical priority)")
    parser.add_argument("--soft", action="store_true", help="Set as SOFT update (optional)")
    parser.add_argument("--msg", type=str, default="تحسينات عامة وإصلاحات للأخطاء", help="Arabic changelog message")
    parser.add_argument("--msg-en", type=str, default="General improvements and bug fixes", help="English changelog message")

    args = parser.parse_args()

    # Logic: Default to FORCE if nothing specified, unless --soft is used.
    is_force = True
    if args.soft:
        is_force = False
    elif args.force:
        is_force = True
    
    print(f"🚀 Preparing version {args.build}...")
    print(f"   Type: {'FORCE (Critical)' if is_force else 'SOFT (Optional)'}")
    print(f"   Message (AR): {args.msg}")
    
    confirmation = input("Proceed? (y/n): ")
    if confirmation.lower() != 'y':
        print("Cancelled.")
        sys.exit(0)

    os.makedirs(STATIC_DIR, exist_ok=True)
    update_files(args.build, is_force, args.msg, args.msg_en)
    print("\n🎉 Done! Now just copy your APK and git push.")

if __name__ == "__main__":
    main()
