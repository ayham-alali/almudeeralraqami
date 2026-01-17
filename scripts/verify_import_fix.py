
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from models import search_messages
    print("SUCCESS: 'search_messages' imported successfully from 'models'.")
except ImportError as e:
    print(f"FAILURE: Could not import 'search_messages' from 'models'. Error: {e}")
    sys.exit(1)
except Exception as e:
    print(f"FAILURE: Unexpected error during import. Error: {e}")
    sys.exit(1)
