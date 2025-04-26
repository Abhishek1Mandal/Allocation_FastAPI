from dotenv import load_dotenv
import os
import json

# Load environment variables from .env file
load_dotenv()

# Load API keys from .env
try:
    api_keys_raw = os.getenv("API_KEYS", "[]")
    API_KEYS = json.loads(api_keys_raw)
    if not isinstance(API_KEYS, list):
        raise ValueError("API_KEYS must be a list")
except json.JSONDecodeError as e:
    raise ValueError(f"Invalid JSON format for API_KEYS in .env file: {str(e)}")
except Exception as e:
    raise ValueError(f"Error loading API_KEYS from .env file: {str(e)}")
