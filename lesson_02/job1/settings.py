import os

AUTH_TOKEN = os.environ.get("API_AUTH_TOKEN")
if not AUTH_TOKEN:
    print("API_AUTH_TOKEN environment variable must be set")
