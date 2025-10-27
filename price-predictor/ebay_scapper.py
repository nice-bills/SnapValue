import requests
import pandas as pd
import time
from datetime import datetime, timezone
from base64 import b64encode
from tqdm import tqdm

# === eBay API credentials ===
APP_ID = YOUR_APP_ID # Replace with real App ID
CERT_ID = YOUR_CERT_ID    # Replace with real Cert ID

# === API endpoints ===
TOKEN_URL = "https://api.ebay.com/identity/v1/oauth2/token"
SEARCH_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"

# === Config ===
SEARCH_TERMS = ["laptop", "shoes"]  # Add more terms if needed
LIMIT = 100  # Max items per request
DATE_THRESHOLD = datetime(2023, 1, 1, tzinfo=timezone.utc)  # Timezone-aware
OUTPUT_FILE = "ebay_items_2023_present.csv"
TARGET_ROWS = 10000  # Total rows across all terms

def get_access_token():
    """Request a new OAuth2 access token from eBay."""
    credentials = f"{APP_ID}:{CERT_ID}"
    encoded_credentials = b64encode(credentials.encode()).decode()

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": f"Basic {encoded_credentials}"
    }
    data = {
        "grant_type": "client_credentials",
        "scope": "https://api.ebay.com/oauth/api_scope"
    }

    response = requests.post(TOKEN_URL, headers=headers, data=data)
    if response.status_code != 200:
        raise Exception(f"Failed to get token: {response.status_code} {response.text}")

    return response.json().get("access_token")

def fetch_items(query, token, target_rows=TARGET_ROWS):
    """Fetch items for a search term until we hit target_rows or no more results."""
    all_items = []
    offset = 0
    headers = {"Authorization": f"Bearer {token}"}
    retries = 5

    with tqdm(total=target_rows, desc=f"Fetching {query}", unit="item") as pbar:
        while len(all_items) < target_rows:
            params = {"q": query, "limit": LIMIT, "offset": offset}
            for attempt in range(retries):
                response = requests.get(SEARCH_URL, headers=headers, params=params)
                if response.status_code == 200:
                    break
                elif attempt < retries - 1 and response.status_code in [429, 500, 502, 503]:
                    wait_time = 2 ** (attempt + 1)
                    print(f"Rate limit/error, retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    print(f"Failed fetching {query}: {response.status_code}")
                    return all_items

            data = response.json()
            items = data.get("itemSummaries", [])
            if not items:
                break  # No more items

            for item in items:
                creation_date = item.get("itemCreationDate")
                if creation_date:
                    try:
                        parsed_date = datetime.fromisoformat(creation_date.replace("Z", "+00:00"))
                    except Exception:
                        continue
                    if parsed_date < DATE_THRESHOLD:
                        continue

                all_items.append({
                    "query": query,
                    "item_id": item.get("itemId", ""),
                    "title": item.get("title", ""),
                    "price": item.get("price", {}).get("value", ""),
                    "currency": item.get("price", {}).get("currency", ""),
                    "condition": item.get("condition", ""),
                    "seller": item.get("seller", {}).get("username", ""),
                    "feedback_score": item.get("seller", {}).get("feedbackScore", ""),
                    "image_url": item.get("image", {}).get("imageUrl", ""),
                    "item_url": item.get("itemWebUrl", ""),
                    "item_creation_date": item.get("itemCreationDate", "")
                })
                pbar.update(1)
                if len(all_items) >= target_rows:
                    break

            offset += LIMIT
            time.sleep(0.5)  # avoid hitting rate limits

    return all_items[:target_rows]

if __name__ == "__main__":
    token = get_access_token()
    all_data = []

    for term in SEARCH_TERMS:
        remaining_rows = TARGET_ROWS - len(all_data)
        if remaining_rows <= 0:
            break
        print(f"\nFetching items for: {term}")
        items = fetch_items(term, token, target_rows=remaining_rows)
        all_data.extend(items)

    df = pd.DataFrame(all_data)
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\n Saved {len(df)} items to '{OUTPUT_FILE}'")
