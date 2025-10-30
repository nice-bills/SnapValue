import pandas as pd
import requests
import mimetypes
from pathlib import Path
from tqdm import tqdm


CSV_FILE = "ebay_items_with_text_embeddings.csv"
IMAGE_FOLDER = Path("images")
IMAGE_FOLDER.mkdir(exist_ok=True)
OUTPUT_CSV = "ebay_items_with_image_paths.csv"

df = pd.read_csv(CSV_FILE)

def get_extension_from_url(url, response=None):
    ext = url.split(".")[-1].split("?")[0]
    if len(ext) > 5 or "/" in ext:
        if response is not None and "Content-Type" in response.headers:
            ext = mimetypes.guess_extension(response.headers["Content-Type"]) or ".jpg"
        else:
            ext = ".jpg"
    elif ext.lower() in ["jpeg", "jpg", "png", "webp"]:
        ext = "." + ext.lower()
    else:
        ext = ".jpg"
    return ext

def make_safe_filename(item_id, ext):
    safe_id = "".join(c if c.isalnum() else "_" for c in str(item_id))
    return safe_id + ext

def download_image(url, save_path):
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            ext = get_extension_from_url(url, response)
            true_save_path = save_path.with_suffix(ext)
            with open(true_save_path, "wb") as f:
                f.write(response.content)
            return str(true_save_path)
    except Exception:
        pass
    return None

image_paths = []

for _, row in tqdm(df.iterrows(), total=len(df), desc="Downloading images"):
    url = str(row.get("image_url", ""))
    item_id = row.get("item_id", "missingid")

    if not url or pd.isna(url) or url.lower().strip() in ["none", "nan", ""]:
        image_paths.append(None)
        continue

    ext = get_extension_from_url(url)
    safe_filename = make_safe_filename(item_id, ext)
    save_path = IMAGE_FOLDER / safe_filename

    if save_path.exists():
        image_paths.append(str(save_path))
        continue

    result_path = download_image(url, save_path)
    image_paths.append(result_path)

df["image_path"] = image_paths
df.to_csv(OUTPUT_CSV, index=False)
print(f"Downloaded images and saved CSV to '{OUTPUT_CSV}'")
