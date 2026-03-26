# Intimport csv
import json
import re
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup


BASE_DIR = Path(__file__).resolve().parent
STORES_JSON = BASE_DIR / "stores.json"
OUT_JSON = BASE_DIR / "homedepot_clearance_all.json"
OUT_CSV = BASE_DIR / "homedepot_clearance_all.csv"

# Pages clearance publiques à scraper pour trouver des productId
CLEARANCE_URLS = [
    "https://www.homedepot.ca/en/home/categories/all/collections/clearance.html?products=",
    "https://www.homedepot.ca/en/home/categories/kitchen/kitchen-appliances/major-appliances-refrigerators/refrigerators-counter-depth/collection/clearance.html?products=",
    "https://www.homedepot.ca/en/home/categories/decor/blinds-and-window-shades/collection/clearance.html?products=",
]

# API interne trouvée dans Network
LOCALIZED_API = "https://www.homedepot.ca/api/productsvc/v1/products-localized-basic"

HEADERS_HTML = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/134.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-CA,en;q=0.9",
    "Referer": "https://www.homedepot.ca/",
}

HEADERS_API = {
    "User-Agent": HEADERS_HTML["User-Agent"],
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-CA,en;q=0.9",
    "Referer": "https://www.homedepot.ca/",
    "Origin": "https://www.homedepot.ca",
}

# Regex pour trouver des productId dans les URLs et le HTML
PRODUCT_ID_PATTERNS = [
    re.compile(r"/product/[^\"'#?]+/(\d{8,12})"),
    re.compile(r'"productId"\s*:\s*"(\d{8,12})"'),
    re.compile(r'"products"\s*:\s*"(\d{8,12})"'),
    re.compile(r'products=(\d{8,12})'),
]


def load_stores() -> list[dict]:
    with open(STORES_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)

    stores = []
    for row in data:
        store_id = str(row.get("storeId", "")).strip()
        postal_code = str(row.get("postalCode", "")).strip()
        if store_id and postal_code:
            stores.append({
                "store_id": store_id,
                "postal_code": postal_code,
                "display_name": row.get("displayName", ""),
                "city": row.get("city", ""),
                "province": row.get("provinceCode", row.get("province", "")),
                "formatted_address": row.get("formattedAddr", ""),
            })
    return stores


def fetch_html(url: str) -> str:
    r = requests.get(url, headers=HEADERS_HTML, timeout=45)
    r.raise_for_status()
    return r.text


def extract_product_ids(html: str) -> list[str]:
    ids = set()
    soup = BeautifulSoup(html, "html.parser")

    for a in soup.find_all("a", href=True):
        href = a["href"]
        for pattern in PRODUCT_ID_PATTERNS:
            for m in pattern.finditer(href):
                ids.add(m.group(1))

    for pattern in PRODUCT_ID_PATTERNS:
        for m in pattern.finditer(html):
            ids.add(m.group(1))

    return sorted(ids)


def call_localized_api(product_id: str, store_id: str, postal_code: str, lang: str = "en") -> dict:
    params = {
        "products": product_id,
        "store": store_id,
        "postalCode": postal_code,
        "lang": lang,
    }
    r = requests.get(LOCALIZED_API, params=params, headers=HEADERS_API, timeout=45)
    r.raise_for_status()
    return r.json()


def first_product(payload: dict | list) -> dict:
    if isinstance(payload, list):
        return payload[0] if payload else {}

    if not isinstance(payload, dict):
        return {}

    for key in ("products", "items", "data"):
        value = payload.get(key)
        if isinstance(value, list) and value:
            if isinstance(value[0], dict):
                return value[0]

    return payload


def normalize_record(product_id: str, store: dict, payload: dict) -> dict:
    product = first_product(payload)

    # Mapping best-effort, à ajuster selon la vraie réponse JSON
    record = {
        "product_id": product_id,
        "store_id": store["store_id"],
        "postal_code": store["postal_code"],
        "store_name": store["display_name"],
        "city": store["city"],
        "province": store["province"],
        "formatted_address": store["formatted_address"],

        "name": product.get("name", ""),
        "brand": product.get("brandName", "") or product.get("brand", ""),
        "price": product.get("price", ""),
        "regular_price": product.get("regularPrice", ""),
        "sale_price": product.get("salePrice", ""),
        "clearance": product.get("isClearance", ""),
        "availability": product.get("availabilityStatus", "") or product.get("availability", ""),
        "quantity": product.get("quantity", "") or product.get("availableQuantity", ""),
        "pickup": product.get("pickup", ""),
        "delivery": product.get("delivery", ""),
        "bopisEligible": product.get("bopisEligible", ""),
        "product_url": product.get("productUrl", ""),
        "raw": payload,
    }

    return record


def main():
    if not STORES_JSON.exists():
        raise FileNotFoundError(f"Fichier introuvable: {STORES_JSON}")

    print("Chargement des magasins...")
    stores = load_stores()
    print(f"Magasins trouvés: {len(stores)}")

    print("\nExtraction des productId clearance...")
    all_product_ids = set()

    for url in CLEARANCE_URLS:
        try:
            html = fetch_html(url)
            ids = extract_product_ids(html)
            print(f"{url} -> {len(ids)} productId")
            all_product_ids.update(ids)
            time.sleep(1)
        except Exception as e:
            print(f"Erreur sur {url}: {e}")

    product_ids = sorted(all_product_ids)
    print(f"\nTotal productId uniques trouvés: {len(product_ids)}")

    if not product_ids:
        raise RuntimeError("Aucun productId trouvé. Il faudra peut-être élargir les pages clearance ou passer par Playwright.")

    results = []
    total_calls = len(product_ids) * len(stores)
    call_no = 0

    for product_id in product_ids:
        for store in stores:
            call_no += 1
            print(f"[{call_no}/{total_calls}] Product {product_id} | Store {store['store_id']}")

            try:
                payload = call_localized_api(
                    product_id=product_id,
                    store_id=store["store_id"],
                    postal_code=store["postal_code"],
                    lang="en"
                )
                record = normalize_record(product_id, store, payload)
                results.append(record)
            except Exception as e:
                results.append({
                    "product_id": product_id,
                    "store_id": store["store_id"],
                    "postal_code": store["postal_code"],
                    "store_name": store["display_name"],
                    "city": store["city"],
                    "province": store["province"],
                    "formatted_address": store["formatted_address"],
                    "name": "",
                    "brand": "",
                    "price": "",
                    "regular_price": "",
                    "sale_price": "",
                    "clearance": "",
                    "availability": "",
                    "quantity": "",
                    "pickup": "",
                    "delivery": "",
                    "bopisEligible": "",
                    "product_url": "",
                    "error": str(e),
                    "raw": None,
                })

            time.sleep(0.25)

    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    csv_fields = [
        "product_id",
        "store_id",
        "postal_code",
        "store_name",
        "city",
        "province",
        "formatted_address",
        "name",
        "brand",
        "price",
        "regular_price",
        "sale_price",
        "clearance",
        "availability",
        "quantity",
        "pickup",
        "delivery",
        "bopisEligible",
        "product_url",
        "error",
    ]

    with open(OUT_CSV, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=csv_fields)
        writer.writeheader()
        for row in results:
            writer.writerow({k: row.get(k, "") for k in csv_fields})

    print("\nTerminé.")
    print(f"JSON: {OUT_JSON}")
    print(f"CSV : {OUT_CSV}")


if __name__ == "__main__":
    main()ernal Home Depot Canada clearance utilities.
