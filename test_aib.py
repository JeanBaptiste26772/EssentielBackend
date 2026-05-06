import requests
import json

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json",
}

# Test 1 : API WordPress REST
urls_api = [
    "https://www.aib.media/wp-json/wp/v2/posts?per_page=20&page=1",
    "https://aib.media/wp-json/wp/v2/posts?per_page=20&page=1",
    "https://www.aib.media/wp-json/wp/v2/posts",
]

print("=== TEST API WORDPRESS ===")
for url in urls_api:
    try:
        r = requests.get(url, headers=headers, timeout=15)
        print(f"\nURL: {url}")
        print(f"Status: {r.status_code}")
        print(f"Content-Type: {r.headers.get('Content-Type', 'N/A')}")
        if r.status_code == 200:
            data = r.json()
            print(f"Articles: {len(data)}")
            if data:
                print(f"Premier: {data[0].get('title', {}).get('rendered', 'N/A')[:60]}")
        else:
            print(f"Réponse: {r.text[:200]}")
    except Exception as e:
        print(f"ERREUR {url}: {e}")

# Test 2 : Page 2, 3 de la homepage
print("\n=== TEST PAGES HOMEPAGE ===")
for page in [1, 2, 3]:
    url = f"https://www.aib.media/page/{page}/"
    try:
        r = requests.get(url, headers=headers, timeout=15)
        print(f"\nPage {page}: Status {r.status_code}, Taille {len(r.text)}")
        if "entry-title" in r.text:
            print("  ✅ Contient des articles (entry-title trouvé)")
        else:
            print("  ❌ Pas d'articles trouvés")
    except Exception as e:
        print(f"ERREUR page {page}: {e}")