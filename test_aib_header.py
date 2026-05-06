import requests

# SANS compression
headers_sans_compression = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9",
    # PAS de Accept-Encoding → le serveur renvoie du texte brut
}

url = "https://www.aib.media/"
print("=== SANS compression ===")
r = requests.get(url, headers=headers_sans_compression, timeout=15)
print(f"Status: {r.status_code}")
print(f"Taille: {len(r.text)}")
print(f"Content-Encoding: {r.headers.get('Content-Encoding', 'aucun')}")
print(f"Contient 'entry-title': {'entry-title' in r.text}")
print(f"Début: {r.text[:500]}")

# AVEC gzip uniquement
headers_gzip = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9",
    "Accept-Encoding": "gzip",
}

print("\n=== AVEC gzip uniquement ===")
r2 = requests.get(url, headers=headers_gzip, timeout=15)
print(f"Status: {r2.status_code}")
print(f"Taille: {len(r2.text)}")
print(f"Content-Encoding: {r2.headers.get('Content-Encoding', 'aucun')}")
print(f"Contient 'entry-title': {'entry-title' in r2.text}")
print(f"Début: {r2.text[:500]}")