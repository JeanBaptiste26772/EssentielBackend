import requests
from bs4 import BeautifulSoup

# Chercher des vrais articles texte depuis la page d'accueil
print("=== Récupération des liens depuis la page d'accueil ===")
r = requests.get("https://lefaso.net/", headers={"User-Agent": "Mozilla/5.0"})
soup = BeautifulSoup(r.text, "html.parser")

liens = []
for a in soup.find_all("a", href=True):
    href = a["href"]
    if "spip.php?article" in href and href not in liens:
        liens.append(href)
    if len(liens) >= 5:
        break

print(f"Articles trouvés : {liens}\n")

# Tester chaque article
for url in liens:
    if not url.startswith("http"):
        url = "https://lefaso.net/" + url.lstrip("/")
    print(f"\n{'='*60}")
    print(f"URL : {url}")
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    soup = BeautifulSoup(r.text, "html.parser")

    div = soup.select_one("div.article_content")
    if div:
        texte = div.get_text(separator=" ", strip=True)
        print(f"article_content ({len(texte)} chars) => {texte[:200]}")
    else:
        print("div.article_content => RATE")