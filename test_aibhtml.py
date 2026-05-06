import requests
from parsel import Selector

url = "https://www.aib.media/"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
}

r = requests.get(url, headers=headers, timeout=15)
sel = Selector(text=r.text)

print("=== TITRES trouvés ===")
for t in sel.css("h1, h2, h3, h4, h5").getall()[:10]:
    print(t[:200])

print("\n=== LIENS vers articles ===")
for a in sel.css("a::attr(href)").getall():
    if "/202" in a or "article" in a or "post" in a:
        print(a)

print("\n=== CLASSES avec 'post' ou 'article' ===")
for cls in sel.css("[class]").getall()[:20]:
    if "post" in cls.lower() or "article" in cls.lower() or "news" in cls.lower():
        print(cls[:200])