from pymongo import MongoClient

db = MongoClient()["burkina_news"]
coll = db["articles"]

print("=== NOMBRE D ARTICLES PAR SOURCE ===")
for src in sorted(coll.distinct("source")):
    n = coll.count_documents({"source": src})
    print(f"  {src:15} : {n} articles")

print("\n=== SIDWAYA : 3 DERNIERS ===")
for a in coll.find({"source": "Sidwaya"}).sort("date_scraping", -1).limit(3):
    print(f"  - {a['titre'][:70]}...")
    print(f"    Corps: {len(a.get('corps', ''))} caracteres")

print("\n=== BURKINA24 : 3 DERNIERS ===")
for a in coll.find({"source": "Burkina24"}).sort("date_scraping", -1).limit(3):
    print(f"  - {a['titre'][:70]}...")

print("\n=== AIB : 3 DERNIERS ===")
for a in coll.find({"source": "AIB"}).sort("date_scraping", -1).limit(3):
    print(f"  - {a['titre'][:70]}...")