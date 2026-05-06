import feedparser

for name, url in [
    ("AIB", "https://aib.media/feed"),
    ("AIB-www", "https://www.aib.media/feed"),
    ("Sidwaya", "https://www.sidwaya.info/feed/"),
]:
    print(f"\n=== {name} ===")
    try:
        f = feedparser.parse(url)
        print(f"Articles: {len(f.entries)}")
        if f.entries:
            e = f.entries[0]
            print(f"Titre: {e.get('title', 'N/A')[:60]}")
            
            # Test content
            if hasattr(e, 'content') and e.content:
                for i, c in enumerate(e.content):
                    print(f"  content[{i}] type='{c.get('type', 'N/A')}' len={len(c.get('value', ''))}")
            else:
                print("  Pas de .content")
                
            # Test content_encoded
            if hasattr(e, 'content_encoded') and e.content_encoded:
                print(f"  content_encoded len={len(e.content_encoded)}")
            else:
                print("  Pas de .content_encoded")
                
            # Test summary
            summary = e.get('summary', '')
            print(f"  summary len={len(summary)}")
    except Exception as ex:
        print(f"ERREUR: {ex}")