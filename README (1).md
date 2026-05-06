# 📰 Scraper Actualité Burkinabè

Scraper Python pour le portail intelligent de paraphrase et résumé automatique
de l'actualité burkinabè — backend Django / MongoDB.

---

## Architecture

```
scraper_burkina/
├── scraper.py          # Code principal du scraper
├── requirements.txt    # Dépendances Python
├── .env.example        # Variables d'environnement à configurer
├── .env                # Votre config locale (ne pas committer)
└── scraper.log         # Généré automatiquement à l'exécution
```

---

## Pipeline par source

### Sources textuelles (AIB, Sidwaya, Lefaso, Burkina24)

```
flux RSS (feedparser)
    → liste d'articles (titre, url, date, résumé)
        → téléchargement HTML (requests)
            → extraction contenu (parsel/CSS)
                → MongoDB (collection articles)
```

### RTB.bf (source vidéo/audio)

```
flux RSS (feedparser)
    → détection URL média (iframe YouTube / balise audio / enclosure)
        → téléchargement audio (yt-dlp ou requests)
            → transcription (Qwen3.5 via Ollama ou Whisper fallback)
                → MongoDB (corps = transcription)
```

---

## Structure du document MongoDB

```json
{
  "source":            "AIB",
  "domaine":           "aib.bf",
  "type_source":       "rss_texte",
  "titre":             "Titre de l'article",
  "url":               "https://...",
  "url_hash":          "md5 de l'URL (index unique)",
  "date_publication":  "2024-01-01T10:00:00Z",
  "resume_rss":        "Résumé extrait du flux RSS",
  "corps":             "Contenu complet de l'article (ou transcription pour RTB)",
  "url_media":         "https://youtube.com/... (RTB uniquement)",
  "date_scraping":     "2024-01-01T10:05:00Z",
  "statut_paraphrase": "en_attente",
  "statut_resume":     "en_attente",
  "langue":            "fr"
}
```

Les champs `statut_paraphrase` et `statut_resume` permettent au pipeline NLP Django
de savoir quels articles restent à traiter.

---

## Installation

```bash
# 1. Cloner / copier les fichiers
cd scraper_burkina

# 2. Créer un environnement virtuel
python3 -m venv venv
source venv/bin/activate   # Windows : venv\Scripts\activate

# 3. Installer FFmpeg (requis pour l'audio)
sudo apt install ffmpeg    # Ubuntu/Debian

# 4. Installer les dépendances Python
pip install -r requirements.txt

# 5. Configurer les variables d'environnement
cp .env.example .env
# Éditer .env avec vos valeurs MongoDB et Qwen

# 6. (Optionnel) Installer Qwen via Ollama
curl -fsSL https://ollama.ai/install.sh | sh
ollama pull qwen2.5:7b

# 7. Lancer le scraper
python scraper.py
```

---

## Intégration Django

Depuis votre vue Django, filtrez les articles à traiter :

```python
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
collection = client["burkina_news"]["articles"]

# Articles en attente de paraphrase
articles_a_traiter = collection.find({
    "statut_paraphrase": "en_attente",
    "corps": {"$ne": ""}
})

# Marquer comme traité après paraphrase
collection.update_one(
    {"_id": article["_id"]},
    {"$set": {"statut_paraphrase": "fait", "paraphrase": texte_paraphrase}}
)
```

---

## Notes importantes

- **Sélecteurs CSS** : si un site met à jour son HTML, ajuster `_obtenir_selecteurs_par_domaine()`
- **RTB** : nécessite FFmpeg installé sur le système
- **Qwen** : si Ollama n'est pas disponible, Whisper s'active automatiquement
- **Doublons** : index unique sur `url` — les articles déjà présents sont ignorés silencieusement
- **Logs** : consultables dans `scraper.log` et dans la console
