#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from bert_score import BERTScorer  # ← on utilise la classe, pas la fonction
from pymongo import MongoClient
from datetime import datetime, timezone, timedelta
import os

MONGO_URI   = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB    = os.getenv("MONGO_DB", "burkina_news")
NB_ARTICLES = 10

client      = MongoClient(MONGO_URI)
db          = client[MONGO_DB]
col_bruts   = db["articles"]
col_traites = db["articles_traites"]

# ── Charger le modèle UNE SEULE FOIS ─────────────────────────────────────────
print("⏳ Chargement du modèle BERTScore (une seule fois)...")
scorer = BERTScorer(
    model_type="xlm-roberta-base",  # ← multilingue, très bon pour le français
    lang="fr",
    rescale_with_baseline=True,
)
print("✅ Modèle chargé\n")

# ── Récupérer les articles traités récents ────────────────────────────────────
limite = datetime.now(timezone.utc) - timedelta(hours=48)

traites = list(col_traites.find(
    {"date_traitement": {"$gte": limite}, "resume_fr": {"$exists": True}},
    {"titre": 1, "resume_fr": 1, "sources": 1, "categorie": 1}
).limit(NB_ARTICLES))

print(f"📋 {len(traites)} articles traités récupérés\n")

resultats = []

for art_traite in traites:
    resume_fr = art_traite.get("resume_fr", "")
    sources   = art_traite.get("sources", [])

    if not resume_fr or not sources:
        continue

    # ── Récupérer TOUS les corps originaux ───────────────────────────────────
    corps_trouves = []
    for source in sources:
        url = source.get("url", "")
        if not url:
            continue
        brut = col_bruts.find_one({"url": url}, {"corps": 1})
        if brut and brut.get("corps") and len(brut["corps"]) > 200:
            corps_trouves.append(brut["corps"])

    if not corps_trouves:
        print(f"⚠️  Pas de corps trouvé pour : {art_traite['titre'][:60]}")
        continue

    # ── Construire la référence ───────────────────────────────────────────────
    if len(corps_trouves) == 1:
        reference = corps_trouves[0][:4000]
        type_eval = "article_simple"
    else:
        reference = " ".join([c[:1500] for c in corps_trouves])[:5000]
        type_eval = f"merge_{len(corps_trouves)}_sources"

    # ── Évaluation ────────────────────────────────────────────────────────────
    P, R, F1 = scorer.score(
        cands=[resume_fr],
        refs=[reference],
    )

    scores = {
        "titre":      art_traite["titre"][:70],
        "categorie":  art_traite.get("categorie", "?"),
        "type_eval":  type_eval,
        "nb_sources": len(corps_trouves),
        "precision":  round(float(P[0]), 4),
        "recall":     round(float(R[0]), 4),
        "f1":         round(float(F1[0]), 4),
    }

    f1 = scores["f1"]
    if f1 >= 0.75:
        scores["qualite"] = "✅ Excellente"
    elif f1 >= 0.60:
        scores["qualite"] = "👍 Acceptable"
    elif f1 >= 0.45:
        scores["qualite"] = "⚠️  Faible"
    else:
        scores["qualite"] = "❌ Très faible"

    resultats.append(scores)

    print(f"📰 {scores['titre']}")
    print(f"   Catégorie  : {scores['categorie']}")
    print(f"   Type eval  : {scores['type_eval']}")
    print(f"   Nb sources : {scores['nb_sources']}")
    print(f"   Precision  : {scores['precision']} | Recall : {scores['recall']} | F1 : {scores['f1']}")
    print(f"   Qualité    : {scores['qualite']}")
    print()

# ── Résumé global ─────────────────────────────────────────────────────────────
if resultats:
    f1_moyen = sum(r["f1"] for r in resultats) / len(resultats)
    simples  = [r for r in resultats if r["type_eval"] == "article_simple"]
    merges   = [r for r in resultats if r["type_eval"].startswith("merge_")]

    print("─" * 60)
    print(f"📊 F1 moyen global ({len(resultats)} articles) : {f1_moyen:.3f}")

    if simples:
        print(f"   └── Articles simples ({len(simples)}) : F1 moyen = {sum(r['f1'] for r in simples)/len(simples):.3f}")
    if merges:
        print(f"   └── Merges          ({len(merges)}) : F1 moyen = {sum(r['f1'] for r in merges)/len(merges):.3f}")

    print()
    print(f" Excellents   : {sum(1 for r in resultats if r['f1'] >= 0.75)}")
    print(f" Acceptables  : {sum(1 for r in resultats if 0.60 <= r['f1'] < 0.75)}")
    print(f"  Faibles      : {sum(1 for r in resultats if 0.45 <= r['f1'] < 0.60)}")
    print(f" Très faibles  : {sum(1 for r in resultats if r['f1'] < 0.45)}")

client.close()