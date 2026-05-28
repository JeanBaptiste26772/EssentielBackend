#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Évaluation ROUGE — articles bruts vs résumés
Lancer indépendamment du pipeline : python evaluer_rouge.py
"""

from rouge_score import rouge_scorer
from pymongo import MongoClient
from datetime import datetime, timezone, timedelta
import os

# ─── Config ───────────────────────────────────────────────────────────────────
MONGO_URI   = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB    = os.getenv("MONGO_DB", "burkina_news")
NB_ARTICLES = 20  # ← Change ce chiffre selon combien tu veux tester

# ─── Connexion MongoDB ────────────────────────────────────────────────────────
client      = MongoClient(MONGO_URI)
db          = client[MONGO_DB]
col_bruts   = db["articles"]
col_traites = db["articles_traites"]

# ─── Charger ROUGE une seule fois ─────────────────────────────────────────────
rouge = rouge_scorer.RougeScorer(
    ["rouge1", "rouge2", "rougeL"],
    use_stemmer=False,  # False = mieux pour le français
)
print(" ROUGE chargé\n")

# ─── Récupérer les articles traités récents ───────────────────────────────────
limite = datetime.now(timezone.utc) - timedelta(hours=48)

traites = list(col_traites.find(
    {"date_traitement": {"$gte": limite}, "resume_fr": {"$exists": True}},
    {"titre": 1, "resume_fr": 1, "sources": 1, "categorie": 1}
).sort("date_traitement", -1).limit(NB_ARTICLES))

print(f" {len(traites)} articles traités récupérés\n")

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
        print(f"  Pas de corps trouvé pour : {art_traite['titre'][:60]}")
        continue

    # ── Construire la référence ───────────────────────────────────────────────
    if len(corps_trouves) == 1:
        reference = corps_trouves[0][:4000]
        type_eval = "article_simple"
    else:
        reference = " ".join([c[:1500] for c in corps_trouves])[:5000]
        type_eval = f"merge_{len(corps_trouves)}_sources"

    # ── Évaluation ROUGE ─────────────────────────────────────────────────────
    scores_rouge = rouge.score(
        target=reference,     # original = référence
        prediction=resume_fr  # résumé = candidat
    )

    rouge1 = round(scores_rouge["rouge1"].fmeasure, 4)
    rouge2 = round(scores_rouge["rouge2"].fmeasure, 4)
    rougeL = round(scores_rouge["rougeL"].fmeasure, 4)

    # ── Interprétation ────────────────────────────────────────────────────────
    if rougeL >= 0.40:
        qualite = " Excellente"
    elif rougeL >= 0.25:
        qualite = " Acceptable"
    elif rougeL >= 0.15:
        qualite = "  Faible"
    else:
        qualite = " Très faible"

    scores = {
        "titre":      art_traite["titre"][:70],
        "categorie":  art_traite.get("categorie", "?"),
        "type_eval":  type_eval,
        "nb_sources": len(corps_trouves),
        "rouge1":     rouge1,
        "rouge2":     rouge2,
        "rougeL":     rougeL,
        "qualite":    qualite,
    }

    resultats.append(scores)

    print(f" {scores['titre']}")
    print(f"   Catégorie  : {scores['categorie']}  |  Type : {scores['type_eval']}  |  Sources : {scores['nb_sources']}")
    print(f"   ROUGE-1 : {rouge1}  |  ROUGE-2 : {rouge2}  |  ROUGE-L : {rougeL}")
    print(f"   Qualité : {qualite}")
    print()

# ── Résumé global ─────────────────────────────────────────────────────────────
if resultats:
    simples = [r for r in resultats if r["type_eval"] == "article_simple"]
    merges  = [r for r in resultats if r["type_eval"].startswith("merge_")]

    rougeL_moyen = sum(r["rougeL"] for r in resultats) / len(resultats)

    print("─" * 60)
    print(f" RÉSULTATS GLOBAUX ({len(resultats)} articles)")
    print(f"   ROUGE-L moyen : {rougeL_moyen:.3f}")
    print()

    if simples:
        rougeL_simples = sum(r["rougeL"] for r in simples) / len(simples)
        print(f"   └── Articles simples ({len(simples)}) : ROUGE-L moyen = {rougeL_simples:.3f}")
    if merges:
        rougeL_merges = sum(r["rougeL"] for r in merges) / len(merges)
        print(f"   └── Merges          ({len(merges)}) : ROUGE-L moyen = {rougeL_merges:.3f}")

    print()
    print(f" Excellents   : {sum(1 for r in resultats if r['rougeL'] >= 0.40)}")
    print(f" Acceptables  : {sum(1 for r in resultats if 0.25 <= r['rougeL'] < 0.40)}")
    print(f"  Faibles      : {sum(1 for r in resultats if 0.15 <= r['rougeL'] < 0.25)}")
    print(f" Très faibles  : {sum(1 for r in resultats if r['rougeL'] < 0.15)}")
else:
    print(" Aucun résultat — vérifie que le pipeline a tourné dans les 48 dernières heures")

client.close()