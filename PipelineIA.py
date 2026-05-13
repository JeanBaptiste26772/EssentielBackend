#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pipeline IA — Burkina News
- Lecture des articles bruts depuis MongoDB
- Classification par catégorie via Groq/Ollama
- Embedding via qwen3-embedding:0.6b
- Calcul de similarité + déduplication
- Fusion + résumé via Groq/Ollama (1/4 de la longueur)
- Traduction en Mooré (placeholder TTS API)
- Stockage dans MongoDB (collection articles_traites)
"""

import os
import re
import sys
import json
import time
import logging
import hashlib
import numpy as np
from datetime import datetime, timezone, timedelta
from pymongo import MongoClient, errors as mongo_errors
import requests as req_lib
from groq import Groq

# ─── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("PipelineIA")

# ─── Configuration ────────────────────────────────────────────────────────────
MONGO_URI        = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB         = os.getenv("MONGO_DB", "burkina_news")
COL_BRUTS        = "articles"
COL_TRAITES      = "articles_traites"

OLLAMA_URL       = os.getenv("OLLAMA_URL", "http://localhost:11434")
MODELE_LLM       = os.getenv("QWEN_MODEL", "qwen3:1.7b")
MODELE_EMBEDDING = os.getenv("EMBEDDING_MODEL", "qwen3-embedding:0.6b")

GROQ_API_KEY     = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL       = "llama-3.3-70b-versatile"

SEUIL_SIMILARITE = 0.85
FENETRE_HEURES   = 120   # 30 jours pour les tests — remettre à 24 en production
RESUME_RATIO     = 0.25  # Résumé = 1/4 de la longueur originale

# ─── Catégories ───────────────────────────────────────────────────────────────
CATEGORIES = [
    "Politique",
    "Sécurité",
    "Économie",
    "Sport",
    "Santé",
    "Éducation",
    "Agriculture",
    "Culture",
    "Justice",
    "International",
    "Société",
    "Environnement",
    "Technologie",
    "Autre",
]


# ═══════════════════════════════════════════════════════════════════════════════
# GROQ + OLLAMA — HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _ollama_generate(prompt: str, system: str = "", max_tokens: int = 512) -> str:
    """
    Appel LLM avec Groq en priorité, Ollama en fallback.
    max_tokens est dynamique selon le contexte (classification vs résumé).
    """
    # ── Étape 1 : Groq en priorité ────────────────────────────────────────────
    if GROQ_API_KEY:
        try:
            client = Groq(api_key=GROQ_API_KEY)
            messages = []
            if system:
                messages.append({"role": "system", "content": system})
            messages.append({"role": "user", "content": prompt})

            response = client.chat.completions.create(
                model=GROQ_MODEL,
                messages=messages,
                max_tokens=max_tokens,
                temperature=0.3,
            )
            texte = response.choices[0].message.content.strip()
            if texte:
                return texte
        except Exception as e:
            if "rate_limit" in str(e).lower() or "429" in str(e):
                logger.warning("  Groq limite atteinte — fallback Ollama...")
            else:
                logger.error(" Groq LLM erreur : %s", e)
    else:
        logger.warning("  GROQ_API_KEY non définie — fallback Ollama...")

    # ── Étape 2 : Ollama local (fallback) ─────────────────────────────────────
    try:
        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})

        r = req_lib.post(
            f"{OLLAMA_URL}/api/chat",
            json={
                "model": MODELE_LLM,
                "messages": messages,
                "stream": False,
                "options": {"temperature": 0.3, "num_predict": max_tokens},
            },
            timeout=300,
        )
        r.raise_for_status()
        return r.json()["message"]["content"].strip()
    except Exception as e:
        logger.error(" Ollama LLM erreur : %s", e)
        return ""


def _ollama_embed(texte: str) -> list[float] | None:
    """Embedding via qwen3-embedding:0.6b."""
    try:
        r = req_lib.post(
            f"{OLLAMA_URL}/api/embed",
            json={"model": MODELE_EMBEDDING, "input": texte[:2000]},
            timeout=60,
        )
        r.raise_for_status()
        data = r.json()
        vecteur = data.get("embeddings", data.get("embedding", None))
        if isinstance(vecteur, list) and len(vecteur) > 0:
            if isinstance(vecteur[0], list):
                return vecteur[0]
            return vecteur
        return None
    except Exception as e:
        logger.error(" Ollama embedding erreur : %s", e)
        return None


def _cosine_similarity(v1: list[float], v2: list[float]) -> float:
    """Calcul de similarité cosinus entre deux vecteurs."""
    a = np.array(v1)
    b = np.array(v2)
    norm_a = np.linalg.norm(a)
    norm_b = np.linalg.norm(b)
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return float(np.dot(a, b) / (norm_a * norm_b))


# ═══════════════════════════════════════════════════════════════════════════════
# ÉTAPE 1 — CLASSIFICATION
# ═══════════════════════════════════════════════════════════════════════════════

def classifier_article(titre: str, corps: str) -> str:
    """Classifie un article dans une catégorie."""
    categories_str = ", ".join(CATEGORIES)
    texte_court = (corps or "")[:300]

    prompt = f"""Tu es un classificateur d'articles de presse burkinabè.
Titre : {titre}
Extrait : {texte_court}

Classe cet article dans UNE SEULE catégorie parmi : {categories_str}
Réponds UNIQUEMENT avec le nom de la catégorie, rien d'autre."""

    # Classification = juste un mot → max_tokens très petit
    reponse = _ollama_generate(prompt, max_tokens=20)

    for cat in CATEGORIES:
        if cat.lower() in reponse.lower():
            return cat
    return "Autre"


def classifier_sujets_rtb(transcription: str) -> list[dict]:
    """
    Découpe la transcription RTB en sujets distincts classifiés.
    Traite par morceaux pour éviter les timeouts.
    """
    morceaux = [transcription[i:i+2000] for i in range(0, min(len(transcription), 8000), 2000)]
    tous_sujets = []

    for i, morceau in enumerate(morceaux):
        logger.info(" RTB chunk %d/%d...", i + 1, len(morceaux))
        prompt = f"""Tu es un éditeur de presse burkinabè. Extrait les sujets de ce fragment de journal télévisé.

FRAGMENT :
{morceau}

Réponds UNIQUEMENT en JSON valide :
[
  {{"titre": "...", "categorie": "...", "resume": "..."}},
]
Catégories possibles : {", ".join(CATEGORIES)}"""

        reponse = _ollama_generate(prompt, max_tokens=1024)
        try:
            match = re.search(r'\[.*?\]', reponse, re.DOTALL)
            if match:
                sujets = json.loads(match.group())
                tous_sujets.extend(sujets)
        except Exception as e:
            logger.error(" Parsing chunk RTB %d : %s", i + 1, e)

    logger.info(" RTB : %d sujets identifiés au total", len(tous_sujets))
    return tous_sujets


# ═══════════════════════════════════════════════════════════════════════════════
# ÉTAPE 2 — EMBEDDING + SIMILARITÉ
# ═══════════════════════════════════════════════════════════════════════════════

def calculer_embeddings(articles: list[dict]) -> list[dict]:
    """Calcule et attache les embeddings à chaque article."""
    logger.info(" Calcul embeddings pour %d articles...", len(articles))
    for art in articles:
        texte = f"{art.get('titre', '')} {art.get('corps', '')[:1000]}"
        vecteur = _ollama_embed(texte)
        art["_embedding"] = vecteur
        if not vecteur:
            logger.warning("⚠️  Embedding manquant pour : %s", art.get('titre', '?')[:50])
    return articles


def verifier_doublon_mongodb(collection_traites, embedding: list[float], seuil: float = SEUIL_SIMILARITE) -> bool:
    """
    Vérifie si un article similaire existe déjà dans articles_traites.
    Retourne True si doublon trouvé.
    """
    try:
        limite = datetime.now(timezone.utc) - timedelta(hours=48)
        existants = list(collection_traites.find(
            {"date_traitement": {"$gte": limite}, "embedding": {"$exists": True}},
            {"embedding": 1, "titre": 1}
        ))

        for doc in existants:
            emb_existant = doc.get("embedding")
            if not emb_existant:
                continue
            sim = _cosine_similarity(embedding, emb_existant)
            if sim >= seuil:
                logger.info(" Doublon détecté (sim=%.2f) avec : %s", sim, doc.get('titre', '?')[:50])
                return True
        return False
    except Exception as e:
        logger.error(" Erreur vérification doublon MongoDB : %s", e)
        return False


def grouper_par_similarite(articles: list[dict], seuil: float = SEUIL_SIMILARITE) -> list[list[dict]]:
    """Groupe les articles similaires ensemble."""
    groupes = []
    assignes = set()

    for i, art_i in enumerate(articles):
        if i in assignes:
            continue
        if not art_i.get("_embedding"):
            groupes.append([art_i])
            assignes.add(i)
            continue

        groupe = [art_i]
        assignes.add(i)

        for j, art_j in enumerate(articles):
            if j in assignes or not art_j.get("_embedding"):
                continue
            sim = _cosine_similarity(art_i["_embedding"], art_j["_embedding"])
            if sim >= seuil:
                logger.info(
                    " Similarité %.2f : '%s' ↔ '%s'",
                    sim,
                    art_i.get('titre', '?')[:40],
                    art_j.get('titre', '?')[:40],
                )
                groupe.append(art_j)
                assignes.add(j)

        groupes.append(groupe)

    logger.info(" %d articles → %d groupes", len(articles), len(groupes))
    return groupes


# ═══════════════════════════════════════════════════════════════════════════════
# ÉTAPE 3 — FUSION + RÉSUMÉ
# ═══════════════════════════════════════════════════════════════════════════════

def merger_et_resumer(groupe: list[dict]) -> dict | None:
    """
    Fusionne un groupe d'articles similaires et produit un résumé à 1/4.
    """
    if not groupe:
        return None

    # ── Article unique → résumé simple ──────────────────────────────────────
    if len(groupe) == 1:
        art = groupe[0]
        corps = art.get("corps", "")
        titre = art.get("titre", "Sans titre")
        categorie = art.get("_categorie", "Autre")

        # Calcul dynamique : 1/4 de la longueur en caractères
        cible_chars = max(200, int(len(corps) * RESUME_RATIO))
        # 1 token ≈ 4 caractères en français
        max_tokens = max(256, int(cible_chars / 2.5) + 150)

        prompt = f"""Tu es un journaliste burkinabè. Résume et reformule cet article en français.
- Le résumé doit faire environ {cible_chars} caractères (1/4 de l'original)
- Reformule avec tes propres mots, n'utilise pas les mêmes formulations que l'original
- Sois factuel, précis, garde les informations essentielles

TITRE : {titre}
ARTICLE : {corps[:4000]}

Donne UNIQUEMENT le résumé reformulé, sans introduction ni commentaire."""

        resume = _ollama_generate(prompt, max_tokens=max_tokens)

        # Fallback uniquement si Groq ET Ollama ont tous les deux échoué
        if not resume:
            logger.warning("  Résumé échoué pour : %s — fallback copie", titre[:50])
            resume = corps[:cible_chars]

        sources = [{"titre": titre, "url": art.get("url", ""), "source": art.get("source", "")}]

        return _construire_article_traite(
            titre=titre,
            resume_fr=resume,
            categorie=categorie,
            sources=sources,
            embedding=art.get("_embedding"),
            date_pub=art.get("date_publication"),
            images=art.get("images", []),
        )

    # ── Plusieurs articles → merge + résumé ─────────────────────────────────
    logger.info(" Merge de %d articles similaires...", len(groupe))

    titres = [a.get("titre", "") for a in groupe]
    corps_combines = "\n\n---\n\n".join([
        f"[Source: {a.get('source', '?')}]\n{a.get('corps', '')[:2000]}"
        for a in groupe
    ])
    categorie = groupe[0].get("_categorie", "Autre")

    # Titre fusionné → juste un titre court
    prompt_titre = f"""Tu es un éditeur de presse burkinabè.
Ces titres parlent du même sujet :
{chr(10).join(f'- {t}' for t in titres)}

Propose UN SEUL titre synthétique en français. Réponds UNIQUEMENT avec le titre."""

    titre_merge = _ollama_generate(prompt_titre, max_tokens=50) or titres[0]

    # Calcul dynamique du 1/4 pour le merge
    total_chars = sum(len(a.get("corps", "")) for a in groupe)
    cible_chars = max(300, int(total_chars * RESUME_RATIO / len(groupe)))
    max_tokens = max(256, int(cible_chars / 2.5) + 150)

    prompt_resume = f"""Tu es un journaliste burkinabè. Plusieurs sources parlent du même sujet.
Fusionne et résume ces informations en UN SEUL article cohérent en français.
Le résumé doit faire environ {cible_chars} caractères (1/4 du total).
Élimine les répétitions, garde les faits essentiels de toutes les sources.

SOURCES :
{corps_combines[:5000]}

Donne UNIQUEMENT le résumé fusionné, sans introduction ni commentaire."""

    resume = _ollama_generate(prompt_resume, max_tokens=max_tokens)

    if not resume:
        logger.warning("  Merge échoué — fallback copie")
        resume = groupe[0].get("corps", "")[:cible_chars]

    sources = [
        {"titre": a.get("titre", ""), "url": a.get("url", ""), "source": a.get("source", "")}
        for a in groupe
    ]

    # Embedding du groupe = moyenne des embeddings
    embeddings_valides = [a["_embedding"] for a in groupe if a.get("_embedding")]
    embedding_moyen = None
    if embeddings_valides:
        embedding_moyen = np.mean(embeddings_valides, axis=0).tolist()

    
        # Collecter toutes les images des articles du groupe (dédupliquées)
    images_merge = []
    vu = set()
    for a in groupe:
        for img in a.get("images", []):
            if img not in vu:
                vu.add(img)
                images_merge.append(img)

    return _construire_article_traite(
        titre=titre_merge,
        resume_fr=resume,
        categorie=categorie,
        sources=sources,
        embedding=embedding_moyen,
        date_pub=groupe[0].get("date_publication"),
        images=images_merge,  # ← AJOUTE CETTE LIGNE
    )


def _construire_article_traite(
    titre: str,
    resume_fr: str,
    categorie: str,
    sources: list[dict],
    embedding: list[float] | None,
    date_pub,
    images: list[str] = None,
) -> dict:
    """Construit le document final pour articles_traites."""
    return {
        "titre":            titre,
        "categorie":        categorie,
        "resume_fr":        resume_fr,
        "resume_moore":     "",
        "audio_moore_url":  "",
        "sources":          sources,
        "images": images or [],
        "embedding":        embedding,
        "date_publication": date_pub,
        "date_traitement":  datetime.now(timezone.utc),
        "statut_tts":       "en_attente",
        "hash":             hashlib.md5(titre.encode()).hexdigest(),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# ÉTAPE 4 — TTS MOORÉ (PLACEHOLDER)
# ═══════════════════════════════════════════════════════════════════════════════

def traduire_et_synthetiser_moore(resume_fr: str) -> tuple[str, str]:
    """
    Traduit le résumé en Mooré et génère l'audio TTS.
    Retourne (texte_moore, url_audio).
    PLACEHOLDER — à compléter avec l'API TTS quand l'accès sera disponible.
    """
    texte_moore = ""
    try:
        max_tokens = max(256, int(len(resume_fr) / 4) + 100)

        prompt = f"""Traduis ce texte du français vers le Mooré (langue du Burkina Faso).
Texte : {resume_fr}
Donne UNIQUEMENT la traduction en Mooré, sans explication."""

        texte_moore = _ollama_generate(prompt, max_tokens=max_tokens)
        logger.info(" Traduction Mooré OK (%d caractères)", len(texte_moore or ""))
    except Exception as e:
        logger.error(" Erreur traduction Mooré : %s", e)

    # ── TTS Mooré via API externe ────────────────────────────────────────────
    # TODO : Remplacer par l'appel réel à ton API TTS Mooré quand accès disponible
    #
    # TTS_API_URL = os.getenv("TTS_MOORE_API_URL", "")
    # TTS_API_KEY = os.getenv("TTS_MOORE_API_KEY", "")
    #
    # if TTS_API_URL and texte_moore:
    #     try:
    #         r = req_lib.post(
    #             TTS_API_URL,
    #             headers={"Authorization": f"Bearer {TTS_API_KEY}"},
    #             json={"text": texte_moore, "language": "mos", "format": "mp3"},
    #             timeout=60,
    #         )
    #         r.raise_for_status()
    #         url_audio = r.json().get("audio_url", "")
    #         logger.info(" TTS Mooré OK : %s", url_audio)
    #         return texte_moore, url_audio
    #     except Exception as e:
    #         logger.error(" Erreur TTS Mooré : %s", e)

    logger.info(" TTS Mooré en attente d'API — placeholder actif")
    return texte_moore, ""


# ═══════════════════════════════════════════════════════════════════════════════
# PIPELINE PRINCIPAL
# ═══════════════════════════════════════════════════════════════════════════════

def run_pipeline():
    debut = datetime.now()
    logger.info(" Pipeline IA démarré — %s", debut.strftime("%Y-%m-%d %H:%M:%S"))

    # ── Connexion MongoDB ────────────────────────────────────────────────────
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        db = client[MONGO_DB]
        col_bruts   = db[COL_BRUTS]
        col_traites = db[COL_TRAITES]

        col_traites.create_index("hash", unique=True)
        col_traites.create_index("categorie")
        col_traites.create_index("date_traitement")
        logger.info(" MongoDB connecté")
    except Exception as e:
        logger.critical(" MongoDB indisponible : %s", e)
        return

    # ── Lecture articles bruts ───────────────────────────────────────────────
    limite_temps = datetime.now(timezone.utc) - timedelta(hours=FENETRE_HEURES)
    filtre = {
        "date_scraping": {"$gte": limite_temps},
        "corps": {"$exists": True, "$ne": ""},
        "statut_paraphrase": "en_attente",
    }
    articles_bruts = list(col_bruts.find(filtre))
    logger.info(" %d articles bruts à traiter", len(articles_bruts))

    if not articles_bruts:
        logger.info("  Rien à traiter — pipeline terminé")
        client.close()
        return

    # ── Traitement spécial RTB ───────────────────────────────────────────────
    articles_a_traiter = []
    for art in articles_bruts:
        if art.get("source") == "RTB" and art.get("type_contenu") == "video":
            logger.info("🎬 Découpe RTB : %s", art.get("titre", "?")[:60])
            sujets = classifier_sujets_rtb(art.get("corps", ""))
            for sujet in sujets:
                articles_a_traiter.append({
                    "titre":            sujet.get("titre", "Sujet RTB"),
                    "corps":            sujet.get("resume", ""),
                    "source":           "RTB",
                    "url":              art.get("url", ""),
                    "date_publication": art.get("date_publication"),
                    "_categorie":       sujet.get("categorie", "Autre"),
                    "_origine_id":      art.get("_id"),
                    "images": [],
                })
        else:
            articles_a_traiter.append(art)

    # ── Classification articles non-RTB ─────────────────────────────────────
    logger.info("  Classification de %d articles...", len(articles_a_traiter))
    for art in articles_a_traiter:
        if "_categorie" not in art:
            art["_categorie"] = classifier_article(
                art.get("titre", ""),
                art.get("corps", ""),
            )
            logger.info("  [%s] → %s", art.get("titre", "?")[:50], art["_categorie"])

    # ── Calcul embeddings ────────────────────────────────────────────────────
    articles_a_traiter = calculer_embeddings(articles_a_traiter)

    # ── Groupement par catégorie puis similarité ─────────────────────────────
    articles_finals = []
    categories_presentes = set(a["_categorie"] for a in articles_a_traiter)

    for categorie in categories_presentes:
        groupe_cat = [a for a in articles_a_traiter if a["_categorie"] == categorie]
        logger.info(" Catégorie '%s' : %d articles", categorie, len(groupe_cat))

        groupes = grouper_par_similarite(groupe_cat)

        for groupe in groupes:
            embedding_ref = groupe[0].get("_embedding")
            if embedding_ref and verifier_doublon_mongodb(col_traites, embedding_ref):
                logger.info("  Groupe ignoré — doublon déjà en base")
                continue

            article_final = merger_et_resumer(groupe)
            if article_final:
                articles_finals.append(article_final)

    logger.info(" %d articles finaux produits", len(articles_finals))

    # ── TTS Mooré + sauvegarde ───────────────────────────────────────────────
    sauvegardes = 0
    for art in articles_finals:
        texte_moore, url_audio = traduire_et_synthetiser_moore(art["resume_fr"])
        art["resume_moore"]    = texte_moore
        art["audio_moore_url"] = url_audio
        art["statut_tts"]      = "genere" if url_audio else "en_attente"

        try:
            col_traites.update_one(
                {"hash": art["hash"]},
                {"$setOnInsert": art},
                upsert=True,
            )
            sauvegardes += 1
            logger.info(" Sauvegardé : [%s] %s", art["categorie"], art["titre"][:60])
        except mongo_errors.DuplicateKeyError:
            logger.debug("  Déjà existant : %s", art["titre"][:60])
        except Exception as e:
            logger.error(" Erreur sauvegarde : %s", e)

    # ── Marquer articles bruts comme traités ─────────────────────────────────
    ids_traites = [a.get("_origine_id") or a.get("_id") for a in articles_bruts if a.get("_id")]
    ids_valides = [i for i in ids_traites if i]
    if ids_valides:
        col_bruts.update_many(
            {"_id": {"$in": ids_valides}},
            {"$set": {"statut_paraphrase": "traite", "statut_resume": "traite"}},
        )

    duree = (datetime.now() - debut).total_seconds()
    logger.info(
        "🏁 Pipeline terminé en %.1fs — %d articles sauvegardés sur %d produits",
        duree, sauvegardes, len(articles_finals)
    )
    client.close()


# ═══════════════════════════════════════════════════════════════════════════════
# LANCEMENT
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    run_pipeline()