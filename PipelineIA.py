#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pipeline IA — Burkina News v2
- Embedding via Cohere embed-multilingual-v3 (fallback Ollama)
- Clustering transitif SANS pré-filtrage par catégorie
- Seuil de similarité abaissé à 0.72
- Sources correctement enregistrées en base
- Essentiel du jour : analyse sociologique + rédaction captivante
- Placeholder TTS Mooré pour l'essentiel du jour
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
from dotenv import load_dotenv
from pathlib import Path



# Debug — affiche ce qui est chargé
env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path, override=True)
print(f"🔍 Chemin .env : {env_path}")
print(f"🔍 Existe : {env_path.exists()}")
# Vérifie que c'est bien chargé
print(f"🔍 MONGO_URI : {os.getenv('MONGO_URI', 'NON TROUVÉ')[:60]}")






# ─── Cohere ──────────────────────────────────────────────────────────────────
try:
    import cohere
    COHERE_DISPONIBLE = True
except ImportError:
    COHERE_DISPONIBLE = False

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
COL_ESSENTIEL    = "essentiel_du_jour"

OLLAMA_URL       = os.getenv("OLLAMA_URL", "http://localhost:11434")
MODELE_LLM       = os.getenv("QWEN_MODEL", "qwen3:1.7b")
MODELE_EMBEDDING = os.getenv("EMBEDDING_MODEL", "qwen3-embedding:0.6b")

GROQ_API_KEY     = os.getenv("GROQ_API_KEY", "")
#GROQ_MODEL       = "llama-3.3-70b-versatile"
GROQ_MODEL       = "llama-3.1-8b-instant"
COHERE_API_KEY   = os.getenv("COHERE_API_KEY", "")

# Seuil abaissé pour capturer les articles du même événement
SEUIL_SIMILARITE = 0.72
FENETRE_HEURES   = 120
RESUME_RATIO     = 0.25

# ─── Catégories ───────────────────────────────────────────────────────────────
CATEGORIES = [
    "Politique", "Sécurité", "Économie", "Sport", "Santé",
    "Éducation", "Agriculture", "Culture", "Justice",
    "International", "Société", "Environnement", "Technologie", "Autre",
]


# ═══════════════════════════════════════════════════════════════════════════════
# EMBEDDING — COHERE EN PRIORITÉ, FALLBACK OLLAMA
# ═══════════════════════════════════════════════════════════════════════════════

def _get_embedding(texte: str) -> list[float] | None:
    """
    Embedding via Cohere embed-multilingual-v3 en priorité.
    Fallback automatique vers Ollama si Cohere indisponible ou en erreur.
    """

    # ── Cohere en priorité ────────────────────────────────────────────────────
    if COHERE_DISPONIBLE and COHERE_API_KEY:
        try:
            co = cohere.ClientV2(api_key=COHERE_API_KEY)
            response = co.embed(
                texts=[texte[:2000]],
                model="embed-multilingual-v3.0",
                input_type="search_document",
                embedding_types=["float"],
            )
            vecteur = response.embeddings.float[0]
            if vecteur:
                logger.debug("✅ Cohere embedding OK (dim=%d)", len(vecteur))
                #time.sleep(0.7)
                return vecteur
        except Exception as e:
            if "429" in str(e) or "rate" in str(e).lower():
                logger.warning("  Cohere rate limit — fallback Ollama...")
            else:
                logger.error(" Cohere embedding erreur : %s — fallback Ollama", e)
    elif not COHERE_API_KEY:
        logger.debug("COHERE_API_KEY non définie — Ollama utilisé")

    # ── Fallback Ollama ───────────────────────────────────────────────────────
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
        logger.error("❌ Ollama embedding erreur : %s", e)
        return None


def _cosine_similarity(v1: list[float], v2: list[float]) -> float:
    """Calcul de similarité cosinus entre deux vecteurs."""
    a = np.array(v1, dtype=np.float32)
    b = np.array(v2, dtype=np.float32)
    # Vecteurs de dimensions différentes (Cohere=1024, Ollama variable)
    if len(a) != len(b):
        logger.warning("⚠️  Dimensions incompatibles : %d vs %d — similarité=0", len(a), len(b))
        return 0.0
    norm_a = np.linalg.norm(a)
    norm_b = np.linalg.norm(b)
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return float(np.dot(a, b) / (norm_a * norm_b))


# ═══════════════════════════════════════════════════════════════════════════════
# GROQ + OLLAMA — LLM HELPER
# ═══════════════════════════════════════════════════════════════════════════════

def _llm_generate(prompt: str, system: str = "", max_tokens: int = 512) -> str:
    """Groq en priorité avec retry, Ollama en fallback."""

    if GROQ_API_KEY:
        client = Groq(api_key=GROQ_API_KEY)
        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})

        # Jusqu'à 3 tentatives Groq avant de tomber sur Ollama
        for tentative in range(3):
            try:
                response = client.chat.completions.create(
                    model=GROQ_MODEL,
                    messages=messages,
                    max_tokens=max_tokens,
                    temperature=0.3,
                )
                texte = response.choices[0].message.content.strip()
                time.sleep(4)  # Pause entre chaque appel pour éviter 429
                if texte:
                    return texte
            except Exception as e:
                if "rate_limit" in str(e).lower() or "429" in str(e):
                    attente = 30 * (tentative + 1)  # 30s, 60s, 90s
                    logger.warning(
                        "⚠️  Groq limite atteinte (tentative %d/3) — attente %ds...",
                        tentative + 1, attente
                    )
                    time.sleep(attente)
                else:
                    logger.error("❌ Groq erreur : %s", e)
                    break  # Erreur non liée au rate limit → fallback direct

        logger.warning("⚠️  Groq épuisé après 3 tentatives — fallback Ollama")

    # Fallback Ollama
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
        logger.error("❌ Ollama LLM erreur : %s", e)
        return ""


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
    reponse = _llm_generate(prompt, max_tokens=20)
    for cat in CATEGORIES:
        if cat.lower() in reponse.lower():
            return cat
    return "Autre"


# ═══════════════════════════════════════════════════════════════════════════════
# ÉTAPE 2 — EMBEDDING + CLUSTERING TRANSITIF
# ═══════════════════════════════════════════════════════════════════════════════

def calculer_embeddings(articles: list[dict]) -> list[dict]:
    """Calcule et attache les embeddings à chaque article."""
    logger.info("🔢 Calcul embeddings pour %d articles...", len(articles))
    for art in articles:
        texte = f"{art.get('titre', '')} {art.get('corps', '')[:1000]}"
        vecteur = _get_embedding(texte)
        art["_embedding"] = vecteur
        if not vecteur:
            logger.warning("⚠️  Embedding manquant : %s", art.get('titre', '?')[:50])
    return articles


def grouper_par_similarite_transitif(articles: list[dict], seuil: float = SEUIL_SIMILARITE) -> list[list[dict]]:
    """
    Clustering TRANSITIF sur TOUS les articles sans pré-filtrage par catégorie.
    
    Si A ≈ B et B ≈ C → A, B, C sont dans le même groupe
    même si A et C ne se ressemblent pas directement.
    
    C'est ce qui permet de grouper tous les articles sur les Étalons
    même si chacun aborde un angle différent.
    """
    n = len(articles)

    # Construire la matrice de similarité
    similaires = {i: set() for i in range(n)}
    for i in range(n):
        for j in range(i + 1, n):
            emb_i = articles[i].get("_embedding")
            emb_j = articles[j].get("_embedding")
            if not emb_i or not emb_j:
                continue
            sim = _cosine_similarity(emb_i, emb_j)
            if sim >= seuil:
                similaires[i].add(j)
                similaires[j].add(i)
                logger.info(
                    "🔗 Similarité %.2f : '%s' ↔ '%s'",
                    sim,
                    articles[i].get('titre', '?')[:40],
                    articles[j].get('titre', '?')[:40],
                )

    # Union-Find pour clustering transitif
    parent = list(range(n))

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(x, y):
        px, py = find(x), find(y)
        if px != py:
            parent[px] = py

    for i in range(n):
        for j in similaires[i]:
            union(i, j)

    # Regrouper par racine commune
    groupes_dict = {}
    for i in range(n):
        racine = find(i)
        groupes_dict.setdefault(racine, []).append(articles[i])

    groupes = list(groupes_dict.values())
    logger.info("📦 %d articles → %d groupes (seuil=%.2f)", n, len(groupes), seuil)
    return groupes


def verifier_doublon_mongodb(collection_traites, embedding: list[float], seuil: float = SEUIL_SIMILARITE) -> bool:
    """Vérifie si un article similaire existe déjà dans articles_traites (48h)."""
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
                logger.info("♻️  Doublon détecté (sim=%.2f) : %s", sim, doc.get('titre', '?')[:50])
                return True
        return False
    except Exception as e:
        logger.error("❌ Erreur vérification doublon : %s", e)
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# ÉTAPE 3 — FUSION + RÉSUMÉ
# ═══════════════════════════════════════════════════════════════════════════════

def merger_et_resumer(groupe: list[dict]) -> dict | None:
    """Fusionne un groupe d'articles similaires et produit un résumé à 1/4."""
    if not groupe:
        return None

    # ── Article unique → résumé simple ──────────────────────────────────────
    if len(groupe) == 1:
        art = groupe[0]
        corps = art.get("corps", "")
        titre = art.get("titre", "Sans titre")
        categorie = art.get("_categorie", "Autre")

        cible_chars = max(200, int(len(corps) * RESUME_RATIO))
        max_tokens = max(256, int(cible_chars / 2.5) + 150)

        prompt = f"""Tu es un journaliste burkinabè. Résume et reformule cet article en français.
- Le résumé doit faire environ {cible_chars} caractères (1/4 de l'original)
- Reformule avec tes propres mots
- Sois factuel, précis, garde les informations essentielles

TITRE : {titre}
ARTICLE : {corps[:4000]}

Donne UNIQUEMENT le résumé reformulé, sans introduction ni commentaire."""

        resume = _llm_generate(prompt, max_tokens=max_tokens)
        if not resume:
            resume = corps[:cible_chars]

        sources = [_extraire_source(art)]

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
    logger.info("🔀 Merge de %d articles similaires...", len(groupe))

    titres = [a.get("titre", "") for a in groupe]

    # Catégorie majoritaire dans le groupe
    from collections import Counter
    categories_groupe = [a.get("_categorie", "Autre") for a in groupe]
    categorie = Counter(categories_groupe).most_common(1)[0][0]

    corps_combines = "\n\n---\n\n".join([
        f"[Source: {a.get('source', '?')}]\n{a.get('corps', '')[:2000]}"
        for a in groupe
    ])

    # Titre fusionné
    prompt_titre = f"""Tu es un éditeur de presse burkinabè.
Ces titres parlent du même sujet ou événement :
{chr(10).join(f'- {t}' for t in titres)}

Propose UN SEUL titre synthétique en français qui couvre tous les angles.
Réponds UNIQUEMENT avec le titre."""
    titre_merge = _llm_generate(prompt_titre, max_tokens=60) or titres[0]

    # Résumé fusionné
    total_chars = sum(len(a.get("corps", "")) for a in groupe)
    cible_chars = max(300, int(total_chars * RESUME_RATIO / len(groupe)))
    max_tokens = max(256, int(cible_chars / 2.5) + 150)

    prompt_resume = f"""Tu es un journaliste burkinabè. Plusieurs sources parlent du même sujet ou événement.
Fusionne et résume ces informations en UN SEUL article cohérent en français.
- Le résumé doit faire environ {cible_chars} caractères
- Élimine les répétitions
- Garde les faits essentiels de TOUTES les sources
- Mentionne les différents angles si pertinent

SOURCES :
{corps_combines[:5000]}

Donne UNIQUEMENT le résumé fusionné, sans introduction ni commentaire."""

    resume = _llm_generate(prompt_resume, max_tokens=max_tokens)
    if not resume:
        resume = groupe[0].get("corps", "")[:cible_chars]

    # ── Sources : toutes enregistrées proprement ─────────────────────────────
    sources = [_extraire_source(a) for a in groupe]

    # Embedding moyen du groupe
    embeddings_valides = [a["_embedding"] for a in groupe if a.get("_embedding")]
    embedding_moyen = None
    if embeddings_valides:
        embedding_moyen = np.mean(embeddings_valides, axis=0).tolist()

    # Images dédupliquées
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
        images=images_merge,
    )


def _extraire_source(art: dict) -> dict:
    """Extrait les informations de source d'un article brut."""
    return {
        "titre":          art.get("titre", ""),
        "url":            art.get("url", ""),
        "source":         art.get("source", ""),
        "domaine":        art.get("domaine", ""),
        "date_publication": art.get("date_publication"),
    }


def extraire_localisation(titre: str, resume: str) -> dict:
    
    # ── Étape 1 : LLM trouve la ville et le type ──────────────────────
    prompt = f"""Analyse cet article de presse burkinabè et extrais :
1. La ville ou région principale mentionnée
2. Le type d'événement (security, economy, politics, health, sport)

Titre : {titre}
Contenu : {resume[:1000]}

Réponds STRICTEMENT en JSON :
{{"ville": "Ouagadougou", "type": "politics"}}

Types valides : security, economy, politics, health, sport"""

    reponse = _llm_generate(prompt, max_tokens=64)
    
    ville = "Ouagadougou"
    type_evt = "politics"
    
    try:
        match = re.search(r'\{.*?\}', reponse, re.DOTALL)
        if match:
            data = json.loads(match.group())
            ville = data.get("ville", "Ouagadougou")
            type_evt = data.get("type", "politics")
    except Exception as e:
        logger.error("❌ Erreur parsing localisation : %s", e)

    # ── Étape 2 : Nominatim trouve les vraies coordonnées ─────────────
    coords = _geocoder_ville(ville)
    
    return {
        "ville":    ville,
        "type":     type_evt,
        "lat":      coords["lat"] if coords else 12.3647,  # fallback Ouaga
        "lon":      coords["lon"] if coords else -1.5332,
    }


def _geocoder_ville(ville: str) -> dict | None:
    """Trouve lat/lon d'une ville via Nominatim (OpenStreetMap)."""
    try:
        time.sleep(1)  # Respect limite 1 req/sec
        r = req_lib.get(
            "https://nominatim.openstreetmap.org/search",
            params={
                "q":      f"{ville}, Burkina Faso",
                "format": "json",
                "limit":  1,
            },
            headers={"User-Agent": "BurkinaNews/1.0"},
            timeout=10,
        )
        data = r.json()
        if data:
            lat = float(data[0]["lat"])
            lon = float(data[0]["lon"])
            logger.info(" Géocodage OK : %s → lat=%.4f, lon=%.4f", ville, lat, lon)
            return {"lat": lat, "lon": lon}
        else:
            logger.warning("  Ville non trouvée : %s", ville)
            return None
    except Exception as e:
        logger.error(" Nominatim erreur : %s", e)
        return None


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
    geo = extraire_localisation(titre, resume_fr)
    return {
        "titre":            titre,
        "categorie":        categorie,
        "resume_fr":        resume_fr,
        "resume_moore":     "",
        "audio_moore_url":  "",
        "sources":          sources,   # ← Liste complète avec titre, url, source, domaine, date
        "images":           images or [],
        "embedding":        embedding,
        "date_publication": date_pub,
        "date_traitement":  datetime.now(timezone.utc),
        "statut_tts":       "en_attente",
        "hash":             hashlib.md5(titre.encode()).hexdigest(),
        "localisation":     geo["ville"],
        "type_evenement":   geo["type"],
        "coordonnees":      {"lat": geo["lat"], "lon": geo["lon"]},
    }


# ═══════════════════════════════════════════════════════════════════════════════
# ÉTAPE 4 — TTS MOORÉ (PLACEHOLDER)
# ═══════════════════════════════════════════════════════════════════════════════

def traduire_et_synthetiser_moore(resume_fr: str) -> tuple[str, str]:
    """
    Traduit le résumé en Mooré et génère l'audio TTS.
    Retourne (texte_moore, url_audio).
    """
    texte_moore = ""
    try:
        max_tokens = max(256, int(len(resume_fr) / 4) + 100)
        prompt = f"""Traduis ce texte du français vers le Mooré (langue du Burkina Faso).
Texte : {resume_fr}
Donne UNIQUEMENT la traduction en Mooré, sans explication."""
        texte_moore = _llm_generate(prompt, max_tokens=max_tokens)
        logger.info(" Traduction Mooré OK (%d caractères)", len(texte_moore or ""))
    except Exception as e:
        logger.error(" Erreur traduction Mooré : %s", e)

    # ── TTS Mooré via modèle local ───────────────────────────────────────────
    # TODO : Intégrer ton modèle TTS local ici
    #
    # Exemple avec un modèle local :
    # TTS_MODEL_PATH = os.getenv("TTS_MOORE_MODEL_PATH", "")
    # if TTS_MODEL_PATH and texte_moore:
    #     try:
    #         # Appel à ton modèle local
    #         url_audio = ton_modele_tts(texte_moore)
    #         return texte_moore, url_audio
    #     except Exception as e:
    #         logger.error(" Erreur TTS Mooré local : %s", e)

    logger.info(" TTS Mooré en attente d'intégration du modèle local")
    return texte_moore, ""


# ═══════════════════════════════════════════════════════════════════════════════
# VOLET 2 — ESSENTIEL DU JOUR
# ═══════════════════════════════════════════════════════════════════════════════

def generer_essentiel_du_jour(col_traites, col_essentiel):
    """
    Analyse TOUS les articles traités du jour et génère :
    - Un essentiel en français : quelques paragraphes captivants sur ce qu'il faut retenir
    - Une traduction en Mooré
    - Un placeholder audio TTS
    
    Sauvegardé dans la collection 'essentiel_du_jour'.
    """
    aujourd_hui = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    demain = aujourd_hui + timedelta(days=1)

    articles_du_jour = list(col_traites.find(
        {"date_traitement": {"$gte": aujourd_hui, "$lt": demain}},
        {"titre": 1, "categorie": 1, "resume_fr": 1, "localisation": 1}
    ))

    if not articles_du_jour:
        logger.info("  Essentiel du jour : aucun article aujourd'hui")
        return

    logger.info(" Génération essentiel du jour — %d articles analysés", len(articles_du_jour))

    # Préparer le contenu pour le LLM
    contenu_articles = "\n\n".join([
        f"[{a.get('categorie', '?')} — {a.get('localisation', '?')}]\n"
        f"Titre : {a.get('titre', '')}\n"
        f"Résumé : {a.get('resume_fr', '')[:400]}"
        for a in articles_du_jour
    ])

    date_str = datetime.now(timezone.utc).strftime("%d %B %Y")

    # ── Prompt sociologique + journalistique ─────────────────────────────────
    system = """Tu es à la fois expert en sociologie burkinabè et journaliste chevronné.
Tu connais parfaitement les enjeux politiques, sécuritaires, économiques et sociaux du Burkina Faso.
Ton rôle : rédiger l'essentiel de l'actualité du jour de façon à la fois rigoureuse et captivante,
pour que chaque Burkinabè comprenne ce qui s'est passé aujourd'hui et ait envie d'en savoir plus."""

    prompt = f"""Voici tous les articles d'actualité burkinabè du {date_str} :

{contenu_articles[:6000]}

---

Rédige "L'essentiel du {date_str}" en français.

CONSIGNES :
1. Identifie les 3 à 5 faits VRAIMENT importants du jour (pas tous les articles, juste l'essentiel)
2. Pour chaque fait : écris un court paragraphe (3-5 phrases) qui :
   - Explique clairement ce qui s'est passé
   - Met en contexte l'importance pour les Burkinabè
   - Donne envie de lire l'article complet (sans spoiler tout)
3. Commence par une phrase d'accroche générale sur la journée
4. Utilise un ton direct, humain, accessible à tous
5. Évite le jargon journalistique pompeux
6. Si un sujet touche la vie quotidienne des gens, mets-le en avant

Format attendu :
[Phrase d'accroche de la journée]

[Paragraphe sur fait 1]

[Paragraphe sur fait 2]

... etc.

Donne UNIQUEMENT le texte de l'essentiel, sans titres ni numéros."""

    max_tokens = 800
    essentiel_fr = _llm_generate(prompt, system=system, max_tokens=max_tokens)

    if not essentiel_fr:
        logger.error(" Échec génération essentiel du jour")
        return

    logger.info(" Essentiel du jour généré (%d caractères)", len(essentiel_fr))

    # ── Traduction Mooré ──────────────────────────────────────────────────────
    essentiel_moore = ""
    try:
        prompt_moore = f"""Traduis ce résumé d'actualité du français vers le Mooré (langue du Burkina Faso).
Texte : {essentiel_fr}
Donne UNIQUEMENT la traduction en Mooré, sans explication."""
        essentiel_moore = _llm_generate(prompt_moore, max_tokens=max_tokens)
        logger.info("✅ Traduction Mooré essentiel OK (%d caractères)", len(essentiel_moore or ""))
    except Exception as e:
        logger.error("❌ Erreur traduction Mooré essentiel : %s", e)

    # ── TTS Mooré essentiel via modèle local ─────────────────────────────────
    audio_moore_url = ""
    # TODO : Intégrer ton modèle TTS local ici pour l'essentiel du jour
    #
    # Exemple :
    # TTS_MODEL_PATH = os.getenv("TTS_MOORE_MODEL_PATH", "")
    # if TTS_MODEL_PATH and essentiel_moore:
    #     try:
    #         audio_moore_url = ton_modele_tts(essentiel_moore)
    #         logger.info(" Audio TTS essentiel OK : %s", audio_moore_url)
    #     except Exception as e:
    #         logger.error(" Erreur TTS essentiel : %s", e)

    # ── Sauvegarde en base ────────────────────────────────────────────────────
    doc_essentiel = {
        "date":              aujourd_hui,
        "date_str":          date_str,
        "nb_articles":       len(articles_du_jour),
        "essentiel_fr":      essentiel_fr,
        "essentiel_moore":   essentiel_moore,
        "audio_moore_url":   audio_moore_url,
        "statut_tts":        "genere" if audio_moore_url else "en_attente",
        "categories_du_jour": list(set(a.get("categorie", "Autre") for a in articles_du_jour)),
        "date_generation":   datetime.now(timezone.utc),
    }

    try:
        col_essentiel.update_one(
            {"date": aujourd_hui},
            {"$set": doc_essentiel},
            upsert=True,
        )
        logger.info(" Essentiel du jour sauvegardé en base (collection: %s)", COL_ESSENTIEL)
    except Exception as e:
        logger.error(" Erreur sauvegarde essentiel : %s", e)


# ═══════════════════════════════════════════════════════════════════════════════
# PIPELINE PRINCIPAL
# ═══════════════════════════════════════════════════════════════════════════════

def run_pipeline():
    debut = datetime.now()
    logger.info("🚀 Pipeline IA v2 démarré — %s", debut.strftime("%Y-%m-%d %H:%M:%S"))

    # ── Connexion MongoDB ────────────────────────────────────────────────────
    try:
        client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=10000,  # juste augmenter le timeout
        )
        client.admin.command("ping")
        db = client[MONGO_DB]
        col_bruts    = db[COL_BRUTS]
        col_traites  = db[COL_TRAITES]
        col_essentiel = db[COL_ESSENTIEL]

        col_traites.create_index("hash", unique=True)
        col_traites.create_index("categorie")
        col_traites.create_index("date_traitement")
        col_essentiel.create_index("date", unique=True)
        logger.info(" MongoDB connecté")
    except Exception as e:
        logger.critical(" MongoDB indisponible : %s", e)
        return

    # ── Lecture articles bruts ───────────────────────────────────────────────
    limite_temps = datetime.now(timezone.utc) - timedelta(hours=FENETRE_HEURES)
    filtre = {
        "date_scraping":      {"$gte": limite_temps},
        "corps":              {"$exists": True, "$ne": ""},
        "statut_paraphrase":  "en_attente",
    }
    articles_bruts = list(col_bruts.find(filtre))
    logger.info("📥 %d articles bruts à traiter", len(articles_bruts))

    if not articles_bruts:
        logger.info("  Rien à traiter")
        # Même sans nouveaux articles, générer l'essentiel du jour si pas encore fait
        generer_essentiel_du_jour(col_traites, col_essentiel)
        client.close()
        return

    # ── Filtrer RTB (traitement spécial non géré ici) ────────────────────────
    articles_a_traiter = [
        a for a in articles_bruts
        if a.get("source") != "RTB"
    ]
    logger.info("📋 %d articles à traiter (RTB exclus)", len(articles_a_traiter))

    if not articles_a_traiter:
        generer_essentiel_du_jour(col_traites, col_essentiel)
        client.close()
        return

    # ── Classification ───────────────────────────────────────────────────────
    logger.info("🏷️  Classification de %d articles...", len(articles_a_traiter))
    for art in articles_a_traiter:
        if "_categorie" not in art:
            art["_categorie"] = classifier_article(
                art.get("titre", ""),
                art.get("corps", ""),
            )
            logger.info("  [%s] → %s", art.get("titre", "?")[:50], art["_categorie"])

    # ── Calcul embeddings ────────────────────────────────────────────────────
    articles_a_traiter = calculer_embeddings(articles_a_traiter)

    # ── Clustering transitif SUR TOUS LES ARTICLES ──────────────────────────
    # Pas de pré-filtrage par catégorie : on compare tout avec tout
    # Cela permet de grouper des articles sur le même événement
    # même s'ils ont été classifiés différemment
    logger.info("🔗 Clustering transitif sur %d articles...", len(articles_a_traiter))
    groupes = grouper_par_similarite_transitif(articles_a_traiter, seuil=SEUIL_SIMILARITE)

    # ── Fusion + résumé ──────────────────────────────────────────────────────
    articles_finals = []
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
        # TTS désactivé pour les articles individuels — placeholder actif
        art["resume_moore"]    = ""
        art["audio_moore_url"] = ""
        art["statut_tts"]      = "en_attente"

        try:
            col_traites.update_one(
                {"hash": art["hash"]},
                {
                    "$setOnInsert": {k: v for k, v in art.items() if k != "sources"},
                    "$addToSet":    {"sources": {"$each": art["sources"]}},
                },
                upsert=True,
            )
            sauvegardes += 1
            logger.info(
                "💾 Sauvegardé : [%s] %s (%d source(s))",
                art["categorie"],
                art["titre"][:60],
                len(art["sources"]),
            )
        except mongo_errors.DuplicateKeyError:
            logger.debug("⏭  Déjà existant : %s", art["titre"][:60])
        except Exception as e:
            logger.error(" Erreur sauvegarde : %s", e)

    # ── Marquer articles bruts comme traités ─────────────────────────────────
    ids_valides = [a.get("_id") for a in articles_bruts if a.get("_id")]
    if ids_valides:
        col_bruts.update_many(
            {"_id": {"$in": ids_valides}},
            {"$set": {"statut_paraphrase": "traite", "statut_resume": "traite"}},
        )

    # ── Essentiel du jour ────────────────────────────────────────────────────
    generer_essentiel_du_jour(col_traites, col_essentiel)

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