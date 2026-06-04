from fastapi import APIRouter, HTTPException, Query, Response
from Api.database import get_db
from Api.models import ArticleTraiteDetail, ArticleTraiteResume
from bson import ObjectId
from typing import List
from datetime import datetime, timezone

router = APIRouter()


def format_article(doc: dict) -> dict:
    """Convertit l'_id ObjectId MongoDB en string"""
    doc["id"] = str(doc["_id"])
    doc["_id"] = str(doc["_id"])
    doc["a_audio"] = bool(doc.get("audio_moore_url"))
    doc["a_moore"] = bool(doc.get("resume_moore"))
    return doc


@router.get("/", response_model=List[ArticleTraiteResume])
async def get_articles(
    response: Response,
    page: int = Query(1, ge=1),
    limite: int = Query(40, ge=1, le=100),
    source: str = Query(None, description="Filtrer par source ex: sidwaya.info")
):
    """
    Retourne la liste des articles résumés.
    Appelé automatiquement au lancement de l'application.
    """
    db = get_db()
    filtre = {}
    if source:
        filtre["source"] = source

    # Compte total pour pagination
    total = await db.articles_traites.count_documents(filtre)
    response.headers["X-Total-Count"] = str(total)

    skip = (page - 1) * limite
    cursor = db.articles_traites.find(filtre).sort("date_publication", -1).skip(skip).limit(limite)
    articles = await cursor.to_list(length=limite)

    return [format_article(a) for a in articles]


@router.get("/essentiel/aujourd-hui")
async def get_essentiel_du_jour():
    """
    Retourne l'essentiel du jour depuis la collection essentiel_du_jour.
    """
    db = get_db()
    aujourd_hui = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    
    doc = await db.essentiel_du_jour.find_one(
        {"date": {"$gte": aujourd_hui}},
        sort=[("date_generation", -1)]
    )
    
    if not doc:
        raise HTTPException(status_code=404, detail="Aucun essentiel disponible aujourd'hui")
    
    # Découper le texte en paragraphes
    texte = doc.get("essentiel_fr", "")
    points = [p.strip() for p in texte.split("\n\n") if p.strip()]
    
    return {
        "date_str":      doc.get("date_str", ""),
        "points":        points,
        "essentiel_moore": doc.get("essentiel_moore", ""),
        "audio_moore_url": doc.get("audio_moore_url", ""),
        "nb_articles":   doc.get("nb_articles", 0),
        "date_generation": doc.get("date_generation"),
    }


@router.get("/evenements")
async def get_evenements(
    limite: int = Query(20, ge=1, le=50),
    type_event: str = Query(None, description="Filtrer par type: security, economy, politics, health, sport")
):
    """
    Retourne les événements géolocalisés pour la carte interactive.
    """
    db = get_db()
    filtre = {"coordonnees": {"$exists": True}}
    
    if type_event:
        filtre["type_evenement"] = type_event

    cursor = db.articles_traites.find(filtre).sort("date_publication", -1).limit(limite)
    evenements = await cursor.to_list(length=limite)

    return [
        {
            "id": str(doc["_id"]),
            "title": doc.get("titre", "Sans titre"),
            "location": doc.get("localisation", "Inconnu"),
            "type": doc.get("type_evenement", "politics"),
            "lat": doc.get("coordonnees", {}).get("lat", 12.3647),
            "lon": doc.get("coordonnees", {}).get("lon", -1.5332),
            "date": doc.get("date_publication"),
            "resume": doc.get("resume_fr", "")[:200],
        }
        for doc in evenements
    ]


@router.get("/{article_id}", response_model=ArticleTraiteDetail)
async def get_article(article_id: str):
    """
    Retourne le détail complet d'un article :
    résumé français + traduction mooré + opinion.
    Appelé quand l'utilisateur clique sur un article.
    """
    db = get_db()
    try:
        oid = ObjectId(article_id)
    except Exception:
        raise HTTPException(status_code=400, detail="ID invalide")

    article = await db.articles_traites.find_one({"_id": oid})
    if not article:
        raise HTTPException(status_code=404, detail="Article introuvable")

    return format_article(article)

@router.get("/essentiel/dates")
async def get_essentiel_dates(
    mois: int = Query(..., ge=1, le=12, description="Mois ex: 6 pour juin"),
    annee: int = Query(..., ge=2024, description="Année ex: 2026")
):
    """
    Retourne la liste des jours du mois qui ont un essentiel.
    Utilisé par le calendrier pour afficher les points rouges.
    """
    db = get_db()

    debut_mois = datetime(annee, mois, 1, tzinfo=timezone.utc)
    if mois == 12:
        fin_mois = datetime(annee + 1, 1, 1, tzinfo=timezone.utc)
    else:
        fin_mois = datetime(annee, mois + 1, 1, tzinfo=timezone.utc)

    cursor = db.essentiel_du_jour.find(
        {"date": {"$gte": debut_mois, "$lt": fin_mois}},
        {"date": 1, "nb_articles": 1, "categories_du_jour": 1, "nb_regenerations": 1}
    ).sort("date", 1)

    docs = await cursor.to_list(length=31)

    return [
        {
            "jour":          doc["date"].day,
            "mois":          doc["date"].month,
            "annee":         doc["date"].year,
            "nb_articles":   doc.get("nb_articles", 0),
            "categories":    doc.get("categories_du_jour", []),
            "nb_regenerations": doc.get("nb_regenerations", 1),
        }
        for doc in docs
    ]


@router.get("/essentiel/{annee}-{mois:02d}-{jour:02d}")
async def get_essentiel_par_date(annee: int, mois: int, jour: int):
    """
    Retourne l'essentiel complet d'un jour précis.
    Appelé quand l'utilisateur clique sur un jour du calendrier.
    """
    db = get_db()

    try:
        date_debut = datetime(annee, mois, jour, 0, 0, 0, tzinfo=timezone.utc)
        date_fin   = datetime(annee, mois, jour, 23, 59, 59, tzinfo=timezone.utc)
    except ValueError:
        raise HTTPException(status_code=400, detail="Date invalide")

    doc = await db.essentiel_du_jour.find_one(
        {"date": {"$gte": date_debut, "$lte": date_fin}}
    )

    if not doc:
        raise HTTPException(
            status_code=404,
            detail=f"Aucun essentiel pour le {jour:02d}/{mois:02d}/{annee}"
        )

    texte = doc.get("essentiel_fr", "")
    points = [p.strip() for p in texte.split("\n\n") if p.strip()]

    return {
        "date_str":        doc.get("date_str", ""),
        "jour":            jour,
        "mois":            mois,
        "annee":           annee,
        "points":          points,
        "essentiel_fr":    texte,
        "essentiel_moore": doc.get("essentiel_moore", ""),
        "audio_moore_url": doc.get("audio_moore_url", ""),
        "nb_articles":     doc.get("nb_articles", 0),
        "categories":      doc.get("categories_du_jour", []),
        "nb_regenerations": doc.get("nb_regenerations", 1),
        "date_generation": doc.get("date_generation"),
    }