from fastapi import APIRouter, HTTPException, Query, Response
from Api.database import get_db
from Api.models import ArticleTraiteDetail, ArticleTraiteResume
from bson import ObjectId
from typing import List

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