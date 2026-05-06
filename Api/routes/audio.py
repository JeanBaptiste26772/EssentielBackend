from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from Api.database import get_db
from bson import ObjectId
import os

router = APIRouter()


@router.get("/{article_id}/moore")
async def get_audio_moore(article_id: str):
    """
    Retourne le fichier audio mooré d'un article.
    Appelé quand l'utilisateur clique sur 'Écouter en mooré'.
    """
    db = get_db()
    try:
        oid = ObjectId(article_id)
    except Exception:
        raise HTTPException(status_code=400, detail="ID invalide")

    article = await db.articles.find_one({"_id": oid}, {"audio_moore_path": 1})
    if not article:
        raise HTTPException(status_code=404, detail="Article introuvable")

    path = article.get("audio_moore_path")
    if not path or not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Audio mooré non disponible")

    return FileResponse(path, media_type="audio/mpeg", filename=f"article_{article_id}_moore.mp3")


@router.get("/{article_id}/fr")
async def get_audio_fr(article_id: str):
    """
    Retourne le fichier audio français d'un article.
    Appelé quand l'utilisateur clique sur 'Écouter en français'.
    """
    db = get_db()
    try:
        oid = ObjectId(article_id)
    except Exception:
        raise HTTPException(status_code=400, detail="ID invalide")

    article = await db.articles.find_one({"_id": oid}, {"audio_fr_path": 1})
    if not article:
        raise HTTPException(status_code=404, detail="Article introuvable")

    path = article.get("audio_fr_path")
    if not path or not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Audio français non disponible")

    return FileResponse(path, media_type="audio/mpeg", filename=f"article_{article_id}_fr.mp3")
