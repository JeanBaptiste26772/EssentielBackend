from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class ArticleResume(BaseModel):
    """
    Version allégée pour la liste d'articles (chargement rapide au lancement de l'app).
    Contient seulement ce dont l'interface a besoin pour afficher la liste.
    """
    id: str = Field(..., alias="_id")
    titre: str
    source: str                         # ex: AIB, Sidwaya, Lefaso.net
    domaine: str                        # ex: aib.media, sidwaya.info
    date_publication: datetime
    resume_rss: str                     # Résumé court issu du flux RSS
    langue: str = "fr"
    a_audio_moore: bool = False         # True quand XTTS aura généré l'audio
    a_traduction_moore: bool = False    # True quand NLLB aura traduit
    statut_resume: str                  # "en_attente" | "traite"

    class Config:
        populate_by_name = True


class ArticleDetail(BaseModel):
    """
    Version complète retournée quand l'utilisateur clique sur un article.
    Contient le corps complet + les enrichissements IA ajoutés après scraping.
    """
    id: str = Field(..., alias="_id")
    titre: str
    source: str
    domaine: str
    type_source: str                        # "rss_texte" | "rss_video"
    url: str                                # URL de l'article original
    date_publication: datetime
    date_scraping: datetime

    # ── Contenu brut (scraper) ──────────────────────────────────────────
    resume_rss: str                         # Résumé RSS brut
    corps: str                              # Texte complet extrait

    # ── Enrichissements IA (ajoutés par le pipeline après scraping) ─────
    resume_fr: Optional[str] = None         # Résumé généré par Qwen
    paraphrase_fr: Optional[str] = None     # Paraphrase générée par Qwen
    traduction_moore: Optional[str] = None  # Traduction NLLB
    opinion: Optional[str] = None           # Opinion / coordonnées extraites

    # ── Audio (généré par XTTS-V2) ──────────────────────────────────────
    audio_fr_path: Optional[str] = None
    audio_moore_path: Optional[str] = None

    # ── Statuts pipeline ────────────────────────────────────────────────
    statut_resume: str      # "en_attente" | "traite"
    statut_paraphrase: str  # "en_attente" | "traite"
    langue: str = "fr"

    class Config:
        populate_by_name = True
