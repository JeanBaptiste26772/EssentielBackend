#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scraper intelligent de l'actualité burkinabè — VERSION SCRAPY v8
- URLs AIB corrigées
- Détection vidéo AIB + RTB
- Pipeline transcription : YouTube API v1 → yt-dlp → audio direct
- Ollama via requests direct (évite bug openai/proxies)
- Nettoyage HTML avec BeautifulSoup
"""

import os
import re
import sys
import time
import hashlib
import tempfile
import logging
import multiprocessing
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse

import feedparser
import requests as req_lib
import scrapy
from scrapy.crawler import CrawlerProcess
from pymongo import MongoClient, errors as mongo_errors

# ─── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("BurkinaScraper")
logging.getLogger("pymongo").setLevel(logging.WARNING)
logging.getLogger("scrapy").setLevel(logging.WARNING)

# ─── MongoDB ─────────────────────────────────────────────────────────────────
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB  = os.getenv("MONGO_DB", "burkina_news")
MONGO_COL = "articles"

# ─── Sources ─────────────────────────────────────────────────────────────────
SOURCES = [
    {
        "nom":     "AIB",
        "urls":    ["https://aib.media/feed", "https://www.aib.media/feed", "https://aib.media/feed/", "https://www.aib.media/feed/"],
        "type":    "rss_texte",
        "domaine": "aib.media",
        "homepage": "https://www.aib.media/",
    },
    {
        "nom":     "Sidwaya",
        "urls":    ["https://www.sidwaya.info/feed/"],
        "type":    "rss_texte",
        "domaine": "sidwaya.info",
    },
    {
        "nom":     "Lefaso.net",
        "urls":    ["https://lefaso.net/spip.php?page=backend"],
        "type":    "rss_texte",
        "domaine": "lefaso.net",
    },
    {
        "nom":     "Burkina24",
        "urls":    ["https://burkina24.com/feed"],
        "type":    "rss_texte",
        "domaine": "burkina24.com",
    },
    {
        "nom":     "RTB",
        "urls":    ["https://www.rtb.bf/feed/"],
        "type":    "rss_video",
        "domaine": "rtb.bf",
    },
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "fr-FR,fr;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

REQUEST_TIMEOUT = 30


# ═══════════════════════════════════════════════════════════════════════════
# UTILITAIRES
# ═══════════════════════════════════════════════════════════════════════════

def _nettoyer_html(html_brut: str) -> str:
    """Nettoie le HTML avec BeautifulSoup ou fallback regex."""
    if not html_brut:
        return ""
    
    # Étape 1 : Extraire le texte avec BeautifulSoup
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html_brut, "html.parser")
        texte = soup.get_text(separator=" ", strip=True)
    except ImportError:
        # Fallback regex si bs4 non installé
        texte = re.sub(r'<(script|style)[^>]*>.*?</\1>', ' ', html_brut, flags=re.DOTALL | re.IGNORECASE)
        texte = re.sub(r'<[^>]+>', ' ', texte)
    
    # Étape 2 : Décoder les entités HTML numériques
    texte = re.sub(r'&#(\d+);', lambda m: chr(int(m.group(1))), texte)
    
    # Étape 3 : Entités HTML nommées
    entites = {
        "&amp;": "&", "&lt;": "<", "&gt;": ">", "&quot;": '"',
        "&#39;": "'", "&nbsp;": " ", "&#160;": " ",
        "&agrave;": "à", "&eacute;": "é",
        "&egrave;": "è", "&ecirc;": "ê", "&ocirc;": "ô", "&ucirc;": "û",
        "&rsquo;": "'", "&lsquo;": "'", "&rdquo;": '"', "&ldquo;": '"',
        "&#8230;": "…", "&hellip;": "…",
        "&mdash;": "—", "&ndash;": "–",
        "&laquo;": "«", "&raquo;": "»",
    }
    for entite, char in entites.items():
        texte = texte.replace(entite, char)
    
    # Étape 4 : Nettoyage final
    texte = re.sub(r'\s+', ' ', texte).strip()
    return texte


def _normaliser_url_youtube(url: str) -> str:
    match = re.search(r"(?:embed/|v=|youtu\.be/)([\w\-]{11})", url)
    if match:
        return f"https://www.youtube.com/watch?v={match.group(1)}"
    return url


def _extraire_content_rss(entree) -> str:
    if hasattr(entree, "content_encoded") and entree.content_encoded:
        return entree.content_encoded
    if hasattr(entree, "content") and entree.content:
        for c in entree.content:
            ctype = (c.get("type", "") or "").lower()
            if "html" in ctype or "text" in ctype or ctype == "":
                val = c.get("value", "")
                if val and len(val) > 100:
                    return val
        for c in entree.content:
            val = c.get("value", "")
            if val and len(val) > 100:
                return val
    summary = entree.get("summary", "") or ""
    if len(summary) > 300:
        return summary
    return ""


def _parse_rss_via_requests(urls: list[str]) -> tuple:
    for url in urls:
        try:
            r = req_lib.get(url, headers=HEADERS, timeout=15, allow_redirects=True)
            r.raise_for_status()
            if len(r.text) < 200:
                continue
            flux = feedparser.parse(r.text)
            if flux.entries:
                return flux, url
        except Exception as e:
            logger.debug("RSS échec %s : %s", url, e)
    return None, None


# ═══════════════════════════════════════════════════════════════════════════
# PIPELINE MONGODB
# ═══════════════════════════════════════════════════════════════════════════

class MongoDBPipeline:
    def __init__(self):
        self.client = None
        self.collection = None

    def open_spider(self, spider):
        try:
            self.client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
            self.client.admin.command("ping")
            db = self.client[MONGO_DB]
            self.collection = db[MONGO_COL]
            self.collection.create_index("url", unique=True)
            self.collection.create_index("source")
            self.collection.create_index("date_publication")
            logger.info(" Connexion MongoDB — base : %s", MONGO_DB)
        except Exception as e:
            logger.critical(" MongoDB indisponible : %s", e)
            raise

    def close_spider(self, spider):
        if self.client:
            self.client.close()
            logger.info("🔌 Connexion MongoDB fermée.")

    def process_item(self, item, spider):
        try:
            resultat = self.collection.update_one(
                {"url": item["url"]},
                {"$setOnInsert": dict(item)},
                upsert=True,
            )
            if resultat.upserted_id:
                logger.info(" [%s] Nouvel article : %s", item["source"], item.get("titre", "?")[:70])
            else:
                logger.debug("  [%s] Déjà existant : %s", item["source"], item.get("url", "?")[:60])
        except mongo_errors.DuplicateKeyError:
            pass
        except Exception as e:
            logger.error(" Erreur MongoDB : %s", e)
        return item


# ═══════════════════════════════════════════════════════════════════════════
# SPIDER
# ═══════════════════════════════════════════════════════════════════════════

class BurkinaNewsSpider(scrapy.Spider):
    name = "burkina_news"
    custom_settings = {
        "LOG_LEVEL": "WARNING",
        "DOWNLOAD_DELAY": 1.5,
        "RANDOMIZE_DOWNLOAD_DELAY": 0.5,
        "USER_AGENT": HEADERS["User-Agent"],
        "ROBOTSTXT_OBEY": False,
        "ITEM_PIPELINES": {"__main__.MongoDBPipeline": 300},
        "DEFAULT_REQUEST_HEADERS": {
            "Accept-Language": "fr-FR,fr;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
        "CONCURRENT_REQUESTS": 4,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.seen_urls = set()

    def start_requests(self):
        for source in SOURCES:
            flux, url_ok = _parse_rss_via_requests(source["urls"])

            if flux and flux.entries:
                logger.info(" %s — %d articles via RSS (%s)", source["nom"], len(flux.entries), url_ok)
                if source["type"] == "rss_video":
                    yield from self._yield_video(source, flux)
                else:
                    yield from self._yield_texte(source, flux)
            else:
                if source.get("homepage"):
                    logger.warning("⚠️  %s RSS indisponible — fallback HTML : %s", source["nom"], source["homepage"])
                    yield scrapy.Request(
                        url=source["homepage"],
                        callback=self.parse_homepage_aib,
                        meta={"source": source},
                        errback=self._handle_error,
                        dont_filter=True,
                    )
                else:
                    logger.error(" %s : aucun flux accessible et pas de fallback", source["nom"])

    def _yield_texte(self, source, flux):
        rss_direct = 0
        for entree in flux.entries:
            url = entree.get("link", "").strip()
            if not url or url in self.seen_urls:
                continue
            self.seen_urls.add(url)

            date_pub = None
            if hasattr(entree, "published_parsed") and entree.published_parsed:
                date_pub = datetime(*entree.published_parsed[:6], tzinfo=timezone.utc)
            elif hasattr(entree, "updated_parsed") and entree.updated_parsed:
                date_pub = datetime(*entree.updated_parsed[:6], tzinfo=timezone.utc)
            else:
                date_pub = datetime.now(timezone.utc)

            resume = entree.get("summary", "") or entree.get("description", "")
            content_encoded = _extraire_content_rss(entree)

            meta = {
                "source": source,
                "titre": entree.get("title", "Sans titre").strip(),
                "url": url,
                "date_pub": date_pub,
                "resume_rss": _nettoyer_html(resume),
                "content_encoded": content_encoded,
            }

            if content_encoded and len(content_encoded) > 300:
                corps = _nettoyer_html(content_encoded)
                rss_direct += 1
                yield self._build_item(source, meta, corps)
            else:
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_texte,
                    meta=meta,
                    errback=self._handle_error,
                    dont_filter=True,
                )
        logger.info(" %s : %d articles insérés directement depuis RSS", source["nom"], rss_direct)

    def _yield_video(self, source, flux):
        for entree in flux.entries:
            url = entree.get("link", "").strip()
            if not url or url in self.seen_urls:
                continue
            self.seen_urls.add(url)

            date_pub = None
            if hasattr(entree, "published_parsed") and entree.published_parsed:
                date_pub = datetime(*entree.published_parsed[:6], tzinfo=timezone.utc)
            elif hasattr(entree, "updated_parsed") and entree.updated_parsed:
                date_pub = datetime(*entree.updated_parsed[:6], tzinfo=timezone.utc)
            else:
                date_pub = datetime.now(timezone.utc)

            meta = {
                "source": source,
                "titre": entree.get("title", "Sans titre").strip(),
                "url": url,
                "date_pub": date_pub,
                "resume_rss": _nettoyer_html(entree.get("summary", "")),
                "entree_brute": dict(entree),
            }
            yield scrapy.Request(
                url=url,
                callback=self.parse_rtb,
                meta=meta,
                errback=self._handle_error,
                dont_filter=True,
            )

    def _handle_error(self, failure):
        logger.error(" Échec requête %s : %s", failure.request.url, failure.value)

    # ── Fallback AIB ────────────────────────────────────────────────────

    def parse_homepage_aib(self, response):
        source = response.meta["source"]
        articles_trouves = 0

        selecteurs_aib = [
            "h3.entry-title.td-module-title a",
            ".td-module-title a",
            ".td-block-span4 .entry-title a",
            ".td_module_wrap .entry-title a",
            ".td-animation-stack .entry-title a",
        ]

        liens_vus = set()
        for css in selecteurs_aib:
            for lien in response.css(css):
                href = lien.css("::attr(href)").get("")
                titre = lien.css("::attr(title)").get("") or lien.css("::text").get("")
                if not href:
                    continue
                href = urljoin(response.url, href).strip()
                if href in liens_vus or href in self.seen_urls:
                    continue
                liens_vus.add(href)
                self.seen_urls.add(href)

                titre = titre.strip()
                if not titre:
                    titre = "Article AIB"

                meta = {
                    "source": source,
                    "titre": titre,
                    "url": href,
                    "date_pub": datetime.now(timezone.utc),
                    "resume_rss": "",
                    "content_encoded": "",
                }

                yield scrapy.Request(
                    url=href,
                    callback=self.parse_texte,
                    meta=meta,
                    errback=self._handle_error,
                    dont_filter=True,
                )
                articles_trouves += 1

        if articles_trouves == 0:
            logger.warning("  AIB : sélecteurs vides, tentative liens ?p=...")
            for href in response.css("a[href*='?p=']::attr(href)").getall():
                href = urljoin(response.url, href).strip()
                if href in self.seen_urls:
                    continue
                self.seen_urls.add(href)

                meta = {
                    "source": source,
                    "titre": "Article AIB",
                    "url": href,
                    "date_pub": datetime.now(timezone.utc),
                    "resume_rss": "",
                    "content_encoded": "",
                }
                yield scrapy.Request(
                    url=href,
                    callback=self.parse_texte,
                    meta=meta,
                    errback=self._handle_error,
                    dont_filter=True,
                )
                articles_trouves += 1
                if articles_trouves >= 20:
                    break

        logger.info(" AIB (fallback HTML) : %d articles à scraper", articles_trouves)

    # ── Parsing textes ──────────────────────────────────────────────────

    def parse_texte(self, response):
        meta = response.meta
        source = meta["source"]
        domaine = source["domaine"]

        if domaine == "aib.media":
            url_video = self._detecter_video_aib(response)
            if url_video:
                logger.info(" AIB vidéo détectée : %s", url_video)
                yield from self._traiter_video(source, meta, url_video)
                return

        corps = self._extraire_texte_scrapy(response, domaine)
        if not corps or len(corps) < 200:
            corps = self._extraire_generique(response)
        if not corps or len(corps) < 200:
            corps = meta.get("resume_rss", "")
            if not corps:
                logger.warning("  [%s] Corps vide : %s", source["nom"], meta["url"])

        item = self._build_item(source, meta, corps)
        yield item

    def _extraire_texte_scrapy(self, response, domaine: str) -> str:
        selecteurs = {
            "aib.media": [
                "article .entry-content", ".td-post-content", ".tdb-block-inner",
                ".td_block_wrap .tdb-block-inner", ".post-content", "article", ".td-main-content",
            ],
            "sidwaya.info": [".entry-content", ".td-post-content", ".post-content", "article"],
            "lefaso.net": [
                ".texte", "#article .texte", ".contenu-principal .texte",
                "#contenu .texte", ".spip_texte", "#spip_article .spip_texte", "article",
            ],
            "burkina24.com": [".entry-content", ".td-post-content", ".post-content", "article"],
        }.get(domaine, [])

        selecteurs += [
            ".entry-content", ".post-content", ".article-content",
            ".content-area article", "main article", "#content article", ".texte",
        ]

        for css in selecteurs:
            noeud = response.css(css)
            if not noeud:
                continue
            textes = noeud.css("p, h2, h3, h4, li, div[dir='auto'] ::text").getall()
            if not textes:
                textes = noeud.css("::text").getall()
            texte = " ".join(t.strip() for t in textes if t.strip())
            texte = re.sub(r"\s+", " ", texte).strip()
            
            # ═══ AJOUT : Nettoyer les résidus HTML ═══
            texte = _nettoyer_html(texte)
            
            if len(texte) > 200:
                return texte
        return ""

    def _extraire_generique(self, response) -> str:
        for cible in ["article", "main", ".content", "#content", "body"]:
            textes = response.css(f"{cible} p ::text").getall()
            texte = " ".join(t.strip() for t in textes if t.strip())
            if len(texte) > 100:
                return _nettoyer_html(texte)
            
            textes = response.css(f'{cible} div[dir="auto"] ::text').getall()
            texte = " ".join(t.strip() for t in textes if t.strip())
            if len(texte) > 100:
                return _nettoyer_html(texte)
        return ""

    # ── Détection vidéo AIB ─────────────────────────────────────────────

    def _detecter_video_aib(self, response) -> str | None:
        for src in response.css("iframe::attr(src)").getall():
            if "youtube.com" in src or "youtu.be" in src:
                return _normaliser_url_youtube(src)

        for src in response.css("video::attr(src), video source::attr(src)").getall():
            if src:
                return urljoin(response.url, src)
        for src in response.css("audio::attr(src), audio source::attr(src)").getall():
            if src:
                return urljoin(response.url, src)

        pattern_media = r'(https?://[^"\']+\.(?:mp4|m4a|mp3|wav|ogg|webm))'
        match = re.search(pattern_media, response.text)
        if match:
            return match.group(1)

        pattern_yt = r"(https?://(?:www\.)?(?:youtube\.com/watch\?v=|youtu\.be/)[\w\-]{11})"
        match = re.search(pattern_yt, response.text)
        if match:
            return match.group(1)

        match = re.search(pattern_yt, response.meta.get("resume_rss", ""))
        if match:
            return match.group(1)

        return None

    # ── Traitement vidéo intelligent ────────────────────────────────────

    def _traiter_video(self, source, meta, url_media):
        transcription = ""
        methode = ""

        try:
            if "youtube.com" in url_media or "youtu.be" in url_media:
                logger.info(" YouTube détecté : %s", url_media)

                # Pour RTB : téléchargement direct + Qwen 3.5
                if source["nom"] == "RTB":
                    logger.info(" RTB détecté — téléchargement audio + Qwen 3.5")
                    transcription = self._transcrire_rtb_via_qwen(url_media)
                    methode = "rtb_qwen35" if transcription else "failed"
                else:
                    # Pour AIB et autres : marquer comme non traité pour l'instant
                    logger.info("  Source %s — vidéo ignorée (focus RTB)", source["nom"])
                    transcription = meta.get("resume_rss", "")
                    methode = "ignored_non_rtb"

            else:
                logger.info("  Fichier direct : %s", url_media)
                transcription = self._transcrire_via_audio_direct(url_media)
                methode = "direct_whisper" if transcription else "failed"

        except Exception as e:
            logger.error(" Erreur traitement vidéo %s : %s", meta["url"], e)
            methode = "error"

        corps_final = transcription or meta.get("resume_rss", "") or f"[Vidéo non transcrite - URL: {url_media}]"

        item = self._build_item(source, meta, corps_final)
        item["url_media"] = url_media
        item["type_contenu"] = "video"
        item["methode_transcription"] = methode
        item["statut_video"] = "transcrite" if transcription else "non_disponible"

        logger.info(" [%s] Vidéo '%s' → %d caractères", source["nom"], methode, len(transcription or ""))
        yield item

    def _transcrire_rtb_via_qwen(self, url_video: str) -> str:
        chemin_audio = None
        transcription = ""
    
        try:
            logger.info("  Téléchargement audio RTB...")
            chemin_audio = self._telecharger_audio_youtube(url_video)
            if not chemin_audio:
                logger.error(" Échec téléchargement audio RTB")
                return ""
        
            logger.info(" Audio téléchargé : %s", chemin_audio)

            base_url = os.getenv("QWEN_BASE_URL", "http://localhost:11434/v1")
            model = os.getenv("QWEN_MODEL", "qwen3.5:8b")
            ollama_ok = False

            try:
                r = req_lib.get(f"{base_url}/models", timeout=5)
                r.raise_for_status()
                ollama_ok = True
                logger.info(" Ollama/Qwen 3.5 disponible")
            except Exception as e:
                logger.warning("  Ollama non disponible : %s — tentative Whisper local...", e)

            if ollama_ok:
                try:
                    with open(chemin_audio, "rb") as f:
                        files = {"file": f}
                        data = {
                            "model": model,
                            "language": "fr",
                            "response_format": "text",
                            "prompt": "Transcris ce journal télévisé en français."
                        }
                        r = req_lib.post(
                            f"{base_url}/audio/transcriptions",
                            files=files, data=data, timeout=300,
                        )
                        r.raise_for_status()
                        result = r.json()
                        texte = result.get("text", "") if isinstance(result, dict) else str(result)
                        if texte and len(texte) > 50:
                            logger.info(" Transcription Qwen 3.5 OK (%d caractères)", len(texte))
                            transcription = texte.strip()
                except Exception as e:
                    logger.error(" Erreur Ollama transcription : %s", e)

            if not transcription:
                logger.info(" Fallback faster-whisper local...")
                try:
                    from faster_whisper import WhisperModel
                    modele = WhisperModel("base", device="cpu", compute_type="int8")
                    segments, _ = modele.transcribe(chemin_audio, language="fr", beam_size=5)
                    texte = " ".join(seg.text.strip() for seg in segments)
                    if texte:
                        logger.info("✅ Whisper OK (%d caractères)", len(texte))
                        transcription = texte.strip()
                except Exception as e:
                    logger.error("❌ Échec faster-whisper : %s", e)

            return transcription

        except Exception as e:
            logger.error("❌ Erreur pipeline RTB/Qwen : %s", e)
            return ""

        finally:
            self._cleanup_temp(chemin_audio, garder_si_echec=(not transcription))
    # ── Méthodes de transcription ───────────────────────────────────────

    def _extraire_youtube_id(self, url: str) -> str | None:
        patterns = [
            r"(?:v=|\/)([\w-]{11}).*",
            r"(?:embed\/)([\w-]{11})",
            r"(?:youtu\.be\/)([\w-]{11})",
        ]
        for p in patterns:
            match = re.search(p, url)
            if match:
                return match.group(1)
        return None

    def _transcrire_via_youtube_api(self, url_video: str) -> str:
        """Compatible youtube-transcript-api v1.x."""
        try:
            video_id = self._extraire_youtube_id(url_video)
            if not video_id:
                return ""

            logger.info("🔍 Sous-titres YouTube pour %s...", video_id)

            from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound

            ytt_api = YouTubeTranscriptApi()

            try:
                transcript = ytt_api.fetch(video_id, languages=['fr'])
                logger.info(" Sous-titres FR trouvés")
            except NoTranscriptFound:
                transcript = ytt_api.fetch(video_id, languages=['en'])
                logger.info(" Sous-titres EN trouvés (fallback)")

            texte = " ".join([snippet.text for snippet in transcript])
            texte = re.sub(r"\s+", " ", texte).strip()

            if len(texte) > 50:
                logger.info(" YouTube API OK (%d caractères)", len(texte))
                return texte
            return ""

        except TranscriptsDisabled:
            logger.warning("  Sous-titres désactivés sur cette vidéo")
            return ""
        except NoTranscriptFound:
            logger.warning("  Aucun sous-titre trouvé (FR ou EN)")
            return ""
        except Exception as e:
            if "429" in str(e) or "Too Many Requests" in str(e) or "IP" in str(e):
                logger.warning("  YouTube rate limit / IP bloquée")
            else:
                logger.warning("⚠️  YouTube API échec : %s", e)
            return ""

    def _transcrire_via_ytdlp(self, url_video: str) -> str:
        chemin_audio = None
        transcription = ""
        try:
            chemin_audio = self._telecharger_audio_youtube(url_video)
            if not chemin_audio:
                return ""
            transcription = self._transcrire_audio(chemin_audio)
            return transcription
        finally:
            if not transcription:  # Si échec, garder le fichier
                self._cleanup_temp(chemin_audio, garder_si_echec=True)
            else:  # Si succès, supprimer
                self._cleanup_temp(chemin_audio, garder_si_echec=False)

    def _transcrire_via_audio_direct(self, url_media: str) -> str:
        chemin_audio = None
        try:
            chemin_audio = self._telecharger_audio_direct(url_media)
            if not chemin_audio:
                return ""
            return self._transcrire_audio(chemin_audio)
        finally:
            self._cleanup_temp(chemin_audio)

    def _cleanup_temp(self, chemin_audio, garder_si_echec=False):
        if not chemin_audio or not os.path.exists(chemin_audio):
            return
    
        if garder_si_echec:
            # Sauvegarde dans un dossier permanent
            dossier_save = os.path.expanduser("~/Desktop/audios_rtb")
            os.makedirs(dossier_save, exist_ok=True)
            nom = os.path.basename(chemin_audio)
            nouveau = os.path.join(dossier_save, f"{int(time.time())}_{nom}")
            os.rename(chemin_audio, nouveau)
            logger.info(" Audio sauvegardé : %s", nouveau)
        else:
            # Suppression normale
            try:
                os.remove(chemin_audio)
                dossier = os.path.dirname(chemin_audio)
                if os.path.isdir(dossier) and not os.listdir(dossier):
                    os.rmdir(dossier)
            except Exception:
             pass

    # ── Parsing RTB ─────────────────────────────────────────────────────

    def parse_rtb(self, response):
        meta = response.meta
        source = meta["source"]
        url_media = None

        try:
            for src in response.css("iframe::attr(src)").getall():
                if "youtube.com" in src or "youtu.be" in src:
                    url_media = _normaliser_url_youtube(src)
                    break

            if not url_media:
                for src in response.css("video source::attr(src), audio source::attr(src)").getall():
                    if src:
                        url_media = src
                        break

            if not url_media:
                pattern = r"(https?://(?:www\.)?(?:youtube\.com/watch\?v=|youtu\.be/)[\w\-]{11})"
                match = re.search(pattern, response.text)
                if match:
                    url_media = match.group(1)

            entree = meta.get("entree_brute", {})
            if not url_media and entree:
                for enc in entree.get("enclosures", []):
                    if enc.get("type", "").startswith(("audio/", "video/")):
                        url_media = enc.get("href")
                        break
                if not url_media:
                    for m in entree.get("media_content", []):
                        if m.get("url"):
                            url_media = m["url"]
                            break

        except Exception as e:
            logger.error(" Erreur détection média RTB %s : %s", meta["url"], e)

        if url_media:
            yield from self._traiter_video(source, meta, url_media)
        else:
            item = self._build_item(source, meta, meta.get("resume_rss", ""))
            item["url_media"] = None
            item["type_contenu"] = "video"
            item["methode_transcription"] = "no_media_found"
            item["statut_video"] = "non_disponible"
            yield item

    # ── Téléchargement audio ────────────────────────────────────────────

    def _telecharger_audio_youtube(self, url_video: str) -> str | None:
        try:
            import yt_dlp
            dossier_temp = tempfile.mkdtemp(prefix="burk_audio_")
            options = {
                "format": "bestaudio[ext=m4a]/bestaudio[ext=webm]/bestaudio",
                "outtmpl": os.path.join(dossier_temp, "%(id)s.%(ext)s"),
                "postprocessors": [],
                "quiet": True,
                "no_warnings": True,
            }
            with yt_dlp.YoutubeDL(options) as ydl:
                info = ydl.extract_info(url_video, download=True)
                vid = info.get("id", "audio")
                ext = info.get("ext", "m4a")
                chemin = os.path.join(dossier_temp, f"{vid}.{ext}")
                if os.path.exists(chemin):
                    return chemin
                for f in os.listdir(dossier_temp):
                    if f.startswith(vid):
                        return os.path.join(dossier_temp, f)
        except Exception as e:
            logger.error(" Échec yt-dlp (%s) : %s", url_video, e)
        return None

    def _telecharger_audio_direct(self, url_audio: str) -> str | None:
        try:
            ext = os.path.splitext(urlparse(url_audio).path)[-1] or ".mp3"
            ftmp = tempfile.NamedTemporaryFile(delete=False, suffix=ext, prefix="burk_audio_")
            with req_lib.get(url_audio, headers=HEADERS, stream=True, timeout=60) as r:
                r.raise_for_status()
                for chunk in r.iter_content(chunk_size=8192):
                    ftmp.write(chunk)
            ftmp.close()
            return ftmp.name
        except Exception as e:
            logger.error(" Échec téléchargement direct (%s) : %s", url_audio, e)
        return None

    def _transcrire_audio(self, chemin_audio: str) -> str:
        # Essai 1 : Ollama via requests direct
        try:
            base_url = os.getenv("QWEN_BASE_URL", "http://localhost:11434/v1")
            model = os.getenv("QWEN_MODEL", "qwen2.5:7b")

            with open(chemin_audio, "rb") as f:
                files = {"file": f}
                data = {"model": model, "language": "fr", "response_format": "text"}
                r = req_lib.post(
                    f"{base_url}/audio/transcriptions",
                    files=files,
                    data=data,
                    timeout=300,
                )
                r.raise_for_status()
                result = r.json()
                texte = result.get("text", "") if isinstance(result, dict) else str(result)
                if texte:
                    logger.info(" Transcription Ollama OK (%d caractères)", len(texte))
                    return texte.strip()
        except Exception as e:
            logger.warning("  Ollama indisponible : %s", e)

        # Essai 2 : whisper-ctranslate2
        try:
            from faster_whisper import WhisperModel 
            logger.info("🔁 Fallback whisper-ctranslate2…")
            modele = WhisperModel("base", device="cpu", compute_type="int8")
            segments, info = modele.transcribe(chemin_audio, language="fr", beam_size=5)
            texte = " ".join(seg.text.strip() for seg in segments)
            logger.info(" Transcription Whisper OK (%d caractères)", len(texte))
            return texte.strip()
        except ImportError:
            logger.warning("  whisper_ctranslate2 non installé (pip install whisper-ctranslate2)")
        except Exception as e:
            logger.error(" Échec whisper-ctranslate2 : %s", e)

        return ""

    # ── Item builder ─────────────────────────────────────────────────────

    def _build_item(self, source, meta, corps: str) -> dict:
        url = meta["url"]
        return {
            "source":             source["nom"],
            "domaine":            source["domaine"],
            "type_source":        source["type"],
            "titre":              meta["titre"],
            "url":                url,
            "url_hash":           hashlib.md5(url.encode()).hexdigest(),
            "date_publication":   meta["date_pub"],
            "resume_rss":         meta.get("resume_rss", ""),
            "corps":              corps,
            "date_scraping":      datetime.now(timezone.utc),
            "statut_paraphrase":  "en_attente",
            "statut_resume":      "en_attente",
            "langue":             "fr",
        }


# ═══════════════════════════════════════════════════════════════════════════
# LANCEMENT
# ═══════════════════════════════════════════════════════════════════════════

def run_single_cycle():
    debut = datetime.now()
    logger.info(" Début cycle — %s", debut.strftime("%Y-%m-%d %H:%M:%S"))
    process = CrawlerProcess()
    process.crawl(BurkinaNewsSpider)
    process.start()
    duree = (datetime.now() - debut).total_seconds()
    logger.info(" Cycle terminé en %.1f secondes", duree)


def main_scheduler():
    multiprocessing.set_start_method("spawn", force=True)
    logger.info("⏱  Premier cycle immédiat…")
    p = multiprocessing.Process(target=run_single_cycle)
    p.start()
    p.join()
    while True:
        logger.info(" Attente 1 heure…")
        time.sleep(3600)
        p = multiprocessing.Process(target=run_single_cycle)
        p.start()
        p.join()


if __name__ == "__main__":
    main_scheduler()