#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scraper intelligent de l'actualité burkinabè — VERSION SCRAPY v5
Correction finale : AIB (sans Accept-Encoding) + tous les sites fonctionnent
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

# Headers SANS Accept-Encoding (évite la compression binaire sur AIB)
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
    if not html_brut:
        return ""
    texte = re.sub(r"<[^>]+>", " ", html_brut)
    entites = {
        "&amp;": "&", "&lt;": "<", "&gt;": ">", "&quot;": '"',
        "&#39;": "'", "&nbsp;": " ", "&agrave;": "à", "&eacute;": "é",
        "&egrave;": "è", "&ecirc;": "ê", "&ocirc;": "ô", "&ucirc;": "û",
        "&rsquo;": "'", "&lsquo;": "'", "&rdquo;": '"', "&ldquo;": '"',
        "&#8230;": "…", "&hellip;": "…",
    }
    for entite, char in entites.items():
        texte = texte.replace(entite, char)
    texte = re.sub(r"\s+", " ", texte).strip()
    return texte


def _normaliser_url_youtube(url: str) -> str:
    match = re.search(r"(?:embed/|v=|youtu\.be/)([\w\-]{11})", url)
    if match:
        return f"https://www.youtube.com/watch?v={match.group(1)}"
    return url


def _extraire_content_rss(entree) -> str:
    """Extraction robuste du contenu RSS."""
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
    """Télécharge le flux RSS avec requests SANS compression, puis parse."""
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
            logger.info("✅ Connexion MongoDB — base : %s", MONGO_DB)
        except Exception as e:
            logger.critical("❌ MongoDB indisponible : %s", e)
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
                logger.info("💾 [%s] Nouvel article : %s", item["source"], item.get("titre", "?")[:70])
            else:
                logger.debug("⏭️  [%s] Déjà existant : %s", item["source"], item.get("url", "?")[:60])
        except mongo_errors.DuplicateKeyError:
            pass
        except Exception as e:
            logger.error("❌ Erreur MongoDB : %s", e)
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
            # PAS de Accept-Encoding ici (évite compression binaire)
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
                logger.info("📡 %s — %d articles via RSS (%s)", source["nom"], len(flux.entries), url_ok)
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
                    logger.error("❌ %s : aucun flux accessible et pas de fallback", source["nom"])

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
        logger.info("📰 %s : %d articles insérés directement depuis RSS", source["nom"], rss_direct)

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
        logger.error("❌ Échec requête %s : %s", failure.request.url, failure.value)

    # ── Fallback AIB : scraping de la page d'accueil ────────────────────

    def parse_homepage_aib(self, response):
        """Extrait les articles de la page d'accueil AIB quand le RSS est bloqué."""
        source = response.meta["source"]
        articles_trouves = 0

        # Sélecteurs spécifiques AIB (thème Newspaper / tagDiv)
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

        # Fallback : liens contenant ?p= (format WordPress AIB)
        if articles_trouves == 0:
            logger.warning("⚠️  AIB : sélecteurs td-module-title vides, tentative liens ?p=...")
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

        logger.info("📰 AIB (fallback HTML) : %d articles à scraper", articles_trouves)

    # ── Parsing textes ───────────────────────────────────────────────────

    def parse_texte(self, response):
        meta = response.meta
        source = meta["source"]
        domaine = source["domaine"]

        corps = self._extraire_texte_scrapy(response, domaine)
        if not corps or len(corps) < 200:
            corps = self._extraire_generique(response)
        if not corps or len(corps) < 200:
            corps = meta.get("resume_rss", "")
            if not corps:
                logger.warning("⚠️  [%s] Corps vide : %s", source["nom"], meta["url"])

        item = self._build_item(source, meta, corps)
        yield item

    def _extraire_texte_scrapy(self, response, domaine: str) -> str:
        selecteurs = {
            "aib.media": [
                "article .entry-content",
                ".td-post-content",
                ".tdb-block-inner",
                ".td_block_wrap .tdb-block-inner",
                ".post-content",
                "article",
                ".td-main-content",
            ],
            "sidwaya.info": [
                ".entry-content", ".td-post-content", ".post-content", "article",
            ],
            "lefaso.net": [
                ".texte", "#article .texte", ".contenu-principal .texte",
                "#contenu .texte", ".spip_texte", "#spip_article .spip_texte", "article",
            ],
            "burkina24.com": [
                ".entry-content", ".td-post-content", ".post-content", "article",
            ],
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
            if len(texte) > 200:
                return texte
        return ""

    def _extraire_generique(self, response) -> str:
        for cible in ["article", "main", ".content", "#content", "body"]:
            textes = response.css(f"{cible} p ::text").getall()
            texte = " ".join(t.strip() for t in textes if t.strip())
            if len(texte) > 100:
                return texte
            textes = response.css(f'{cible} div[dir="auto"] ::text').getall()
            texte = " ".join(t.strip() for t in textes if t.strip())
            if len(texte) > 100:
                return texte
        return ""

    # ── Parsing RTB ──────────────────────────────────────────────────────

    def parse_rtb(self, response):
        meta = response.meta
        source = meta["source"]
        url_media = None
        transcription = ""
        chemin_audio = None

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

            if url_media:
                if "youtube.com" in url_media or "youtu.be" in url_media:
                    chemin_audio = self._telecharger_audio_youtube(url_media)
                else:
                    chemin_audio = self._telecharger_audio_direct(url_media)
                if chemin_audio:
                    transcription = self._transcrire_audio(chemin_audio)
            else:
                transcription = meta.get("resume_rss", "")

        except Exception as e:
            logger.error("❌ Erreur RTB %s : %s", meta["url"], e)
            transcription = meta.get("resume_rss", "")
        finally:
            if chemin_audio and os.path.exists(chemin_audio):
                try:
                    os.remove(chemin_audio)
                    dossier = os.path.dirname(chemin_audio)
                    if os.path.isdir(dossier) and not os.listdir(dossier):
                        os.rmdir(dossier)
                except Exception:
                    pass

        item = self._build_item(source, meta, transcription or meta.get("resume_rss", ""))
        item["url_media"] = url_media
        yield item

    # ── Audio / transcription ────────────────────────────────────────────

    def _telecharger_audio_youtube(self, url_video: str) -> str | None:
        try:
            import yt_dlp
            dossier_temp = tempfile.mkdtemp(prefix="rtb_audio_")
            options = {
                "format": "bestaudio[ext=m4a]/bestaudio[ext=webm]/bestaudio",
                "outtmpl": os.path.join(dossier_temp, "%(id)s.%(ext)s"),
                "postprocessors": [],  # SANS FFmpeg
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
            logger.error("❌ Échec yt-dlp (%s) : %s", url_video, e)
        return None

    def _telecharger_audio_direct(self, url_audio: str) -> str | None:
        try:
            ext = os.path.splitext(urlparse(url_audio).path)[-1] or ".mp3"
            ftmp = tempfile.NamedTemporaryFile(delete=False, suffix=ext, prefix="rtb_audio_")
            with req_lib.get(url_audio, headers=HEADERS, stream=True, timeout=60) as r:
                r.raise_for_status()
                for chunk in r.iter_content(chunk_size=8192):
                    ftmp.write(chunk)
            ftmp.close()
            return ftmp.name
        except Exception as e:
            logger.error("❌ Échec téléchargement direct (%s) : %s", url_audio, e)
        return None

    def _transcrire_audio(self, chemin_audio: str) -> str:
        try:
            from openai import OpenAI
            base_url = os.getenv("QWEN_BASE_URL", "http://localhost:11434/v1")
            model = os.getenv("QWEN_MODEL", "qwen2.5:7b")
            client = OpenAI(api_key="ollama", base_url=base_url)
            with open(chemin_audio, "rb") as f:
                tr = client.audio.transcriptions.create(
                    model=model, file=f, language="fr", response_format="text"
                )
            texte = tr if isinstance(tr, str) else tr.text
            logger.info("✅ Transcription Qwen OK (%d caractères)", len(texte))
            return texte.strip()
        except Exception as e:
            logger.warning("⚠️  Qwen indisponible : %s", e)

        try:
            from whisper_ctranslate2 import WhisperModel
            logger.info("🔁 Fallback whisper-ctranslate2…")
            modele = WhisperModel("base", device="cpu", compute_type="int8")
            segments, info = modele.transcribe(chemin_audio, language="fr", beam_size=5)
            texte = " ".join(seg.text.strip() for seg in segments)
            logger.info("✅ Transcription Whisper OK (%d caractères)", len(texte))
            return texte.strip()
        except Exception as e:
            logger.error("❌ Échec transcription : %s", e)
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
    logger.info("🚀 Début cycle — %s", debut.strftime("%Y-%m-%d %H:%M:%S"))
    process = CrawlerProcess()
    process.crawl(BurkinaNewsSpider)
    process.start()
    duree = (datetime.now() - debut).total_seconds()
    logger.info("🏁 Cycle terminé en %.1f secondes", duree)


def main_scheduler():
    multiprocessing.set_start_method("spawn", force=True)
    logger.info("⏱️  Premier cycle immédiat…")
    p = multiprocessing.Process(target=run_single_cycle)
    p.start()
    p.join()
    while True:
        logger.info("😴 Attente 1 heure…")
        time.sleep(3600)
        p = multiprocessing.Process(target=run_single_cycle)
        p.start()
        p.join()


if __name__ == "__main__":
    main_scheduler()