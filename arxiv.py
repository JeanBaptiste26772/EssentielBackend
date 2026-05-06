import urllib.request
import urllib.parse
import json
import time
import os

# ============================================================
# CONFIGURATION — Modifie uniquement cette section
# ============================================================

EQUATIONS = {
    "eq1_summarization": "automatic summarization abstractive summarization text summarization news French francophone",
    "eq2_paraphrase":    "paraphrase generation text paraphrasing transformer GPT language model",
    "eq3_moore":         "machine translation Moore low-resource African languages Burkina Faso",
    "eq4_scraping":      "web scraping data extraction news press Python BeautifulSoup Scrapy",
    "eq5_ner":           "named entity recognition sentiment analysis news NLP natural language processing",
    "eq6_tts":           "text-to-speech TTS African languages low-resource",
}

MAX_RESULTS   = 500
OUTPUT_FOLDER = "SLR_BibTeX_SemanticScholar"

# ============================================================

API_BASE = "https://api.semanticscholar.org/graph/v1/paper/search"
FIELDS   = "title,authors,year,abstract,externalIds,venue,publicationTypes"

def sanitize(text):
    if not text:
        return ""
    return text.replace("{", "").replace("}", "").replace("\\", "").replace("\n", " ").strip()

def to_bibtex(paper):
    title    = sanitize(paper.get("title", "No title"))
    authors  = [sanitize(a.get("name", "")) for a in paper.get("authors", [])]
    year     = str(paper.get("year", "0000") or "0000")
    venue    = sanitize(paper.get("venue", "") or "")
    abstract = sanitize(paper.get("abstract", "") or "")[:300]

    ext_ids  = paper.get("externalIds", {}) or {}
    doi      = ext_ids.get("DOI", "")
    arxiv_id = ext_ids.get("ArXiv", "")
    paper_id = paper.get("paperId", "unknown")

    pub_types = paper.get("publicationTypes", []) or []
    bib_type  = "inproceedings" if any("Conference" in t for t in pub_types) else "article"

    first_author = authors[0].split()[-1].lower() if authors else "unknown"
    cite_key     = f"{first_author}{year}_{paper_id[:8]}"

    bib  = f"@{bib_type}{{{cite_key},\n"
    bib += f"  title     = {{{title}}},\n"
    if authors:
        bib += f"  author    = {{{' and '.join(authors)}}},\n"
    bib += f"  year      = {{{year}}},\n"
    if venue:
        key = "booktitle" if bib_type == "inproceedings" else "journal"
        bib += f"  {key:<9} = {{{venue}}},\n"
    if doi:
        bib += f"  doi       = {{{doi}}},\n"
        bib += f"  url       = {{https://doi.org/{doi}}},\n"
    elif arxiv_id:
        bib += f"  url       = {{https://arxiv.org/abs/{arxiv_id}}},\n"
        bib += f"  note      = {{arXiv:{arxiv_id}}},\n"
    if abstract:
        bib += f"  abstract  = {{{abstract}...}},\n"
    bib += "}\n"
    return bib

def fetch_semantic_scholar(query, max_results=500):
    all_papers = []
    offset     = 0
    batch_size = 100

    print(f"  Recherche : {query[:60]}...")

    while len(all_papers) < max_results:
        params = {
            "query":  query,
            "fields": FIELDS,
            "limit":  min(batch_size, max_results - len(all_papers)),
            "offset": offset,
        }

        url     = f"{API_BASE}?{urllib.parse.urlencode(params)}"
        request = urllib.request.Request(url, headers={
            "User-Agent": "SLR-Research-Tool/1.0",
            "Accept":     "application/json",
        })

        try:
            with urllib.request.urlopen(request, timeout=30) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 429:
                print("  Rate limit atteint, attente 15 secondes...")
                time.sleep(15)
                continue
            else:
                print(f"  Erreur HTTP {e.code}: {e.reason}")
                break
        except Exception as e:
            print(f"  Erreur reseau : {e}")
            break

        papers = data.get("data", [])
        total  = data.get("total", 0)

        if not papers:
            break

        all_papers.extend(papers)
        print(f"  -> {len(all_papers)} / {min(max_results, total)} articles recuperes...")

        offset += batch_size

        if offset >= total or offset >= max_results:
            break

        time.sleep(2)

    return all_papers[:max_results]

def main():
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    total_global = 0

    print("=" * 60)
    print("  SLR Semantic Scholar BibTeX Exporter v2")
    print("  (Couvre : arXiv, PubMed, IEEE, ACM...)")
    print("=" * 60)

    for name, query in EQUATIONS.items():
        print(f"\n[{name}]")
        papers = fetch_semantic_scholar(query, MAX_RESULTS)

        if not papers:
            print(f"  Aucun resultat trouve.")
            continue

        bib_content  = f"% Equation : {name}\n"
        bib_content += f"% Requete  : {query}\n"
        bib_content += f"% Resultats: {len(papers)} articles\n\n"
        bib_content += "\n".join(to_bibtex(p) for p in papers)

        filename = os.path.join(OUTPUT_FOLDER, f"SS_{name}.bib")
        with open(filename, "w", encoding="utf-8") as f:
            f.write(bib_content)

        print(f"  Sauvegarde : {filename} ({len(papers)} articles)")
        total_global += len(papers)

    print("\n" + "=" * 60)
    print(f"  TERMINE ! {total_global} articles exportes au total")
    print(f"  Fichiers dans le dossier : {OUTPUT_FOLDER}/")
    print("  Importe chaque fichier .bib dans Zotero !")
    print("=" * 60)

if __name__ == "__main__":
    main()