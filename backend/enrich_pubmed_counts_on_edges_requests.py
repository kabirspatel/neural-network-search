import csv
import os
import time
import requests
import xml.etree.ElementTree as ET

INPUT_CSV = "data/biomarker_disease_edges.csv"
OUTPUT_CSV = "data/biomarker_disease_edges_pubmed.csv"

NCBI_EMAIL = os.environ.get("NCBI_EMAIL")
NCBI_API_KEY = os.environ.get("NCBI_API_KEY")
NCBI_TOOL = os.environ.get("NCBI_TOOL", "urine_biomarker_project")

BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"

if not NCBI_EMAIL:
    raise SystemExit("NCBI_EMAIL env var is required for PubMed (Entrez)")

def build_term(biomarker_name: str, disease_name: str) -> str:
    """
    Build a safe PubMed term.
    We use [tiab] (title/abstract) to keep it focused and avoid super long queries.
    """
    biomarker_name = biomarker_name.strip()
    disease_name = disease_name.strip()
    return f'{biomarker_name}[tiab] AND {disease_name}[tiab]'

def query_pubmed(term: str) -> int:
    params = {
        "db": "pubmed",
        "term": term,
        "retmode": "xml",
        "tool": NCBI_TOOL,
        "email": NCBI_EMAIL,
    }
    if NCBI_API_KEY:
        params["api_key"] = NCBI_API_KEY

    try:
        r = requests.get(BASE_URL, params=params, timeout=20)
        if r.status_code != 200:
            print(f"[WARN] HTTP {r.status_code} for term {term!r}")
            return 0

        root = ET.fromstring(r.text)
        count_text = root.findtext("Count", default="0")
        return int(count_text)
    except Exception as e:
        print(f"[WARN] Exception for term {term!r}: {e}")
        return 0

def main():
    print(f"[INFO] Loading edges from {INPUT_CSV} ...")
    with open(INPUT_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        edges = list(reader)

    print(f"[INFO] Loaded {len(edges)} biomarker–disease edges.")

    # Build unique (biomarker_name, disease_name) pairs
    unique_pairs = {}
    for row in edges:
        key = (row["biomarker_name"], row["disease_name"])
        if key not in unique_pairs:
            unique_pairs[key] = None

    print(f"[INFO] Found {len(unique_pairs)} unique biomarker–disease pairs.")

    # Query PubMed for each unique pair
    for i, (bk, dk) in enumerate(unique_pairs.keys(), start=1):
        term = build_term(bk, dk)
        print(f"[INFO] [{i}/{len(unique_pairs)}] Querying PubMed for: {term!r}")
        count = query_pubmed(term)
        unique_pairs[(bk, dk)] = (term, count)

        # Be nice to NCBI
        if NCBI_API_KEY:
            time.sleep(0.12)  # ~8–9 req/s
        else:
            time.sleep(0.35)  # ~3 req/s

    # Attach counts back onto each edge row
    for row in edges:
        key = (row["biomarker_name"], row["disease_name"])
        term, count = unique_pairs[key]
        row["pubmed_query"] = term
        row["pubmed_count"] = str(count)

    fieldnames = list(edges[0].keys())
    # Ensure columns exist, in a stable order
    for extra in ["pubmed_query", "pubmed_count"]:
        if extra not in fieldnames:
            fieldnames.append(extra)

    print(f"[INFO] Writing enriched edges to {OUTPUT_CSV} ...")
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(edges)

    print("[INFO] DONE. Enriched file written:", OUTPUT_CSV)

if __name__ == "__main__":
    main()
