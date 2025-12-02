import os
import csv
import time
import logging
from typing import List, Dict

from Bio import Entrez

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")

INPUT_EDGES = "data/biomarker_disease_edges.csv"
OUTPUT_EDGES = "data/biomarker_disease_edges_pubmed.csv"

# --- Configure Entrez (NCBI) -------------------------------------------------
NCBI_EMAIL = os.environ.get("NCBI_EMAIL") or os.environ.get("NCBI_EMAIL".upper())
NCBI_API_KEY = os.environ.get("NCBI_API_KEY")
NCBI_TOOL = os.environ.get("NCBI_TOOL", "urine_biomarker_project")

if not NCBI_EMAIL:
    raise RuntimeError("NCBI_EMAIL env var is required for NCBI access")

Entrez.email = NCBI_EMAIL
if NCBI_API_KEY:
    Entrez.api_key = NCBI_API_KEY
Entrez.tool = NCBI_TOOL


def fetch_pubmed_count(term: str) -> int:
    """Return PubMed hit count for a search term, or 0 on failure."""
    if not term:
        return 0

    try:
        handle = Entrez.esearch(db="pubmed", term=term, rettype="count", retmode="xml")
        record = Entrez.read(handle)
        handle.close()
        count_str = record.get("Count", "0")
        return int(count_str)
    except Exception as e:
        logging.warning("PubMed query failed for term %r: %s", term, e)
        return 0


def main() -> None:
    logging.info("Loading edges from %s ...", INPUT_EDGES)
    with open(INPUT_EDGES, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows: List[Dict[str, str]] = list(reader)

    logging.info("Found %d biomarker-disease edges", len(rows))
    if not rows:
        logging.error("No rows found in %s", INPUT_EDGES)
        return

    # Deduplicate queries so we don't hammer NCBI unnecessarily
    unique_terms = sorted({row.get("pubmed_query", "") for row in rows if row.get("pubmed_query")})
    logging.info("Unique PubMed queries: %d", len(unique_terms))

    term_to_count: Dict[str, int] = {}

    for i, term in enumerate(unique_terms, start=1):
        logging.info("[%d/%d] Querying PubMed for: %s", i, len(unique_terms), term)
        count = fetch_pubmed_count(term)
        term_to_count[term] = count
        # be nice to NCBI – even with an API key, don't hammer it
        time.sleep(0.35)

    # Attach counts back onto rows
    for row in rows:
        term = row.get("pubmed_query", "")
        count = term_to_count.get(term, 0)
        row["pubmed_count"] = str(count)

    # Write updated file
    logging.info("Writing enriched edges to %s ...", OUTPUT_EDGES)
    fieldnames = list(rows[0].keys())
    with open(OUTPUT_EDGES, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    logging.info("DONE. Wrote PubMed counts for %d edges.", len(rows))


if __name__ == "__main__":
    main()
