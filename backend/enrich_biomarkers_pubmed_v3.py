import os
import time
import logging
import argparse
from typing import List
import requests
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(message)s"
)

BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"

EMAIL = os.getenv("NCBI_EMAIL")
TOOL = os.getenv("NCBI_TOOL", "urine_biomarker_project")
API_KEY = os.getenv("NCBI_API_KEY")  # optional but recommended

if not EMAIL:
    raise RuntimeError("NCBI_EMAIL environment variable is required")

def pubmed_count(term: str, sleep_sec: float = 0.34) -> int:
    """
    Call PubMed ESearch safely and return the hit count for a term.
    Uses requests params so the term is correctly URL-encoded.
    """
    params = {
        "db": "pubmed",
        "term": term,
        "retmode": "json",
        "retmax": 0,
        "tool": TOOL,
        "email": EMAIL,
    }
    if API_KEY:
        params["api_key"] = API_KEY

    resp = requests.get(BASE_URL, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    count_str = data.get("esearchresult", {}).get("count", "0")
    try:
        count = int(count_str)
    except ValueError:
        count = 0

    # Be nice to NCBI (max ~3 req/s without key, ~10 req/s with key)
    time.sleep(sleep_sec)
    return count

def build_pairs(df: pd.DataFrame, max_rows: int) -> List[dict]:
    """
    Build biomarker–disease pairs from biomarker_matrix_full.csv.
    Assumes columns: biomarker_id, name, diseases
    where 'diseases' is a ';'-separated string.
    """
    records = []
    total_rows = min(len(df), max_rows)
    logging.info(f"Building PubMed queries for first {total_rows} biomarker rows")

    for i, row in enumerate(df.head(max_rows).itertuples(index=False), start=1):
        biomarker_id = getattr(row, "biomarker_id")
        biomarker_name = getattr(row, "name")
        diseases_field = getattr(row, "diseases")

        # split multi-disease field
        diseases = [d.strip() for d in str(diseases_field).split(";") if d.strip()]
        if not diseases:
            continue

        for disease_name in diseases:
            # Query syntax: "BIOMARKER"[Title/Abstract] AND "DISEASE"[Title/Abstract]
            term = f"\"{biomarker_name}\"[Title/Abstract] AND \"{disease_name}\"[Title/Abstract]"
            records.append({
                "biomarker_id": biomarker_id,
                "biomarker_name": biomarker_name,
                "disease_name": disease_name,
                "pubmed_query": term,
                "pubmed_count": None,  # to be filled later
            })

        if i % 50 == 0:
            logging.info(f"Prepared pairs for {i}/{total_rows} biomarkers")

    logging.info(f"Prepared {len(records)} biomarker–disease pairs total")
    return records

def enrich_pubmed_counts(pairs: List[dict]) -> None:
    for i, rec in enumerate(pairs, start=1):
        term = rec["pubmed_query"]
        try:
            count = pubmed_count(term)
        except Exception as e:
            logging.warning(f"HTTP error for query: {term!r}: {e}")
            count = 0

        rec["pubmed_count"] = count
        logging.info(f"[{i}/{len(pairs)}] {rec['biomarker_name']} AND {rec['disease_name']} -> {count}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--max-rows",
        type=int,
        default=200,
        help="Number of biomarker rows from biomarker_matrix_full.csv to process",
    )
    parser.add_argument(
        "--out",
        type=str,
        default="data/enriched_biomarker_diseases_v3.csv",
        help="Output CSV path",
    )
    args = parser.parse_args()

    logging.info(f"Using EMAIL={EMAIL}, TOOL={TOOL}, API_KEY={'set' if API_KEY else 'not set'}")

    df = pd.read_csv("data/biomarker_matrix_full.csv")
    logging.info(f"Loaded biomarker matrix with {len(df)} rows")

    pairs = build_pairs(df, args.max_rows)
    enrich_pubmed_counts(pairs)

    out_df = pd.DataFrame(pairs)
    out_df.to_csv(args.out, index=False)
    logging.info(f"Saved {len(out_df)} enriched rows to {args.out}")

if __name__ == "__main__":
    main()
