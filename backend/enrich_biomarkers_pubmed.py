#!/usr/bin/env python3
"""
enrich_biomarkers_pubmed.py

Query PubMed for each biomarker / disease pair and record
whether there is at least one co-mention in the literature.

Input:
  data/biomarker_matrix_full.csv   (already built)

Output:
  data/enriched_biomarker_diseases.csv

You can control how many rows to process with --max-rows.
"""

import csv
import os
import time
import argparse
import logging
from pathlib import Path

import pandas as pd
import requests
from urllib.parse import quote_plus

# ---------- Config ----------
EUTILS_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
EMAIL = os.environ.get("NCBI_EMAIL", "your.email@example.com")
TOOL = os.environ.get("NCBI_TOOL", "urine_biomarker_project")
API_KEY = os.environ.get("NCBI_API_KEY")  # optional
DB = "pubmed"

# PubMed polite rate limit (with API key you can go faster, but this is safe)
REQUEST_DELAY = 0.34  # seconds

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


def load_biomarker_matrix(path: str) -> pd.DataFrame:
    if not Path(path).exists():
        raise FileNotFoundError(f"{path} not found. Did you run merge_weak_into_matrix.py?")
    df = pd.read_csv(path)
    # We only need biomarker_id, name, diseases
    required = ["biomarker_id", "name", "diseases"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in biomarker matrix: {missing}")
    return df[required]


def build_pubmed_query(biomarker_name: str, disease_name: str) -> str:
    """
    VERY simple query builder: "(BIOMARKER_NAME[Title/Abstract]) AND (DISEASE_NAME[Title/Abstract])"
    Escapes problematic characters; this is not meant to be perfect, just robust.
    """
    def clean(s: str) -> str:
        # Strip quotes and excessive whitespace, keep it simple
        s = s.replace('"', " ").replace("'", " ")
        s = " ".join(s.split())
        return s

    b = clean(biomarker_name)
    d = clean(disease_name)
    # PubMed syntax
    return f'("{b}"[Title/Abstract]) AND ("{d}"[Title/Abstract])'


def esearch_count(query: str) -> int:
    """
    Run an ESearch query and return the hit count.
    Returns 0 if anything goes wrong.
    """
    params = {
        "db": DB,
        "term": query,
        "retmode": "json",
        "email": EMAIL,
        "tool": TOOL,
    }
    if API_KEY:
        params["api_key"] = API_KEY

    try:
        r = requests.get(EUTILS_BASE, params=params, timeout=15)
        r.raise_for_status()
        js = r.json()
        count_str = js.get("esearchresult", {}).get("count", "0")
        return int(count_str)
    except requests.exceptions.HTTPError as e:
        log.warning("PubMed HTTP error (%s) for query=%s", e, query)
    except Exception as e:
        log.warning("PubMed query failed (%s) for query=%s", e, query)
    return 0


def build_enriched_pairs(
    biomarker_matrix_path: str,
    output_path: str,
    max_rows: int = 200
) -> None:
    df = load_biomarker_matrix(biomarker_matrix_path)

    # Explode diseases into rows
    # biomarker_id | biomarker_name | disease (single)
    records = []
    for _, row in df.iterrows():
        b_id = row["biomarker_id"]
        b_name = row["name"]
        diseases_field = row["diseases"]
        if pd.isna(diseases_field):
            continue
        diseases = [d.strip() for d in str(diseases_field).split(";") if d.strip()]
        for d in diseases:
            records.append((b_id, b_name, d))

    log.info("Total biomarker–disease candidate pairs: %d", len(records))

    # Optionally limit for now
    if max_rows is not None and max_rows > 0:
        records = records[:max_rows]
        log.info("Limiting to first %d pairs for enrichment.", len(records))

    out_rows = []
    for idx, (b_id, b_name, d_name) in enumerate(records, start=1):
        query = build_pubmed_query(b_name, d_name)
        count = esearch_count(query)
        out_rows.append(
            {
                "biomarker_id": b_id,
                "biomarker_name": b_name,
                "disease_name": d_name,
                "pubmed_query": query,
                "pubmed_count": count,
            }
        )

        if idx % 10 == 0:
            log.info("Processed %d / %d pairs...", idx, len(records))

        time.sleep(REQUEST_DELAY)

    # Write CSV
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "biomarker_id",
                "biomarker_name",
                "disease_name",
                "pubmed_query",
                "pubmed_count",
            ],
        )
        writer.writeheader()
        writer.writerows(out_rows)

    log.info("Wrote %d enriched rows to %s", len(out_rows), output_path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--matrix",
        default="data/biomarker_matrix_full.csv",
        help="Input biomarker matrix CSV",
    )
    parser.add_argument(
        "--output",
        default="data/enriched_biomarker_diseases.csv",
        help="Output CSV for enriched disease links",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=200,
        help="Max biomarker–disease pairs to query (0 or negative = no limit)",
    )
    args = parser.parse_args()

    log.info("Using EMAIL=%s TOOL=%s API_KEY=%s", EMAIL, TOOL, "yes" if API_KEY else "no")
    build_enriched_pairs(args.matrix, args.output, max_rows=args.max_rows)


if __name__ == "__main__":
    main()
