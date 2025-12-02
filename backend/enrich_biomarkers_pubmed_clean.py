#!/usr/bin/env python3
import argparse
import csv
import logging
import os
import re
import time
from typing import Iterable, List, Tuple

import pandas as pd
import requests

EUTILS_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"


# ---------- Logging ----------

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


# ---------- Helpers ----------

def load_biomarker_pairs(
    matrix_csv: str,
    max_pairs: int | None = None
) -> List[Tuple[str, str, str]]:
    """
    Read biomarker_matrix_full.csv and return unique (id, biomarker_name, disease_name) pairs.

    Assumes columns:
      - biomarker_id
      - name
      - diseases  (semicolon-separated list)
    """
    df = pd.read_csv(matrix_csv, low_memory=False)

    # Basic sanity check
    for col in ["biomarker_id", "name", "diseases"]:
        if col not in df.columns:
            raise ValueError(f"Expected column '{col}' in {matrix_csv}")

    pairs: set[Tuple[str, str, str]] = set()

    for _, row in df.iterrows():
        biomarker_id = str(row["biomarker_id"])
        biomarker_name = str(row["name"])
        diseases = str(row["diseases"]) if not pd.isna(row["diseases"]) else ""

        for disease in diseases.split(";"):
            disease = disease.strip()
            if not disease:
                continue
            pairs.add((biomarker_id, biomarker_name, disease))

    pairs_list = sorted(pairs, key=lambda x: (int(x[0]), x[2]))

    if max_pairs is not None:
        pairs_list = pairs_list[:max_pairs]

    log.info("Prepared %d unique biomarker–disease pairs.", len(pairs_list))
    return pairs_list


_gene_like = re.compile(r"^[A-Z0-9\-]+$")


def clean_biomarker_term(raw_name: str) -> str:
    """
    Convert a full biomarker name like
        'EZH2 (A692V,Y646C,Y646F,Y646H,Y646N,Y646S,A682G)'
    or 'ERBB2 amplification'
    into a reasonable search term.

    Strategy:
      - take the first token before '('  → 'EZH2', 'ERBB2'
      - if that token looks like a gene symbol, use it
      - otherwise, fall back to a simpler cleaned phrase
    """
    # Split off mutation list etc.
    base = raw_name.split("(")[0].strip()
    # Often something like "ERBB2 amplification"
    first_token = base.split()[0]

    if _gene_like.match(first_token):
        return first_token

    # Fallback: strip punctuation, keep a short phrase
    base = re.sub(r"[^A-Za-z0-9\s]", " ", base)
    base = re.sub(r"\s+", " ", base).strip()
    # avoid extremely long phrases
    return " ".join(base.split()[:4]) or first_token


def clean_disease_term(raw_disease: str) -> str:
    """
    Simplify disease names:
      - use first piece before ';'
      - strip parenthetical suffixes
      - remove punctuation except spaces and hyphens
    """
    text = raw_disease.split(";")[0]
    text = text.split("(")[0]
    text = re.sub(r"[^A-Za-z0-9\s\-]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def build_pubmed_term(bio_name: str, disease_name: str) -> str:
    """
    Build a PubMed search term.
    e.g.  (EZH2[Title/Abstract]) AND (Follicular lymphoma[Title/Abstract])
    """
    bio_term = clean_biomarker_term(bio_name)
    dis_term = clean_disease_term(disease_name)

    # If disease term got empty somehow, just search biomarker alone
    if not dis_term:
        return f"{bio_term}[Title/Abstract]"

    return f"{bio_term}[Title/Abstract] AND {dis_term}[Title/Abstract]"


def pubmed_count(term: str, session: requests.Session, email: str, tool: str,
                 api_key: str | None = None, sleep: float = 0.34) -> int:
    """
    Query PubMed via ESearch and return the hit count.
    On any HTTP / parse error, return 0.
    """
    params = {
        "db": "pubmed",
        "term": term,
        "retmode": "json",
        "rettype": "count",
        "retmax": 0,
        "email": email,
        "tool": tool,
    }
    if api_key:
        params["api_key"] = api_key

    try:
        resp = session.get(EUTILS_BASE, params=params, timeout=20)
    except Exception as e:
        log.warning("Request error for term %r: %s", term, e)
        return 0

    if resp.status_code != 200:
        # Try to parse any JSON error message; otherwise just log code.
        try:
            msg = resp.json()
        except Exception:
            msg = resp.text[:200]
        log.warning("HTTP %s for term %r: %s", resp.status_code, term, msg)
        return 0

    try:
        data = resp.json()
        count_str = data["esearchresult"]["count"]
        count = int(count_str)
    except Exception as e:
        log.warning("Failed to parse count for term %r: %s", term, e)
        return 0
    finally:
        # be nice to NCBI
        time.sleep(sleep)

    return count


# ---------- Main enrichment ----------

def enrich_pairs(
    input_csv: str,
    output_csv: str,
    max_pairs: int | None = None,
) -> None:
    """
    Main driver: load biomarker–disease pairs, query PubMed for each,
    and write enriched_biomarker_diseases.csv (or whatever name you pass).
    """
    email = os.environ.get("NCBI_EMAIL")
    tool = os.environ.get("NCBI_TOOL", "urine_biomarker_project")
    api_key = os.environ.get("NCBI_API_KEY")

    if not email:
        raise SystemExit("NCBI_EMAIL env var is required for PubMed access.")

    pairs = load_biomarker_pairs(input_csv, max_pairs=max_pairs)

    out_rows: List[dict] = []

    with requests.Session() as session:
        for idx, (bio_id, bio_name, disease_name) in enumerate(pairs, start=1):
            term = build_pubmed_term(bio_name, disease_name)
            count = pubmed_count(term, session, email=email, tool=tool, api_key=api_key)

            log.info("[%d/%d] %s AND %s -> %d",
                     idx, len(pairs), clean_biomarker_term(bio_name),
                     clean_disease_term(disease_name), count)

            out_rows.append({
                "biomarker_id": bio_id,
                "biomarker_name": bio_name,
                "disease_name": disease_name,
                "pubmed_query": term,
                "pubmed_count": count,
            })

    # Write CSV
    fieldnames = [
        "biomarker_id",
        "biomarker_name",
        "disease_name",
        "pubmed_query",
        "pubmed_count",
    ]
    os.makedirs(os.path.dirname(output_csv) or ".", exist_ok=True)
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(out_rows)

    log.info("Wrote %d rows to %s", len(out_rows), output_csv)


def main():
    parser = argparse.ArgumentParser(
        description="Enrich biomarker–disease pairs with PubMed hit counts."
    )
    parser.add_argument(
        "--input",
        default="data/biomarker_matrix_full.csv",
        help="Input biomarker matrix CSV (default: data/biomarker_matrix_full.csv)",
    )
    parser.add_argument(
        "--out",
        default="data/enriched_biomarker_diseases.csv",
        help="Output CSV (default: data/enriched_biomarker_diseases.csv)",
    )
    parser.add_argument(
        "--max-pairs",
        type=int,
        default=None,
        help="Optional cap on number of pairs (for testing).",
    )

    args = parser.parse_args()
    enrich_pairs(args.input, args.out, max_pairs=args.max_pairs)


if __name__ == "__main__":
    main()
