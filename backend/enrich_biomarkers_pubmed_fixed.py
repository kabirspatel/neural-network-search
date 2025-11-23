#!/usr/bin/env python3
"""
Enrich biomarker–disease pairs with PubMed hit counts.

Reads:
    data/biomarker_matrix_full.csv

Writes:
    data/enriched_biomarker_diseases.csv

Columns in output:
    biomarker_id, biomarker_name, disease_name, pubmed_query, pubmed_count

Environment variables (required/recommended):
    NCBI_EMAIL      - your email (required by NCBI)
    NCBI_TOOL       - tool name (e.g., "urine_biomarker_project")
    NCBI_API_KEY    - NCBI API key (optional but recommended)
"""

import os
import re
import time
import math
import logging
import argparse
from typing import Iterable, Tuple, List

import requests
import pandas as pd

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

NCBI_EMAIL = os.getenv("NCBI_EMAIL")
NCBI_TOOL = os.getenv("NCBI_TOOL", "urine_biomarker_project")
NCBI_API_KEY = os.getenv("NCBI_API_KEY")

BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"

# polite delay between requests (seconds)
REQUEST_DELAY = 0.12  # ~8–9 requests/second with API key

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(message)s",
)


# ---------------------------------------------------------------------------
# Helper functions: term cleaning
# ---------------------------------------------------------------------------

GENE_TOKEN_RE = re.compile(r"\b[A-Z0-9]{2,}\b")


def pick_gene_symbol(biomarker_name: str) -> str:
    """
    Try to extract a clean gene symbol from a biomarker name.
    Examples:
        "BRCA1 oncogenic mutation"  -> "BRCA1"
        "EZH2 (A692V,Y646C,...)"    -> "EZH2"
        "CD274 (PD-L1) +"           -> "CD274"
    """
    # Strip parentheses content to avoid long mutation lists
    cleaned = re.sub(r"\(.*?\)", " ", biomarker_name)
    cleaned = cleaned.replace("+", " ").replace("-", " ")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    # Look for an all-caps token ≥ 2 chars (typical gene symbol)
    match = GENE_TOKEN_RE.search(cleaned)
    if match:
        return match.group(0)

    # Fallback: first word
    return cleaned.split()[0]


def clean_disease_name(disease_name: str) -> str:
    """
    Simplify disease names into something PubMed will like.

    Examples:
        "Breast adenocarcinoma" -> "breast cancer"
        "Ovary; Pancreas"        -> "ovarian cancer"
    """
    if not isinstance(disease_name, str):
        return ""

    # Only take the first segment before ; or |
    seg = re.split(r"[;|]", disease_name)[0]
    seg = seg.strip()

    # Replace some common long forms with simpler cancer names
    lower = seg.lower()

    # crude but effective mapping; you can extend as needed
    if "breast" in lower:
        return "breast cancer"
    if "ovary" in lower or "ovarian" in lower:
        return "ovarian cancer"
    if "colon" in lower or "colorectal" in lower:
        return "colorectal cancer"
    if "lung" in lower:
        return "lung cancer"
    if "pancreas" in lower or "pancreatic" in lower:
        return "pancreatic cancer"
    if "prostate" in lower:
        return "prostate cancer"
    if "stomach" in lower or "gastric" in lower:
        return "stomach cancer"
    if "melanoma" in lower:
        return "melanoma"
    if "glioma" in lower or "glioblastoma" in lower:
        return "glioma"
    if "lymphoma" in lower:
        return "lymphoma"
    if "leukemia" in lower:
        return "leukemia"

    # generic fallback – use original segment
    return seg


def build_pubmed_query(biomarker_name: str, disease_name: str) -> str:
    """
    Build a PubMed query like:
        "BRCA1"[Title/Abstract] AND "breast cancer"[Title/Abstract]
    """
    gene = pick_gene_symbol(biomarker_name)
    disease = clean_disease_name(disease_name)

    # Final safety cleanup
    gene = gene.strip(' "\'')
    disease = disease.strip(' "\'')

    if not gene or not disease:
        return ""

    return f'"{gene}"[Title/Abstract] AND "{disease}"[Title/Abstract]'


# ---------------------------------------------------------------------------
# PubMed request helper
# ---------------------------------------------------------------------------

def query_pubmed_count(term: str, session: requests.Session) -> int:
    """
    Call NCBI ESearch and return the hit count for a query term.
    Returns 0 on any error but never raises.
    """
    if not term:
        return 0

    params = {
        "db": "pubmed",
        "retmode": "json",
        "retmax": 0,
        "term": term,
        "tool": NCBI_TOOL,
        "email": NCBI_EMAIL or "",
    }
    if NCBI_API_KEY:
        params["api_key"] = NCBI_API_KEY

    try:
        resp = session.get(BASE_URL, params=params, timeout=20)
    except Exception as e:
        logging.warning("HTTP error for term %r: %s", term, e)
        return 0

    if resp.status_code != 200:
        # Truncate body to avoid enormous logs
        body_preview = resp.text[:200].replace("\n", " ")
        logging.warning(
            "HTTP %s for term %r: %s",
            resp.status_code,
            term,
            body_preview,
        )
        return 0

    try:
        data = resp.json()
        count_str = data["esearchresult"]["count"]
        count = int(count_str)
    except Exception as e:
        logging.warning("JSON parse error for term %r: %s", term, e)
        return 0

    # Be polite
    time.sleep(REQUEST_DELAY)
    return count


# ---------------------------------------------------------------------------
# Pair generation from biomarker_matrix_full.csv
# ---------------------------------------------------------------------------

def iter_biomarker_disease_pairs(df: pd.DataFrame) -> Iterable[Tuple[int, str, str]]:
    """
    Yield (biomarker_id, biomarker_name, disease_name) pairs.

    Uses:
        - biomarker_id  from 'biomarker_id'
        - biomarker name from 'name'
        - diseases from 'disease_list' if present,
          otherwise from 'diseases' column.
    """
    if "biomarker_id" not in df.columns:
        raise ValueError("Expected column 'biomarker_id' in biomarker_matrix_full.csv")
    if "name" not in df.columns:
        raise ValueError("Expected column 'name' in biomarker_matrix_full.csv")

    if "disease_list" in df.columns:
        disease_col = "disease_list"
    elif "diseases" in df.columns:
        disease_col = "diseases"
    else:
        raise ValueError("Expected a column 'disease_list' or 'diseases' in biomarker_matrix_full.csv")

    for _, row in df.iterrows():
        biomarker_id = row["biomarker_id"]
        biomarker_name = str(row["name"])

        raw = str(row.get(disease_col) or "")
        if not raw.strip():
            continue

        # allow ; or | as separators; commas inside diseases are messy, so avoid
        pieces = re.split(r"[;|]", raw)
        for dis in pieces:
            disease_name = dis.strip()
            if not disease_name:
                continue
            yield (biomarker_id, biomarker_name, disease_name)


def deduplicate_pairs(pairs: Iterable[Tuple[int, str, str]]) -> List[Tuple[int, str, str]]:
    """
    Remove duplicate (biomarker_id, biomarker_name, disease_name) triples.
    """
    seen = set()
    out: List[Tuple[int, str, str]] = []
    for triple in pairs:
        if triple not in seen:
            seen.add(triple)
            out.append(triple)
    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Enrich biomarker-disease pairs with PubMed hit counts."
    )
    parser.add_argument(
        "--input",
        default="data/biomarker_matrix_full.csv",
        help="Path to biomarker matrix CSV (default: data/biomarker_matrix_full.csv)",
    )
    parser.add_argument(
        "--output",
        default="data/enriched_biomarker_diseases.csv",
        help="Output CSV path (default: data/enriched_biomarker_diseases.csv)",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="Optional maximum number of pairs to process (for testing).",
    )
    args = parser.parse_args()

    if not NCBI_EMAIL:
        logging.warning(
            "NCBI_EMAIL is not set; NCBI strongly recommends providing an email."
        )

    logging.info("Reading biomarker matrix from %s ...", args.input)
    df = pd.read_csv(args.input)

    logging.info("Generating biomarker–disease pairs ...")
    pairs = deduplicate_pairs(list(iter_biomarker_disease_pairs(df)))
    total_pairs = len(pairs)
    logging.info("Total unique biomarker–disease pairs: %d", total_pairs)

    if args.max_rows is not None:
        pairs = pairs[: args.max_rows]
        logging.info("Restricting to first %d pairs for this run.", args.max_rows)

    session = requests.Session()

    records = []
    n = len(pairs)
    logging.info("Querying PubMed for %d pairs ...", n)
    for idx, (bid, bname, dname) in enumerate(pairs, start=1):
        query = build_pubmed_query(bname, dname)
        count = query_pubmed_count(query, session)

        logging.info(
            "[%d/%d] %s AND %s -> %d",
            idx,
            n,
            pick_gene_symbol(bname),
            clean_disease_name(dname),
            count,
        )

        records.append(
            {
                "biomarker_id": bid,
                "biomarker_name": bname,
                "disease_name": dname,
                "pubmed_query": query,
                "pubmed_count": count,
            }
        )

    out_df = pd.DataFrame.from_records(records)
    logging.info("Writing %d rows to %s ...", len(out_df), args.output)
    out_df.to_csv(args.output, index=False)
    logging.info("DONE.")


if __name__ == "__main__":
    main()
