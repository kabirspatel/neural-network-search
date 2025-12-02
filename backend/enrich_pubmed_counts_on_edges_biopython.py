#!/usr/bin/env python
"""
enrich_pubmed_counts_on_edges_biopython.py

Read biomarker–disease edges from CSV, query PubMed for each unique
(biomarker_name, disease_name) pair using Biopython Entrez, and write
back an enriched CSV with `pubmed_query` and `pubmed_count` columns.
"""

import time
from pathlib import Path

import pandas as pd
from Bio import Entrez


# --------- CONFIG ---------

# Input and output files
INPUT_PATH = Path("data/biomarker_disease_edges.csv")
OUTPUT_PATH = Path("data/biomarker_disease_edges_pubmed.csv")

# REQUIRED: put your real email here (NCBI requirement)
Entrez.email = "your_email@example.com"
Entrez.tool = "neural-network-search"


# --------- HELPER FUNCTIONS ---------


def build_pubmed_term(biomarker_name: str, disease_name: str) -> str | None:
    """
    Build a safe PubMed search term from free-text biomarker and disease names.

    Example:
        biomarker_name = 'ERBB2 amplification'
        disease_name   = 'Breast adenocarcinoma'
        -> '(ERBB2 amplification[Title/Abstract]) AND (Breast adenocarcinoma[Title/Abstract])'
    """
    if pd.isna(biomarker_name) or pd.isna(disease_name):
        return None

    # Convert to strings, strip whitespace, remove internal double quotes
    b = str(biomarker_name).replace('"', "").strip()
    d = str(disease_name).replace('"', "").strip()

    if not b or not d:
        return None

    term = f"({b}[Title/Abstract]) AND ({d}[Title/Abstract])"
    return term


def fetch_pubmed_count(term: str | None) -> int:
    """
    Run an Entrez.esearch query and return the 'Count' as int.
    Returns 0 if term is None or if the query fails for any reason.
    """
    if not term:
        return 0

    try:
        handle = Entrez.esearch(db="pubmed", term=term, retmode="xml")
        results = Entrez.read(handle)
        handle.close()
        return int(results["Count"])
    except Exception as e:
        print(f"[WARN] PubMed query failed for term {term!r}: {e}")
        return 0


# --------- MAIN PIPELINE ---------


def main():
    # 1) Load edges
    print(f"[INFO] Loading biomarker–disease edges from {INPUT_PATH} ...")
    df = pd.read_csv(INPUT_PATH)

    if not {"biomarker_name", "disease_name"}.issubset(df.columns):
        raise RuntimeError(
            "Input CSV must contain 'biomarker_name' and 'disease_name' columns."
        )

    # 2) Get unique pairs
    pair_cols = ["biomarker_name", "disease_name"]
    unique_pairs = df[pair_cols].drop_duplicates().reset_index(drop=True)
    n_pairs = len(unique_pairs)
    print(f"[INFO] Found {n_pairs} unique biomarker–disease pairs.")

    # 3) Query PubMed for each unique pair
    pair_to_result: dict[tuple[str, str], tuple[str | None, int]] = {}

    for i, row in unique_pairs.iterrows():
        biomarker = row["biomarker_name"]
        disease = row["disease_name"]
        key = (biomarker, disease)

        term = build_pubmed_term(biomarker, disease)
        print(f"[{i + 1}/{n_pairs}] Querying PubMed for: {term}")

        count = fetch_pubmed_count(term)

        pair_to_result[key] = (term, count)

        # Be gentle to NCBI: ~3 requests/second max
        time.sleep(0.35)

    # 4) Attach results back to full dataframe
    pubmed_terms = []
    pubmed_counts = []

    for _, row in df.iterrows():
        key = (row["biomarker_name"], row["disease_name"])
        term, count = pair_to_result.get(key, (None, 0))
        pubmed_terms.append(term)
        pubmed_counts.append(count)

    df["pubmed_query"] = pubmed_terms
    df["pubmed_count"] = pubmed_counts

    # 5) Write enriched CSV
    print(f"[INFO] Writing enriched edges to {OUTPUT_PATH} ...")
    df.to_csv(OUTPUT_PATH, index=False)
    print("[INFO] DONE. Enriched file written.")


if __name__ == "__main__":
    main()
