#!/usr/bin/env python3
"""
Example script: pull extra biomarker–disease edges from MarkerDB (or another CSV),
normalize them, and append to data/biomarker_disease_edges_pubmed.csv.

You MUST fill in the SOURCE_CSV path / URL and the column mappings
for your specific dataset.
"""

import csv
import os
from pathlib import Path

import pandas as pd

BASE = Path(__file__).resolve().parent.parent
EDGES_CSV = BASE / "data" / "biomarker_disease_edges_pubmed.csv"

# TODO: set this to wherever you put the MarkerDB export
MARKERDB_CSV = BASE / "data" / "markerdb_metabolic_export.csv"


def load_existing_pairs():
    """Return a set of (biomarker_id, doid) already in the edges CSV."""
    if not EDGES_CSV.exists():
        return set()

    df = pd.read_csv(EDGES_CSV)
    pairs = set(
        (str(bm).strip(), str(d).strip())
        for bm, d in zip(df["biomarker_id"], df["doid"])
        if str(bm).strip() and str(d).strip()
    )
    return pairs


def build_new_rows(existing_pairs):
    """
    Read the MarkerDB CSV and yield rows in our schema:
      biomarker_id, doid, pubmed_query, pubmed_count, is_cancer_like
    """
    df = pd.read_csv(MARKERDB_CSV)

    # TODO: adjust these to the actual column names in your MarkerDB export
    BM_COL = "Biomarker ID"       # e.g. "HMDB_ID" or some stable ID
    DISEASE_COL = "Disease_DOID"  # you may need to map disease name -> DOID separately
    PMID_COL = "PMIDs"            # optional

    rows = []
    for _, row in df.iterrows():
        biomarker_id = str(row[BM_COL]).strip()
        doid = str(row[DISEASE_COL]).strip()
        if not biomarker_id or not doid:
            continue

        key = (biomarker_id, doid)
        if key in existing_pairs:
            continue  # already have this pair

        # simple PubMed count from a semicolon-separated "PMIDs" column, if available
        pmids_raw = str(row.get(PMID_COL, "") or "").strip()
        pubmed_count = 0
        if pmids_raw:
            pubmed_count = len([p for p in pmids_raw.split(";") if p.strip()])

        rows.append(
            {
                "biomarker_id": biomarker_id,
                "doid": doid,
                "pubmed_query": None,      # could construct a query string if you want
                "pubmed_count": pubmed_count,
                "is_cancer_like": 0,       # metabolic / renal → not cancer
            }
        )

    return rows


def append_rows(rows):
    if not rows:
        print("No new rows to append.")
        return

    file_exists = EDGES_CSV.exists()

    with open(EDGES_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "biomarker_id",
                "doid",
                "pubmed_query",
                "pubmed_count",
                "is_cancer_like",
            ],
        )
        if not file_exists:
            writer.writeheader()
        for r in rows:
            writer.writerow(r)

    print(f"Appended {len(rows)} new rows to {EDGES_CSV}")


def main():
    existing = load_existing_pairs()
    rows = build_new_rows(existing)
    append_rows(rows)


if __name__ == "__main__":
    main()
