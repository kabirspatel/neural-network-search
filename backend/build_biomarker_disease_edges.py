# backend/build_biomarker_disease_edges.py

import logging
import pathlib

import pandas as pd


logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(message)s",
)

DATA_DIR = pathlib.Path("data")
ENRICHED_BIOMARKER_DISEASES = DATA_DIR / "enriched_biomarker_diseases.csv"
ENRICHED_DISEASES = DATA_DIR / "diseases_enriched.csv"
OUT_EDGES = DATA_DIR / "biomarker_disease_edges.csv"


def main() -> None:
    logging.info("Loading biomarker–disease table from %s ...", ENRICHED_BIOMARKER_DISEASES)
    bdf = pd.read_csv(ENRICHED_BIOMARKER_DISEASES, low_memory=False)

    logging.info("Loading enriched diseases table from %s ...", ENRICHED_DISEASES)
    ddf = pd.read_csv(ENRICHED_DISEASES, low_memory=False)

    # Keep only the columns we need from diseases_enriched
    d_keep = ddf[["doid", "name", "is_cancer_like"]].copy()

    logging.info("Merging biomarker–disease rows with DOID on exact disease name match...")
    merged = bdf.merge(
        d_keep,
        left_on="disease_name",
        right_on="name",
        how="left",
        indicator=True,
    )

    total_rows = len(merged)
    matched = (merged["_merge"] == "both").sum()
    left_only = (merged["_merge"] == "left_only").sum()

    logging.info("Total biomarker–disease rows: %d", total_rows)
    logging.info("Rows matched to DOID by name: %d", matched)
    logging.info("Rows with NO DOID match (left_only): %d", left_only)

    if left_only > 0:
        logging.warning(
            "There are %d rows with disease_name not found in diseases_enriched.csv. "
            "These will be dropped from the edges table.",
            left_only,
        )

    # Keep only rows that actually matched a DOID
    keep = merged[merged["_merge"] == "both"].copy()

    # Build edges table
    edges = keep[
        [
            "biomarker_id",
            "biomarker_name",
            "disease_name",   # original name
            "doid",           # canonical DOID
            "is_cancer_like",
            "pubmed_query",
            "pubmed_count",
        ]
    ].copy()

    logging.info("Resulting edges table has %d rows.", len(edges))

    logging.info("Writing edges to %s ...", OUT_EDGES)
    edges.to_csv(OUT_EDGES, index=False)
    logging.info("DONE. biomarker_disease_edges.csv created.")


if __name__ == "__main__":
    main()
