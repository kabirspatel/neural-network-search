#!/usr/bin/env python3
"""
Enrich biomarker–disease pairs with PubMed co-mention counts.

Reads biomarker_matrix_full.csv (or a custom CSV), extracts unique
(biomarker_name, disease_name) pairs, queries PubMed, and writes
data/enriched_biomarker_diseases.csv (or a custom output path).

Environment variables used (optional but recommended):
  NCBI_EMAIL  - your email for NCBI E-utilities
  NCBI_TOOL   - short name of this app, e.g. "urine_biomarker_project"
  NCBI_API_KEY - PubMed API key (OPTIONAL – script works without it)
"""

import os
import time
import argparse
import logging
from typing import Optional

import pandas as pd
import requests


# -------------------------------------------------------------------
# PubMed helper
# -------------------------------------------------------------------

def fetch_pubmed_count(
    biomarker: str,
    disease: str,
    email: Optional[str] = None,
    tool: Optional[str] = None,
    api_key: Optional[str] = None,
    session: Optional[requests.Session] = None,
) -> int:
    """
    Return the number of PubMed articles that mention BOTH the biomarker
    and the disease in the Title/Abstract.

    If anything goes wrong (HTTP error, JSON error, etc.), returns 0
    and logs a warning.
    """
    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"

    # Example:
    #   "BRCA1 mutation"[Title/Abstract] AND "breast cancer"[Title/Abstract]
    term = f'"{biomarker}"[Title/Abstract] AND "{disease}"[Title/Abstract]'

    params = {
        "db": "pubmed",
        "term": term,
        "retmode": "json",
        "retmax": 0,          # we only need the count
    }

    if email:
        params["email"] = email
    if tool:
        params["tool"] = tool

    # Only include api_key if it is non-empty and not a placeholder
    if api_key and not api_key.lower().startswith("your_"):
        params["api_key"] = api_key

    sess = session or requests

    try:
        resp = sess.get(base_url, params=params, timeout=15)
        resp.raise_for_status()

        data = resp.json()
        count_str = data.get("esearchresult", {}).get("count", "0")
        count = int(count_str)
        logging.info("[INFO] %s AND %s -> %d", biomarker, disease, count)
        return count

    except Exception as e:
        logging.warning(
            "[WARN] PubMed query failed for %r: %s", term, e
        )
        # On failure, we just return 0 so pipeline still finishes
        return 0


# -------------------------------------------------------------------
# Main enrichment logic
# -------------------------------------------------------------------

def build_enrichment_table(
    input_csv: str,
    output_csv: str,
    max_pairs: Optional[int] = None,
    sleep_seconds: float = 0.35,
) -> None:
    """
    Read biomarker matrix CSV, construct unique (biomarker, disease) pairs,
    query PubMed for each, and write out an enriched CSV.
    """
    logging.info("[INFO] Reading biomarker matrix from %s ...", input_csv)
    df = pd.read_csv(input_csv, low_memory=False)

    # Basic expectation: there is a 'biomarker_id', 'name', and 'diseases' column
    if "biomarker_id" not in df.columns or "name" not in df.columns:
        raise ValueError(
            "Input CSV must contain 'biomarker_id' and 'name' columns."
        )

    if "diseases" not in df.columns:
        raise ValueError(
            "Input CSV must contain a 'diseases' column "
            "with one or more disease names (semicolon-separated)."
        )

    # Expand semicolon-separated diseases into one row per biomarker–disease pair
    records = []
    for _, row in df.iterrows():
        biomarker_id = row["biomarker_id"]
        biomarker_name = str(row["name"]).strip()
        diseases_field = str(row["diseases"]) if not pd.isna(row["diseases"]) else ""

        if not diseases_field or diseases_field.lower() in ("nan", "none"):
            continue

        for dis in str(diseases_field).split(";"):
            disease_name = dis.strip()
            if not disease_name:
                continue
            records.append((biomarker_id, biomarker_name, disease_name))

    pairs_df = pd.DataFrame(
        records, columns=["biomarker_id", "biomarker_name", "disease_name"]
    ).drop_duplicates()

    total_pairs = len(pairs_df)
    logging.info("[INFO] Prepared %d unique biomarker–disease pairs.", total_pairs)

    if max_pairs is not None:
        pairs_df = pairs_df.head(max_pairs)
        logging.info("[INFO] Limiting to first %d pairs for this run.", max_pairs)

    # Environment info for PubMed
    email = os.environ.get("NCBI_EMAIL")
    tool = os.environ.get("NCBI_TOOL")
    api_key = os.environ.get("NCBI_API_KEY")

    if not email:
        logging.warning(
            "[WARN] NCBI_EMAIL is not set. NCBI recommends providing an email "
            "address with all E-utilities requests."
        )

    if not tool:
        logging.warning(
            "[WARN] NCBI_TOOL is not set. Consider setting it to a short name "
            "like 'urine_biomarker_project'."
        )

    if api_key:
        logging.info("[INFO] Using provided NCBI_API_KEY.")
    else:
        logging.info("[INFO] No NCBI_API_KEY set – running without an API key.")

    results = []
    session = requests.Session()

    for idx, row in pairs_df.iterrows():
        biomarker_id = row["biomarker_id"]
        biomarker_name = row["biomarker_name"]
        disease_name = row["disease_name"]

        count = fetch_pubmed_count(
            biomarker_name,
            disease_name,
            email=email,
            tool=tool,
            api_key=api_key,
            session=session,
        )

        results.append(
            {
                "biomarker_id": biomarker_id,
                "biomarker_name": biomarker_name,
                "disease_name": disease_name,
                "pubmed_query": f'"{biomarker_name}"[Title/Abstract] '
                                f'AND "{disease_name}"[Title/Abstract]',
                "pubmed_count": count,
            }
        )

        # Be nice to NCBI’s servers
        time.sleep(sleep_seconds)

    out_df = pd.DataFrame(results)
    logging.info(
        "[INFO] Writing %d enriched rows to %s ...", len(out_df), output_csv
    )
    out_df.to_csv(output_csv, index=False)
    logging.info("[INFO] DONE. Wrote enriched biomarker–disease table.")


# -------------------------------------------------------------------
# CLI
# -------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Enrich biomarker–disease pairs with PubMed co-mention counts."
    )
    parser.add_argument(
        "--input-csv",
        default="data/biomarker_matrix_full.csv",
        help="Input biomarker matrix CSV (default: data/biomarker_matrix_full.csv)",
    )
    parser.add_argument(
        "--out",
        default="data/enriched_biomarker_diseases.csv",
        help="Output CSV path (default: data/enriched_biomarker_diseases.csv)",
    )
    parser.add_argument(
        "--max-pairs",
        type=int,
        default=None,
        help="Optional limit on number of biomarker–disease pairs to query "
             "(useful for testing).",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.35,
        help="Seconds to sleep between PubMed requests (default: 0.35).",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
    )

    args = parse_args()
    build_enrichment_table(
        input_csv=args.input_csv,
        output_csv=args.out,
        max_pairs=args.max_pairs,
        sleep_seconds=args.sleep,
    )


if __name__ == "__main__":
    main()
