#!/usr/bin/env python3
"""
Merge all curated biomarker–disease CSVs into a single master CSV.

Input folder:
    data/biomarker_disease/

We merge every file that ends with `_curated.csv`.

Output:
    data/master_biomarker_disease_edges.csv

This file is what import_biomarker_disease_edges.py will read.
"""

import csv
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(message)s",
)

DATA_DIR = Path("data/biomarker_disease")
OUTPUT_PATH = Path("data/master_biomarker_disease_edges.csv")


def find_curated_files():
    """Return a sorted list of *_curated.csv files in DATA_DIR."""
    if not DATA_DIR.exists():
        logging.error("Folder %s does not exist", DATA_DIR)
        return []

    files = sorted(DATA_DIR.glob("*_curated.csv"))
    logging.info("Found %d curated CSVs:", len(files))
    for f in files:
        logging.info("  - %s", f.name)
    return files


def merge_curated_files(files):
    """
    Merge curated CSVs into one list of dictionaries.

    We assume they all share the same header:
        biomarker_name, biomarker_id, biomarker_source_id,
        disease_name, doid, is_cancer_like, specimen_type,
        evidence_type, evidence_source, evidence_ref,
        pubmed_query, pubmed_count, strength, notes
    """
    merged_rows = []
    expected_header = None

    for path in files:
        logging.info("Reading %s ...", path.name)
        with path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            header = reader.fieldnames

            if expected_header is None:
                expected_header = header
                logging.info("  Header: %s", header)
            else:
                # Basic schema check
                if header != expected_header:
                    logging.warning(
                        "  WARNING: Header mismatch in %s.\n"
                        "           Expected: %s\n"
                        "           Found:    %s",
                        path.name,
                        expected_header,
                        header,
                    )

            row_count = 0
            for row in reader:
                # Skip completely empty lines
                if all((v is None or str(v).strip() == "") for v in row.values()):
                    continue
                merged_rows.append(row)
                row_count += 1

            logging.info("  Added %d data rows from %s", row_count, path.name)

    logging.info("Total merged rows: %d", len(merged_rows))
    return expected_header, merged_rows


def write_master_csv(header, rows):
    """Write merged rows to OUTPUT_PATH."""
    if header is None:
        logging.info("No header/rows found; writing an empty master file.")
        header = [
            "biomarker_name",
            "biomarker_id",
            "biomarker_source_id",
            "disease_name",
            "doid",
            "is_cancer_like",
            "specimen_type",
            "evidence_type",
            "evidence_source",
            "evidence_ref",
            "pubmed_query",
            "pubmed_count",
            "strength",
            "notes",
        ]

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    logging.info("Writing master CSV to %s ...", OUTPUT_PATH)

    with OUTPUT_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    logging.info("DONE. Wrote %d rows (plus header) to %s", len(rows), OUTPUT_PATH)


def main():
    files = find_curated_files()
    header, rows = merge_curated_files(files)
    write_master_csv(header, rows)


if __name__ == "__main__":
    main()
