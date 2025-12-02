#!/usr/bin/env python
import csv
from pathlib import Path

INPUT_DIR = Path("data/biomarker_disease")
OUTPUT_FILE = Path("data/master_biomarker_disease_edges.csv")

# our universal schema
COLUMNS = [
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

def main():
    files = sorted(INPUT_DIR.glob("*.csv"))
    print("[INFO] Merging files:", [f.name for f in files])

    rows = []
    seen = set()  # to prevent duplicates

    for f in files:
        with f.open(newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                key = (
                    row["biomarker_name"],
                    row["disease_name"],
                    row["specimen_type"]
                )
                if key in seen:
                    continue
                seen.add(key)
                rows.append({col: row.get(col, "") for col in COLUMNS})

    print(f"[INFO] Writing master file with {len(rows)} rows...")
    with OUTPUT_FILE.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    print(f"[INFO] DONE → {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
