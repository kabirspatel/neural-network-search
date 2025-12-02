#!/usr/bin/env python3
"""
Build biomarker–disease edges automatically by querying PubMed
for every biomarker × disease pair in our small seed lists.

Input:
  data/biomarker_list.csv
  data/disease_list.csv

Output:
  data/biomarker_disease_edges_auto.csv
"""

import csv
import time
import urllib.parse
import urllib.request
from pathlib import Path

DATA_DIR = Path("data")
BIOMARKER_CSV = DATA_DIR / "biomarker_list.csv"
DISEASE_CSV = DATA_DIR / "disease_list.csv"
OUT_CSV = DATA_DIR / "biomarker_disease_edges_auto.csv"

# Put your real email here so NCBI is happy
NCBI_EMAIL = "your_email@example.com"
NCBI_TOOL = "gt_biomarker_graph"
BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"

# Be polite: NCBI recommends <= 3 requests/sec without an API key
SLEEP_SECONDS = 0.4


def load_biomarkers(path: Path):
    rows = []
    with path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row.get("biomarker_name"):
                continue
            rows.append(row)
    return rows


def load_diseases(path: Path):
    rows = []
    with path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row.get("disease_name"):
                continue
            rows.append(row)
    return rows


def query_pubmed_count(biomarker: str, disease: str) -> int:
    """
    Use NCBI ESearch to get count of articles where both biomarker and disease
    appear in Title/Abstract.
    """
    term = f'{biomarker}[Title/Abstract] AND {disease}[Title/Abstract]'
    params = {
        "db": "pubmed",
        "term": term,
        "retmode": "xml",
        "rettype": "count",
        "tool": NCBI_TOOL,
        "email": NCBI_EMAIL,
    }
    url = BASE_URL + "?" + urllib.parse.urlencode(params)
    with urllib.request.urlopen(url) as resp:
        xml = resp.read().decode("utf-8", errors="ignore")
    # Very small XML: look for <Count>123</Count>
    start = xml.find("<Count>")
    end = xml.find("</Count>")
    if start == -1 or end == -1:
        return 0
    count_str = xml[start + len("<Count>"): end]
    try:
        return int(count_str)
    except ValueError:
        return 0


def main():
    print(f"[INFO] Loading biomarkers from {BIOMARKER_CSV} ...")
    biomarkers = load_biomarkers(BIOMARKER_CSV)
    print(f"[INFO] Loaded {len(biomarkers)} biomarkers.")

    print(f"[INFO] Loading diseases from {DISEASE_CSV} ...")
    diseases = load_diseases(DISEASE_CSV)
    print(f"[INFO] Loaded {len(diseases)} diseases.")

    out_fields = [
        "biomarker_name",
        "biomarker_category",
        "disease_name",
        "doid",
        "disease_category",
        "is_cancer_like",
        "specimen_type",
        "pubmed_query",
        "pubmed_count",
    ]

    out_rows = []

    total_pairs = len(biomarkers) * len(diseases)
    pair_idx = 0

    for b in biomarkers:
        for d in diseases:
            pair_idx += 1
            b_name = b["biomarker_name"].strip()
            d_name = d["disease_name"].strip()

            query = f'{b_name}[Title/Abstract] AND {d_name}[Title/Abstract]'
            print(
                f"[{pair_idx}/{total_pairs}] Querying PubMed for: "
                f"{b_name} × {d_name}"
            )
            count = query_pubmed_count(b_name, d_name)
            print(f"     -> count = {count}")

            row = {
                "biomarker_name": b_name,
                "biomarker_category": b.get("category", ""),
                "disease_name": d_name,
                "doid": d.get("doid", ""),
                "disease_category": d.get("category", ""),
                "is_cancer_like": d.get("is_cancer_like", ""),
                "specimen_type": b.get("specimen_type", ""),
                "pubmed_query": query,
                "pubmed_count": count,
            }
            out_rows.append(row)

            time.sleep(SLEEP_SECONDS)

    # Write CSV
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    print(f"[INFO] Writing {len(out_rows)} rows to {OUT_CSV} ...")
    with OUT_CSV.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=out_fields)
        writer.writeheader()
        writer.writerows(out_rows)

    print("[INFO] Done.")


if __name__ == "__main__":
    main()
