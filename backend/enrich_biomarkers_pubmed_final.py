import pandas as pd
import requests
import urllib.parse
import time
import sys
import argparse

API_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"

EMAIL = "kabirspatel@outlook.com"
TOOL  = "urine_biomarker_project"
API_KEY = "YOUR_KEY_HERE"

# --- Simple gene synonym table (expand later programmatically)
GENE_SYNONYMS = {
    "ERBB2": ["HER2", "NEU", "ERBB2"],
    "BRCA1": ["BRCA1"],
    "BRCA2": ["BRCA2"],
    "PIK3CA": ["PIK3CA"],
    "ALK": ["ALK"],
    "FGFR3": ["FGFR3"],
    "KIT": ["KIT"],
    "KRAS": ["KRAS", "K-RAS"]
}

def run_pubmed_query(query):
    """Runs a PubMed query and returns count (or 0 on error)."""
    encoded_query = urllib.parse.quote(query)

    url = f"{API_BASE}?db=pubmed&retmode=json&term={encoded_query}&email={EMAIL}&tool={TOOL}&api_key={API_KEY}"

    try:
        r = requests.get(url, timeout=10)
        if r.status_code != 200:
            print(f"[WARN] HTTP {r.status_code} for query: {query}")
            return 0

        data = r.json()
        return int(data.get("esearchresult", {}).get("count", 0))

    except Exception as e:
        print(f"[ERR] Query failed: {query} : {e}")
        return 0


def tiered_pubmed_count(biomarker, disease):
    """Attempts Tier A → B → C queries."""
    # Tier A: biomarker AND disease
    qA = f'"{biomarker}"[Title/Abstract] AND "{disease}"[Title/Abstract]'
    count = run_pubmed_query(qA)
    if count > 0:
        return qA, count

    # Tier B: biomarker alone
    qB = f'"{biomarker}"[Title/Abstract]'
    count = run_pubmed_query(qB)
    if count > 0:
        return qB, count

    # Tier C: biomarker synonyms
    gene = biomarker.split()[0].replace("+","").replace("-","")
    synonyms = GENE_SYNONYMS.get(gene, [gene])

    for syn in synonyms:
        qC = f'"{syn}"[Title/Abstract]'
        count = run_pubmed_query(qC)
        if count > 0:
            return qC, count

    # Nothing found
    return qA, 0


def main(max_pairs):
    df = pd.read_csv("data/biomarker_matrix_full.csv")

    out_rows = []
    total = len(df)
    limit = min(total, max_pairs)

    print(f"[INFO] Enriching {limit} biomarker–disease pairs")

    for i in range(limit):
        row = df.iloc[i]
        biomarker = row["name"]
        diseases = str(row["diseases"]).split(";")

        for disease in diseases:
            disease = disease.strip()

            if disease == "":
                continue

            query, count = tiered_pubmed_count(biomarker, disease)
            print(f"[INFO] {biomarker} AND {disease} → {count}")

            out_rows.append({
                "biomarker": biomarker,
                "disease": disease,
                "pubmed_query": query,
                "pubmed_count": count
            })

            time.sleep(0.34)  # stay under NCBI limit

    pd.DataFrame(out_rows).to_csv("data/enriched_biomarker_diseases_v2.csv", index=False)
    print("[DONE] Saved to data/enriched_biomarker_diseases_v2.csv")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--max_pairs", type=int, default=200)
    args = ap.parse_args()
    main(args.max_pairs)
