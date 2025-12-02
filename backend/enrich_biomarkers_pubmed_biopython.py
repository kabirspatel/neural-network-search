import pandas as pd
from Bio import Entrez
import time

Entrez.email = "kabispatel@outlook.com"
Entrez.tool = "urine_biomarker_project"

def pubmed_count(query):
    """Return PubMed hit count for a query."""
    try:
        handle = Entrez.esearch(db="pubmed", term=query, retmode="xml")
        record = Entrez.read(handle)
        return int(record["Count"])
    except Exception as e:
        print("[WARN] PubMed error:", e)
        return 0

def clean_term(text):
    """Clean the biomarker/disease names for PubMed."""
    return text.replace('"', '').replace("'", "").strip()

def build_query(biomarker, disease):
    return f"{clean_term(biomarker)}[Title/Abstract] AND {clean_term(disease)}[Title/Abstract]"

def run(input_csv, output_csv, max_rows=None):
    df = pd.read_csv(input_csv)
    if max_rows:
        df = df.head(max_rows)

    rows = []

    for i, row in df.iterrows():
        biomarker = row["name"]
        disease_list = str(row["diseases"]).split(";")

        for disease in disease_list:
            disease = disease.strip()
            if not disease:
                continue

            query = build_query(biomarker, disease)
            count = pubmed_count(query)

            print(f"[INFO] {biomarker} AND {disease} → {count}")
            rows.append({
                "biomarker": biomarker,
                "disease": disease,
                "query": query,
                "pubmed_count": count
            })

            time.sleep(0.34)  # safe rate limit

    out = pd.DataFrame(rows)
    out.to_csv(output_csv, index=False)
    print("[DONE] wrote", output_csv)


if __name__ == "__main__":
    run(
        input_csv="data/biomarker_matrix_full.csv",
        output_csv="data/enriched_biomarker_diseases_biopython.csv",
        max_rows=30  # change to None for full run
    )
