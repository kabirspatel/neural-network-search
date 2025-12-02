#!/usr/bin/env python
import csv
from pathlib import Path

# Folder where all biomarker–disease CSVs will live
INPUT_DIR = Path("data/biomarker_disease")

# Unified schema for all files
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

# Minimal starter rows for each file.
# These are just examples so you can see the structure;
# you can edit / add more later.
STARTER_FILES = {
    # 1) Cancer-focused biomarkers (you already have many of these)
    "01_cancer_curated.csv": [
        {
            "biomarker_name": "ERBB2 amplification",
            "biomarker_id": "",
            "biomarker_source_id": "HGNC:3430",
            "disease_name": "Breast adenocarcinoma",
            "doid": "DOID:3458",
            "is_cancer_like": "1",
            "specimen_type": "tumor_tissue",
            "evidence_type": "literature_review",
            "evidence_source": "PubMed",
            "evidence_ref": "PMIDs: add_later",
            "pubmed_query": 'ERBB2 amplification[Title/Abstract] AND "breast cancer"[Title/Abstract]',
            "pubmed_count": "",
            "strength": "strong",
            "notes": "HER2+ breast cancer context",
        },
        {
            "biomarker_name": "EZH2 (A692V,Y646C,Y646F,Y646H,Y646N,Y646S,A682G)",
            "biomarker_id": "",
            "biomarker_source_id": "HGNC:3527",
            "disease_name": "Follicular lymphoma",
            "doid": "DOID:0050873",
            "is_cancer_like": "1",
            "specimen_type": "tumor_tissue",
            "evidence_type": "literature_review",
            "evidence_source": "PubMed",
            "evidence_ref": "PMIDs: add_later",
            "pubmed_query": 'EZH2[Title/Abstract] AND "follicular lymphoma"[Title/Abstract]',
            "pubmed_count": "",
            "strength": "strong",
            "notes": "",
        },
    ],

    # 2) Metabolic / renal (non-cancer; includes urine)
    "02_metabolic_renal_curated.csv": [
        {
            "biomarker_name": "Urinary albumin-to-creatinine ratio (ACR)",
            "biomarker_id": "",
            "biomarker_source_id": "",
            "disease_name": "Diabetic kidney disease",
            "doid": "",
            "is_cancer_like": "0",
            "specimen_type": "urine",
            "evidence_type": "clinical_guideline",
            "evidence_source": "KDIGO / ADA",
            "evidence_ref": "guidelines: add_later",
            "pubmed_query": '"albumin creatinine ratio"[Title/Abstract] AND "diabetic kidney disease"[Title/Abstract]',
            "pubmed_count": "",
            "strength": "strong",
            "notes": "Classic marker for microalbuminuria / DKD",
        },
        {
            "biomarker_name": "Urinary glucose",
            "biomarker_id": "",
            "biomarker_source_id": "",
            "disease_name": "Poorly controlled diabetes mellitus",
            "doid": "",
            "is_cancer_like": "0",
            "specimen_type": "urine",
            "evidence_type": "clinical_guideline",
            "evidence_source": "ADA",
            "evidence_ref": "guidelines: add_later",
            "pubmed_query": '"urinary glucose"[Title/Abstract] AND diabetes[Title/Abstract]',
            "pubmed_count": "",
            "strength": "moderate",
            "notes": "",
        },
    ],

    # 3) Cardiovascular / systemic (mostly blood, non-cancer)
    "03_cardiovascular_curated.csv": [
        {
            "biomarker_name": "High-sensitivity troponin I",
            "biomarker_id": "",
            "biomarker_source_id": "",
            "disease_name": "Acute myocardial infarction",
            "doid": "",
            "is_cancer_like": "0",
            "specimen_type": "blood",
            "evidence_type": "clinical_guideline",
            "evidence_source": "ESC/ACC",
            "evidence_ref": "guidelines: add_later",
            "pubmed_query": '"high sensitivity troponin"[Title/Abstract] AND "myocardial infarction"[Title/Abstract]',
            "pubmed_count": "",
            "strength": "strong",
            "notes": "",
        },
        {
            "biomarker_name": "NT-proBNP",
            "biomarker_id": "",
            "biomarker_source_id": "",
            "disease_name": "Heart failure",
            "doid": "",
            "is_cancer_like": "0",
            "specimen_type": "blood",
            "evidence_type": "clinical_guideline",
            "evidence_source": "ESC/ACC",
            "evidence_ref": "guidelines: add_later",
            "pubmed_query": '"NT-proBNP"[Title/Abstract] AND "heart failure"[Title/Abstract]',
            "pubmed_count": "",
            "strength": "strong",
            "notes": "",
        },
    ],

    # 4) Infectious / urologic & other urine-based conditions
    "04_infectious_urologic_curated.csv": [
        {
            "biomarker_name": "Leukocyte esterase (dipstick)",
            "biomarker_id": "",
            "biomarker_source_id": "",
            "disease_name": "Urinary tract infection",
            "doid": "",
            "is_cancer_like": "0",
            "specimen_type": "urine",
            "evidence_type": "clinical_guideline",
            "evidence_source": "IDSA",
            "evidence_ref": "guidelines: add_later",
            "pubmed_query": '"leukocyte esterase"[Title/Abstract] AND "urinary tract infection"[Title/Abstract]',
            "pubmed_count": "",
            "strength": "moderate",
            "notes": "",
        },
        {
            "biomarker_name": "Urinary nitrites (dipstick)",
            "biomarker_id": "",
            "biomarker_source_id": "",
            "disease_name": "Urinary tract infection by nitrate-reducing bacteria",
            "doid": "",
            "is_cancer_like": "0",
            "specimen_type": "urine",
            "evidence_type": "clinical_guideline",
            "evidence_source": "IDSA",
            "evidence_ref": "guidelines: add_later",
            "pubmed_query": '"urinary nitrite"[Title/Abstract] AND "urinary tract infection"[Title/Abstract]',
            "pubmed_count": "",
            "strength": "moderate",
            "notes": "",
        },
        {
            "biomarker_name": "Urinary hCG",
            "biomarker_id": "",
            "biomarker_source_id": "",
            "disease_name": "Pregnancy",
            "doid": "",
            "is_cancer_like": "0",
            "specimen_type": "urine",
            "evidence_type": "diagnostic_test",
            "evidence_source": "clinical_practice",
            "evidence_ref": "standard_pregnancy_tests",
            "pubmed_query": '"urinary hCG"[Title/Abstract] AND pregnancy[Title/Abstract]',
            "pubmed_count": "",
            "strength": "strong",
            "notes": "",
        },
    ],
}


def main():
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    for filename, starter_rows in STARTER_FILES.items():
        path = INPUT_DIR / filename
        if path.exists():
            print(f"[INFO] {path} already exists, skipping (delete it if you want to regenerate).")
            continue

        print(f"[INFO] Creating {path}")
        with path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=COLUMNS)
            writer.writeheader()
            for row in starter_rows:
                # ensure all columns exist
                full_row = {col: row.get(col, "") for col in COLUMNS}
                writer.writerow(full_row)

    print("[INFO] Done. You can now edit the CSVs in data/biomarker_disease/.")

if __name__ == "__main__":
    main()
