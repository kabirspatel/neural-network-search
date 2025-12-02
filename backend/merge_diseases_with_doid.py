import pandas as pd
import os

INPUT_DISEASES = "data/diseases.csv"          # your original disease table
INPUT_DOID = "data/disease_ontology.csv"      # built from obonet
OUTPUT = "data/diseases_enriched.csv"

def main():

    print("[INFO] Loading local disease table...")
    df_local = pd.read_csv(INPUT_DISEASES)

    print("[INFO] Loading DOID ontology table...")
    df_doid = pd.read_csv(INPUT_DOID)

    # --------------------------
    # Normalize disease names
    # --------------------------
    df_local["name_norm"] = df_local["name"].str.lower().str.strip()
    df_doid["name_norm"] = df_doid["name"].str.lower().str.strip()

    print("[INFO] Merging local diseases with DOID...")
    merged = pd.merge(
        df_local,
        df_doid[["doid", "name_norm", "synonyms", "is_cancer_like", "umls_ids", "icd10_ids"]],
        on="name_norm",
        how="left"
    )

    # -------------------------------------------------------
    # Add new DOID-only diseases not in local table
    # -------------------------------------------------------
    print("[INFO] Adding DOID-only diseases...")
    df_doid_only = df_doid[~df_doid["name_norm"].isin(df_local["name_norm"])]
    df_doid_only = df_doid_only.rename(columns={"doid": "doid_id"})

    # Create placeholder columns so concatenation works
    for col in ["disease_id", "name", "definition", "device_ids", "biomarker_ids"]:
        if col not in df_doid_only.columns:
            df_doid_only[col] = None

    df_final = pd.concat([merged, df_doid_only], ignore_index=True)

    print(f"[INFO] Writing enriched table → {OUTPUT}")
    df_final.to_csv(OUTPUT, index=False)

    print("[INFO] DONE — diseases_enriched.csv created successfully.")

if __name__ == "__main__":
    main()
