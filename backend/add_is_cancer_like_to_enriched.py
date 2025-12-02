import pandas as pd
from pathlib import Path

ENRICHED_PATH = Path("data/diseases_enriched.csv")
DOID_PATH = Path("data/disease_ontology.csv")

print(f"[INFO] Loading enriched diseases from {ENRICHED_PATH} ...")
df_enriched = pd.read_csv(ENRICHED_PATH, low_memory=False)
print(f"[INFO] Enriched table rows: {len(df_enriched)}")
print(f"[INFO] Enriched columns: {list(df_enriched.columns)}")

print(f"[INFO] Loading DOID ontology from {DOID_PATH} ...")
df_doid = pd.read_csv(DOID_PATH, low_memory=False)

if "is_cancer_like" not in df_doid.columns:
    raise SystemExit("[ERROR] 'is_cancer_like' column not found in disease_ontology.csv")

print("[INFO] DOID is_cancer_like value_counts:")
print(df_doid["is_cancer_like"].value_counts(dropna=False))

# Keep only DOID + flag from ontology
df_doid_flag = df_doid[["doid", "is_cancer_like"]].copy()

# Normalise DOID strings just in case
df_doid_flag["doid"] = df_doid_flag["doid"].astype(str).str.strip()
df_enriched["doid"] = df_enriched["doid"].astype(str).str.strip()

# If enriched already has a cancer flag, drop it so we can replace cleanly
for col in ["is_cancer_like", "is_cancer_like_x", "is_cancer_like_y"]:
    if col in df_enriched.columns:
        print(f"[INFO] Dropping existing column '{col}' from enriched table.")
        df_enriched = df_enriched.drop(columns=[col])

print("[INFO] Merging DOID cancer flags into enriched table on 'doid' ...")
df_merged = df_enriched.merge(
    df_doid_flag,
    on="doid",
    how="left",
    validate="m:1",   # many enriched rows per DOID, exactly one flag per DOID
)

# Any DOID with no flag gets 0 (non-cancer-like)
df_merged["is_cancer_like"] = df_merged["is_cancer_like"].fillna(0).astype(int)

print("[INFO] New is_cancer_like value_counts in enriched table:")
print(df_merged["is_cancer_like"].value_counts(dropna=False))

backup_path = ENRICHED_PATH.with_suffix(".csv.bak_before_cancer_flag")
if not backup_path.exists():
    ENRICHED_PATH.rename(backup_path)
    print(f"[INFO] Backed up original enriched table to {backup_path}")
else:
    print(f"[WARN] Backup file {backup_path} already exists; not overwriting.")

print(f"[INFO] Writing updated enriched table back to {ENRICHED_PATH} ...")
df_merged.to_csv(ENRICHED_PATH, index=False)
print("[INFO] DONE. diseases_enriched.csv now includes is_cancer_like.")
