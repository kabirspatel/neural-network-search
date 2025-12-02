#!/usr/bin/env python
"""
Merge weak relationships (from data/weak_biomarkers.csv) into the main
biomarker matrix (data/biomarker_matrix.csv).

Output: data/biomarker_matrix_full.csv

Columns preserved:
    biomarker_id, biomarker, specimens, devices, diseases, methods

We treat weak relationships as additional entries and take the union
of strong + weak targets per biomarker.
"""

import pandas as pd
from pathlib import Path


DATA_DIR = Path("data")
MATRIX_PATH = DATA_DIR / "biomarker_matrix.csv"
WEAK_PATH = DATA_DIR / "weak_biomarkers.csv"
OUTPUT_PATH = DATA_DIR / "biomarker_matrix_full.csv"


def load_data():
    print(f"Reading main matrix from {MATRIX_PATH} ...")
    matrix = pd.read_csv(MATRIX_PATH)

    print(f"Reading weak relationships from {WEAK_PATH} ...")
    weak = pd.read_csv(WEAK_PATH)

    # Basic sanity check
    required_cols = {"biomarker_id", "biomarker", "rel_type", "target_label", "target_name"}
    missing = required_cols - set(weak.columns)
    if missing:
        raise ValueError(f"weak_biomarkers.csv is missing columns: {missing}")

    return matrix, weak


def aggregate_weak_by_label(weak: pd.DataFrame):
    """
    Aggregate weak relationships by biomarker_id and target_label.

    Returns a dict mapping label -> DataFrame with:
        biomarker_id, <column_name>

    where <column_name> is one of: 'diseases_weak', 'specimens_weak', 'devices_weak', 'methods_weak'
    """
    label_to_col = {
        "Disease": "diseases_weak",
        "Specimen": "specimens_weak",
        "Device": "devices_weak",
        "Method": "methods_weak",
    }

    result = {}
    for label, col in label_to_col.items():
        subset = weak[weak["target_label"] == label]

        if subset.empty:
            # No rows of this label; skip to avoid empty joins
            print(f"[INFO] No weak relationships for label '{label}'")
            continue

        # Group by biomarker_id, aggregate unique target_name values
        agg = (
            subset.groupby("biomarker_id")["target_name"]
            .apply(lambda s: "; ".join(sorted(set(str(x).strip() for x in s if str(x).strip()))))
            .reset_index()
            .rename(columns={"target_name": col})
        )

        print(f"[INFO] Aggregated {len(agg)} biomarker rows for label '{label}' "
              f"into column '{col}'")
        result[label] = agg

    return result


def combine_text_lists(strong_val, weak_val):
    """
    Combine two semicolon-separated strings into a unique, sorted list.

    Examples:
        ("A; B", "B; C") -> "A; B; C"
        ("", "X") -> "X"
        (NaN, "X; Y") -> "X; Y"
    """
    items = []

    for val in (strong_val, weak_val):
        if isinstance(val, str) and val.strip():
            parts = [p.strip() for p in val.split(";") if p.strip()]
            items.extend(parts)

    if not items:
        return ""

    # Unique + sorted for determinism
    unique_sorted = sorted(set(items))
    return "; ".join(unique_sorted)


def main():
    matrix, weak = load_data()

    # Make sure biomarker_id is present and used as key
    if "biomarker_id" not in matrix.columns:
        raise ValueError("biomarker_matrix.csv must have a 'biomarker_id' column.")

    # Aggregate weak edges by label
    by_label = aggregate_weak_by_label(weak)

    # Start with matrix and left-join each aggregated table
    merged = matrix.copy()

    for label, agg_df in by_label.items():
        merged = merged.merge(agg_df, on="biomarker_id", how="left")

    # Now union strong + weak columns into the original columns
    col_pairs = [
        ("diseases", "diseases_weak"),
        ("specimens", "specimens_weak"),
        ("devices", "devices_weak"),
        ("methods", "methods_weak"),
    ]

    for strong_col, weak_col in col_pairs:
        if strong_col not in merged.columns and weak_col not in merged.columns:
            # nothing to do
            continue

        if strong_col not in merged.columns:
            # if there was only weak data, just rename
            if weak_col in merged.columns:
                merged[strong_col] = merged[weak_col]
                continue

        if weak_col not in merged.columns:
            # only strong exists, leave as-is
            continue

        # both exist: combine
        print(f"[INFO] Combining '{strong_col}' with '{weak_col}' ...")
        merged[strong_col] = merged.apply(
            lambda row: combine_text_lists(row.get(strong_col), row.get(weak_col)),
            axis=1,
        )

    # Drop *_weak helper columns
    drop_cols = [c for c in merged.columns if c.endswith("_weak")]
    if drop_cols:
        print(f"[INFO] Dropping helper columns: {drop_cols}")
        merged = merged.drop(columns=drop_cols)

    print(f"Writing enriched matrix to {OUTPUT_PATH} ...")
    merged.to_csv(OUTPUT_PATH, index=False)
    print("DONE. Preview:")
    print(merged.head(10))


if __name__ == "__main__":
    main()
