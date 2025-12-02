import pandas as pd
from pathlib import Path

DATA_DIR = Path("data")

def pick_id_and_name(df, label: str):
    """
    Try to infer the ID column and human-readable name column
    from a dataframe, based on common naming patterns.
    """
    cols = list(df.columns)
    lower = [c.lower() for c in cols]

    # ID column: something ending in _id or just 'id'
    id_col = None
    for c in cols:
        cl = c.lower()
        if cl.endswith("_id") or cl == "id":
            id_col = c
            break
    if id_col is None:
        id_col = cols[0]  # fall back to first column

    # Name column: contains 'name', 'label', or 'description'
    name_col = None
    for c in cols:
        cl = c.lower()
        if "name" in cl or "label" in cl or "desc" in cl:
            name_col = c
            break
    if name_col is None:
        # fall back to 2nd column if it exists, else same as id_col
        name_col = cols[1] if len(cols) > 1 else id_col

    print(f"[{label}] using id_col='{id_col}', name_col='{name_col}'")
    return id_col, name_col


def main():
    biomarkers = pd.read_csv(DATA_DIR / "biomarkers.csv")
    devices    = pd.read_csv(DATA_DIR / "devices.csv")
    diseases   = pd.read_csv(DATA_DIR / "diseases.csv")
    methods    = pd.read_csv(DATA_DIR / "methods.csv")
    specimens  = pd.read_csv(DATA_DIR / "specimens.csv")

    # Infer id/name columns for each table
    dev_id, dev_name       = pick_id_and_name(devices,   "devices")
    dis_id, dis_name       = pick_id_and_name(diseases,  "diseases")
    met_id, met_name       = pick_id_and_name(methods,   "methods")
    spec_id, spec_name     = pick_id_and_name(specimens, "specimens")

    id_to_name = {
        "device":   devices.set_index(dev_id)[dev_name].astype(str).to_dict(),
        "disease":  diseases.set_index(dis_id)[dis_name].astype(str).to_dict(),
        "method":   methods.set_index(met_id)[met_name].astype(str).to_dict(),
        "specimen": specimens.set_index(spec_id)[spec_name].astype(str).to_dict(),
    }

    biomatrix = biomarkers.copy()

    # Ensure these linkage columns exist, even if empty
    for col in ["device_ids", "disease_ids", "method_ids", "specimen_ids"]:
        if col not in biomatrix.columns:
            biomatrix[col] = ""

    def map_ids_to_names(id_str, mapping):
        if not isinstance(id_str, str) or not id_str.strip():
            return ""
        ids = [x.strip() for x in id_str.split(";") if x.strip()]
        names = [mapping.get(i, i) for i in ids]
        return "; ".join(sorted(set(names)))

    biomatrix["devices"]   = biomatrix["device_ids"].apply(
        lambda s: map_ids_to_names(s, id_to_name["device"])
    )
    biomatrix["diseases"]  = biomatrix["disease_ids"].apply(
        lambda s: map_ids_to_names(s, id_to_name["disease"])
    )
    biomatrix["methods"]   = biomatrix["method_ids"].apply(
        lambda s: map_ids_to_names(s, id_to_name["method"])
    )
    biomatrix["specimens"] = biomatrix["specimen_ids"].apply(
        lambda s: map_ids_to_names(s, id_to_name["specimen"])
    )

    out_csv = DATA_DIR / "biomarker_matrix.csv"
    biomatrix.to_csv(out_csv, index=False)
    print(f"Wrote {len(biomatrix)} rows to {out_csv}")

if __name__ == "__main__":
    main()
