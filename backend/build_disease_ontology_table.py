import pathlib
import requests
import obonet
import pandas as pd

DOID_OBO_URL = (
    "https://raw.githubusercontent.com/"
    "DiseaseOntology/HumanDiseaseOntology/main/src/ontology/doid.obo"
)

DATA_DIR = pathlib.Path("data")
DOID_OBO_PATH = DATA_DIR / "doid.obo"
OUT_CSV = DATA_DIR / "disease_ontology.csv"


def download_doid_obo():
    DATA_DIR.mkdir(exist_ok=True)
    if DOID_OBO_PATH.exists():
        print(f"[INFO] {DOID_OBO_PATH} already exists, skipping download.")
        return

    print(f"[INFO] Downloading Disease Ontology OBO from:\n      {DOID_OBO_URL}")
    resp = requests.get(DOID_OBO_URL, timeout=60)
    resp.raise_for_status()
    DOID_OBO_PATH.write_bytes(resp.content)
    print(f"[INFO] Saved OBO to {DOID_OBO_PATH}")


def parse_doid_obo_to_table():
    print(f"[INFO] Parsing {DOID_OBO_PATH} with obonet ...")
    graph = obonet.read_obo(DOID_OBO_PATH)

    rows = []

    for node_id, data in graph.nodes(data=True):
        # Skip non-DOID nodes, and obsolete terms
        if not node_id.startswith("DOID:"):
            continue
        if data.get("is_obsolete") == "true":
            continue

        name = data.get("name", "")

        # synonyms field may be a list of strings like:
        # '"foo" EXACT []'
        raw_syns = data.get("synonym", [])
        if isinstance(raw_syns, str):
            raw_syns = [raw_syns]

        def extract_syn_text(s):
            # naive but works: first quoted segment
            if '"' in s:
                return s.split('"')[1]
            return s

        synonyms = [extract_syn_text(s) for s in raw_syns]

        # parent DOIDs from 'is_a'
        parent_ids = data.get("is_a", [])
        if isinstance(parent_ids, str):
            parent_ids = [parent_ids]

        # xrefs: MeSH, UMLS, ICD, etc.
        xrefs = data.get("xref", [])
        if isinstance(xrefs, str):
            xrefs = [xrefs]

        mesh_ids = []
        umls_ids = []
        icd10_ids = []

        for x in xrefs:
            if x.startswith("MESH:"):
                mesh_ids.append(x.split("MESH:")[1])
            elif x.startswith("UMLS_CUI:"):
                umls_ids.append(x.split("UMLS_CUI:")[1])
            elif x.startswith("ICD10CM:"):
                icd10_ids.append(x.split("ICD10CM:")[1])

        # Very rough "is_cancer" tag:
        # check if "cancer" or "carcinoma" appears in name or synonyms
        text_for_flag = " ".join([name] + synonyms).lower()
        is_cancer = int("cancer" in text_for_flag or "carcinoma" in text_for_flag)

        rows.append(
            {
                "doid": node_id,
                "name": name,
                "synonyms": "; ".join(synonyms),
                "parent_doids": "; ".join(parent_ids),
                "mesh_ids": "; ".join(mesh_ids),
                "umls_ids": "; ".join(umls_ids),
                "icd10_ids": "; ".join(icd10_ids),
                "is_cancer_like": is_cancer,
            }
        )

    df = pd.DataFrame(rows).sort_values("name").reset_index(drop=True)
    df.to_csv(OUT_CSV, index=False)
    print(f"[INFO] Wrote {len(df)} rows to {OUT_CSV}")


def main():
    download_doid_obo()
    parse_doid_obo_to_table()


if __name__ == "__main__":
    main()
