import requests
import json
from neo4j import GraphDatabase
import os

# -----------------------
#  CONFIG
# -----------------------
MESH_LOOKUP_URL = "https://id.nlm.nih.gov/mesh/lookup/descriptor"

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


# -----------------------
#  Query MeSH
# -----------------------
def mesh_lookup(term: str):
    """
    Returns list of matched MeSH descriptors.
    """
    params = {"label": term, "match": "exact"}

    r = requests.get(MESH_LOOKUP_URL, params=params, headers={"Accept": "application/json"})
    
    # MeSH will return empty list if no match
    try:
        data = r.json()
    except Exception:
        print(f"[WARN] Non-JSON reply from MeSH for: {term}")
        return []

    return data


# -----------------------
#  Extract methods from MeSH tree numbers
# -----------------------
def extract_methods(mesh_records):
    """
    Pulls method-type MeSH fields (e.g., analytical techniques).
    """
    methods = set()

    for rec in mesh_records:
        # descriptorUI, label, terms, treeNumbers, etc.
        label = rec.get("label", "").lower()
        trees = rec.get("treeNumberList", [])

        # Method-related MeSH branches begin with:
        #   E05 — Investigative Techniques
        #   E05.318 — Analytical Techniques
        #   E05.598 — Laboratory Techniques
        #   E05.200 — Diagnostic Techniques
        for tree in trees:
            if tree.startswith(("E05", "E01")):
                methods.add(label)

    return list(methods)


# -----------------------
#  Update Neo4j
# -----------------------
def store_method_in_neo4j(biomarker, methods):
    with driver.session() as session:
        for m in methods:
            session.run("""
                MERGE (meth:DetectionMethod {name: $method})
                WITH meth
                MATCH (b:Biomarker {name: $biomarker})
                MERGE (b)-[:MEASURED_IN_METHOD]->(meth)
            """, method=m, biomarker=biomarker)


# -----------------------
#  MAIN
# -----------------------
def main():
    print("[INFO] Enriching methods using MeSH…")

    # pull biomarkers from Neo4j
    with driver.session() as session:
        result = session.run("MATCH (b:Biomarker) RETURN b.name AS biomarker LIMIT 200")  # sample for testing
        biomarkers = [r["biomarker"] for r in result]

    print(f"[INFO] Found {len(biomarkers)} biomarkers")

    for bio in biomarkers:
        print(f"[INFO] Querying MeSH for: {bio}")
        recs = mesh_lookup(bio)

        if not recs:
            print(f"  -> No MeSH match")
            continue

        methods = extract_methods(recs)

        if methods:
            print(f"  -> Found methods: {methods}")
            store_method_in_neo4j(bio, methods)
        else:
            print("  -> No method info found")

    print("[INFO] DONE — MeSH enrichment complete!")


if __name__ == "__main__":
    main()
