# backend/import_enriched_diseases.py

import os
import csv
from neo4j import GraphDatabase

NEO4J_URI = os.environ.get("NEO4J_URI")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD")

CSV_PATH = "data/diseases_enriched.csv"
BATCH_SIZE = 500


def read_enriched_diseases(path):
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            # Normalise empty strings to None
            clean = {k: (v if v != "" else None) for k, v in r.items()}

            # Make sure we have a name; otherwise skip
            if not clean.get("name"):
                continue

            # Convert is_cancer_like "0/1" to bool if present
            ic = clean.get("is_cancer_like")
            if ic in ("0", "1"):
                clean["is_cancer_like"] = (ic == "1")

            rows.append(clean)
    return rows


def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


MERGE_CYPHER = """
UNWIND $rows AS row
WITH row
WHERE row.name IS NOT NULL

// upsert by disease name
MERGE (d:Disease {name: row.name})

// Add / update properties from CSV.
// SET d += row merges all key/value pairs into d,
// overwriting existing properties with the new values.
SET d += row
"""


def import_enriched_diseases():
    if not NEO4J_URI or not NEO4J_PASSWORD:
        raise RuntimeError("NEO4J_URI and NEO4J_PASSWORD must be set in the environment")

    print(f"[INFO] Loading enriched disease table from {CSV_PATH} ...")
    rows = read_enriched_diseases(CSV_PATH)
    total = len(rows)
    print(f"[INFO] Loaded {total:,} disease rows from CSV.")

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    with driver:
        with driver.session(database="neo4j") as session:
            print(f"[INFO] Connecting to Neo4j at {NEO4J_URI} as {NEO4J_USER} ...")
            batch_count = 0
            for batch in chunked(rows, BATCH_SIZE):
                start = batch_count * BATCH_SIZE
                end = start + len(batch) - 1
                batch_count += 1
                print(
                    f"[INFO] [Batch {batch_count}] Upserting rows {start}–{end} "
                    f"({len(batch)} diseases) ..."
                )

                session.execute_write(lambda tx, b: tx.run(MERGE_CYPHER, rows=b), batch)

    print("[INFO] DONE. Enriched diseases upserted into Neo4j.")
    

if __name__ == "__main__":
    import_enriched_diseases()
