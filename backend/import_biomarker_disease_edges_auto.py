#!/usr/bin/env python3
"""
Import automatically generated biomarker–disease edges into Neo4j
from data/biomarker_disease_edges_auto.csv
"""

import csv
import os
from pathlib import Path

from neo4j import GraphDatabase

DATA_DIR = Path("data")
EDGES_CSV = DATA_DIR / "biomarker_disease_edges_auto.csv"

NEO4J_URI = os.environ.get("NEO4J_URI")
NEO4J_USER = os.environ.get("NEO4J_USER")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD")


def load_edges(path: Path):
    rows = []
    with path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Only keep pairs with some support
            try:
                count = int(row.get("pubmed_count", "0"))
            except ValueError:
                count = 0
            if count <= 0:
                continue
            rows.append(row)
    return rows


def import_edges(driver, rows):
    cypher = """
    MERGE (b:Biomarker {name: $biomarker_name})
      ON CREATE SET
        b.category = $biomarker_category
    MERGE (d:Disease {name: $disease_name})
      ON CREATE SET
        d.doid          = $doid,
        d.category      = $disease_category,
        d.is_cancer_like = CASE
            WHEN $is_cancer_like IS NULL OR $is_cancer_like = '' THEN 0
            ELSE toInteger($is_cancer_like)
        END
    MERGE (b)-[r:BIOMARKER_ASSOCIATED_WITH_DISEASE]->(d)
      ON CREATE SET
        r.specimen_type = $specimen_type,
        r.pubmed_query  = $pubmed_query,
        r.pubmed_count  = $pubmed_count
      ON MATCH SET
        r.pubmed_count  = $pubmed_count  // overwrite with latest count
    """

    with driver.session() as session:
        total = len(rows)
        for idx, row in enumerate(rows, start=1):
            if idx % 20 == 1 or idx == total:
                print(f"[INFO] Upserting row {idx}/{total} ...")
            session.run(
                cypher,
                biomarker_name=row["biomarker_name"],
                biomarker_category=row.get("biomarker_category", ""),
                disease_name=row["disease_name"],
                doid=row.get("doid", ""),
                disease_category=row.get("disease_category", ""),
                is_cancer_like=row.get("is_cancer_like", ""),
                specimen_type=row.get("specimen_type", ""),
                pubmed_query=row.get("pubmed_query", ""),
                pubmed_count=int(row.get("pubmed_count", "0") or 0),
            )


def main():
    if not EDGES_CSV.exists():
        raise SystemExit(f"{EDGES_CSV} does not exist. Run build_pubmed_edges_from_lists.py first.")

    print(f"[INFO] Using edges CSV: {EDGES_CSV}")
    rows = load_edges(EDGES_CSV)
    print(f"[INFO] Loaded {len(rows)} edges with pubmed_count > 0")

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    import_edges(driver, rows)
    driver.close()
    print("[INFO] DONE. Auto biomarker–disease edges imported into Neo4j.")


if __name__ == "__main__":
    main()
