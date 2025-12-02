#!/usr/bin/env python3
"""
Very simple importer: read biomarker–disease edges from a single CSV
and upsert them into Neo4j.

CSV: data/biomarker_disease_edges_pubmed.csv

Required columns:
    biomarker_id   (string, matches Biomarker.biomarker_id)
    doid           (string, matches Disease.doid)

Optional columns (if present they’ll be used, otherwise ignored):
    pubmed_query
    pubmed_count
    is_cancer_like
"""

import csv
import logging
import os
from typing import List, Dict, Any

from neo4j import GraphDatabase

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")

EDGES_CSV = "data/biomarker_disease_edges_pubmed.csv"  # <-- adjust name if needed

NEO4J_URI = os.environ.get("NEO4J_URI")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD")

if not NEO4J_URI or not NEO4J_PASSWORD:
    raise SystemExit(
        "NEO4J_URI and NEO4J_PASSWORD must be set. Example:\n"
        '  export NEO4J_URI="neo4j+s://<your-instance>.databases.neo4j.io"\n'
        '  export NEO4J_USER="neo4j"\n'
        '  export NEO4J_PASSWORD="your-password"'
    )


def _clean_int(value, default=0):
    if value is None:
        return default
    s = str(value).strip()
    if not s or s.lower() == "nan":
        return default
    try:
        return int(float(s))
    except ValueError:
        return default


def read_edges(path: str) -> List[Dict[str, Any]]:
    logging.info("Using edges CSV: %s", path)
    edges: List[Dict[str, Any]] = []

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=2):
            biomarker_id = (row.get("biomarker_id") or "").strip()
            doid = (row.get("doid") or "").strip()
            if not biomarker_id or not doid:
                logging.debug("Skipping row %d: missing biomarker_id or doid", i)
                continue

            edge = {
                "biomarker_id": biomarker_id,
                "doid": doid,
                "pubmed_query": (row.get("pubmed_query") or "").strip() or None,
                "pubmed_count": _clean_int(row.get("pubmed_count"), 0),
                "is_cancer_like": None,
            }

            is_cl = (row.get("is_cancer_like") or "").strip()
            if is_cl in {"0", "1"}:
                edge["is_cancer_like"] = int(is_cl)

            edges.append(edge)

    logging.info("Loaded %d biomarker–disease edges from %s ...", len(edges), path)
    return edges


def import_edges(edges: List[Dict[str, Any]]) -> None:
    if not edges:
        logging.info("No edges to import. Nothing to do.")
        return

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    cypher = """
    UNWIND $rows AS row
    MATCH (b:Biomarker {biomarker_id: row.biomarker_id})
    MATCH (d:Disease   {doid:          row.doid})
    MERGE (b)-[r:BIOMARKER_ASSOCIATED_WITH_DISEASE]->(d)
    SET r.pubmed_query   = row.pubmed_query,
        r.pubmed_count   = row.pubmed_count,
        r.is_cancer_like = coalesce(row.is_cancer_like, r.is_cancer_like)
    RETURN count(*) AS updated
    """

    with driver.session() as session:
        updated = session.run(cypher, rows=edges).single()["updated"]
        logging.info("Upserted %d biomarker–disease relationships.", updated)

    driver.close()


def main():
    edges = read_edges(EDGES_CSV)
    import_edges(edges)


if __name__ == "__main__":
    main()
