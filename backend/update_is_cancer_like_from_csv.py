import os
import logging

import pandas as pd
from neo4j import GraphDatabase

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")

NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "password")

DATA_PATH = os.path.join("data", "diseases_enriched.csv")


def load_truthy_rows() -> pd.DataFrame:
    logging.info("Loading cancer flags from %s ...", DATA_PATH)
    df = pd.read_csv(DATA_PATH, low_memory=False)
    truthy = df[df["is_cancer_like"] == 1].copy()
    logging.info("Found %d rows with is_cancer_like = 1", len(truthy))
    logging.info("Rows with non-null DOID: %d", truthy["doid"].notna().sum())
    return truthy


def update_by_doid(session, truthy: pd.DataFrame) -> int:
    # Use only DOID column, drop nulls + duplicates
    rows = (
        truthy[["doid"]]
        .dropna()
        .drop_duplicates()
        .to_dict("records")
    )
    logging.info("Updating by DOID for %d unique DOIDs", len(rows))
    if not rows:
        logging.info("No DOIDs to update.")
        return 0


    query = """
    UNWIND $rows AS row
    MATCH (d:Disease {doid: row.doid})
    // store as integer 1 so any queries using = 1 will match
    SET d.is_cancer_like = 1
    RETURN count(d) AS updated
    """


    result = session.run(query, rows=rows)
    updated = result.single()["updated"]
    logging.info("DOID-based update touched %d Disease nodes", updated)
    return updated


def main():
    truthy = load_truthy_rows()

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    try:
        with driver.session() as session:
            updated = update_by_doid(session, truthy)
    finally:
        driver.close()

    logging.info("DONE. is_cancer_like set on %d Disease nodes in Neo4j.", updated)


if __name__ == "__main__":
    main()
