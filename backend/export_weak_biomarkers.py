import os
import pandas as pd
from neo4j import GraphDatabase
from dotenv import load_dotenv

# Load env vars from .env if present
load_dotenv()

NEO4J_URI = os.environ["NEO4J_URI"]
NEO4J_USER = os.environ["NEO4J_USER"]
NEO4J_PASSWORD = os.environ["NEO4J_PASSWORD"]

# Where to write the CSV
OUTPUT_PATH = os.path.join("data", "weak_biomarkers.csv")

# How many rows to pull per round-trip to Neo4j
BATCH_SIZE = 2000


def get_driver():
    """Create a Neo4j driver."""
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


def fetch_batch(tx, skip, limit):
    """
    Fetch a single page of biomarker relationships.

    IMPORTANT: keep the query fairly small so Aura doesn't blow the memory limit.
    """
    query = """
    MATCH (b:Biomarker)-[r]->(x)
    RETURN
        id(b)            AS biomarker_id,
        b.name           AS biomarker,
        type(r)          AS rel_type,
        labels(x)[0]     AS target_label,
        x.name           AS target_name
    SKIP $skip LIMIT $limit
    """
    return list(tx.run(query, skip=skip, limit=limit))


def export_weak():
    """
    Export biomarker relationship data in pages to avoid Neo4j Aura memory errors.
    """
    # If CSV already exists, start fresh
    if os.path.exists(OUTPUT_PATH):
        os.remove(OUTPUT_PATH)

    driver = get_driver()

    total_rows = 0
    skip = 0
    first_batch = True

    with driver.session() as session:
        while True:
            # Fetch one page
            results = session.execute_read(fetch_batch, skip, BATCH_SIZE)
            if not results:
                break  # no more data

            # Convert Neo4j records -> pandas DataFrame
            df = pd.DataFrame([r.data() for r in results])

            # Append to CSV in chunks to keep memory low
            df.to_csv(
                OUTPUT_PATH,
                mode="a",
                header=first_batch,   # only write header once
                index=False
            )
            first_batch = False

            batch_size = len(df)
            total_rows += batch_size
            skip += BATCH_SIZE

            print(f"Wrote batch of {batch_size} rows (total so far: {total_rows})")

    driver.close()
    print(f"DONE. Wrote {total_rows} rows to {OUTPUT_PATH}")


if __name__ == "__main__":
    export_weak()
