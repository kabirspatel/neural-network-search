import os
from neo4j import GraphDatabase

URI = os.getenv("NEO4J_URI")
USER = os.getenv("NEO4J_USER")
PASSWORD = os.getenv("NEO4J_PASSWORD")


def main():
    if not URI or not USER or not PASSWORD:
        raise SystemExit("NEO4J_URI / NEO4J_USER / NEO4J_PASSWORD must be set")

    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

    with driver.session() as session:
        print("[INFO] Cleaning old DetectionMethod nodes and USES_METHOD edges ...")

        # 1) Drop all DetectionMethod nodes (and their edges)
        drop_cypher = """
        MATCH (m:DetectionMethod)
        DETACH DELETE m;
        """
        session.run(drop_cypher).consume()

        print("[INFO] Rebuilding DetectionMethod nodes from Device names ...")

        # 2) Classify methods from Device.device_name
        classify_cypher = """
        MATCH (d:Device)
        WITH d, toLower(d.device_name) AS name
        WITH d,
          CASE
            WHEN name CONTAINS 'lamp' THEN 'LAMP'
            WHEN name CONTAINS ' pcr' OR name CONTAINS 'polymerase chain reaction' THEN 'PCR'
            WHEN name CONTAINS 'fluorescen' THEN 'Fluorescence'
            WHEN name CONTAINS 'immunoassay' OR name CONTAINS 'radioimmunoassay'
                 OR name CONTAINS 'immuno assay' OR name CONTAINS 'elisa' THEN 'Immunoassay'
            WHEN name CONTAINS 'dipstick' OR name CONTAINS 'lateral flow'
                 OR name CONTAINS 'test strip' OR name CONTAINS 'strip test' THEN 'Dipstick'
            WHEN name CONTAINS 'colorimetric' OR name CONTAINS 'colourimetric' THEN 'Colorimetric'
            WHEN name CONTAINS 'biosensor' THEN 'Biosensor'
            ELSE 'Analyzer'
          END AS method
        MERGE (m:DetectionMethod {name: method})
        MERGE (d)-[:USES_METHOD]->(m);
        """

        summary = session.run(classify_cypher).consume().counters
        print(
            f"[INFO] DetectionMethod nodes created: {summary.nodes_created}, "
            f"USES_METHOD relationships created: {summary.relationships_created}"
        )

    driver.close()
    print("[INFO] DONE. Detection methods wired into the graph.")


if __name__ == "__main__":
    main()
