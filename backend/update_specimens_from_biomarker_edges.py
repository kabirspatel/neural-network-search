import os
from neo4j import GraphDatabase

URI = os.getenv("NEO4J_URI")
USER = os.getenv("NEO4J_USER")
PASSWORD = os.getenv("NEO4J_PASSWORD")


def main():
    if not URI or not USER or not PASSWORD:
        raise SystemExit("NEO4J_URI / NEO4J_USER / NEO4J_PASSWORD must be set in the env")

    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

    with driver.session() as session:
        print("[INFO] Creating Specimen nodes from biomarker–disease edges ...")

        # 1) Create / match Specimen nodes from relationship properties
        create_specimens_cypher = """
        MATCH (b:Biomarker)-[r:BIOMARKER_ASSOCIATED_WITH_DISEASE]->(d:Disease)
        WITH DISTINCT trim(toLower(coalesce(r.specimen, r.specimen_type))) AS specimen
        WHERE specimen IS NOT NULL AND specimen <> ''
        MERGE (s:Specimen {name: specimen});
        """

        summary_nodes = session.run(create_specimens_cypher).consume().counters
        print(
            f"[INFO] Specimen nodes created: {summary_nodes.nodes_created}, "
            f"matched (approx) = {summary_nodes.nodes_created + summary_nodes.nodes_deleted * 0}"
        )

        # 2) Connect biomarkers and diseases to specimens
        create_rels_cypher = """
        MATCH (b:Biomarker)-[r:BIOMARKER_ASSOCIATED_WITH_DISEASE]->(d:Disease)
        WITH b, d, trim(toLower(coalesce(r.specimen, r.specimen_type))) AS specimen
        WHERE specimen IS NOT NULL AND specimen <> ''
        MATCH (s:Specimen {name: specimen})
        MERGE (b)-[:MEASURED_IN_SPECIMEN]->(s)
        MERGE (d)-[:DETECTED_IN_SPECIMEN]->(s);
        """

        summary_rels = session.run(create_rels_cypher).consume().counters
        print(
            f"[INFO] Specimen relationships created: {summary_rels.relationships_created}"
        )

    driver.close()
    print("[INFO] DONE. Specimens wired into the graph.")


if __name__ == "__main__":
    main()
