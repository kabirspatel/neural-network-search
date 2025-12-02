import os
from neo4j import GraphDatabase, basic_auth


def get_driver():
    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USER")
    password = os.getenv("NEO4J_PASSWORD")

    if not uri or not user or not password:
        raise RuntimeError(
            "NEO4J_URI, NEO4J_USER, and NEO4J_PASSWORD must be set as environment variables."
        )

    return GraphDatabase.driver(uri, auth=basic_auth(user, password))


def infer_from_diseases(session):
    """
    Infer biomarker -> method via:

        (b:Biomarker)-[:BIOMARKER_ASSOCIATED_WITH_DISEASE]->(d:Disease)
        (dev:Device)-[:INTENDED_FOR]->(d)
        (dev)-[:USES_METHOD]->(m:DetectionMethod)

    and create:

        (b)-[:MEASURED_BY_METHOD {source:'disease_path'}]->(m)
    """
    cypher = """
    MATCH (b:Biomarker)-[:BIOMARKER_ASSOCIATED_WITH_DISEASE]->(d:Disease)
    MATCH (dev:Device)-[:INTENDED_FOR]->(d)
    MATCH (dev)-[:USES_METHOD]->(m:DetectionMethod)
    WITH DISTINCT b, m

    MERGE (b)-[r:MEASURED_BY_METHOD]->(m)
      ON CREATE SET
        r.source = 'disease_path',
        r.created_at = datetime()
    RETURN count(r) AS total_relationships
    """

    result = session.run(cypher).single()
    total = result["total_relationships"] if result else 0
    print(f"[disease_path] Total biomarker–method relationships now in DB: {total}")


def infer_from_specimens(session):
    """
    Optional second pass using specimens:

        (b:Biomarker)-[:MEASURED_IN_SPECIMEN]->(s:Specimen)
        (d:Disease)-[:DETECTED_IN_SPECIMEN]->(s)
        (dev:Device)-[:INTENDED_FOR]->(d)
        (dev)-[:USES_METHOD]->(m:DetectionMethod)

    then:

        (b)-[:MEASURED_BY_METHOD {source:'specimen_path'}]->(m)
    """
    cypher = """
    MATCH (b:Biomarker)-[:MEASURED_IN_SPECIMEN]->(s:Specimen)
    MATCH (d:Disease)-[:DETECTED_IN_SPECIMEN]->(s)
    MATCH (dev:Device)-[:INTENDED_FOR]->(d)
    MATCH (dev)-[:USES_METHOD]->(m:DetectionMethod)
    WITH DISTINCT b, m

    MERGE (b)-[r:MEASURED_BY_METHOD]->(m)
      ON CREATE SET
        r.source = 'specimen_path',
        r.created_at = datetime()
    RETURN count(r) AS total_relationships
    """

    result = session.run(cypher).single()
    total = result["total_relationships"] if result else 0
    print(f"[specimen_path] Total biomarker–method relationships now in DB: {total}")


def main():
    driver = get_driver()
    with driver.session() as session:
        print("=== Inferring biomarker → detection method relationships ===")
        infer_from_diseases(session)
        infer_from_specimens(session)
    driver.close()
    print("Done.")


if __name__ == "__main__":
    main()
