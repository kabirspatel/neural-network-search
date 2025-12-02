# backend/check_biomarker_disease_edges_import.py
from neo4j import GraphDatabase
import os


def get_driver():
    uri = os.environ.get("NEO4J_URI")
    user = os.environ.get("NEO4J_USER")
    password = os.environ.get("NEO4J_PASSWORD")
    if not uri or not user or not password:
        raise RuntimeError(
            "NEO4J_URI, NEO4J_USER, and NEO4J_PASSWORD must be set in the environment"
        )
    return GraphDatabase.driver(uri, auth=(user, password))


def main():
    driver = get_driver()
    with driver.session() as session:
        total = session.run(
            """
            MATCH (:Biomarker)-[r:BIOMARKER_ASSOCIATED_WITH_DISEASE]->(:Disease)
            RETURN count(r) AS c
            """
        ).single()["c"]
        print(f"Total biomarker–disease edges: {total}")

        if total > 0:
            print("Sample edges:")
            result = session.run(
                """
                MATCH (b:Biomarker)-[r:BIOMARKER_ASSOCIATED_WITH_DISEASE]->(d:Disease)
                RETURN toString(b.biomarker_id) AS biomarker_id,
                       b.name                   AS biomarker_name,
                       d.doid                   AS doid,
                       d.name                   AS disease_name,
                       r.pubmed_count           AS pubmed_count
                LIMIT 10
                """
            )
            for row in result:
                print(
                    f"  {row['biomarker_id']} ({row['biomarker_name']}) "
                    f"→ {row['doid']} {row['disease_name']} "
                    f"(PubMed n={row['pubmed_count']})"
                )
        else:
            print("No BIOMARKER_ASSOCIATED_WITH_DISEASE edges found.")

    driver.close()


if __name__ == "__main__":
    main()
