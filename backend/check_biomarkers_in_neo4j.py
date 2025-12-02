# backend/check_biomarkers_in_neo4j.py
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
        total = session.run("MATCH (b:Biomarker) RETURN count(b) AS c").single()["c"]
        print(f"Total Biomarker nodes: {total}")

        if total > 0:
            print("Sample biomarkers:")
            result = session.run(
                """
                MATCH (b:Biomarker)
                RETURN b.biomarker_id AS biomarker_id,
                       b.name          AS name
                LIMIT 5
                """
            )
            for row in result:
                print(f"  {row['biomarker_id']} → {row['name']}")
        else:
            print("No Biomarker nodes found. You need to import them before creating edges.")
    driver.close()


if __name__ == "__main__":
    main()
