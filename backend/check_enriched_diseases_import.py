import os
from neo4j import GraphDatabase

URI      = os.environ.get("NEO4J_URI")
USER     = os.environ.get("NEO4J_USER", "neo4j")
PASSWORD = os.environ.get("NEO4J_PASSWORD")

driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

with driver.session() as session:
    total = session.run("MATCH (d:Disease) RETURN count(d) AS c").single()["c"]
    cancer_like = session.run(
        "MATCH (d:Disease) WHERE d.is_cancer_like = 1 RETURN count(d) AS c"
    ).single()["c"]
    sample = session.run(
        "MATCH (d:Disease) RETURN d.doid AS doid, d.name AS name LIMIT 5"
    ).data()

print("Total Disease nodes:", total)
print("Cancer-like diseases:", cancer_like)
print("Sample diseases:")
for row in sample:
    print(" -", row["doid"], "→", row["name"])

driver.close()
