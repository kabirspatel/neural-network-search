from neo4j import GraphDatabase
import os

uri = os.getenv("NEO4J_URI")
auth = (os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD"))

print("Connecting to:", uri)

driver = GraphDatabase.driver(uri, auth=auth)

with driver.session() as session:
    result = session.run("RETURN 1 AS ok").single()
    print("Neo4j OK =", result["ok"])
