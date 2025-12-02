# backend/neo4j_client.py
import os
from neo4j import GraphDatabase

_driver = None

def get_driver():
    """
    Return a singleton Neo4j driver using env vars.
    """
    global _driver
    if _driver is None:
        uri = os.environ.get("NEO4J_URI")
        user = os.environ.get("NEO4J_USER")
        password = os.environ.get("NEO4J_PASSWORD")
        if not (uri and user and password):
            raise RuntimeError("NEO4J_URI / NEO4J_USER / NEO4J_PASSWORD not set")
        _driver = GraphDatabase.driver(uri, auth=(user, password))
    return _driver
