# backend/search_queries.py

from typing import List, Dict, Any
from .neo4j_client import get_driver


# --------- Low-level helpers --------- #

def _run_read(cypher: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    driver = get_driver()
    with driver.session() as session:
        result = session.run(cypher, **params)
        return [record.data() for record in result]


# --------- High-level search functions --------- #

def search_by_disease(term: str, limit: int = 50) -> List[Dict[str, Any]]:
    """
    Search diseases by name and return biomarker–disease pairs
    with PubMed counts and specimens.
    """
    cypher = """
    MATCH (d:Disease)
    WHERE toLower(d.name) CONTAINS toLower($term)
    OPTIONAL MATCH (b:Biomarker)-[r:BIOMARKER_ASSOCIATED_WITH_DISEASE]->(d)
    OPTIONAL MATCH (d)-[:DETECTED_IN_SPECIMEN]->(ds:Specimen)
    OPTIONAL MATCH (b)-[:MEASURED_IN_SPECIMEN]->(bs:Specimen)
    WITH d, b, r,
         collect(DISTINCT ds.name) AS disease_specimens,
         collect(DISTINCT bs.name) AS biomarker_specimens
    RETURN
        d.name AS disease,
        coalesce(d.category, 'unknown') AS disease_category,
        b.name AS biomarker,
        r.pubmed_count AS pubmed_count,
        disease_specimens,
        biomarker_specimens
    ORDER BY pubmed_count DESC
    LIMIT $limit
    """
    return _run_read(cypher, {"term": term, "limit": limit})


def search_by_biomarker(term: str, limit: int = 50) -> List[Dict[str, Any]]:
    """
    Search biomarkers by name and return their associated diseases,
    PubMed counts, and specimens.
    """
    cypher = """
    MATCH (b:Biomarker)
    WHERE toLower(b.name) CONTAINS toLower($term)
    OPTIONAL MATCH (b)-[r:BIOMARKER_ASSOCIATED_WITH_DISEASE]->(d:Disease)
    OPTIONAL MATCH (b)-[:MEASURED_IN_SPECIMEN]->(s:Specimen)
    WITH b, d, r, collect(DISTINCT s.name) AS specimens
    RETURN
        b.name AS biomarker,
        d.name AS disease,
        coalesce(d.category, 'unknown') AS disease_category,
        r.pubmed_count AS pubmed_count,
        specimens
    ORDER BY pubmed_count DESC
    LIMIT $limit
    """
    return _run_read(cypher, {"term": term, "limit": limit})


def search_devices_by_method(method_term: str, limit: int = 100) -> List[Dict[str, Any]]:
    """
    Search devices by detection method (Immunoassay, Biosensor, LAMP, etc.)
    """
    cypher = """
    MATCH (d:Device)-[:USES_METHOD]->(m:DetectionMethod)
    WHERE toLower(m.name) CONTAINS toLower($method)
    RETURN
        d.device_name AS device,
        d.product_code AS product_code,
        d.k_number AS k_number,
        m.name AS method
    LIMIT $limit
    """
    return _run_read(cypher, {"method": method_term, "limit": limit})


def search_methods_summary() -> List[Dict[str, Any]]:
    """
    Small helper to see how many devices use each detection method.
    """
    cypher = """
    MATCH (d:Device)-[:USES_METHOD]->(m:DetectionMethod)
    RETURN m.name AS method, count(d) AS device_count
    ORDER BY device_count DESC
    """
    return _run_read(cypher, {})
