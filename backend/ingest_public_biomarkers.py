import os
import time
import json
import requests
from tqdm import tqdm
from neo4j import GraphDatabase

from pathlib import Path

# -----------------------------
# 1. Configuration
# -----------------------------

# Load secrets from .streamlit/secrets.toml-like env if running locally.
# Easiest: export them manually or just hard-code for now if needed.
NEO4J_URI = os.environ.get("NEO4J_URI") or "neo4j+s://<your-aura-uri>.databases.neo4j.io"
NEO4J_USER = os.environ.get("NEO4J_USER") or "neo4j"
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD") or "<your-password>"

NCBI_EMAIL = os.environ.get("NCBI_EMAIL") or "your_email@example.com"
NCBI_TOOL = os.environ.get("NCBI_TOOL") or "urine_biomarker_project"

EUTILS_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"

# You can expand this list over time.
# These will ALL be guaranteed to be present in your graph.
BIOMARKER_SEEDS = [
    {
        "name": "glucose",
        "aliases": ["blood glucose", "plasma glucose", "glucose test"],
    },
    {
        "name": "HbA1c",
        "aliases": ["hemoglobin A1c", "glycated hemoglobin", "HbA1c test"],
    },
    {
        "name": "creatinine",
        "aliases": ["serum creatinine", "creatinine test"],
    },
    {
        "name": "albumin",
        "aliases": ["serum albumin", "urine albumin", "microalbuminuria"],
    },
    {
        "name": "PSA",
        "aliases": ["prostate specific antigen", "PSA test"],
    },
]

# Simple specimen vocabulary
SPECIMEN_KEYWORDS = {
    "urine": ["urine", "urinary"],
    "blood": ["blood", "whole blood"],
    "plasma": ["plasma"],
    "serum": ["serum"],
    "csf": ["cerebrospinal fluid", "csf"],
    "saliva": ["saliva", "salivary"],
}


# -----------------------------
# 2. NCBI helper functions
# -----------------------------

def pubmed_search(term, retmax=30):
    """Search PubMed for a term + 'biomarker', return list of PMIDs."""
    params = {
        "db": "pubmed",
        "term": f"{term}[Title/Abstract] AND biomarker",
        "retmode": "json",
        "retmax": retmax,
        "tool": NCBI_TOOL,
        "email": NCBI_EMAIL,
    }
    r = requests.get(f"{EUTILS_BASE}/esearch.fcgi", params=params, timeout=20)
    r.raise_for_status()
    data = r.json()
    return data.get("esearchresult", {}).get("idlist", [])


def pubmed_fetch_xml(pmids):
    """Fetch XML for a list of PMIDs (max ~200 at a time)."""
    if not pmids:
        return ""

    params = {
        "db": "pubmed",
        "id": ",".join(pmids),
        "retmode": "xml",
        "tool": NCBI_TOOL,
        "email": NCBI_EMAIL,
    }
    r = requests.get(f"{EUTILS_BASE}/efetch.fcgi", params=params, timeout=30)
    r.raise_for_status()
    return r.text


def infer_specimens_from_text(text):
    """Very simple heuristic: look for specimen keywords in title/abstract."""
    found = set()
    lower = text.lower()
    for specimen, keywords in SPECIMEN_KEYWORDS.items():
        for kw in keywords:
            if kw in lower:
                found.add(specimen)
                break
    return sorted(found)


def extract_diseases_from_mesh(xml_text):
    """Parse XML and pull out MeSH headings that look like diseases."""
    import xml.etree.ElementTree as ET

    diseases = set()

    if not xml_text:
        return []

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []

    # Very simple: get all DescriptorName elements
    for desc in root.findall(".//DescriptorName"):
        name = "".join(desc.itertext()).strip()
        if not name:
            continue
        # crude heuristic: consider it a "disease" if it contains words
        # like 'disease', 'cancer', 'syndrome', etc.
        lname = name.lower()
        if any(tok in lname for tok in ["disease", "cancer", "syndrome", "carcinoma", "diabetes", "lymphoma"]):
            diseases.add(name)

    return sorted(diseases)


def extract_specimens_and_diseases(term):
    """High-level helper: search PubMed, fetch XML, infer specimens + diseases."""
    pmids = pubmed_search(term)
    if not pmids:
        return [], []

    # Fetch in one chunk for now
    xml_text = pubmed_fetch_xml(pmids)

    # Specimens: scan titles + abstracts
    import re
    specimens = set()

    # Grab Title and AbstractText elements as raw text
    try:
        import xml.etree.ElementTree as ET
        root = ET.fromstring(xml_text)
    except Exception:
        root = None

    if root is not None:
        for art in root.findall(".//PubmedArticle"):
            # Title
            title_el = art.find(".//ArticleTitle")
            title = "".join(title_el.itertext()) if title_el is not None else ""
            # Abstract
            abstract_texts = []
            for a in art.findall(".//AbstractText"):
                abstract_texts.append("".join(a.itertext()))
            abstract = " ".join(abstract_texts)

            text = f"{title} {abstract}"
            for sp in infer_specimens_from_text(text):
                specimens.add(sp)

    diseases = set(extract_diseases_from_mesh(xml_text))
    return sorted(specimens), sorted(diseases)


# -----------------------------
# 3. Neo4j writing logic
# -----------------------------

def get_driver():
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


def upsert_biomarker(tx, biomarker_name, specimens, diseases, source, aliases):
    """
    Create / update:
      (b:Biomarker)
      (s:Specimen)
      (d:Disease)
      relationships:
        (b)-[:MEASURED_IN]->(s)
        (b)-[:ASSOCIATED_WITH]->(d)
    """
    cypher = """
    MERGE (b:Biomarker {name: $name})
      ON CREATE SET b.created_at = timestamp(),
                    b.specimen_source = $source,
                    b.aliases = $aliases
      ON MATCH SET  b.specimen_source = coalesce(b.specimen_source, $source),
                    b.aliases = coalesce(b.aliases, $aliases)

    WITH b
    UNWIND $specimens AS sp
      MERGE (s:Specimen {name: sp})
      MERGE (b)-[:MEASURED_IN]->(s)

    WITH b
    UNWIND $diseases AS ds
      MERGE (d:Disease {name: ds})
      MERGE (b)-[:ASSOCIATED_WITH]->(d)
    """
    tx.run(
        cypher,
        name=biomarker_name,
        specimens=specimens,
        diseases=diseases,
        source=source,
        aliases=aliases,
    )


def ingest_all_biomarkers():
    driver = get_driver()
    total = 0

    with driver.session() as session:
        for seed in tqdm(BIOMARKER_SEEDS, desc="Biomarkers"):
            name = seed["name"]
            aliases = seed.get("aliases", [])

            # Use "name" as the main search term; aliases could be used later
            specimens, diseases = extract_specimens_and_diseases(name)

            # If we failed to infer anything, at least tag with "unknown"
            if not specimens:
                specimens = ["unknown"]

            print(f"\n{name}: specimens={specimens}, diseases={diseases[:5]}...")

            session.execute_write(
                upsert_biomarker,
                biomarker_name=name,
                specimens=specimens,
                diseases=diseases,
                source="ncbi_pubmed_v1",
                aliases=aliases,
            )
            total += 1
            time.sleep(0.34)  # be kind to NCBI

    driver.close()
    print(f"\nDONE. Biomarkers ingested/updated: {total}")


if __name__ == "__main__":
    ingest_all_biomarkers()
