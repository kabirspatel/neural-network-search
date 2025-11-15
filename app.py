import os

import streamlit as st
from neo4j import GraphDatabase
from dotenv import load_dotenv


# -----------------------------
#  Neo4j connection
# -----------------------------
load_dotenv()  # loads NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD from .env

def get_secret(name: str) -> str | None:
    # Try Streamlit Cloud secrets first, then .env
    if "secrets" in dir(st) and name in st.secrets:
        return st.secrets[name]
    return os.getenv(name)

NEO4J_URI = get_secret("NEO4J_URI")
NEO4J_USER = get_secret("NEO4J_USER")
NEO4J_PASSWORD = get_secret("NEO4J_PASSWORD")

driver = GraphDatabase.driver(
    NEO4J_URI,
    auth=(NEO4J_USER, NEO4J_PASSWORD),
)

@st.cache_resource
def get_driver():
    if not (NEO4J_URI and NEO4J_USER and NEO4J_PASSWORD):
        raise RuntimeError("Neo4j connection variables are missing. Check your .env file.")
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


driver = get_driver()


def run_query(query: str, params: dict | None = None):
    """Helper to run Cypher and return list of dicts."""
    with driver.session(database="neo4j") as session:
        result = session.run(query, params or {})
        return [record.data() for record in result]


@st.cache_data
def get_specimen_options():
    q = "MATCH (s:Specimen) RETURN DISTINCT s.name AS name ORDER BY name"
    rows = run_query(q)
    return [r["name"] for r in rows]


@st.cache_data
def get_method_options():
    q = "MATCH (m:Method) RETURN DISTINCT m.name AS name ORDER BY name"
    rows = run_query(q)
    return [r["name"] for r in rows]


def search_biomarkers(term: str, specimen: str | None, method: str | None, limit: int = 30):
    """
    Search biomarkers by biomarker name or disease name,
    optionally filtered by specimen + method.
    """
    cypher = """
    MATCH (b:Biomarker)
    OPTIONAL MATCH (b)-[:ASSOCIATED_WITH]->(d:Disease)
    OPTIONAL MATCH (b)-[:MEASURED_IN]->(s:Specimen)
    OPTIONAL MATCH (b)-[:DETECTED_BY]->(m:Method)
    WHERE
      ($term = '' OR
       toLower(b.name) CONTAINS toLower($term) OR
       toLower(d.name) CONTAINS toLower($term))
      AND ($specimen IS NULL OR s.name = $specimen)
      AND ($method IS NULL OR m.name = $method)
    RETURN
      b.name AS biomarker,
      collect(DISTINCT d.name) AS diseases,
      collect(DISTINCT s.name) AS specimens,
      collect(DISTINCT m.name) AS methods
    ORDER BY b.name
    LIMIT $limit
    """
    params = {
        "term": term or "",
        "specimen": specimen,
        "method": method,
        "limit": limit,
    }
    return run_query(cypher, params)


# -----------------------------
#  Streamlit UI
# -----------------------------
st.title("Biomarker / Disease / Method Search")

st.write(
    "Search across your **biomarkers**, linked **diseases**, "
    "**specimens** (biofluids), and **detection methods** from the Aura graph."
)

search_term = st.text_input(
    "Search by biomarker or disease name",
    placeholder="e.g., BRCA1, breast adenocarcinoma, troponin, etc.",
)

col1, col2, col3 = st.columns([1, 1, 1])

with col1:
    specimen_choice = st.selectbox(
        "Specimen filter",
        options=["(any)"] + get_specimen_options(),
        index=0,
    )
    specimen_filter = None if specimen_choice == "(any)" else specimen_choice

with col2:
    method_choice = st.selectbox(
        "Detection method filter",
        options=["(any)"] + get_method_options(),
        index=0,
    )
    method_filter = None if method_choice == "(any)" else method_choice

with col3:
    limit = st.number_input("Max results", min_value=5, max_value=200, value=30, step=5)

if st.button("Run search"):
    try:
        rows = search_biomarkers(search_term, specimen_filter, method_filter, limit)
    except Exception as e:
        st.error(f"Error querying Neo4j: {e}")
        st.stop()

    if not rows:
        st.info("No results found. Try broadening your search.")
    else:
        st.success(f"Found {len(rows)} biomarkers")
        for row in rows:
            with st.expander(row["biomarker"]):
                st.write("**Diseases:** ", ", ".join(row["diseases"]) or "—")
                st.write("**Specimens:** ", ", ".join(row["specimens"]) or "—")
                st.write("**Methods:** ", ", ".join(row["methods"]) or "—")
