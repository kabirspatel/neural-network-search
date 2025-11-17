import os
from pathlib import Path
import tempfile

import pandas as pd
import streamlit as st
from neo4j import GraphDatabase
from pyvis.network import Network


# -------------------------
# Neo4j connection helpers
# -------------------------

@st.cache_resource
def get_driver():
    """Create and cache a single Neo4j driver."""
    # Prefer Streamlit secrets (for Streamlit Cloud)
    uri = st.secrets.get("NEO4J_URI", os.getenv("NEO4J_URI"))
    user = st.secrets.get("NEO4J_USER", os.getenv("NEO4J_USER"))
    password = st.secrets.get("NEO4J_PASSWORD", os.getenv("NEO4J_PASSWORD"))

    if not uri or not user or not password:
        raise RuntimeError(
            "Neo4j connection details not found. "
            "Set NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD in Streamlit secrets or env vars."
        )

    driver = GraphDatabase.driver(uri, auth=(user, password))
    return driver


def run_cypher(query: str, params: dict | None = None):
    """Run a Cypher query and return result.data()."""
    driver = get_driver()
    with driver.session() as session:
        result = session.run(query, params or {})
        return result.data()


# -------------------------
# Data queries
# -------------------------

TABULAR_QUERY = """
// Tabular biomarker search with neighbor counts
CALL db.index.fulltext.queryNodes('biomarkerNameIndex', $q)
YIELD node, score
WITH node, score
ORDER BY score DESC
LIMIT $limit

OPTIONAL MATCH (node)<-[:MEASURES]-(d:Device)
OPTIONAL MATCH (node)-[:ASSOCIATED_DISEASE|INDICATES|CAUSES]->(di:Disease)
OPTIONAL MATCH (node)-[:MEASURED_IN]->(s:Specimen)
OPTIONAL MATCH (node)-[:DETECTED_BY]->(m:Method)

RETURN
  node.name                           AS biomarker,
  round(max(score), 3)                AS score,
  count(DISTINCT d)                   AS devices,
  count(DISTINCT di)                  AS diseases,
  count(DISTINCT s)                   AS specimens,
  count(DISTINCT m)                   AS methods
ORDER BY score DESC;
"""


GRAPH_QUERY = """
// Neighborhood around top N biomarkers
CALL db.index.fulltext.queryNodes('biomarkerNameIndex', $q)
YIELD node, score
WITH node, score
ORDER BY score DESC
LIMIT $max_biomarkers

WITH collect(node) AS biomarkers
UNWIND biomarkers AS b

OPTIONAL MATCH (b)<-[:MEASURES]-(d:Device)
OPTIONAL MATCH (b)-[:ASSOCIATED_DISEASE|INDICATES|CAUSES]->(di:Disease)
OPTIONAL MATCH (b)-[:MEASURED_IN]->(s:Specimen)
OPTIONAL MATCH (b)-[:DETECTED_BY]->(m:Method)

RETURN DISTINCT
  id(b)            AS bid,
  b.name           AS biomarker,
  id(d)            AS did,
  d.device_name    AS device,
  id(di)           AS disid,
  di.name          AS disease,
  id(s)            AS sid,
  s.name           AS specimen,
  id(m)            AS mid,
  m.name           AS method;
"""


def fetch_tabular_results(query: str, max_rows: int) -> pd.DataFrame:
    rows = run_cypher(TABULAR_QUERY, {"q": query, "limit": max_rows})
    if not rows:
        return pd.DataFrame(
            columns=["Biomarker", "Score", "Devices", "Diseases", "Specimens", "Methods"]
        )

    df = pd.DataFrame(rows)
    df.rename(
        columns={
            "biomarker": "Biomarker",
            "score": "Score",
            "devices": "Devices",
            "diseases": "Diseases",
            "specimens": "Specimens",
            "methods": "Methods",
        },
        inplace=True,
    )
    return df


def fetch_graph_rows(query: str, max_biomarkers: int):
    return run_cypher(GRAPH_QUERY, {"q": query, "max_biomarkers": max_biomarkers})


# -------------------------
# Graph construction
# -------------------------

def build_network(graph_rows: list[dict]) -> Network | None:
    """Build a PyVis Network from Cypher rows."""
    if not graph_rows:
        return None

    net = Network(height="600px", width="100%", bgcolor="#111111", font_color="white")
    net.barnes_hut()

    added_nodes: set[int] = set()

    def add_node(node_id: int | None, label: str | None, group: str):
        if node_id is None or label is None:
            return
        if node_id in added_nodes:
            return
        added_nodes.add(node_id)
        net.add_node(
            node_id,
            label=label,
            title=f"{group}: {label}",
            group=group,
        )

    for row in graph_rows:
        bid = row.get("bid")
        biomarker = row.get("biomarker")

        did = row.get("did")
        device = row.get("device")

        disid = row.get("disid")
        disease = row.get("disease")

        sid = row.get("sid")
        specimen = row.get("specimen")

        mid = row.get("mid")
        method = row.get("method")

        # Add nodes
        add_node(bid, biomarker, "Biomarker")
        add_node(did, device, "Device")
        add_node(disid, disease, "Disease")
        add_node(sid, specimen, "Specimen")
        add_node(mid, method, "Method")

        # Add edges from biomarker to its neighbors
        if bid is not None:
            if did is not None:
                net.add_edge(bid, did, title="MEASURES", color="#F39C12")
            if disid is not None:
                net.add_edge(bid, disid, title="ASSOCIATED_DISEASE", color="#E74C3C")
            if sid is not None:
                net.add_edge(bid, sid, title="MEASURED_IN", color="#1ABC9C")
            if mid is not None:
                net.add_edge(bid, mid, title="DETECTED_BY", color="#9B59B6")

    return net


def render_network(net: Network):
    """Render a PyVis network inside Streamlit."""
    if net is None:
        st.info("No graph neighborhood available for this query.")
        return

    # Save to a temporary HTML file and embed
    with tempfile.NamedTemporaryFile(delete=False, suffix=".html") as tmp:
        net.show(tmp.name)
        html_path = tmp.name

    with open(html_path, "r", encoding="utf-8") as f:
        html = f.read()

    # Use components.html without importing heavy modules at top level
    from streamlit.components.v1 import html as st_html

    st_html(html, height=650, scrolling=True)


# -------------------------
# Streamlit UI
# -------------------------

def main():
    st.set_page_config(
        page_title="Biomarker / Disease / Method Search",
        layout="wide",
        page_icon="🧬",
    )

    st.title("Biomarker / Disease / Method Search")
    st.write(
        "Search across your **biomarkers**, linked **diseases**, **devices**, "
        "**specimens** (biofluids) and **detection methods** from the Neo4j Aura graph."
    )

    # Sidebar controls
    st.sidebar.header("Search settings")

    query = st.sidebar.text_input(
        "Search by biomarker name or keyword", value="BRCA1"
    )

    # Filters are placeholders for now (we’re not using them yet in Cypher)
    st.sidebar.text_input("Specimen filter (optional)", placeholder="e.g., urine, blood")
    st.sidebar.text_input(
        "Method filter (optional)", placeholder="e.g., colorimetric assay"
    )

    max_rows = st.sidebar.slider("Max table rows", min_value=10, max_value=200, value=50)
    max_graph_biomarkers = st.sidebar.slider(
        "Max graph biomarkers", min_value=5, max_value=100, value=40
    )

    run = st.sidebar.button("Run search")

    if not run:
        st.info("Enter a query and click **Run search** to begin.")
        return

    query_str = query.strip()
    if not query_str:
        st.warning("Please enter a non-empty search term.")
        return

    # 1. Tabular results
    st.subheader("Tabular results")
    df = fetch_tabular_results(query_str, max_rows)

    if df.empty:
        st.warning("No biomarkers matched your query/filters.")
    else:
        st.dataframe(df, use_container_width=True)

    # 2. Network view
    st.subheader("Network view")

    try:
        graph_rows = fetch_graph_rows(query_str, max_graph_biomarkers)
    except Exception as e:
        st.error(f"Graph query error: {e}")
        return

    if not graph_rows:
        st.info("No graph neighborhood available for this query with current filters.")
        return

    net = build_network(graph_rows)
    render_network(net)


if __name__ == "__main__":
    main()
