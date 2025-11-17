import os
from typing import List, Dict, Any

import streamlit as st
from neo4j import GraphDatabase
import graphviz

# ---------------------------------------------------------------------
# Neo4j connection
# ---------------------------------------------------------------------

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

@st.cache_resource
def get_driver():
    if not NEO4J_URI or not NEO4J_PASSWORD:
        raise RuntimeError("NEO4J_URI or NEO4J_PASSWORD is not set in environment.")
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

driver = get_driver()

def run_cypher(query: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    with driver.session() as session:
        result = session.run(query, params or {})
        return [r.data() for r in result]

# ---------------------------------------------------------------------
# Cypher queries
# ---------------------------------------------------------------------

BIOMARKER_SEARCH_QUERY = """
// Full-text style biomarker search with counts of neighbors
WITH toLower($q) AS q
MATCH (b:Biomarker)
WHERE q IS NULL OR q = '' OR toLower(b.name) CONTAINS q
OPTIONAL MATCH (d:Device)-[:MEASURES]->(b)
OPTIONAL MATCH (b)-[:ASSOCIATED_WITH]->(di:Disease)
OPTIONAL MATCH (b)-[:MEASURED_IN]->(s:Specimen)
OPTIONAL MATCH (b)-[:USES_METHOD]->(m:Method)
WITH b,
     count(DISTINCT d)  AS device_count,
     count(DISTINCT di) AS disease_count,
     count(DISTINCT s)  AS specimen_count,
     count(DISTINCT m)  AS method_count
RETURN
  elementId(b)          AS biomarker_id,
  b.name                AS biomarker,
  1.0                   AS score,           // placeholder score
  device_count          AS devices,
  disease_count         AS diseases,
  specimen_count        AS specimens,
  method_count          AS methods
ORDER BY score DESC, biomarker
LIMIT $limit;
"""

NEIGHBORHOOD_QUERY = """
// Return a small neighborhood around a set of biomarker IDs
WITH $biomarker_ids AS ids

// Devices -> Biomarkers
MATCH (d:Device)-[:MEASURES]->(b:Biomarker)
WHERE elementId(b) IN ids
WITH ids,
     collect({
       src_id: elementId(d),
       src_label: 'Device',
       src_name: coalesce(d.device_name, d.generic_name),
       dst_id: elementId(b),
       dst_label: 'Biomarker',
       dst_name: b.name,
       rel_type: 'MEASURES'
     }) AS dev_edges

// Biomarkers -> Diseases
MATCH (b:Biomarker)-[:ASSOCIATED_WITH]->(di:Disease)
WHERE elementId(b) IN ids
WITH ids, dev_edges,
     collect({
       src_id: elementId(b),
       src_label: 'Biomarker',
       src_name: b.name,
       dst_id: elementId(di),
       dst_label: 'Disease',
       dst_name: di.name,
       rel_type: 'ASSOCIATED_WITH'
     }) AS dis_edges

// Biomarkers -> Specimens
MATCH (b:Biomarker)-[:MEASURED_IN]->(s:Specimen)
WHERE elementId(b) IN ids
WITH ids, dev_edges, dis_edges,
     collect({
       src_id: elementId(b),
       src_label: 'Biomarker',
       src_name: b.name,
       dst_id: elementId(s),
       dst_label: 'Specimen',
       dst_name: s.name,
       rel_type: 'MEASURED_IN'
     }) AS spec_edges

// Biomarkers -> Methods
MATCH (b:Biomarker)-[:USES_METHOD]->(m:Method)
WHERE elementId(b) IN ids
WITH
  dev_edges + dis_edges + spec_edges +
  collect({
    src_id: elementId(b),
    src_label: 'Biomarker',
    src_name: b.name,
    dst_id: elementId(m),
    dst_label: 'Method',
    dst_name: m.name,
    rel_type: 'USES_METHOD'
  }) AS all_edges

UNWIND all_edges AS e
RETURN DISTINCT e;
"""

# ---------------------------------------------------------------------
# Graph building (Graphviz)
# ---------------------------------------------------------------------

NODE_STYLE = {
    "Biomarker": {"color": "#1f77b4", "shape": "ellipse"},
    "Device":    {"color": "#2ca02c", "shape": "box"},
    "Disease":   {"color": "#d62728", "shape": "diamond"},
    "Specimen":  {"color": "#9467bd", "shape": "oval"},
    "Method":    {"color": "#ff7f0e", "shape": "hexagon"},
}

def build_graphviz(edges: List[Dict[str, Any]]) -> graphviz.Graph:
    """
    Build a Graphviz graph from edge rows of NEIGHBORHOOD_QUERY.
    Each edge row has: src_id, src_label, src_name, dst_id, dst_label, dst_name, rel_type
    """
    if not edges:
        return None

    dot = graphviz.Digraph(engine="dot")
    dot.attr("graph", bgcolor="transparent")

    nodes: Dict[str, Dict[str, str]] = {}

    for e in edges:
        # Register source node
        sid = e["e"]["src_id"]
        if sid not in nodes:
            nodes[sid] = {
                "label": e["e"]["src_label"],
                "name":  e["e"]["src_name"],
            }

        # Register destination node
        did = e["e"]["dst_id"]
        if did not in nodes:
            nodes[did] = {
                "label": e["e"]["dst_label"],
                "name":  e["e"]["dst_name"],
            }

    # Add nodes with styling
    for nid, info in nodes.items():
        label = info["label"]
        name = info["name"] or "(unknown)"
        style = NODE_STYLE.get(label, {"color": "#7f7f7f", "shape": "ellipse"})
        dot.node(
            nid,
            f"{name}\n({label})",
            style="filled",
            color=style["color"],
            shape=style["shape"],
            fontname="Helvetica",
        )

    # Add edges
    for e in edges:
        sid = e["e"]["src_id"]
        did = e["e"]["dst_id"]
        rel = e["e"]["rel_type"]
        dot.edge(sid, did, label=rel, fontname="Helvetica", fontsize="10")

    return dot

# ---------------------------------------------------------------------
# Streamlit UI
# ---------------------------------------------------------------------

st.set_page_config(page_title="Biomarker / Disease / Method Search", layout="wide")

st.title("Biomarker / Disease / Method Search")
st.write(
    "Search across your **biomarkers**, linked **diseases**, **devices**, "
    "**specimens** (biofluids) and **detection methods** from the Neo4j Aura graph."
)

with st.sidebar:
    st.header("Search settings")

    query = st.text_input("Search by biomarker name or keyword", value="BRCA1")

    specimen_filter = st.text_input("Specimen filter (optional)", placeholder="e.g., urine, blood")
    method_filter = st.text_input("Method filter (optional)", placeholder="e.g., colorimetric assay")

    max_rows = st.slider("Max table rows", min_value=10, max_value=200, value=50, step=10)
    max_graph_biomarkers = st.slider("Max graph biomarkers", min_value=5, max_value=80, value=40, step=5)

    run_search = st.button("Run search")

if run_search:
    # -----------------------------------------------------------------
    # 1. Tabular biomarker search
    # -----------------------------------------------------------------
    params = {"q": query.strip(), "limit": max_rows}
    biomarker_rows = run_cypher(BIOMARKER_SEARCH_QUERY, params)

    st.subheader("Tabular results")

    if not biomarker_rows:
        st.warning("No biomarkers matched your query/filters.")
    else:
        # Simple specimen/method filtering on name text (post-query)
        if specimen_filter:
            sf = specimen_filter.lower()
            biomarker_rows = [r for r in biomarker_rows if sf in (r["biomarker"] or "").lower()]

        if method_filter:
            mf = method_filter.lower()
            biomarker_rows = [r for r in biomarker_rows if mf in (r["biomarker"] or "").lower()]

        if not biomarker_rows:
            st.warning("No biomarkers remained after applying text filters.")
        else:
            # Show table
            st.dataframe(biomarker_rows, use_container_width=True)

    # -----------------------------------------------------------------
    # 2. Network view (Graphviz)
    # -----------------------------------------------------------------
    st.subheader("Network view")

    if not biomarker_rows:
        st.info("No graph neighborhood available for this query with current filters.")
    else:
        # Take the top N biomarkers by score and pass their IDs
        selected_ids = [row["biomarker_id"] for row in biomarker_rows[:max_graph_biomarkers]]

        neigh_edges = run_cypher(NEIGHBORHOOD_QUERY, {"biomarker_ids": selected_ids})

        if not neigh_edges:
            st.info("No graph neighborhood available for this query with current filters.")
        else:
            dot = build_graphviz(neigh_edges)
            if dot is None:
                st.info("No graph neighborhood available for this query with current filters.")
            else:
                st.graphviz_chart(dot, use_container_width=True)

else:
    st.info("Enter a biomarker keyword (e.g., *BRCA1*) and click **Run search**.")

