import os
from pathlib import Path
import tempfile

from neo4j import GraphDatabase
import streamlit as st
from pyvis.network import Network


# ---------- Neo4j driver ----------

def get_neo4j_driver():
    uri = os.environ.get("NEO4J_URI")
    user = os.environ.get("NEO4J_USER", "neo4j")
    pwd = os.environ.get("NEO4J_PASSWORD")

    if "st_secrets" in dir(st) and "NEO4J_URI" in st.secrets:
        uri = st.secrets["NEO4J_URI"]
        user = st.secrets.get("NEO4J_USER", user)
        pwd = st.secrets.get("NEO4J_PASSWORD", pwd)

    if not uri or not pwd:
        raise RuntimeError(
            "Neo4j connection details are missing. "
            "Set NEO4J_URI / NEO4J_USER / NEO4J_PASSWORD "
            "as env vars or in Streamlit secrets."
        )

    return GraphDatabase.driver(uri, auth=(user, pwd))


driver = get_neo4j_driver()


def run_query(cypher: str, params: dict | None = None):
    params = params or {}
    with driver.session() as session:
        result = session.run(cypher, **params)
        return [record.data() for record in result]


# ---------- Cypher helpers ----------

SEARCH_CYPHER_FULLTEXT = """
CALL db.index.fulltext.queryNodes('biomarkerNameIndex', $q)
YIELD node, score
WITH node, score
ORDER BY score DESC
LIMIT $limit
// count devices
OPTIONAL MATCH (node)<-[:MEASURES]-(d:Device)
WITH node, score, count(DISTINCT d) AS device_count
// count diseases
OPTIONAL MATCH (node)-[:ASSOCIATED_DISEASE|INDICATES|CAUSES]->(di:Disease)
WITH node, score, device_count, count(DISTINCT di) AS disease_count
// count specimens (biofluids)
OPTIONAL MATCH (node)-[:MEASURED_IN]->(s:Specimen)
WITH node, score, device_count, disease_count, count(DISTINCT s) AS specimen_count
// count methods
OPTIONAL MATCH (node)-[:MEASURED_BY]->(m:Method)
RETURN
  id(node)                AS biomarker_id,
  node.name               AS biomarker,
  score                   AS score,
  device_count            AS devices,
  disease_count           AS diseases,
  specimen_count          AS specimens,
  count(DISTINCT m)       AS methods
ORDER BY score DESC
LIMIT $limit
"""

# fallback if fulltext finds nothing
SEARCH_CYPHER_CONTAINS = """
MATCH (b:Biomarker)
WHERE toLower(b.name) CONTAINS toLower($q)
WITH b
OPTIONAL MATCH (b)<-[:MEASURES]-(d:Device)
WITH b, count(DISTINCT d) AS device_count
OPTIONAL MATCH (b)-[:ASSOCIATED_DISEASE|INDICATES|CAUSES]->(di:Disease)
WITH b, device_count, count(DISTINCT di) AS disease_count
OPTIONAL MATCH (b)-[:MEASURED_IN]->(s:Specimen)
WITH b, device_count, disease_count, count(DISTINCT s) AS specimen_count
OPTIONAL MATCH (b)-[:MEASURED_BY]->(m:Method)
RETURN
  id(b)                   AS biomarker_id,
  b.name                  AS biomarker,
  1.0                     AS score,
  device_count            AS devices,
  disease_count           AS diseases,
  specimen_count          AS specimens,
  count(DISTINCT m)       AS methods
ORDER BY score DESC
LIMIT $limit
"""

# neighborhood for graph
GRAPH_CYPHER = """
MATCH (b:Biomarker)
WHERE id(b) IN $biomarker_ids
WITH b
// devices
OPTIONAL MATCH (d:Device)-[:MEASURES]->(b)
WITH b, collect(DISTINCT d)[0..$max_neighbors] AS devices
// diseases
OPTIONAL MATCH (b)-[bd:ASSOCIATED_DISEASE|INDICATES|CAUSES]->(di:Disease)
WITH b, devices,
     collect(DISTINCT {node: di, rel: type(bd)})[0..$max_neighbors] AS diseases
// specimens
OPTIONAL MATCH (b)-[:MEASURED_IN]->(s:Specimen)
WITH b, devices, diseases,
     collect(DISTINCT s)[0..$max_neighbors] AS specimens
// methods
OPTIONAL MATCH (b)-[:MEASURED_BY]->(m:Method)
RETURN
  elementId(b)                    AS biomarker_eid,
  b.name                          AS biomarker_name,
  [node IN devices |
     {id: elementId(node), label: coalesce(node.device_name, node.generic_name, 'device'),
      type: 'Device', rel: 'MEASURES'}
  ]                               AS device_nodes,
  [d IN diseases |
     {id: elementId(d.node), label: d.node.name,
      type: 'Disease', rel: d.rel}
  ]                               AS disease_nodes,
  [node IN specimens |
     {id: elementId(node), label: node.name,
      type: 'Specimen', rel: 'MEASURED_IN'}
  ]                               AS specimen_nodes,
  [node IN collect(DISTINCT m) |
     {id: elementId(node), label: node.name,
      type: 'Method', rel: 'MEASURED_BY'}
  ]                               AS method_nodes
ORDER BY biomarker_name
"""


# ---------- Search & graph logic ----------

def search_biomarkers(q: str, limit: int):
    if not q:
        return []

    params = {"q": q, "limit": limit}
    rows = run_query(SEARCH_CYPHER_FULLTEXT, params)

    # Fallback if fulltext finds nothing (e.g., "glucose" right now)
    if not rows and len(q) >= 3:
        rows = run_query(SEARCH_CYPHER_CONTAINS, params)

    return rows


def fetch_graph_neighborhood(biomarker_ids, max_neighbors: int):
    if not biomarker_ids:
        return []

    params = {
        "biomarker_ids": biomarker_ids,
        "max_neighbors": max_neighbors,
    }
    return run_query(GRAPH_CYPHER, params)


def build_pyvis_graph(neighborhood_rows, max_biomarkers: int):
    net = Network(height="600px", width="100%", bgcolor="#111111", font_color="white")
    net.barnes_hut(gravity=-8000, central_gravity=0.3, spring_length=200)

    nodes: dict[str, dict] = {}
    edges: list[tuple[str, str, str]] = []

    def add_node(node_id: str, label: str, group: str):
        if node_id not in nodes:
            nodes[node_id] = {"id": node_id, "label": label, "group": group}

    # colors per type
    group_colors = {
        "Biomarker": "#ffcc00",
        "Device": "#00ccff",
        "Disease": "#ff6666",
        "Specimen": "#66ff66",
        "Method": "#b266ff",
    }

    for row in neighborhood_rows[:max_biomarkers]:
        b_id = row["biomarker_eid"]
        b_name = row["biomarker_name"]
        add_node(b_id, b_name, "Biomarker")

        for n in row["device_nodes"]:
            nid = n["id"]
            add_node(nid, n["label"], n["type"])
            edges.append((nid, b_id, n["rel"]))

        for n in row["disease_nodes"]:
            nid = n["id"]
            add_node(nid, n["label"], n["type"])
            edges.append((b_id, nid, n["rel"]))

        for n in row["specimen_nodes"]:
            nid = n["id"]
            add_node(nid, n["label"], n["type"])
            edges.append((b_id, nid, n["rel"]))

        for n in row["method_nodes"]:
            nid = n["id"]
            add_node(nid, n["label"], n["type"])
            edges.append((nid, b_id, n["rel"]))

    # add to pyvis
    for n in nodes.values():
        color = group_colors.get(n["group"], "#cccccc")
        net.add_node(n["id"], label=n["label"], title=n["label"], color=color)

    for src, tgt, rel in edges:
        net.add_edge(src, tgt, title=rel)

    net.repulsion(node_distance=200, spring_length=200)

    # render to temporary HTML
    tmp_dir = tempfile.gettempdir()
    html_path = Path(tmp_dir) / "biomarker_graph.html"
    net.show(str(html_path))
    return html_path


# ---------- Streamlit UI ----------

st.set_page_config(
    page_title="Biomarker / Disease / Method Search",
    layout="wide",
    page_icon="🧬",
)

st.sidebar.header("Search settings")

query = st.sidebar.text_input(
    "Search by biomarker name or keyword",
    value="BRCA1",
    help="Free-text search over biomarker names.",
)

specimen_filter = st.sidebar.text_input(
    "Specimen filter (optional)",
    value="",
    help="Filter to biofluid names (e.g., urine, blood). Currently not applied in Cypher but kept for future use.",
)

method_filter = st.sidebar.text_input(
    "Method filter (optional)",
    value="",
    help="Filter to detection method keywords. Currently not applied in Cypher but kept for future use.",
)

max_rows = st.sidebar.slider("Max table rows", min_value=10, max_value=200, value=50)
max_graph_biomarkers = st.sidebar.slider(
    "Max graph biomarkers", min_value=5, max_value=80, value=40
)

run_button = st.sidebar.button("Run search")

st.title("Biomarker / Disease / Method Search")
st.write(
    "Search across your **biomarkers**, linked **diseases**, **devices**, "
    "**specimens** (biofluids) and **detection methods** from the Neo4j Aura graph."
)

if run_button:
    with st.spinner("Querying Neo4j…"):
        rows = search_biomarkers(query, max_rows)

    if not rows:
        st.warning("No biomarkers matched your query/filters.")
    else:
        # Tabular section
        st.subheader("Tabular results")
        st.dataframe(
            rows,
            use_container_width=True,
            hide_index=True,
        )

        # Graph section
        st.subheader("Network view")

        biomarker_ids = [row["biomarker_id"] for row in rows]
        with st.spinner("Building graph neighborhood…"):
            neighborhood = fetch_graph_neighborhood(
                biomarker_ids, max_neighbors=40
            )

        if not neighborhood:
            st.info(
                "No graph neighborhood available for this query with current filters."
            )
        else:
            try:
                html_path = build_pyvis_graph(neighborhood, max_graph_biomarkers)
                with open(html_path, "r", encoding="utf-8") as f:
                    html = f.read()
                st.components.v1.html(html, height=600, scrolling=True)
            except Exception as e:
                st.error(f"Graph rendering error: {e}")
else:
    st.info("Enter a query and click **Run search** to start.")
