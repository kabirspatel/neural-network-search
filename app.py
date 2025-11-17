import os
from neo4j import GraphDatabase
import streamlit as st
from pyvis.network import Network
import tempfile
from pathlib import Path

# -------------------------------------------------------------------
# Connection helpers
# -------------------------------------------------------------------

@st.cache_resource
def get_driver():
    uri = os.environ.get("NEO4J_URI")
    user = os.environ.get("NEO4J_USER", "neo4j")
    password = os.environ.get("NEO4J_PASSWORD")

    if not uri or not password:
        raise RuntimeError(
            "NEO4J_URI and NEO4J_PASSWORD must be set as environment variables / secrets."
        )

    return GraphDatabase.driver(uri, auth=(user, password))

# -------------------------------------------------------------------
# Query helpers
# -------------------------------------------------------------------

def run_query(cypher, params=None):
    driver = get_driver()
    with driver.session(database="neo4j") as session:
        result = session.run(cypher, params or {})
        return [record.data() for record in result]


def search_biomarkers(q, specimen, method, limit):
    """
    Main query backing the table + graph.
    Uses fulltext index on Biomarker name and then optional filters.
    """
    cypher = """
    // 1) Fulltext search over Biomarker names
    CALL db.index.fulltext.queryNodes('biomarkerNameIndex', $q) YIELD node AS b, score
    WHERE b:Biomarker

    // 2) Optional joins to Devices / Specimens / Methods
    OPTIONAL MATCH (d:Device)-[:MEASURES]->(b)
    OPTIONAL MATCH (b)-[:FOUND_IN]->(s:Specimen)
    OPTIONAL MATCH (b)-[:MEASURED_BY]->(m:Method)

    // 3) Apply filters if provided
    WITH b, score, d, s, m
    WHERE ($specimen IS NULL OR toLower(s.name) = toLower($specimen))
      AND ($method   IS NULL OR toLower(m.name) = toLower($method))

    // 4) Collect some basic info
    WITH b,
         score,
         collect(DISTINCT d) AS devices,
         collect(DISTINCT s) AS specimens,
         collect(DISTINCT m) AS methods
    ORDER BY score DESC
    LIMIT $limit

    RETURN
      b as biomarker,
      score,
      [d IN devices | d.device_name]   AS device_names,
      [s IN specimens | s.name]        AS specimen_names,
      [m IN methods | m.name]          AS method_names
    """

    return run_query(
        cypher,
        {
            "q": q if q.strip() else "*",
            "specimen": specimen,
            "method": method,
            "limit": limit,
        },
    )


def build_graph(q, specimen, method, limit_nodes=40):
    """
    Build a small neighborhood graph around the search term.
    We use the same fulltext seed as the table, then expand to
    Diseases / Devices / Specimens / Methods.
    """
    cypher = """
    // Seed biomarkers using fulltext
    CALL db.index.fulltext.queryNodes('biomarkerNameIndex', $q) YIELD node AS b, score
    WHERE b:Biomarker
    WITH b, score
    ORDER BY score DESC
    LIMIT 20

    // Expand to diseases & devices & context
    OPTIONAL MATCH (b)-[bd_rel]->(d:Disease)
    OPTIONAL MATCH (dev:Device)-[md_rel:MEASURES]->(b)
    OPTIONAL MATCH (b)-[bs_rel:FOUND_IN]->(s:Specimen)
    OPTIONAL MATCH (b)-[bm_rel:MEASURED_BY]->(m:Method)

    WHERE ($specimen IS NULL OR s.name = $specimen)
      AND ($method   IS NULL OR m.name = $method)

    RETURN
      b AS biomarker,
      collect(DISTINCT d)  AS diseases,
      collect(DISTINCT dev) AS devices,
      collect(DISTINCT s)  AS specimens,
      collect(DISTINCT m)  AS methods
    LIMIT $limit_nodes
    """

    rows = run_query(
        cypher,
        {
            "q": q if q.strip() else "*",
            "specimen": specimen,
            "method": method,
            "limit_nodes": limit_nodes,
        },
    )

    # Flatten into simple node / edge lists for pyvis
    nodes = {}
    edges = []

    def add_node(node, label, color):
        if not node:
            return
        nid = node.element_id  # works in Aura / Neo4j 5 driver
        if nid not in nodes:
            nodes[nid] = {
                "id": nid,
                "label": label,
                "color": color,
            }
        return nid

    for row in rows:
        b = row["biomarker"]
        if not b:
            continue
        b_id = add_node(b, b.get("name", "Biomarker"), "#a855f7")  # purple

        for d in row["diseases"]:
            d_id = add_node(d, d.get("name", "Disease"), "#ef4444")  # red
            edges.append((b_id, d_id, "INDICATES"))

        for dev in row["devices"]:
            dv_id = add_node(dev, dev.get("device_name", "Device"), "#3b82f6")  # blue
            edges.append((dv_id, b_id, "MEASURES"))

        for s in row["specimens"]:
            s_id = add_node(s, s.get("name", "Specimen"), "#22c55e")  # green
            edges.append((b_id, s_id, "FOUND_IN"))

        for m in row["methods"]:
            m_id = add_node(m, m.get("name", "Method"), "#f97316")  # orange
            edges.append((b_id, m_id, "MEASURED_BY"))

    return list(nodes.values()), edges

# -------------------------------------------------------------------
# Streamlit UI
# -------------------------------------------------------------------

st.set_page_config(
    page_title="Biomarker / Disease / Device Network Search",
    layout="wide",
)

st.title("Biomarker / Disease / Method Search")

st.write(
    "Search across your **biomarkers**, linked **diseases**, "
    "**devices**, **specimens** (biofluids) and **detection methods** "
    "from the Neo4j Aura graph."
)

# --- Sidebar filters ------------------------------------------------

with st.sidebar:
    st.header("Search settings")

    q = st.text_input(
        "Search by biomarker name or keyword",
        value="glucose",
        help="Fulltext search over biomarker names. Use * for everything.",
    )

    specimen_filter = st.text_input(
        "Specimen filter (optional)",
        value="",
        placeholder="e.g., urine, blood",
    )
    specimen_filter = specimen_filter.strip() or None

    method_filter = st.text_input(
        "Method filter (optional)",
        value="",
        placeholder="e.g., colorimetric assay",
    )
    method_filter = method_filter.strip() or None

    max_results = st.slider(
        "Max table rows",
        min_value=10,
        max_value=200,
        value=50,
        step=10,
    )

    max_graph_nodes = st.slider(
        "Max graph biomarkers",
        min_value=10,
        max_value=60,
        value=40,
        step=10,
        help="Upper limit on how many biomarker-centered nodes to visualize.",
    )

    run_btn = st.button("Run search")

# --- Main area ------------------------------------------------------

if run_btn:
    # 1) Table results
    st.subheader("Tabular results")

    try:
        rows = search_biomarkers(q, specimen_filter, method_filter, max_results)
    except Exception as e:
        st.error(f"Query error: {e}")
        st.stop()

    if not rows:
        st.info("No biomarkers matched your query/filters.")
    else:
        table_rows = []
        for r in rows:
            b = r["biomarker"]
            table_rows.append(
                {
                    "Biomarker": b.get("name", ""),
                    "Score": round(r["score"], 3),
                    "Devices": ", ".join(sorted(set(r["device_names"]))),
                    "Specimens": ", ".join(sorted(set(r["specimen_names"]))),
                    "Methods": ", ".join(sorted(set(r["method_names"]))),
                }
            )
        st.dataframe(table_rows, use_container_width=True)

    # 2) Network graph
    st.subheader("Network view")

    try:
        nodes, edges = build_graph(q, specimen_filter, method_filter, max_graph_nodes)
    except Exception as e:
        st.error(f"Graph query error: {e}")
        st.stop()

    if not nodes:
        st.info("No graph neighborhood available for this query with current filters.")
    else:
        net = Network(
            height="600px",
            width="100%",
            bgcolor="#111111",
            font_color="white",
        )
        net.barnes_hut()

        for n in nodes:
            net.add_node(
                n["id"],
                label=n["label"],
                color=n["color"],
                title=n["label"],
            )

        for src, tgt, rel in edges:
            net.add_edge(src, tgt, title=rel)

        # Write to a temporary HTML file and embed
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir) / "graph.html"
            net.show(str(tmp_path))
            html = tmp_path.read_text(encoding="utf-8")

        st.components.v1.html(html, height=600, scrolling=True)

else:
    st.info("Set your query and filters in the sidebar, then click **Run search**.")
