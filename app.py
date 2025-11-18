# app.py
import os
import tempfile

import streamlit as st
import pandas as pd
from neo4j import GraphDatabase
import networkx as nx
from pyvis.network import Network
import streamlit.components.v1 as components


# -----------------------------
# Neo4j connection helpers
# -----------------------------

@st.cache_resource
def get_driver():
    """
    Create a single Neo4j driver instance.
    Reads from Streamlit secrets first, then from environment variables.
    """
    uri = None
    user = None
    password = None

    # 1) Try Streamlit secrets (Streamlit Cloud)
    if "neo4j" in st.secrets:
        uri = st.secrets["neo4j"].get("uri")
        user = st.secrets["neo4j"].get("user")
        password = st.secrets["neo4j"].get("password")

    # 2) Fallback to environment variables (local)
    uri = uri or os.environ.get("NEO4J_URI")
    user = user or os.environ.get("NEO4J_USER")
    password = password or os.environ.get("NEO4J_PASSWORD")

    if not uri or not user or not password:
        raise RuntimeError(
            "Neo4j connection info missing. "
            "Set st.secrets['neo4j'] or environment variables "
            "NEO4J_URI / NEO4J_USER / NEO4J_PASSWORD."
        )

    return GraphDatabase.driver(uri, auth=(user, password))


def run_query(cypher: str, params: dict) -> list[dict]:
    """
    Run a Cypher query and return a list of dictionaries.
    """
    driver = get_driver()
    with driver.session() as session:
        result = session.run(cypher, params)
        rows = [r.data() for r in result]
    return rows


# -----------------------------
# Data and graph construction
# -----------------------------

def search_biomarkers(
    query_text: str,
    max_rows: int,
) -> pd.DataFrame:
    """
    Search biomarkers + neighbors for a free-text query on biomarker name.
    Returns a DataFrame with one row per biomarker.
    """
    cypher = """
    MATCH (b:Biomarker)
    WHERE toLower(b.name) CONTAINS toLower($q)
    OPTIONAL MATCH (b)-[:ASSOCIATED_WITH]->(d:Disease)
    OPTIONAL MATCH (b)<-[:MEASURES]-(dev:Device)
    OPTIONAL MATCH (dev)-[:USES_METHOD]->(m:Method)
    RETURN
      b.biomarker_id     AS biomarker_id,
      b.name             AS biomarker,
      b.specimen_list    AS specimens,
      collect(DISTINCT d.name)       AS diseases,
      collect(DISTINCT dev.device_name) AS devices,
      collect(DISTINCT m.name)       AS methods
    ORDER BY biomarker
    LIMIT $limit
    """

    rows = run_query(cypher, {"q": query_text, "limit": max_rows})
    if not rows:
        return pd.DataFrame(
            columns=["biomarker_id", "biomarker", "specimens", "devices", "diseases", "methods"]
        )

    df = pd.DataFrame(rows)

    # Ensure columns exist and are in a fixed order, avoiding duplicates
    for col in ["biomarker_id", "biomarker", "specimens", "devices", "diseases", "methods"]:
        if col not in df.columns:
            df[col] = None

    df = df[["biomarker_id", "biomarker", "specimens", "devices", "diseases", "methods"]]
    return df


def apply_filters(df: pd.DataFrame, specimen_filter: str, method_filter: str) -> pd.DataFrame:
    """
    Apply simple text filters on specimen and method columns.
    """
    filtered = df.copy()

    # Specimen filter (substring match in the specimens string)
    if specimen_filter:
        specimen_filter_l = specimen_filter.lower()
        filtered = filtered[
            filtered["specimens"].fillna("").str.lower().str.contains(specimen_filter_l)
        ]

    # Method filter (substring match in any of the methods list)
    if method_filter:
        method_filter_l = method_filter.lower()

        def method_matches(row_methods):
            if not isinstance(row_methods, (list, tuple)):
                return False
            return any(method_filter_l in str(m).lower() for m in row_methods)

        filtered = filtered[filtered["methods"].apply(method_matches)]

    return filtered


def build_nx_graph(df: pd.DataFrame) -> nx.Graph:
    """
    Build a NetworkX graph from the search dataframe.

    Nodes:
      - Biomarker (blue)
      - Disease (orange)
      - Device (green)

    Edges:
      - biomarker -- disease
      - biomarker -- device
    """
    G = nx.Graph()

    for _, row in df.iterrows():
        biomarker_name = row["biomarker"]
        biomarker_id = row["biomarker_id"]
        biomarker_node_id = f"b_{biomarker_id or biomarker_name}"

        # Add biomarker node
        G.add_node(
            biomarker_node_id,
            label=str(biomarker_name),
            kind="biomarker",
            title=f"Biomarker: {biomarker_name}",
        )

        # Diseases
        diseases = row.get("diseases") or []
        for d_name in diseases:
            if not d_name:
                continue
            d_node_id = f"d_{d_name}"
            if d_node_id not in G:
                G.add_node(
                    d_node_id,
                    label=str(d_name),
                    kind="disease",
                    title=f"Disease: {d_name}",
                )
            G.add_edge(biomarker_node_id, d_node_id)

        # Devices
        devices = row.get("devices") or []
        for dev_name in devices:
            if not dev_name:
                continue
            dev_node_id = f"dev_{dev_name}"
            if dev_node_id not in G:
                G.add_node(
                    dev_node_id,
                    label=str(dev_name),
                    kind="device",
                    title=f"Device: {dev_name}",
                )
            G.add_edge(biomarker_node_id, dev_node_id)

    return G


def build_pyvis_html(G: nx.Graph, height: int = 700, width: str = "100%") -> str | None:
    """
    Convert a NetworkX graph to an interactive PyVis HTML snippet.

    Uses a dark background + nicer label styling.
    """
    if G.number_of_nodes() == 0:
        return None

    # Create PyVis network
    net = Network(
        height=f"{height}px",
        width=width,
        bgcolor="#111827",       # dark gray background
        font_color="#E5E7EB",    # light text
        notebook=False,
        cdn_resources="in_line"  # embed JS/CSS in HTML (good for Streamlit Cloud)
    )

    # Physics / layout tuning for a cleaner look
    net.barnes_hut(
        gravity=-20000,
        central_gravity=0.3,
        spring_length=120,
        spring_strength=0.01,
        damping=0.09,
    )

    # Color mapping by node type
    color_map = {
        "biomarker": "#3B82F6",  # blue
        "disease": "#F97316",    # orange
        "device": "#10B981",     # green
    }

    # Add nodes with styling
    for node_id, data in G.nodes(data=True):
        kind = data.get("kind", "other")
        label = data.get("label", str(node_id))
        title = data.get("title", label)

        size = 22 if kind == "biomarker" else 16

        net.add_node(
            node_id,
            label=label,
            title=title,
            color=color_map.get(kind, "#9CA3AF"),
            shape="dot",
            size=size,
            font={"size": 18 if kind == "biomarker" else 14},
        )

    # Add edges
    for u, v in G.edges():
        net.add_edge(u, v, color="#4B5563")

    # Write to a temporary HTML file and read it back
    with tempfile.NamedTemporaryFile(delete=False, suffix=".html") as tmp:
        tmp_path = tmp.name
    net.write_html(tmp_path, local=True, notebook=False)
    with open(tmp_path, "r", encoding="utf-8") as f:
        html = f.read()
    os.remove(tmp_path)

    return html


# -----------------------------
# Streamlit UI
# -----------------------------

def main():
    st.set_page_config(
        page_title="Biomarker / Disease / Method Search",
        layout="wide",
        page_icon="🧬",
    )

    st.title("Biomarker / Disease / Method Search")

    st.markdown(
        """
        Search across your **biomarkers**, linked **diseases**, **devices**, **specimens (biofluids)**  
        and **detection methods** from the Neo4j Aura graph.
        """
    )

    # Sidebar controls
    with st.sidebar:
        st.header("Search settings")

        query_text = st.text_input(
            "Search by biomarker name or keyword",
            value="brca1",
            help="Example: BRCA1, glucose, troponin"
        )

        specimen_filter = st.text_input(
            "Specimen filter (optional)",
            value="",
            placeholder="e.g., urine, blood",
        )

        method_filter = st.text_input(
            "Method filter (optional)",
            value="",
            placeholder="e.g., colorimetric assay",
        )

        max_table_rows = st.slider(
            "Max table rows",
            min_value=10,
            max_value=200,
            value=50,
        )

        max_graph_biomarkers = st.slider(
            "Max graph biomarkers",
            min_value=5,
            max_value=100,
            value=40,
            help="How many top biomarkers to include in the network view."
        )

        run_button = st.button("Run search")

    if not run_button:
        st.info("Enter a query and click **Run search** to begin.")
        return

    # -----------------------------
    # Run search + show table
    # -----------------------------
    with st.spinner("Running query against Neo4j..."):
        df_raw = search_biomarkers(query_text, max_rows=max_table_rows)

    if df_raw.empty:
        st.warning("No biomarkers found for that query.")
        return

    df_filtered = apply_filters(df_raw, specimen_filter, method_filter)

    if df_filtered.empty:
        st.warning("No biomarkers match the current filters.")
        return

    st.subheader("Tabular results")
    st.dataframe(
        df_filtered,
        use_container_width=True,
        hide_index=True,
    )

    # -----------------------------
    # Build network graph
    # -----------------------------
    st.subheader("Network view")

    graph_df = df_filtered.head(max_graph_biomarkers)
    G = build_nx_graph(graph_df)

    if G.number_of_nodes() == 0:
        st.info("No graph neighborhood found for these biomarkers.")
        return

    html = build_pyvis_html(G, height=750, width="100%")

    if html is None:
        st.info("No graph neighborhood found for these biomarkers.")
    else:
        components.html(html, height=750, scrolling=True)


if __name__ == "__main__":
    main()
