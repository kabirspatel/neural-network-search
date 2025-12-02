import os
from typing import Dict, Any, List, Tuple

import pandas as pd
import streamlit as st
from neo4j import GraphDatabase, basic_auth

# --- optional interactive graph deps ---
try:
    from pyvis.network import Network
    import streamlit.components.v1 as components

    PYVIS_AVAILABLE = True
except ImportError:
    PYVIS_AVAILABLE = False

# -----------------------------
# Neo4j connection helpers
# -----------------------------


@st.cache_resource(show_spinner=False)
def get_neo4j_driver():
    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USER")
    password = os.getenv("NEO4J_PASSWORD")

    if not uri or not user or not password:
        raise RuntimeError(
            "NEO4J_URI, NEO4J_USER, and NEO4J_PASSWORD must be set as environment variables."
        )

    driver = GraphDatabase.driver(uri, auth=basic_auth(user, password))
    return driver


def run_cypher(query: str, params: Dict[str, Any] | None = None) -> List[Dict[str, Any]]:
    driver = get_neo4j_driver()
    with driver.session() as session:
        result = session.run(query, params or {})
        return [dict(record) for record in result]


# -----------------------------
# Cached summary + metadata
# -----------------------------


@st.cache_data(show_spinner=False)
def get_summary_counts() -> Dict[str, int]:
    query = """
    CALL {
      MATCH (b:Biomarker) RETURN count(b) AS biomarkers
    }
    CALL {
      MATCH (d:Disease) RETURN count(d) AS diseases
    }
    CALL {
      MATCH (s:Specimen) RETURN count(s) AS specimens
    }
    CALL {
      MATCH (m:DetectionMethod) RETURN count(m) AS methods
    }
    CALL {
      MATCH (d:Device) RETURN count(d) AS devices
    }
    CALL {
      MATCH ()-[r:BIOMARKER_ASSOCIATED_WITH_DISEASE]->() RETURN count(r) AS biomarker_disease_edges
    }
    RETURN biomarkers, diseases, specimens, methods, devices, biomarker_disease_edges
    """
    rows = run_cypher(query)
    return rows[0] if rows else {}


@st.cache_data(show_spinner=False)
def get_disease_categories() -> List[str]:
    query = """
    MATCH ()-[r:BIOMARKER_ASSOCIATED_WITH_DISEASE]->()
    WHERE r.disease_category IS NOT NULL
    RETURN DISTINCT r.disease_category AS cat
    ORDER BY cat
    """
    rows = run_cypher(query)
    return [r["cat"] for r in rows if r.get("cat")]


# -----------------------------
# Path-graph data builder
# -----------------------------


def get_path_graph_data(q: str, max_pairs: int = 10) -> Tuple[Dict[str, Dict[str, Any]], List[Tuple[str, str, str]]]:
    """
    Build a node/edge set for a path-style graph centered on biomarker–disease
    edges that match the search term.

    Returns:
      nodes: {id_str: {"label": str, "kind": "Biomarker"/"Disease"/...}}
      edges: [(src_id_str, dst_id_str, rel_type), ...]
    """
    cypher = """
    MATCH (b:Biomarker)-[:BIOMARKER_ASSOCIATED_WITH_DISEASE]->(d:Disease)
    WHERE toLower(b.name) CONTAINS $q OR toLower(d.name) CONTAINS $q
    WITH DISTINCT b, d
    LIMIT $max_pairs

    OPTIONAL MATCH (b)-[:BIOMARKER_ASSOCIATED_WITH_DISEASE]->(d)
    WITH DISTINCT b, d
    AS bd, collect({b:b, d:d}) AS bd_pairs

    UNWIND bd_pairs AS pair
    WITH pair.b AS b, pair.d AS d

    OPTIONAL MATCH (b)-[:MEASURED_IN_SPECIMEN]->(s1:Specimen)
    OPTIONAL MATCH (d)-[:DETECTED_IN_SPECIMEN]->(s2:Specimen)
    OPTIONAL MATCH (dev:Device)-[:INTENDED_FOR]->(d)
    OPTIONAL MATCH (dev)-[:USES_METHOD]->(m:DetectionMethod)

    RETURN
      id(b) AS b_id, b.name AS b_name,
      id(d) AS d_id, d.name AS d_name,
      id(s1) AS s1_id, s1.name AS s1_name,
      id(s2) AS s2_id, s2.name AS s2_name,
      id(dev) AS dev_id, dev.device_name AS dev_name,
      id(m) AS m_id, m.name AS m_name
    """

    rows = run_cypher(cypher, {"q": q.lower(), "max_pairs": int(max_pairs)})

    nodes: Dict[str, Dict[str, Any]] = {}
    edges_set: set[Tuple[str, str, str]] = set()

    def add_node(node_id, label, kind):
        if node_id is None or label is None:
            return
        nid = str(node_id)
        if nid not in nodes:
            nodes[nid] = {"label": label, "kind": kind}

    def add_edge(src_id, dst_id, rel_type):
        if src_id is None or dst_id is None:
            return
        edges_set.add((str(src_id), str(dst_id), rel_type))

    for r in rows:
        b_id, b_name = r.get("b_id"), r.get("b_name")
        d_id, d_name = r.get("d_id"), r.get("d_name")
        s1_id, s1_name = r.get("s1_id"), r.get("s1_name")
        s2_id, s2_name = r.get("s2_id"), r.get("s2_name")
        dev_id, dev_name = r.get("dev_id"), r.get("dev_name")
        m_id, m_name = r.get("m_id"), r.get("m_name")

        # Nodes
        add_node(b_id, b_name, "Biomarker")
        add_node(d_id, d_name, "Disease")
        add_node(s1_id, s1_name, "Specimen")
        add_node(s2_id, s2_name, "Specimen")
        add_node(dev_id, dev_name, "Device")
        add_node(m_id, m_name, "DetectionMethod")

        # Edges
        add_edge(b_id, d_id, "BIOMARKER_ASSOCIATED_WITH_DISEASE")
        add_edge(b_id, s1_id, "MEASURED_IN_SPECIMEN")
        add_edge(d_id, s2_id, "DETECTED_IN_SPECIMEN")
        add_edge(dev_id, d_id, "INTENDED_FOR")
        add_edge(dev_id, m_id, "USES_METHOD")

    return nodes, list(edges_set)


def render_path_graph(nodes: Dict[str, Dict[str, Any]], edges: List[Tuple[str, str, str]], height: int = 650):
    if not PYVIS_AVAILABLE:
        st.error(
            "pyvis is not installed. Run `pip install pyvis` in your environment "
            "to enable the interactive graph."
        )
        return

    if not nodes:
        st.warning("No nodes/edges to display for this query.")
        return

    net = Network(
        height=f"{height}px",
        width="100%",
        bgcolor="#ffffff",
        font_color="#000000",
        directed=True,
    )

    net.barnes_hut()
    net.toggle_physics(True)
    net.show_buttons(filter_=["physics"])

    style_map = {
        "Biomarker": {"color": "#FF7F0E", "shape": "dot"},
        "Disease": {"color": "#1F77B4", "shape": "dot"},
        "Specimen": {"color": "#2CA02C", "shape": "dot"},
        "Device": {"color": "#9467BD", "shape": "square"},
        "DetectionMethod": {"color": "#8C564B", "shape": "triangle"},
    }

    for nid, meta in nodes.items():
        label = meta["label"]
        kind = meta.get("kind", "Node")
        style = style_map.get(kind, {"color": "#7F7F7F", "shape": "dot"})
        net.add_node(
            nid,
            label=label,
            title=f"{kind}: {label}",
            color=style["color"],
            shape=style["shape"],
        )

    for src, dst, rel in edges:
        net.add_edge(src, dst, label="", title=rel, arrows="to")

    html = net.generate_html(notebook=False)
    components.html(html, height=height, scrolling=True)


# -----------------------------
# Streamlit UI
# -----------------------------

st.set_page_config(
    page_title="Biomarker–Disease & Device Explorer",
    layout="wide",
)

st.title("Biomarker–Disease & Device Knowledge Graph Explorer")

with st.sidebar:
    st.markdown("### Connection")
    try:
        driver = get_neo4j_driver()
        st.success("Connected to Neo4j")
    except Exception as e:
        st.error(f"Neo4j connection error: {e}")
        st.stop()

    st.markdown("### Search")
    search_term = st.text_input(
        "Search term",
        value="",
        placeholder="e.g., breast cancer, CRP, urine, immunoassay, urinalysis",
    )
    search_clicked = st.button("Search")  # explicit button

    min_pubmed = st.number_input(
        "Minimum PubMed count (for biomarker–disease edges)",
        min_value=0,
        value=0,
        step=1,
    )

    disease_category_options = ["(All)"] + get_disease_categories()
    selected_category = st.selectbox(
        "Disease category filter (for edges)",
        options=disease_category_options,
        index=0,
    )

    st.markdown("---")
    st.caption("This app queries your Aura Neo4j knowledge graph and live FDA 510(k) device data.")

q = search_term.strip()

summary = get_summary_counts()
if summary:
    st.markdown(
        f"""
        **Graph summary**

        - Biomarkers: `{summary.get("biomarkers", 0)}`
        - Diseases: `{summary.get("diseases", 0)}`
        - Specimens: `{summary.get("specimens", 0)}`
        - Detection methods: `{summary.get("methods", 0)}`
        - FDA devices (subset): `{summary.get("devices", 0)}`
        - Biomarker–Disease edges: `{summary.get("biomarker_disease_edges", 0)}`
        """
    )

# -----------------------------
# Global interactive path graph (under summary, above tabs)
# -----------------------------
st.markdown("### Path-style interactive graph")

if not q:
    st.info(
        "Enter a search term in the sidebar and click **Search** to see an interactive graph of "
        "biomarkers, diseases, specimens, devices, and detection methods."
    )
else:
    max_pairs = st.slider(
        "Max biomarker–disease pairs to include in the graph",
        min_value=1,
        max_value=30,
        value=10,
        key="graph_max_pairs",
    )

    nodes, edges = get_path_graph_data(q, max_pairs=max_pairs)

    st.caption(
        "Graph nodes include Biomarkers, Diseases, Specimens, Devices, and Detection Methods. "
        "Edges show the relationships between them."
    )

    render_path_graph(nodes, edges, height=650)

    if nodes:
        node_df = pd.DataFrame(
            [{"id": nid, "label": meta["label"], "kind": meta["kind"]} for nid, meta in nodes.items()]
        ).sort_values("kind")
        with st.expander("Show node list"):
            st.dataframe(node_df, use_container_width=True)

st.markdown("---")

# -----------------------------
# Tabs (3 only)
# -----------------------------
tabs = st.tabs(
    [
        "Biomarkers & Diseases",
        "Specimens",
        "Devices & Detection Methods",
    ]
)

# -----------------------------
# Tab 1: Biomarkers & Diseases
# -----------------------------
with tabs[0]:
    st.subheader("Biomarker ↔ Disease edges")

    if not q:
        st.info("Enter a search term in the sidebar and click **Search** to see biomarker–disease edges.")
    else:
        params = {
            "q": q.lower(),
            "min_pubmed": int(min_pubmed),
        }

        category_filter = ""
        if selected_category != "(All)":
            category_filter = "AND r.disease_category = $category"
            params["category"] = selected_category

        query = f"""
        MATCH (b:Biomarker)-[r:BIOMARKER_ASSOCIATED_WITH_DISEASE]->(d:Disease)
        WHERE
          (toLower(b.name) CONTAINS $q OR toLower(d.name) CONTAINS $q)
          AND r.pubmed_count >= $min_pubmed
          {category_filter}
        OPTIONAL MATCH (b)-[:MEASURED_IN_SPECIMEN]->(bs:Specimen)
        OPTIONAL MATCH (d)-[:DETECTED_IN_SPECIMEN]->(ds:Specimen)
        RETURN
          b.name AS biomarker,
          d.name AS disease,
          r.disease_category AS disease_category,
          d.is_cancer_like AS disease_is_cancer_like,
          r.pubmed_count AS pubmed_count,
          collect(DISTINCT bs.name) AS biomarker_specimens,
          collect(DISTINCT ds.name) AS disease_specimens
        ORDER BY pubmed_count DESC, biomarker, disease
        LIMIT 500
        """
        rows = run_cypher(query, params)

        if not rows:
            st.warning("No biomarker–disease edges matched your search/filters.")
        else:
            df = pd.DataFrame(rows)
            if "biomarker_specimens" in df.columns:
                df["biomarker_specimens"] = df["biomarker_specimens"].apply(
                    lambda xs: ", ".join(sorted({x for x in xs if x})) if isinstance(xs, list) else ""
                )
            if "disease_specimens" in df.columns:
                df["disease_specimens"] = df["disease_specimens"].apply(
                    lambda xs: ", ".join(sorted({x for x in xs if x})) if isinstance(xs, list) else ""
                )

            st.caption(f"Showing {len(df)} biomarker–disease edges (max 500).")
            st.dataframe(df, use_container_width=True)


# -----------------------------
# Tab 2: Specimens
# -----------------------------
with tabs[1]:
    st.subheader("Specimens linked to biomarkers and diseases")

    if not q:
        st.info("Enter a search term in the sidebar and click **Search** to explore specimens.")
    else:
        params = {"q": q.lower()}

        q_disease_specimen = """
        MATCH (d:Disease)-[:DETECTED_IN_SPECIMEN]->(s:Specimen)
        WHERE toLower(d.name) CONTAINS $q
        RETURN
          d.name AS disease,
          s.name AS specimen
        ORDER BY disease, specimen
        LIMIT 300
        """
        disease_rows = run_cypher(q_disease_specimen, params)
        df_disease = pd.DataFrame(disease_rows)

        q_biomarker_specimen = """
        MATCH (b:Biomarker)-[:MEASURED_IN_SPECIMEN]->(s:Specimen)
        WHERE toLower(b.name) CONTAINS $q
        RETURN
          b.name AS biomarker,
          s.name AS specimen
        ORDER BY biomarker, specimen
        LIMIT 300
        """
        biomarker_rows = run_cypher(q_biomarker_specimen, params)
        df_biomarker = pd.DataFrame(biomarker_rows)

        cols = st.columns(2)

        with cols[0]:
            st.markdown("**Diseases → Specimens**")
            if df_disease.empty:
                st.caption("No disease–specimen matches for this term.")
            else:
                st.caption(f"{len(df_disease)} disease–specimen pairs")
                st.dataframe(df_disease, use_container_width=True)

        with cols[1]:
            st.markdown("**Biomarkers → Specimens**")
            if df_biomarker.empty:
                st.caption("No biomarker–specimen matches for this term.")
            else:
                st.caption(f"{len(df_biomarker)} biomarker–specimen pairs")
                st.dataframe(df_biomarker, use_container_width=True)

    st.markdown("---")
    st.markdown("#### All specimen types (for reference)")
    all_specimen_rows = run_cypher(
        "MATCH (s:Specimen) RETURN s.name AS specimen ORDER BY specimen LIMIT 200"
    )
    if all_specimen_rows:
        df_all_spec = pd.DataFrame(all_specimen_rows)
        st.dataframe(df_all_spec, use_container_width=True, height=260)
    else:
        st.caption("No Specimen nodes found.")


# -----------------------------
# Tab 3: Devices & Detection Methods
# -----------------------------
with tabs[2]:
    st.subheader("FDA Devices & Detection Methods")

    if not q:
        st.info("Enter a search term in the sidebar and click **Search** to search devices and methods.")
    else:
        params = {"q": q.lower()}

        q_devices = """
        MATCH (d:Device)
        WHERE
          toLower(d.device_name) CONTAINS $q
          OR toLower(coalesce(d.product_code, '')) CONTAINS $q
          OR toLower(coalesce(d.k_number, '')) CONTAINS $q
        OPTIONAL MATCH (d)-[:USES_METHOD]->(m:DetectionMethod)
        RETURN
          d.device_name AS device_name,
          d.k_number AS k_number,
          d.product_code AS product_code,
          m.name AS detection_method
        ORDER BY device_name
        LIMIT 500
        """
        device_rows = run_cypher(q_devices, params)
        df_devices = pd.DataFrame(device_rows)

        q_methods = """
        MATCH (m:DetectionMethod)
        WHERE toLower(m.name) CONTAINS $q
        OPTIONAL MATCH (d:Device)-[:USES_METHOD]->(m)
        RETURN
          m.name AS detection_method,
          count(DISTINCT d) AS num_devices
        ORDER BY num_devices DESC, detection_method
        LIMIT 100
        """
        method_rows = run_cypher(q_methods, params)
        df_methods = pd.DataFrame(method_rows)

        cols = st.columns(2)

        with cols[0]:
            st.markdown("**Devices matching search term**")
            if df_devices.empty:
                st.caption("No devices matched this search term.")
            else:
                st.caption(f"{len(df_devices)} devices (max 500).")
                st.dataframe(df_devices, use_container_width=True)

        with cols[1]:
            st.markdown("**Detection methods matching search term**")
            if df_methods.empty:
                st.caption("No detection methods matched this search term.")
            else:
                st.caption("Methods and how many devices use each one.")
                st.dataframe(df_methods, use_container_width=True)

    st.markdown(
        """
        _Note_: Devices are pulled from the public FDA 510(k) device API and
        automatically mapped to generic detection methods (e.g., *Immunoassay*,
        *Colorimetric*, *Fluorescence*).
        """
    )
