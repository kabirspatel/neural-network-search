import os
from dataclasses import dataclass
from typing import List, Dict, Any, Tuple

import streamlit as st
from neo4j import GraphDatabase
import networkx as nx
import matplotlib.pyplot as plt
import pandas as pd


# -----------------------------
# Neo4j connection helpers
# -----------------------------

@dataclass
class Neo4jConfig:
    uri: str
    user: str
    password: str


@st.cache_resource(show_spinner=False)
def get_driver(cfg: Neo4jConfig):
    return GraphDatabase.driver(cfg.uri, auth=(cfg.user, cfg.password))


def run_query(driver, query: str, params: Dict[str, Any] | None = None) -> List[Dict[str, Any]]:
    with driver.session() as session:
        result = session.run(query, **(params or {}))
        return [r.data() for r in result]


# -----------------------------
# Graph summary queries
# -----------------------------

def get_graph_summary(driver) -> Dict[str, int]:
    summary: Dict[str, int] = {}

    q_nodes = {
        "biomarkers": "MATCH (b:Biomarker) RETURN count(b) AS n",
        "diseases": "MATCH (d:Disease) RETURN count(d) AS n",
        "specimens": "MATCH (s:Specimen) RETURN count(s) AS n",
        "methods": "MATCH (m:DetectionMethod) RETURN count(m) AS n",
        "devices": "MATCH (dev:Device) RETURN count(dev) AS n",
    }
    q_edges = {
        "bd_edges": """
            MATCH ()-[r:BIOMARKER_ASSOCIATED_WITH_DISEASE]-()
            RETURN count(r) AS n
        """
    }

    for key, q in q_nodes.items():
        rows = run_query(driver, q)
        summary[key] = rows[0]["n"] if rows else 0

    for key, q in q_edges.items():
        rows = run_query(driver, q)
        summary[key] = rows[0]["n"] if rows else 0

    return summary


# -----------------------------
# Graph building
# -----------------------------

def get_paths_for_search(
    driver,
    search_term: str,
    min_pubmed: int,
    max_pairs: int,
    max_devices_per_biomarker: int,
) -> Tuple[nx.Graph, Dict[str, str]]:
    """
    Build a NetworkX graph around the search term, including:
      Biomarker --(BIOMARKER_ASSOCIATED_WITH_DISEASE)--> Disease
      Biomarker <-> Specimen
      Disease   <-> Specimen
      Device    --(MEASURES)--> Biomarker
      Device    --(USES_METHOD)--> DetectionMethod
    """
    q = search_term.strip().lower()

    # 1) Core biomarker–disease pairs
    bd_rows = run_query(
        driver,
        """
        MATCH (b:Biomarker)-[r:BIOMARKER_ASSOCIATED_WITH_DISEASE]->(d:Disease)
        WHERE ($q = '' OR toLower(b.name) CONTAINS $q OR toLower(d.name) CONTAINS $q)
          AND (r.pubmed_count IS NULL OR r.pubmed_count >= $min_pubmed)
        WITH b, d, r
        ORDER BY coalesce(r.pubmed_count, 0) DESC
        LIMIT $max_pairs
        RETURN b, d, r.pubmed_count AS pubmed_count
        """,
        {"q": q, "min_pubmed": min_pubmed, "max_pairs": max_pairs},
    )

    G = nx.Graph()
    node_types: Dict[str, str] = {}

    if not bd_rows:
        return G, node_types  # empty

    biomarkers: set[str] = set()
    diseases: set[str] = set()

    for row in bd_rows:
        b = row["b"]
        d = row["d"]
        pubmed = row.get("pubmed_count")

        b_name = b["name"]
        d_name = d["name"]
        biomarkers.add(b_name)
        diseases.add(d_name)

        G.add_node(b_name)
        node_types[b_name] = "biomarker"

        G.add_node(d_name)
        node_types[d_name] = "disease"

        G.add_edge(
            b_name,
            d_name,
            relation="BIOMARKER_ASSOCIATED_WITH_DISEASE",
            pubmed_count=pubmed,
        )

    # 2) Specimens linked to these biomarkers and diseases
    spec_rows_b = run_query(
        driver,
        """
        MATCH (b:Biomarker)-[:MEASURED_IN_SPECIMEN]->(s:Specimen)
        WHERE b.name IN $biomarkers
        RETURN b.name AS biomarker, s.name AS specimen
        """,
        {"biomarkers": list(biomarkers)},
    )

    spec_rows_d = run_query(
        driver,
        """
        MATCH (d:Disease)-[:DETECTED_IN_SPECIMEN]->(s:Specimen)
        WHERE d.name IN $diseases
        RETURN d.name AS disease, s.name AS specimen
        """,
        {"diseases": list(diseases)},
    )

    for row in spec_rows_b:
        b_name = row["biomarker"]
        s_name = row["specimen"]
        G.add_node(s_name)
        node_types[s_name] = "specimen"
        G.add_edge(b_name, s_name, relation="MEASURED_IN_SPECIMEN")

    for row in spec_rows_d:
        d_name = row["disease"]
        s_name = row["specimen"]
        G.add_node(s_name)
        node_types[s_name] = "specimen"
        G.add_edge(d_name, s_name, relation="DETECTED_IN_SPECIMEN")

    # 3) Devices and detection methods for these biomarkers
    if max_devices_per_biomarker > 0:
        dev_rows = run_query(
            driver,
            """
            MATCH (dev:Device)-[:MEASURES]->(b:Biomarker)
            WHERE b.name IN $biomarkers
            WITH b.name AS biomarker, dev
            ORDER BY dev.device_name
            WITH biomarker, collect(dev)[0..$max_devices] AS devs
            UNWIND devs AS dev
            OPTIONAL MATCH (dev)-[:USES_METHOD]->(m:DetectionMethod)
            RETURN biomarker,
                   dev.device_name AS device_name,
                   m.name AS method_name
            """,
            {
                "biomarkers": list(biomarkers),
                "max_devices": max_devices_per_biomarker,
            },
        )

        for row in dev_rows:
            biomarker_name = row["biomarker"]
            device_name = row["device_name"]
            method_name = row.get("method_name")

            if device_name:
                G.add_node(device_name)
                node_types[device_name] = "device"
                G.add_edge(device_name, biomarker_name, relation="MEASURES")

            if method_name:
                G.add_node(method_name)
                node_types[method_name] = "method"
                G.add_edge(device_name, method_name, relation="USES_METHOD")

    return G, node_types


def draw_graph(G: nx.Graph, node_types: Dict[str, str]):
    if len(G.nodes) == 0:
        st.info("No graph paths found for this search / filters.")
        return

    type_colors = {
        "biomarker": "#7b48ff",       # purple
        "disease": "#ff7f0e",         # orange
        "specimen": "#2ca02c",        # green
        "device": "#1f77b4",          # blue
        "method": "#d62728",          # red
    }

    node_color_list = [
        type_colors.get(node_types.get(n, "biomarker"), "#7b48ff")
        for n in G.nodes()
    ]

    # Layout
    pos = nx.spring_layout(G, k=0.6, iterations=60, seed=42)

    fig, ax = plt.subplots(figsize=(8, 8))
    nx.draw_networkx_nodes(
        G,
        pos,
        node_size=40,
        node_color=node_color_list,
        alpha=0.9,
        ax=ax,
    )
    nx.draw_networkx_edges(
        G,
        pos,
        alpha=0.25,
        width=0.5,
        ax=ax,
    )

    # Only label a subset to avoid clutter: high-degree nodes
    degrees = dict(G.degree())
    label_nodes = sorted(degrees, key=degrees.get, reverse=True)[:30]
    labels = {n: n for n in label_nodes}

    nx.draw_networkx_labels(
        G,
        pos,
        labels=labels,
        font_size=6,
        ax=ax,
    )

    ax.axis("off")
    st.pyplot(fig)

    # Legend / key
    st.markdown(
        """
**Color key**

- <span style="color:#7b48ff;">●</span> **Biomarker**
- <span style="color:#ff7f0e;">●</span> **Disease**
- <span style="color:#2ca02c;">●</span> **Specimen**
- <span style="color:#1f77b4;">●</span> **Device**
- <span style="color:#d62728;">●</span> **Detection method**
        """,
        unsafe_allow_html=True,
    )


# -----------------------------
# Streamlit UI
# -----------------------------

def main():
    st.set_page_config(
        page_title="Biomarker–Disease & Device Knowledge Graph Explorer",
        layout="wide",
    )

    st.title("Biomarker–Disease & Device Knowledge Graph Explorer")

    # --- Neo4j connection ---

    uri = os.getenv("NEO4J_URI", "")
    user = os.getenv("NEO4J_USER", "")
    password = os.getenv("NEO4J_PASSWORD", "")

    col_conn, col_main = st.columns([1, 3])

    with col_conn:
        st.subheader("Connection")
        if not uri or not user or not password:
            st.error(
                "Neo4j connection error: NEO4J_URI, NEO4J_USER, and NEO4J_PASSWORD "
                "must be set as environment variables."
            )
            driver = None
        else:
            cfg = Neo4jConfig(uri=uri, user=user, password=password)
            try:
                driver = get_driver(cfg)
                st.success("Connected to Neo4j")
            except Exception as e:
                st.error(f"Could not connect to Neo4j: {e}")
                driver = None

        st.markdown("---")
        st.subheader("Search")

        search_term = st.text_input(
            "Search term (biomarker or disease name)",
            value="glucose",
            help="Example: 'glucose', 'troponin', 'acute myeloid leukemia', etc.",
        )

        min_pubmed = st.number_input(
            "Minimum PubMed count (for biomarker–disease edges)",
            value=0,
            min_value=0,
            step=1,
        )

    with col_main:
        if driver is None:
            st.info("Configure Neo4j environment variables to explore the graph.")
            return

        # Graph summary
        summary = get_graph_summary(driver)
        st.subheader("Graph summary")
        st.markdown(
            f"""
- Biomarkers: <span style="background-color:#111;padding:2px 6px;border-radius:4px;">{summary.get("biomarkers", 0)}</span>  
- Diseases: <span style="background-color:#111;padding:2px 6px;border-radius:4px;">{summary.get("diseases", 0)}</span>  
- Specimens: <span style="background-color:#111;padding:2px 6px;border-radius:4px;">{summary.get("specimens", 0)}</span>  
- Detection methods: <span style="background-color:#111;padding:2px 6px;border-radius:4px;">{summary.get("methods", 0)}</span>  
- FDA devices (subset): <span style="background-color:#111;padding:2px 6px;border-radius:4px;">{summary.get("devices", 0)}</span>  
- Biomarker–Disease edges: <span style="background-color:#111;padding:2px 6px;border-radius:4px;">{summary.get("bd_edges", 0)}</span>
            """,
            unsafe_allow_html=True,
        )

        st.markdown("### Path-style interactive graph")

        col_slider1, col_slider2 = st.columns([1, 1])

        with col_slider1:
            max_pairs = st.slider(
                "Max biomarker–disease pairs to include in the graph",
                min_value=1,
                max_value=30,
                value=10,
            )
        with col_slider2:
            max_devices_per_biomarker = st.slider(
                "Max devices per biomarker (to keep graph readable)",
                min_value=0,
                max_value=50,
                value=10,
            )

        # Build and draw graph
        G, node_types = get_paths_for_search(
            driver,
            search_term=search_term,
            min_pubmed=min_pubmed,
            max_pairs=max_pairs,
            max_devices_per_biomarker=max_devices_per_biomarker,
        )

        draw_graph(G, node_types)

        # ----------------- Tabs with detail tables -----------------
        st.markdown("---")
        tab_bd, tab_spec, tab_dev = st.tabs(
            ["Biomarkers & Diseases", "Specimens", "Devices & Detection Methods"]
        )

        # Tab 1: biomarker–disease table
        with tab_bd:
            bd_table = run_query(
                driver,
                """
                MATCH (b:Biomarker)-[r:BIOMARKER_ASSOCIATED_WITH_DISEASE]->(d:Disease)
                WHERE ($q = '' OR toLower(b.name) CONTAINS $q OR toLower(d.name) CONTAINS $q)
                  AND (r.pubmed_count IS NULL OR r.pubmed_count >= $min_pubmed)
                RETURN b.name AS biomarker,
                       d.name AS disease,
                       r.pubmed_count AS pubmed_count,
                       r.disease_category AS disease_category
                ORDER BY coalesce(r.pubmed_count, 0) DESC
                LIMIT 200
                """,
                {"q": search_term.lower().strip(), "min_pubmed": min_pubmed},
            )
            if bd_table:
                st.dataframe(pd.DataFrame(bd_table))
            else:
                st.info("No biomarker–disease edges matched this search / filter.")

        # Tab 2: specimen view
        with tab_spec:
            spec_table = run_query(
                driver,
                """
                MATCH (b:Biomarker)-[:MEASURED_IN_SPECIMEN]->(s:Specimen)
                WHERE ($q = '' OR toLower(b.name) CONTAINS $q)
                RETURN b.name AS biomarker,
                       s.name AS specimen,
                       'biomarker → specimen' AS relation
                UNION ALL
                MATCH (d:Disease)-[:DETECTED_IN_SPECIMEN]->(s:Specimen)
                WHERE ($q = '' OR toLower(d.name) CONTAINS $q)
                RETURN d.name AS biomarker,
                       s.name AS specimen,
                       'disease → specimen' AS relation
                LIMIT 200
                """,
                {"q": search_term.lower().strip()},
            )
            if spec_table:
                st.dataframe(pd.DataFrame(spec_table))
            else:
                st.info("No specimen relationships matched this search.")

        # Tab 3: devices & detection methods
        with tab_dev:
            dev_table = run_query(
                driver,
                """
                MATCH (dev:Device)-[:MEASURES]->(b:Biomarker)
                WHERE ($q = '' OR toLower(b.name) CONTAINS $q OR toLower(dev.device_name) CONTAINS $q)
                OPTIONAL MATCH (dev)-[:USES_METHOD]->(m:DetectionMethod)
                RETURN b.name AS biomarker,
                       dev.device_name AS device_name,
                       dev.k_number AS k_number,
                       dev.product_code AS product_code,
                       m.name AS detection_method
                LIMIT 500
                """,
                {"q": search_term.lower().strip()},
            )
            if dev_table:
                st.caption(
                    "Devices are pulled from the public FDA 510(k) API and mapped to generic detection methods."
                )
                st.dataframe(pd.DataFrame(dev_table))
            else:
                st.info("No devices matched this search / filter.")

    # end col_main


if __name__ == "__main__":
    main()
