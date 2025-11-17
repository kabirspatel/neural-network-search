import os
from typing import List, Dict, Any

import streamlit as st
import streamlit.components.v1 as components
from neo4j import GraphDatabase
from pyvis.network import Network
import pandas as pd

# -------------------------------------------------------------------
# Neo4j connection
# -------------------------------------------------------------------


@st.cache_resource
def get_driver():
    """
    Create and cache a Neo4j driver.
    Reads credentials from Streamlit secrets (preferred) or env vars.
    """
    # Prefer Streamlit secrets, fall back to environment variables
    secrets = getattr(st, "secrets", {})
    uri = secrets.get("NEO4J_URI", os.getenv("NEO4J_URI"))
    user = secrets.get("NEO4J_USER", os.getenv("NEO4J_USER"))
    password = secrets.get("NEO4J_PASSWORD", os.getenv("NEO4J_PASSWORD"))

    if not uri or not user or not password:
        raise RuntimeError(
            "Neo4j connection info not found. "
            "Set NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD in Streamlit secrets or env vars."
        )

    driver = GraphDatabase.driver(uri, auth=(user, password))
    return driver


# -------------------------------------------------------------------
# Cypher query helpers
# -------------------------------------------------------------------


def _run_biomarker_search(
    driver,
    query_text: str,
    specimen_filter: str,
    method_filter: str,
    limit: int,
) -> List[Dict[str, Any]]:
    """
    Run a single Cypher query to:
      - fulltext search biomarkers by name
      - pull linked devices, diseases, specimens, methods
      - return both counts and neighbor lists
    """

    # Basic Lucene-ish query; treat empty string as wildcard
    if query_text.strip():
        fulltext_query = query_text.strip()
    else:
        fulltext_query = "*"

    specimen_filter = specimen_filter.strip()
    method_filter = method_filter.strip()

    cypher = """
    // Full-text search over Biomarker.name
    CALL db.index.fulltext.queryNodes('biomarkerNameIndex', $q)
    YIELD node AS b, score
    WITH b, score
    ORDER BY score DESC
    LIMIT $limit

    // Gather neighbors
    OPTIONAL MATCH (d:Device)-[:MEASURES]->(b)
    OPTIONAL MATCH (b)-[:ASSOCIATED_WITH]->(di:Disease)
    OPTIONAL MATCH (b)-[:MEASURED_IN]->(s:Specimen)
    OPTIONAL MATCH (b)-[:USES_METHOD]->(m:Method)
    WITH b, score,
         collect(DISTINCT d)  AS devices,
         collect(DISTINCT di) AS diseases,
         collect(DISTINCT s)  AS specimens,
         collect(DISTINCT m)  AS methods

    // Optional text filters on specimen & method
    WHERE ($specimen IS NULL OR $specimen = '' OR
           ANY(sp IN specimens WHERE toLower(sp.name) CONTAINS toLower($specimen)))
      AND ($method   IS NULL OR $method   = '' OR
           ANY(mt IN methods   WHERE toLower(mt.name) CONTAINS toLower($method)))

    RETURN b AS biomarker, score,
           devices, diseases, specimens, methods,
           size(devices)   AS device_count,
           size(diseases)  AS disease_count,
           size(specimens) AS specimen_count,
           size(methods)   AS method_count
    ORDER BY score DESC, biomarker.name ASC
    """

    with driver.session() as session:
        result = session.run(
            cypher,
            q=fulltext_query,
            specimen=specimen_filter if specimen_filter else None,
            method=method_filter if method_filter else None,
            limit=limit,
        )
        rows = []
        for rec in result:
            rows.append(
                {
                    "biomarker": rec["biomarker"],
                    "score": rec["score"],
                    "devices": rec["devices"],
                    "diseases": rec["diseases"],
                    "specimens": rec["specimens"],
                    "methods": rec["methods"],
                    "device_count": rec["device_count"],
                    "disease_count": rec["disease_count"],
                    "specimen_count": rec["specimen_count"],
                    "method_count": rec["method_count"],
                }
            )
    return rows


# -------------------------------------------------------------------
# PyVis graph building
# -------------------------------------------------------------------


def build_pyvis_graph(
    rows: List[Dict[str, Any]], max_biomarkers: int
) -> Network:
    """
    Build a PyVis Network from the list of biomarker rows.
    Colors:
       Biomarker:  cyan
       Disease:    red
       Device:     orange
       Specimen:   green
       Method:     purple
    """

    net = Network(
        height="600px",
        width="100%",
        bgcolor="#ffffff",
        font_color="#222222",
        notebook=False,
        directed=False,
    )
    net.barnes_hut()

    # Limit how many biomarkers we visualise
    rows_for_graph = rows[:max_biomarkers]

    # We'll track which node IDs we've already added
    added_nodes = set()

    for row in rows_for_graph:
        b = row["biomarker"]
        bm_id = f"b_{b.id}"
        bm_label = b.get("name", "Biomarker")

        # Biomarker node
        if bm_id not in added_nodes:
            net.add_node(
                bm_id,
                label=bm_label,
                color="#00c8ff",
                title=f"Biomarker: {bm_label}",
            )
            added_nodes.add(bm_id)

        # Devices
        for d in row["devices"]:
            if d is None:
                continue
            d_id = f"d_{d.id}"
            d_label = d.get("device_name", d.get("generic_name", "Device"))
            if d_id not in added_nodes:
                net.add_node(
                    d_id,
                    label=d_label,
                    color="#ffb347",
                    title=f"Device: {d_label}",
                )
                added_nodes.add(d_id)
            net.add_edge(bm_id, d_id, title="MEASURES")

        # Diseases
        for di in row["diseases"]:
            if di is None:
                continue
            di_id = f"di_{di.id}"
            di_label = di.get("name", "Disease")
            if di_id not in added_nodes:
                net.add_node(
                    di_id,
                    label=di_label,
                    color="#ff6f69",
                    title=f"Disease: {di_label}",
                )
                added_nodes.add(di_id)
            net.add_edge(bm_id, di_id, title="ASSOCIATED_WITH")

        # Specimens
        for s in row["specimens"]:
            if s is None:
                continue
            s_id = f"s_{s.id}"
            s_label = s.get("name", "Specimen")
            if s_id not in added_nodes:
                net.add_node(
                    s_id,
                    label=s_label,
                    color="#77dd77",
                    title=f"Specimen: {s_label}",
                )
                added_nodes.add(s_id)
            net.add_edge(bm_id, s_id, title="MEASURED_IN")

        # Methods
        for m in row["methods"]:
            if m is None:
                continue
            m_id = f"m_{m.id}"
            m_label = m.get("name", "Method")
            if m_id not in added_nodes:
                net.add_node(
                    m_id,
                    label=m_label,
                    color="#c299ff",
                    title=f"Method: {m_label}",
                )
                added_nodes.add(m_id)
            net.add_edge(bm_id, m_id, title="USES_METHOD")

    return net


# -------------------------------------------------------------------
# Streamlit UI
# -------------------------------------------------------------------


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

    # Sidebar search controls
    with st.sidebar:
        st.header("Search settings")
        query_text = st.text_input(
            "Search by biomarker name or keyword",
            value="BRCA1",
            help="Full-text search over biomarker names.",
        )

        specimen_filter = st.text_input(
            "Specimen filter (optional)",
            placeholder="e.g., urine, blood",
            help="Filter to biomarkers that are measured in specimens whose names contain this text.",
        )

        method_filter = st.text_input(
            "Method filter (optional)",
            placeholder="e.g., colorimetric assay",
            help="Filter to biomarkers that use methods whose names contain this text.",
        )

        max_table_rows = st.slider(
            "Max table rows",
            min_value=10,
            max_value=200,
            value=50,
            step=10,
        )

        max_graph_biomarkers = st.slider(
            "Max graph biomarkers",
            min_value=5,
            max_value=100,
            value=40,
            step=5,
        )

        run_button = st.button("Run search")

    # If user hasn't explicitly clicked, we still run once with defaults
    if not run_button and not query_text:
        st.info("Enter a biomarker keyword on the left and click **Run search**.")
        return

    # Neo4j query
    try:
        driver = get_driver()
    except Exception as e:
        st.error(f"Error connecting to Neo4j: {e}")
        return

    query_limit = max(max_table_rows, max_graph_biomarkers)

    with st.spinner("Querying Neo4j…"):
        try:
            rows = _run_biomarker_search(
                driver=driver,
                query_text=query_text,
                specimen_filter=specimen_filter,
                method_filter=method_filter,
                limit=query_limit,
            )
        except Exception as e:
            st.error(f"Error running Neo4j query: {e}")
            return

    if not rows:
        st.warning("No biomarkers matched your query/filters.")
        return

    # ----------------------------------------------------------------
    # Tabular results
    # ----------------------------------------------------------------
    st.subheader("Tabular results")

    table_rows = []
    for row in rows[:max_table_rows]:
        b = row["biomarker"]
        table_rows.append(
            {
                "biomarker_id": b.get("biomarker_id", ""),
                "biomarker": b.get("name", ""),
                "score": round(float(row["score"]), 3),
                "devices": row["device_count"],
                "diseases": row["disease_count"],
                "specimens": row["specimen_count"],
                "methods": row["method_count"],
            }
        )

    df = pd.DataFrame(table_rows)
    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
    )

    # ----------------------------------------------------------------
    # Network view
    # ----------------------------------------------------------------
    st.subheader("Network view")

    try:
        net = build_pyvis_graph(rows, max_biomarkers=max_graph_biomarkers)
        # Save to a temporary HTML file
        html_path = "network.html"
        net.show_buttons(filter_=["physics"])  # allow user to tweak layout
        net.save_graph(html_path)

        with open(html_path, "r", encoding="utf-8") as f:
            html = f.read()

        components.html(html, height=600, scrolling=True)
    except Exception as e:
        st.error(f"Graph rendering error: {e}")


if __name__ == "__main__":
    main()
