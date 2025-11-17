import os
from pathlib import Path
import tempfile

import streamlit as st
import streamlit.components.v1 as components
from neo4j import GraphDatabase
from pyvis.network import Network


# ---------- Neo4j connection ----------

@st.cache_resource
def get_driver():
    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD")

    if not uri or not password:
        raise RuntimeError(
            "Missing Neo4j connection details. "
            "Set NEO4J_URI / NEO4J_PASSWORD in your environment or Streamlit secrets."
        )

    return GraphDatabase.driver(uri, auth=(user, password))


# ---------- Queries ----------

def run_table_query(driver, query_text, specimen_filter, method_filter, max_rows):
    """
    Return top biomarkers for the table, with counts of linked devices,
    diseases, specimens and methods.
    """
    cypher = """
    CALL db.index.fulltext.queryNodes('biomarkerNameIndex', $q)
        YIELD node, score
    WITH node, score
    // specimen filter using specimen_list text
    WHERE ($specimen IS NULL OR $specimen = '' OR
           (node.specimen_list IS NOT NULL AND
            toLower(node.specimen_list) CONTAINS toLower($specimen)))

    // pre-collect methods for optional method filter
    OPTIONAL MATCH (node)<-[:USES_METHOD]-(m:Method)
    WITH node, score, collect(DISTINCT m) AS methods
    WHERE ($method IS NULL OR $method = '' OR
           any(mm IN methods WHERE toLower(mm.name) CONTAINS toLower($method)))

    // collect devices
    OPTIONAL MATCH (node)<-[:MEASURES]-(d:Device)
    WITH node, score,
         methods,
         collect(DISTINCT d) AS devices

    WITH node, score,
         size(devices) AS device_count,
         size(methods) AS method_count,
         CASE
           WHEN node.disease_list IS NULL THEN 0
           ELSE size([x IN split(node.disease_list, ';') WHERE trim(x) <> ''])
         END AS disease_count,
         CASE
           WHEN node.specimen_list IS NULL THEN 0
           ELSE size([x IN split(node.specimen_list, ';') WHERE trim(x) <> ''])
         END AS specimen_count

    RETURN
        elementId(node)      AS biomarker_id,
        node.name            AS biomarker,
        round(score, 3)      AS score,
        device_count         AS devices,
        disease_count        AS diseases,
        specimen_count       AS specimens,
        method_count         AS methods
    ORDER BY score DESC
    LIMIT $limit
    """

    params = {
        "q": query_text,
        "specimen": specimen_filter or "",
        "method": method_filter or "",
        "limit": int(max_rows),
    }

    with driver.session() as session:
        records = session.run(cypher, **params)
        return [r.data() for r in records]


def run_graph_query(driver, query_text, specimen_filter, method_filter, max_biomarkers):
    """
    Return biomarker + connected device / disease / specimen / method names
    for building the network graph.
    """
    cypher = """
    CALL db.index.fulltext.queryNodes('biomarkerNameIndex', $q)
        YIELD node, score
    WITH node, score
    WHERE ($specimen IS NULL OR $specimen = '' OR
           (node.specimen_list IS NOT NULL AND
            toLower(node.specimen_list) CONTAINS toLower($specimen)))

    OPTIONAL MATCH (node)<-[:USES_METHOD]-(m:Method)
    WITH node, score, collect(DISTINCT m) AS methods
    WHERE ($method IS NULL OR $method = '' OR
           any(mm IN methods WHERE toLower(mm.name) CONTAINS toLower($method)))

    OPTIONAL MATCH (node)<-[:MEASURES]-(d:Device)
    WITH node, score,
         methods,
         collect(DISTINCT d) AS devices,
         CASE
           WHEN node.disease_list IS NULL THEN []
           ELSE [x IN split(node.disease_list, ';') WHERE trim(x) <> '']
         END AS diseases,
         CASE
           WHEN node.specimen_list IS NULL THEN []
           ELSE [x IN split(node.specimen_list, ';') WHERE trim(x) <> '']
         END AS specimens

    RETURN
        elementId(node) AS biomarker_id,
        node.name       AS biomarker,
        diseases,
        specimens,
        [d IN devices | coalesce(d.device_name, d.generic_name, '(unnamed device)')] AS devices,
        [m IN methods | m.name] AS methods
    ORDER BY score DESC
    LIMIT $limit
    """

    params = {
        "q": query_text,
        "specimen": specimen_filter or "",
        "method": method_filter or "",
        "limit": int(max_biomarkers),
    }

    with driver.session() as session:
        records = session.run(cypher, **params)
        return [r.data() for r in records]


# ---------- Graph building (PyVis) ----------

def build_network(graph_rows):
    """
    Build a PyVis network from the rows returned by run_graph_query.
    Nodes are created on-the-fly (we do not rely on Neo4j element_id anymore).
    """
    net = Network(height="600px", width="100%", bgcolor="#111111", font_color="white")
    net.barnes_hut()

    added_nodes = set()

    for row in graph_rows:
        biomarker_label = row.get("biomarker") or "Unnamed biomarker"
        biomarker_key = f"b|{row.get('biomarker_id', biomarker_label)}"

        if biomarker_key not in added_nodes:
            net.add_node(
                biomarker_key,
                label=biomarker_label,
                color="#00d1ff",
                title=f"Biomarker: {biomarker_label}",
            )
            added_nodes.add(biomarker_key)

        # Diseases
        for disease in row.get("diseases", []) or []:
            disease = disease.strip()
            if not disease:
                continue
            node_id = f"d|{disease}"
            if node_id not in added_nodes:
                net.add_node(
                    node_id,
                    label=disease,
                    color="#ff6b6b",
                    title=f"Disease: {disease}",
                )
                added_nodes.add(node_id)
            net.add_edge(biomarker_key, node_id)

        # Specimens / biofluids
        for specimen in row.get("specimens", []) or []:
            specimen = specimen.strip()
            if not specimen:
                continue
            node_id = f"s|{specimen}"
            if node_id not in added_nodes:
                net.add_node(
                    node_id,
                    label=specimen,
                    color="#feca57",
                    title=f"Specimen / biofluid: {specimen}",
                )
                added_nodes.add(node_id)
            net.add_edge(biomarker_key, node_id)

        # Devices
        for device in row.get("devices", []) or []:
            device = device.strip()
            if not device:
                continue
            node_id = f"v|{device}"
            if node_id not in added_nodes:
                net.add_node(
                    node_id,
                    label=device,
                    color="#1dd1a1",
                    title=f"Device: {device}",
                )
                added_nodes.add(node_id)
            net.add_edge(biomarker_key, node_id)

        # Methods
        for method in row.get("methods", []) or []:
            method = method.strip()
            if not method:
                continue
            node_id = f"m|{method}"
            if node_id not in added_nodes:
                net.add_node(
                    node_id,
                    label=method,
                    color="#5f27cd",
                    title=f"Detection method: {method}",
                )
                added_nodes.add(node_id)
            net.add_edge(biomarker_key, node_id)

    # Save HTML to a temp file
    tmp_dir = Path(tempfile.gettempdir())
    html_path = tmp_dir / "biomarker_network.html"
    net.show(str(html_path))
    return html_path


# ---------- Streamlit UI ----------

st.set_page_config(
    page_title="Biomarker / Disease / Method Search",
    layout="wide",
)

st.title("Biomarker / Disease / Method Search")
st.write(
    "Search across your **biomarkers**, linked **diseases**, **devices**, "
    "**specimens (biofluids)** and **detection methods** from the Neo4j Aura graph."
)

driver = get_driver()

with st.sidebar:
    st.header("Search settings")
    biomarker_query = st.text_input(
        "Search by biomarker name or keyword",
        value="BRCA1",
        help="Full-text search over biomarker names.",
    )
    specimen_filter = st.text_input(
        "Specimen filter (optional)",
        value="",
        help="Filter using text in the biomarker's specimen_list (e.g., urine, blood).",
    )
    method_filter = st.text_input(
        "Method filter (optional)",
        value="",
        help="Filter biomarkers that are linked to methods matching this text.",
    )
    max_table_rows = st.slider("Max table rows", 10, 200, 50, step=10)
    max_graph_biomarkers = st.slider("Max graph biomarkers", 5, 80, 40, step=5)

    run_button = st.button("Run search")

if run_button and biomarker_query.strip():
    try:
        # ----- Table -----
        table_rows = run_table_query(
            driver,
            biomarker_query.strip(),
            specimen_filter.strip(),
            method_filter.strip(),
            max_table_rows,
        )

        st.subheader("Tabular results")
        if table_rows:
            st.dataframe(table_rows, use_container_width=True)
        else:
            st.warning("No biomarkers matched your query/filters.")

        # ----- Graph -----
        st.subheader("Network view")

        graph_rows = run_graph_query(
            driver,
            biomarker_query.strip(),
            specimen_filter.strip(),
            method_filter.strip(),
            max_graph_biomarkers,
        )

        if not graph_rows:
            st.info("No graph neighborhood available for this query with current filters.")
        else:
            try:
                html_path = build_network(graph_rows)
                with open(html_path, "r", encoding="utf-8") as f:
                    html = f.read()
                components.html(html, height=650, scrolling=True)
            except Exception as e:
                st.error(f"Graph rendering error: {e}")

    except Exception as e:
        st.error(f"Search error: {e}")

else:
    st.info("Enter a biomarker keyword and click **Run search** to begin.")
