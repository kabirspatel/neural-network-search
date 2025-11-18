import streamlit as st
import pandas as pd
from neo4j import GraphDatabase
from pyvis.network import Network
import tempfile
import os

# --------------------------------------------------------------------
# Neo4j connection helpers
# --------------------------------------------------------------------


@st.cache_resource
def get_driver():
    """
    Create a single Neo4j driver instance using Streamlit secrets.
    Expect secrets.toml to contain:

    [neo4j]
    uri = "neo4j+s://<your-instance>.databases.neo4j.io"
    user = "neo4j"
    password = "your-password"
    """
    uri = st.secrets["neo4j"]["uri"]
    user = st.secrets["neo4j"]["user"]
    password = st.secrets["neo4j"]["password"]
    return GraphDatabase.driver(uri, auth=(user, password))


def run_query(cypher, params=None):
    driver = get_driver()
    with driver.session() as session:
        result = session.run(cypher, params or {})
        return result.data()  # list[dict]


# --------------------------------------------------------------------
# Cypher queries
# --------------------------------------------------------------------


TABLE_QUERY = """
MATCH (b:Biomarker)
WHERE toLower(b.name) CONTAINS toLower($q)
OPTIONAL MATCH (b)<-[:MEASURES]-(d:Device)
OPTIONAL MATCH (b)-[:ASSOCIATED_WITH]->(dis:Disease)
WITH b,
     count(DISTINCT d)   AS n_devices,
     count(DISTINCT dis) AS n_diseases
RETURN
  b.biomarker_id                   AS biomarker_id,
  b.name                           AS biomarker,
  coalesce(b.score, 0.0)           AS score,
  n_devices                        AS devices,
  n_diseases                       AS diseases,
  coalesce(b.specimen_list, "")    AS specimens,
  coalesce(b.method_list, "")      AS methods
ORDER BY score DESC, biomarker
LIMIT $limit
"""


GRAPH_QUERY = """
// get up to $max_biomarkers biomarker nodes matching query
MATCH (b:Biomarker)
WHERE toLower(b.name) CONTAINS toLower($q)
WITH b
ORDER BY coalesce(b.score, 0.0) DESC
LIMIT $max_biomarkers

// pull connected devices & diseases
OPTIONAL MATCH (b)<-[:MEASURES]-(d:Device)
OPTIONAL MATCH (b)-[:ASSOCIATED_WITH]->(dis:Disease)
RETURN
  id(b)                         AS b_id,
  b.name                        AS biomarker,
  collect(DISTINCT {
      id: id(d),
      name: d.device_name
  })                            AS devices,
  collect(DISTINCT {
      id: id(dis),
      name: dis.name
  })                            AS diseases
"""


# --------------------------------------------------------------------
# Graph rendering with PyVis (no matplotlib)
# --------------------------------------------------------------------


def build_pyvis_graph(rows):
    """
    Build a PyVis Network from Neo4j query rows.
    Each row is a dict with: b_id, biomarker, devices, diseases.
    """
    net = Network(height="650px", width="100%", bgcolor="#ffffff", font_color="#222")
    net.barnes_hut()

    # Add biomarker, device, and disease nodes + edges
    for row in rows:
        b_id = row["b_id"]
        biomarker = row["biomarker"]
        devices = row.get("devices") or []
        diseases = row.get("diseases") or []

        biomarker_node_id = f"b_{b_id}"
        net.add_node(
            biomarker_node_id,
            label=biomarker,
            title=f"Biomarker: {biomarker}",
            color="#1f77b4",
            shape="dot",
            size=18,
        )

        # Devices
        for dev in devices:
            if dev["id"] is None or dev["name"] is None:
                continue
            dev_node_id = f"d_{dev['id']}"
            net.add_node(
                dev_node_id,
                label=dev["name"],
                title=f"Device: {dev['name']}",
                color="#2ca02c",
                shape="dot",
                size=12,
            )
            net.add_edge(biomarker_node_id, dev_node_id)

        # Diseases
        for dis in diseases:
            if dis["id"] is None or dis["name"] is None:
                continue
            dis_node_id = f"dis_{dis['id']}"
            net.add_node(
                dis_node_id,
                label=dis["name"],
                title=f"Disease: {dis['name']}",
                color="#d62728",
                shape="dot",
                size=12,
            )
            net.add_edge(biomarker_node_id, dis_node_id)

    return net


def show_pyvis(net: Network):
    """
    Render a PyVis Network inside Streamlit using a temporary HTML file.
    """
    # create a temporary file for the HTML
    with tempfile.NamedTemporaryFile(delete=False, suffix=".html") as tmp_file:
        path = tmp_file.name
    net.show(path)

    # read the HTML and embed it
    with open(path, "r", encoding="utf-8") as f:
        html = f.read()
    os.remove(path)

    st.components.v1.html(html, height=650, scrolling=True)


# --------------------------------------------------------------------
# Streamlit UI
# --------------------------------------------------------------------


st.set_page_config(page_title="Biomarker / Disease / Method Search", layout="wide")

st.sidebar.header("Search settings")
query_text = st.sidebar.text_input(
    "Search by biomarker name or keyword", value="glucose"
)

max_rows = st.sidebar.slider("Max table rows", min_value=10, max_value=200, value=50)
max_graph_biomarkers = st.sidebar.slider(
    "Max graph biomarkers", min_value=5, max_value=100, value=40
)

run = st.sidebar.button("Run search")

st.title("Biomarker / Disease / Method Search")
st.write(
    "Search across your **biomarkers**, linked **diseases**, **devices**, "
    "**specimens** (biofluids) and **detection methods** from the Neo4j Aura graph."
)

if run:
    if not query_text.strip():
        st.warning("Please enter a biomarker name or keyword.")
        st.stop()

    # ----------------- Tabular results -----------------
    with st.spinner("Running Neo4j query for table results..."):
        table_rows = run_query(TABLE_QUERY, {"q": query_text, "limit": max_rows})

    if not table_rows:
        st.warning("No biomarkers matched your query/filters.")
        st.stop()

    df = pd.DataFrame(table_rows)

    # Make specimens & methods a bit nicer to read (split on ';')
    if "specimens" in df.columns:
        df["specimens"] = df["specimens"].fillna("").apply(
            lambda s: ", ".join([x.strip() for x in s.split(";") if x.strip()])
        )
    if "methods" in df.columns:
        df["methods"] = df["methods"].fillna("").apply(
            lambda s: ", ".join([x.strip() for x in s.split(";") if x.strip()])
        )

    st.subheader("Tabular results")
    st.dataframe(df)

    # ----------------- Network view -----------------
    st.subheader("Network view")
    with st.spinner("Building biomarker network..."):
        graph_rows = run_query(
            GRAPH_QUERY,
            {"q": query_text, "max_biomarkers": max_graph_biomarkers},
        )

    if not graph_rows:
        st.info("No graph neighborhood available for this query with current filters.")
    else:
        net = build_pyvis_graph(graph_rows)
        show_pyvis(net)
else:
    st.info("Enter a search term on the left and click **Run search**.")
