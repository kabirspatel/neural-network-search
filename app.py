import streamlit as st
from neo4j import GraphDatabase
import pandas as pd
import networkx as nx
from pyvis.network import Network


# ----------------------------
# Neo4j connection
# ----------------------------

@st.cache_resource
def get_driver():
    """Create a single Neo4j driver for the lifecycle of the app."""
    uri = st.secrets["NEO4J_URI"]
    user = st.secrets["NEO4J_USER"]
    password = st.secrets["NEO4J_PASSWORD"]
    driver = GraphDatabase.driver(uri, auth=(user, password))
    return driver


def run_biomarker_search(
    driver,
    query_text,
    specimen_filter,
    provenance_mode,
    max_rows,
):
    """
    Search biomarkers by name using the full-text index, and return a table
    including specimen_list and specimen_source.
    """
    cypher = """
    CALL db.index.fulltext.queryNodes('biomarkerNameIndex', $q)
    YIELD node, score
    WITH node AS b, score
    WHERE
        // optional specimen text filter (e.g. 'urine')
        ($specimen IS NULL OR toLower(b.specimen_list) CONTAINS toLower($specimen))
        AND
        (
            $prov_mode = 'all'
            OR (
                $prov_mode = 'exclude_heuristic'
                AND (b.specimen_source IS NULL OR NOT b.specimen_source STARTS WITH 'heuristic_')
            )
            OR (
                $prov_mode = 'heuristic_only'
                AND b.specimen_source STARTS WITH 'heuristic_'
            )
        )
    OPTIONAL MATCH (b)<-[:MEASURES]-(d:Device)
    OPTIONAL MATCH (b)-[:ASSOCIATED_WITH]->(di:Disease)
    RETURN
        b.biomarker_id AS biomarker_id,
        b.name         AS biomarker,
        b.specimen_list AS specimen,
        b.specimen_source AS specimen_source,
        score,
        size(collect(DISTINCT d))  AS device_count,
        size(collect(DISTINCT di)) AS disease_count
    ORDER BY score DESC
    LIMIT $limit
    """

    params = {
        "q": query_text if query_text.strip() != "" else "*",
        "specimen": specimen_filter if specimen_filter.strip() != "" else None,
        "prov_mode": provenance_mode,
        "limit": max_rows,
    }

    with driver.session() as session:
        records = session.run(cypher, params)
        rows = [r.data() for r in records]

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    return df


def load_neighborhood_graph(driver, biomarker_ids, max_nodes=150):
    """
    Build a simple networkx graph for a list of biomarker_ids.
    Nodes: biomarkers, devices, diseases.
    """

    if not biomarker_ids:
        return nx.Graph()

    cypher = """
    MATCH (b:Biomarker)
    WHERE b.biomarker_id IN $ids
    OPTIONAL MATCH (b)<-[m:MEASURES]-(d:Device)
    OPTIONAL MATCH (b)-[a:ASSOCIATED_WITH]->(di:Disease)
    RETURN b, collect(DISTINCT d) AS devices, collect(DISTINCT di) AS diseases
    """

    with driver.session() as session:
        records = session.run(cypher, {"ids": biomarker_ids})
        rows = [r.data() for r in records]

    G = nx.Graph()

    # Add nodes & edges
    for row in rows:
        b = row["b"]
        b_id = b["biomarker_id"]
        b_name = b.get("name", f"BMK {b_id}")
        G.add_node(b_id, label=b_name, kind="biomarker")

        for dev in row["devices"]:
            if dev is None:
                continue
            d_id = dev["device_id"]
            d_name = dev.get("device_name", f"Device {d_id}")
            G.add_node(d_id, label=d_name, kind="device")
            G.add_edge(b_id, d_id)

        for di in row["diseases"]:
            if di is None:
                continue
            dis_id = di["disease_id"]
            dis_name = di.get("name", f"Disease {dis_id}")
            G.add_node(dis_id, label=dis_name, kind="disease")
            G.add_edge(b_id, dis_id)

    # If graph is too large, trim to first N nodes
    if G.number_of_nodes() > max_nodes:
        # simple trimming: keep first max_nodes nodes
        nodes_to_keep = list(G.nodes())[:max_nodes]
        G = G.subgraph(nodes_to_keep).copy()

    return G


def draw_graph(G):
    """Render the NetworkX graph in Streamlit using PyVis (no matplotlib)."""

    if G.number_of_nodes() == 0:
        st.info("No graph neighborhood available for this query.")
        return

    # Create PyVis network
    net = Network(height="650px", width="100%", bgcolor="#ffffff", font_color="black")

    # Improve physics (layout)
    net.force_atlas_2based(gravity=-30, central_gravity=0.01, spring_length=100, spring_strength=0.01)

    # Add nodes with colors based on kind
    for n, d in G.nodes(data=True):
        kind = d.get("kind", "")

        if kind == "biomarker":
            color = "#1f77b4"
        elif kind == "device":
            color = "#2ca02c"
        elif kind == "disease":
            color = "#d62728"
        else:
            color = "#7f7f7f"

        net.add_node(
            n,
            label=d.get("label", str(n)),
            color=color,
            title=f"{kind}: {d.get('label','')}"
        )

    # Add edges
    for u, v in G.edges():
        net.add_edge(u, v)

    # Save and display
    net.save_graph("graph.html")
    st.components.v1.html(open("graph.html", "r").read(), height=650, scrolling=True)


# ----------------------------
# Streamlit layout
# ----------------------------

def main():
    st.set_page_config(page_title="Biomarker / Disease / Method Search", layout="wide")

    st.title("Biomarker / Disease / Method Search")
    st.write(
        "Search across your **biomarkers**, linked diseases, devices and specimen "
        "(biofluid) information from the Neo4j Aura graph. "
        "Specimen provenance is shown so you can distinguish heuristic vs curated data."
    )

    driver = get_driver()

    # Sidebar controls
    with st.sidebar:
        st.header("Search settings")

        query_text = st.text_input("Search by biomarker name or keyword", "BRCA1")

        specimen_filter = st.text_input(
            "Specimen text filter (optional)",
            help="e.g. 'urine', 'blood', 'tumor tissue'. "
                 "This matches against the specimen_list text."
        )

        provenance_mode = st.radio(
            "Specimen provenance",
            options=["all", "exclude_heuristic", "heuristic_only"],
            index=0,
            help=(
                "'exclude_heuristic' hides any specimens inferred by heuristics "
                "(sources starting with 'heuristic_'). "
                "'heuristic_only' shows only those."
            ),
        )

        max_rows = st.slider("Max table rows", min_value=10, max_value=200, value=50, step=10)
        max_graph_biomarkers = st.slider(
            "Max biomarkers in graph", min_value=5, max_value=100, value=40, step=5
        )

        run_button = st.button("Run search")

    if not run_button:
        st.info("Set your filters on the left and click **Run search**.")
        return

    # Run query
    with st.spinner("Querying Neo4j..."):
        df = run_biomarker_search(
            driver,
            query_text=query_text,
            specimen_filter=specimen_filter,
            provenance_mode=provenance_mode,
            max_rows=max_rows,
        )

    st.subheader("Tabular results")
    if df.empty:
        st.warning("No biomarkers matched your query/filters.")
    else:
        st.dataframe(df)

    # Network view for top N biomarkers
    st.subheader("Network view")

    if df.empty:
        st.info("No graph neighborhood available for this query.")
        return

    biomarker_ids = list(df["biomarker_id"].astype(str))[:max_graph_biomarkers]

    with st.spinner("Loading graph neighborhood..."):
        G = load_neighborhood_graph(driver, biomarker_ids)

    draw_graph(G)

    st.caption(
        "Note: specimen_source indicates where the specimen_list value came from "
        "(e.g. heuristic_text_v1, heuristic_text_v2, curated_xxx, etc.)."
    )


if __name__ == "__main__":
    main()
