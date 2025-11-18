import streamlit as st
from neo4j import GraphDatabase
import networkx as nx
from pyvis.network import Network

# -------------------------------
# Neo4j driver
# -------------------------------

@st.cache_resource
def get_driver():
    uri = st.secrets["neo4j"]["uri"]
    user = st.secrets["neo4j"]["user"]
    password = st.secrets["neo4j"]["password"]
    return GraphDatabase.driver(uri, auth=(user, password))


def run_query(cypher, params=None):
    driver = get_driver()
    with driver.session() as session:
        result = session.run(cypher, params or {})
        return [r.data() for r in result]


# -------------------------------
# Graph rendering (PyVis)
# -------------------------------

def draw_graph(G):
    """Render the NetworkX graph in Streamlit using PyVis (no matplotlib)."""

    if G.number_of_nodes() == 0:
        st.info("No graph neighborhood available for this query.")
        return

    net = Network(height="650px", width="100%", bgcolor="#ffffff", font_color="black")

    net.force_atlas_2based(
        gravity=-30,
        central_gravity=0.01,
        spring_length=100,
        spring_strength=0.01,
    )

    for n, d in G.nodes(data=True):
        kind = d.get("kind", "")
        if kind == "biomarker":
            color = "#1f77b4"
        elif kind == "disease":
            color = "#d62728"
        elif kind == "specimen":
            color = "#9467bd"
        else:
            color = "#7f7f7f"

        net.add_node(
            n,
            label=d.get("label", str(n)),
            color=color,
            title=f"{kind}: {d.get('label','')}",
        )

    for u, v in G.edges():
        net.add_edge(u, v)

    net.save_graph("graph.html")
    with open("graph.html", "r", encoding="utf-8") as f:
        html = f.read()
    st.components.v1.html(html, height=650, scrolling=True)


# -------------------------------
# App layout
# -------------------------------

st.set_page_config(page_title="Biomarker / Disease / Specimen Search", layout="wide")

st.title("Biomarker / Disease / Specimen Search")
st.write(
    "Search across your **biomarkers**, linked **diseases**, and **specimens** "
    "loaded from public NCBI / PubMed data plus other sources."
)

with st.sidebar:
    st.header("Search settings")
    query = st.text_input("Search by biomarker name or keyword", "glucose")
    max_rows = st.slider("Max table rows", 10, 200, 50)
    max_biomarkers_graph = st.slider("Max graph biomarkers", 5, 100, 40)
    run_btn = st.button("Run search")

if run_btn:
    # 1) Main search query: gather biomarker + specimens + diseases
    cypher = """
    MATCH (b:Biomarker)
    WHERE toLower(b.name) CONTAINS toLower($q)
       OR any(a IN coalesce(b.aliases, []) WHERE toLower(a) CONTAINS toLower($q))
    OPTIONAL MATCH (b)-[:MEASURED_IN]->(s:Specimen)
    OPTIONAL MATCH (b)-[:ASSOCIATED_WITH]->(d:Disease)
    RETURN
        id(b) AS biomarker_id,
        b.name AS biomarker,
        collect(DISTINCT s.name) AS specimens,
        collect(DISTINCT d.name) AS diseases
    ORDER BY biomarker
    LIMIT $limit
    """
    rows = run_query(cypher, {"q": query, "limit": max_rows})

    st.subheader("Tabular results")
    if not rows:
        st.warning("No biomarkers matched your query.")
    else:
        # Format for display
        table_rows = []
        biomarker_ids_for_graph = []
        for r in rows:
            biomarker_ids_for_graph.append(r["biomarker_id"])
            table_rows.append(
                {
                    "Biomarker": r["biomarker"],
                    "Specimens": ", ".join(sorted(set(r["specimens"])) or ["—"]),
                    "Diseases": "; ".join(sorted(set(r["diseases"])) or ["—"]),
                }
            )

        st.dataframe(table_rows, use_container_width=True)

        # 2) Build graph neighborhood
        st.subheader("Network view")

        graph_cypher = """
        MATCH (b:Biomarker)
        WHERE id(b) IN $ids
        OPTIONAL MATCH (b)-[r]->(x)
        WHERE x:Biomarker OR x:Disease OR x:Specimen
        RETURN b, r, x
        LIMIT 1000
        """
        graph_rows = run_query(
            graph_cypher,
            {
                "ids": biomarker_ids_for_graph[:max_biomarkers_graph]
            },
        )

        G = nx.Graph()

        for row in graph_rows:
            b = row["b"]
            x = row.get("x")
            # Add biomarker node
            b_id = b.id
            G.add_node(
                b_id,
                kind="biomarker",
                label=b.get("name", "Biomarker"),
            )

            if x is not None:
                x_id = x.id
                label = x.get("name", "Node")
                if "Disease" in x.labels:
                    kind = "disease"
                elif "Specimen" in x.labels:
                    kind = "specimen"
                else:
                    kind = "other"

                G.add_node(
                    x_id,
                    kind=kind,
                    label=label,
                )
                G.add_edge(b_id, x_id)

        draw_graph(G)

else:
    st.info("Enter a biomarker keyword (e.g. **glucose**) and click **Run search**.")

