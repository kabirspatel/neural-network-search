import requests
import pandas as pd
from neo4j import GraphDatabase
import os

FDA_ENDPOINT = "https://api.fda.gov/device/510k.json"
MAX = 1000  
QUERY = (
    'device_name:("assay" OR "analyzer" OR "biosensor" OR "immunoassay" OR "pcr" OR "lamp" '
    'OR "fluorescence" OR "urinalysis" OR "dipstick" OR "colorimetric")'
)

NEO4J_URI = os.environ["NEO4J_URI"]
NEO4J_USER = os.environ["NEO4J_USER"]
NEO4J_PASSWORD = os.environ["NEO4J_PASSWORD"]

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def fetch_fda_devices():
    url = f"{FDA_ENDPOINT}?search={QUERY}&limit={MAX}"
    print("[INFO] Fetching from:", url)
    r = requests.get(url)
    r.raise_for_status()
    return r.json().get("results", [])

def clean_device(rec):
    return {
        "k_number": rec.get("k_number"),
        "device_name": rec.get("device_name"),
        "product_code": rec.get("product_code"),
        "specialty": rec.get("medical_specialty_description"),
        "date": rec.get("decision_date"),
        "statement": rec.get("statement_or_summary")
    }

def import_devices(devices):
    with driver.session() as session:
        for d in devices:
            session.run(
                """
                MERGE (dev:Device {k_number: $k})
                SET dev.name = $name,
                    dev.product_code = $pcode,
                    dev.specialty = $spec,
                    dev.summary = $sum,
                    dev.date = $dt
                """,
                k=d["k_number"],
                name=d["device_name"],
                pcode=d["product_code"],
                spec=d["specialty"],
                sum=d["statement"],
                dt=d["date"]
            )

            # detection method node
            method = infer_detection_method(d["device_name"])
            session.run(
                """
                MERGE (m:DetectionMethod {name: $method})
                MERGE (dev:Device {k_number: $k})
                MERGE (dev)-[:USES_METHOD]->(m)
                """,
                method=method,
                k=d["k_number"]
            )

def infer_detection_method(name):
    name_lower = name.lower()
    if "pcr" in name_lower or "polymerase" in name_lower: return "PCR"
    if "lamp" in name_lower: return "LAMP"
    if "fluor" in name_lower: return "Fluorescence"
    if "color" in name_lower or "colorimetric" in name_lower: return "Colorimetric"
    if "immuno" in name_lower: return "Immunoassay"
    if "analyzer" in name_lower: return "Analyzer"
    if "biosensor" in name_lower: return "Biosensor"
    if "dipstick" in name_lower or "urine" in name_lower: return "Urinalysis"
    return "General Assay"

if __name__ == "__main__":
    print("[INFO] Pulling FDA devices...")
    records = fetch_fda_devices()
    cleaned = [clean_device(r) for r in records]

    print(f"[INFO] Importing {len(cleaned)} devices into Neo4j...")
    import_devices(cleaned)
    print("[INFO] DONE.")
