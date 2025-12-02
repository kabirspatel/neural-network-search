import csv
import os
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

INPUT_CSV = "data/biomarker_disease_edges.csv"
OUTPUT_CSV = "data/biomarker_disease_edges_pubmed.csv"

EMAIL = os.environ.get("NCBI_EMAIL", "your_email@example.com")
TOOL = os.environ.get("NCBI_TOOL", "urine_biomarker_project")
API_KEY = os.environ.get("NCBI_API_KEY")  # optional


def fetch_pubmed_count(term: str) -> int:
    """
    Query PubMed for a simple text term and return the hit count.
    Uses esearch.fcgi with retmode=xml and rettype=count.
    """
    params = {
        "db": "pubmed",
        "term": term,
        "retmode": "xml",
        "rettype": "count",
        "tool": TOOL,
        "email": EMAIL,
    }
    if API_KEY:
        params["api_key"] = API_KEY

    url = (
        "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?"
        + urllib.parse.urlencode(params)
    )

    with urllib.request.urlopen(url) as resp:
        xml_bytes = resp.read()

    root = ET.fromstring(xml_bytes)
    count_text = root.findtext(".//Count")
    try:
        return int(count_text) if count_text is not None else 0
    except ValueError:
        return 0


def main():
    print(f"[INFO] Loading edges from {INPUT_CSV} ...")
    with open(INPUT_CSV, newline="", encoding="utf-8") as f:
        reader = list(csv.DictReader(f))

    print(f"[INFO] Found {len(reader)} biomarker–disease edges.")

    # Build unique (biomarker_name, disease_name) pairs
    pairs = {}
    for row in reader:
        key = (row["biomarker_name"], row["disease_name"])
        pairs.setdefault(key, None)

    print(f"[INFO] Unique name-based pairs: {len(pairs)}")

    # Query PubMed for each pair
    for i, (key, _) in enumerate(pairs.items(), start=1):
        biomarker_name, disease_name = key
        # Simple robust term – same spirit as your working test
        term = f"{biomarker_name} {disease_name}"
        print(f"[INFO] [{i}/{len(pairs)}] Querying PubMed for: {term!r}")
        try:
            count = fetch_pubmed_count(term)
        except Exception as e:
            print(f"[WARN] PubMed query failed for {term!r}: {e}")
            count = 0
        pairs[key] = count
        # polite rate limit; we only have 48 pairs, so this is fine
        time.sleep(0.35)

    # Attach counts back onto each edge row
    for row in reader:
        key = (row["biomarker_name"], row["disease_name"])
        row["pubmed_query"] = f"{row['biomarker_name']} {row['disease_name']}"
        row["pubmed_count"] = str(pairs.get(key, 0))

    print(f"[INFO] Writing enriched edges to {OUTPUT_CSV} ...")
    fieldnames = list(reader[0].keys())
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(reader)

    print("[INFO] DONE. Enriched file written.")


if __name__ == "__main__":
    main()
