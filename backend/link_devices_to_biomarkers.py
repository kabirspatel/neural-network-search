#!/usr/bin/env python
"""
Link Devices -> Biomarkers using rules defined in data/biomarker_device_rules.csv.

Each row in the CSV has:
- biomarker_name  : human-readable biomarker name (we'll match via CONTAINS on b.name)
- match_tokens    : semicolon-separated tokens that must all appear in the FDA device_name

For every rule we:
1) Find matching Biomarker nodes.
2) Find Device nodes whose device_name contains all tokens.
3) MERGE (Device)-[:MEASURES {source: 'fda_name_rule', rule_id: ...}]->(Biomarker)
"""

import csv
import os
from pathlib import Path

from neo4j import GraphDatabase

NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "password")

RULES_CSV = Path("data/biomarker_device_rules.csv")


def load_rules(path: Path):
    rules = []
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=1):
            biomarker_name = row["biomarker_name"].strip()
            tokens_raw = row["match_tokens"].strip()
            tokens = [t.strip().lower() for t in tokens_raw.split(";") if t.strip()]
            if not biomarker_name or not tokens:
                continue
            rules.append(
                {
                    "rule_id": i,
                    "biomarker_name": biomarker_name,
                    "tokens": tokens,
                }
            )
    return rules


def link_for_rule(tx, rule):
    """
    For a single rule:
    - Match Biomarkers whose name contains biomarker_name (case-insensitive)
    - Match Devices whose device_name contains ALL tokens
    - MERGE (Device)-[MEASURES]->(Biomarker)
    """
    cypher = """
    WITH $rule AS rule
    WITH rule.rule_id AS rule_id,
         rule.biomarker_name AS biomarker_name,
         rule.tokens AS tokens

    // 1) Find matching biomarkers
    MATCH (b:Biomarker)
    WHERE toLower(b.name) CONTAINS toLower(biomarker_name)
    WITH rule_id, tokens, COLLECT(b) AS biomarkers

    // If no biomarkers, stop early
    WHERE size(biomarkers) > 0

    UNWIND biomarkers AS b

    // 2) Find devices whose device_name contains ALL tokens
    MATCH (d:Device)
    WHERE ALL(t IN tokens WHERE toLower(d.device_name) CONTAINS t)

    // 3) Merge MEASURES edges
    MERGE (d)-[r:MEASURES]->(b)
      ON CREATE SET
        r.source = 'fda_name_rule',
        r.rule_id = rule_id

    RETURN
      rule_id AS rule_id,
      COUNT(DISTINCT b) AS biomarkers_touched,
      COUNT(DISTINCT d) AS devices_matched,
      COUNT(r) AS relationships_created
    """
    result = tx.run(cypher, rule=rule)
    # Single row or no row
    records = list(result)
    if not records:
        return {
            "rule_id": rule["rule_id"],
            "biomarkers_touched": 0,
            "devices_matched": 0,
            "relationships_created": 0,
        }
    rec = records[0]
    return {
        "rule_id": rec["rule_id"],
        "biomarkers_touched": rec["biomarkers_touched"],
        "devices_matched": rec["devices_matched"],
        "relationships_created": rec["relationships_created"],
    }


def main():
    if not RULES_CSV.exists():
        raise SystemExit(f"Rules CSV not found: {RULES_CSV}")

    rules = load_rules(RULES_CSV)
    print(f"[INFO] Loaded {len(rules)} rules from {RULES_CSV} ...")

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    total_biomarkers = 0
    total_devices = 0
    total_rels = 0

    with driver:
        with driver.session() as session:
            for rule in rules:
                print(
                    f"\n[INFO] === Rule {rule['rule_id']} for biomarker '{rule['biomarker_name']}' "
                    f"(tokens={rule['tokens']}) ==="
                )
                stats = session.execute_write(link_for_rule, rule)
                print(
                    f"[INFO] Biomarkers touched: {stats['biomarkers_touched']}, "
                    f"Devices matched: {stats['devices_matched']}, "
                    f"MEASURES relationships created (or matched): {stats['relationships_created']}"
                )
                total_biomarkers += stats["biomarkers_touched"]
                total_devices += stats["devices_matched"]
                total_rels += stats["relationships_created"]

    print(
        f"\n[INFO] DONE. Summary across all rules:\n"
        f"       Biomarkers touched: {total_biomarkers}\n"
        f"       Devices matched (distinct per rule): {total_devices}\n"
        f"       MEASURES relationships processed: {total_rels}"
    )


if __name__ == "__main__":
    main()
