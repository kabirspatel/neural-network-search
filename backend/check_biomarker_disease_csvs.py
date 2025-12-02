#!/usr/bin/env python
import csv
from pathlib import Path

INPUT_DIR = Path("data/biomarker_disease")

def main():
    files = sorted(INPUT_DIR.glob("*.csv"))
    print("[INFO] Found files:", [f.name for f in files])

    all_columns = None
    total_rows = 0

    for f in files:
        with f.open(newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            cols = reader.fieldnames
            print(f"[INFO] {f.name} columns: {cols}")
            if all_columns is None:
                all_columns = cols
            elif cols != all_columns:
                print(f"[WARN] Column mismatch in {f.name} vs first file!")
            count = sum(1 for _ in reader)
            print(f"[INFO] {f.name} has {count} data rows (excluding header)")
            total_rows += count

    print(f"[INFO] Total biomarker–disease rows across all files: {total_rows}")

if __name__ == "__main__":
    main()
