"""
Generates SQL to add and populate an 'ontology' column in lu_brand.
Input : brands_wikidata_final.csv (same folder as this script)
Output: update_lu_brand_ontology.sql (same folder)
"""
import pandas as pd
from pathlib import Path

HERE     = Path(__file__).parent
CSV_FILE = HERE / "brands_wikidata_final.csv"
OUT_FILE = HERE / "update_lu_brand_ontology.sql"

df = pd.read_csv(CSV_FILE)

lines = []

# ── 1. ALTER TABLE ────────────────────────────────────────────────────────────
lines.append("-- Add ontology column")
lines.append("ALTER TABLE lu_brand ADD COLUMN ontology VARCHAR(500);\n")

# ── 2. UPDATE statements ──────────────────────────────────────────────────────
lines.append("-- Populate ontology from Wikidata_Label")
for _, row in df.iterrows():

    if pd.isna(row["Brand_ID"]):
        continue
    brand_id = int(row["Brand_ID"])

    label = str(row["Wikidata_Label"]).strip()
    if label in ("no direct Wikidata match", "--", "", "nan"):
        ontology = "No Ontology available"
    else:
        ontology = label

    # escape single quotes for SQL string literals
    ontology_escaped = ontology.replace("'", "''")

    lines.append(
        f"UPDATE lu_brand SET ontology = '{ontology_escaped}' WHERE Brand_ID = {brand_id};"
    )

sql = "\n".join(lines)

OUT_FILE.write_text(sql, encoding="utf-8")
print(f"Written {len(df)} UPDATE statements → {OUT_FILE}")
print("\nPreview (first 5 lines):")
for line in sql.splitlines()[:7]:
    print(" ", line)
