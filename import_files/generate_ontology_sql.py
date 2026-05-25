"""
Generates SQL to add and populate an 'ontology' column in any lookup table.

Usage (CLI):
    python generate_ontology_sql.py --csv brands_wikidata_final.csv \
                                    --table lu_brand \
                                    --id-col Brand_ID \
                                    --label-col Wikidata_Label \
                                    --out update_lu_brand_ontology.sql

Usage (import):
    from generate_ontology_sql import generate_ontology_sql
    generate_ontology_sql(
        csv_file   = "brands_wikidata_final.csv",
        table_name = "lu_brand",
        id_col     = "Brand_ID",
        label_col  = "Wikidata_Label",
    )
"""
import argparse
import pandas as pd
from pathlib import Path

NO_MATCH_VALUES = {"no direct Wikidata match", "--", "", "nan"}
FALLBACK_LABEL  = "No Ontology available"


def generate_ontology_sql(
    csv_file:   str | Path,
    table_name: str,
    id_col:     str,
    label_col:  str,
    out_file:   str | Path | None = None,
    ontology_col: str = "ontology",
    col_type:   str = "VARCHAR(500)",
) -> Path:
    """
    Read *csv_file*, generate ALTER TABLE + UPDATE statements, write to *out_file*.

    Parameters
    ----------
    csv_file     : path to input CSV
    table_name   : SQL table to update  (e.g. 'lu_brand')
    id_col       : CSV column used in WHERE clause  (e.g. 'Brand_ID')
    label_col    : CSV column whose value becomes the ontology label
    out_file     : output .sql path; defaults to  update_<table_name>_ontology.sql
                   next to the CSV file
    ontology_col : name of the new column (default 'ontology')
    col_type     : SQL data type for the new column (default 'VARCHAR(500)')

    Returns
    -------
    Path of the written SQL file
    """
    csv_file = Path(csv_file)
    if out_file is None:
        out_file = csv_file.parent / f"update_{table_name}_{ontology_col}.sql"
    out_file = Path(out_file)

    df = pd.read_csv(csv_file)

    missing = [c for c in (id_col, label_col) if c not in df.columns]
    if missing:
        raise ValueError(f"Columns not found in CSV: {missing}. Available: {df.columns.tolist()}")

    lines = []

    # ── ALTER TABLE ───────────────────────────────────────────────────────────
    lines.append(f"-- Add {ontology_col} column to {table_name}")
    lines.append(f"ALTER TABLE {table_name} ADD COLUMN {ontology_col} {col_type};\n")

    # ── UPDATE statements ─────────────────────────────────────────────────────
    lines.append(f"-- Populate {ontology_col} from {label_col}")
    updated = skipped = 0
    for _, row in df.iterrows():
        if pd.isna(row[id_col]):
            skipped += 1
            continue

        row_id = row[id_col]
        # keep numeric IDs as integers (drop trailing .0)
        if isinstance(row_id, float) and row_id.is_integer():
            row_id = int(row_id)

        label = str(row[label_col]).strip()
        ontology = FALLBACK_LABEL if label in NO_MATCH_VALUES else label
        ontology_escaped = ontology.replace("'", "''")

        lines.append(
            f"UPDATE {table_name} "
            f"SET {ontology_col} = '{ontology_escaped}' "
            f"WHERE {id_col} = {row_id};"
        )
        updated += 1

    sql = "\n".join(lines)
    out_file.write_text(sql, encoding="utf-8")

    print(f"[{table_name}] {updated} UPDATE statements written → {out_file}")
    if skipped:
        print(f"  ({skipped} rows skipped — missing {id_col})")
    return out_file


# ── pre-configured calls ──────────────────────────────────────────────────────
HERE = Path(__file__).parent

CONFIGS = [
    dict(
        csv_file   = HERE / "brands_wikidata_final.csv",
        table_name = "lu_brand",
        id_col     = "Brand_ID",
        label_col  = "Wikidata_Label",
    ),
    dict(
        csv_file   = HERE / "items_wikidata_lookup.csv",
        table_name = "lu_item",          # adjust table name if different
        id_col     = "Item_ID",          # adjust if column is named differently
        label_col  = "Wikidata_Label",   # adjust if column is named differently
    ),
]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate ontology UPDATE SQL from a Wikidata CSV.")
    parser.add_argument("--csv",       help="CSV input file")
    parser.add_argument("--table",     help="SQL table name")
    parser.add_argument("--id-col",    help="ID column name (WHERE clause)")
    parser.add_argument("--label-col", help="Label column name (SET value)")
    parser.add_argument("--out",       help="Output .sql file (optional)")
    args = parser.parse_args()

    if args.csv:
        # explicit CLI invocation
        generate_ontology_sql(
            csv_file   = args.csv,
            table_name = args.table,
            id_col     = args.id_col,
            label_col  = args.label_col,
            out_file   = args.out,
        )
    else:
        # run all pre-configured tables
        for cfg in CONFIGS:
            if Path(cfg["csv_file"]).exists():
                generate_ontology_sql(**cfg)
            else:
                print(f"[SKIP] CSV not found: {cfg['csv_file']}")
