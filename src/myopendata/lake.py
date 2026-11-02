"""DuckLake connection shared by publisher (read-write) and reader (read-only)."""

from __future__ import annotations

import duckdb

from myopendata.config import StoreConfig

LAKE = "lake"
REF_DDL = (
    "CREATE SCHEMA IF NOT EXISTS lake.ref",
    "CREATE SCHEMA IF NOT EXISTS lake.facts",
    "CREATE SCHEMA IF NOT EXISTS lake.derived",
    """CREATE TABLE IF NOT EXISTS lake.ref.contracts (
        name VARCHAR, major INTEGER, version VARCHAR, fingerprint VARCHAR,
        definition_json VARCHAR, approved_by VARCHAR, approved_at TIMESTAMPTZ)""",
    """CREATE TABLE IF NOT EXISTS lake.ref.batches (
        batch_id VARCHAR, contract VARCHAR, major INTEGER, version VARCHAR, producer VARCHAR,
        run_id VARCHAR, rows BIGINT, content_sha256 VARCHAR, published_at TIMESTAMPTZ)""",
    # Additive ledger column (chantier 8): rows the audit found already published. Added by
    # ALTER, never in the CREATE, so new and migrated catalogues share one column order; a
    # no-op ALTER commits no snapshot.
    "ALTER TABLE lake.ref.batches ADD COLUMN IF NOT EXISTS rows_skipped BIGINT",
    """CREATE TABLE IF NOT EXISTS lake.ref.entity_identifiers (
        entity_id VARCHAR, scheme VARCHAR, value VARCHAR, source_code VARCHAR,
        first_known_at TIMESTAMPTZ, batch_id VARCHAR)""",
    """CREATE TABLE IF NOT EXISTS lake.ref.entity_attributes (
        entity_id VARCHAR, name VARCHAR, country VARCHAR, valid_from DATE, valid_to DATE,
        known_at TIMESTAMPTZ, source_code VARCHAR, batch_id VARCHAR)""",
)


def connect(cfg: StoreConfig, *, read_only: bool) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL ducklake")
    con.execute("LOAD ducklake")
    data_path = cfg.lake_data.as_posix().replace("'", "''")
    opts = [f"DATA_PATH '{data_path}/'"]
    if read_only:
        opts.append("READ_ONLY")
    else:
        cfg.lake_data.mkdir(parents=True, exist_ok=True)
        opts.append(f"DATA_INLINING_ROW_LIMIT {int(cfg.data_inlining_row_limit)}")
    catalog = cfg.catalog.replace("'", "''")
    con.execute(f"ATTACH 'ducklake:{catalog}' AS {LAKE} ({', '.join(opts)})")
    if not read_only:
        for ddl in REF_DDL:
            con.execute(ddl)
    return con
