"""DuckLake connection shared by publisher (read-write) and reader (read-only)."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

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


def quote_ident(name: str) -> str:
    """One identifier (schema, table, column, view) as DuckDB SQL: always double-quoted, an
    embedded double quote doubled. Every name taken from a contract, the catalogue or a caller
    reaches SQL text through it; only the library's own constants stay raw (the attach alias
    LAKE, the ref schema's tables and columns in REF_DDL, internal registrations like _batch).

    Contract names already pass NAME_RE, which still admits SQL keywords (`order`, `group`);
    names read back from a catalogue were never checked. Quoting makes any string name exactly
    that identifier and nothing else, so it can neither break the statement nor inject into it.
    Quoting does not change what is stored: DuckDB matches identifiers case-insensitively
    whether quoted or not, and the names here are lowercase.
    """
    if not isinstance(name, str) or not name or "\x00" in name:
        raise ValueError(f"not an identifier: {name!r}")
    return '"' + name.replace('"', '""') + '"'


def qualified(*parts: str) -> str:
    """A dotted name, every part quoted: qualified("lake", "facts", "t") -> "lake"."facts"."t"."""
    return ".".join(quote_ident(p) for p in parts)


def connect(cfg: StoreConfig, *, read_only: bool) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL ducklake")
    con.execute("LOAD ducklake")
    data_path = cfg.lake_data.as_posix().replace("'", "''")
    opts = [f"DATA_PATH '{data_path}/'"]
    if read_only:
        # A reader may see the files under another path than the publisher did (a NAS
        # publisher, a reader on a mounted share). DuckLake records data files relative to its
        # data path, so a read-only session may point elsewhere; the catalogue keeps its own
        # path. A writer never overrides: a wrong data path must fail, not split the lake.
        opts += ["READ_ONLY", "OVERRIDE_DATA_PATH true"]
    else:
        cfg.lake_data.mkdir(parents=True, exist_ok=True)
        opts.append(f"DATA_INLINING_ROW_LIMIT {int(cfg.data_inlining_row_limit)}")
    catalog = cfg.catalog.replace("'", "''")
    con.execute(f"ATTACH 'ducklake:{catalog}' AS {LAKE} ({', '.join(opts)})")
    if not read_only:
        for ddl in REF_DDL:
            con.execute(ddl)
    return con


@contextmanager
def registered(con: duckdb.DuckDBPyConnection, name: str, table: object) -> Iterator[None]:
    """Register `table` as the view `name` for the block, and unregister it after.

    When a statement of the block fails inside a transaction, DuckDB aborts the transaction and
    refuses every later call until ROLLBACK, `unregister` included ("Current transaction is
    aborted"). Unregistering in a plain `finally` would then replace the error that matters with
    that one in the run report. So: when the block raised, a failing unregister is dropped (the
    caller's ROLLBACK clears the view anyway) and the block's error propagates; when the block
    succeeded, a failing unregister raises as usual.
    On DuckDB 2.0 the caller's ROLLBACK drops the view with the aborted transaction, so a dropped
    unregister leaves nothing behind. Only if that ROLLBACK fails too does the connection keep the
    Arrow table, until the same name is registered again or the connection closes.
    """
    con.register(name, table)
    try:
        yield
    except BaseException:
        try:
            con.unregister(name)
        except duckdb.Error:
            pass
        raise
    con.unregister(name)
