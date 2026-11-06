"""Read side: the interface the MCP, analyses and strategies code against (spec §2, §6).

    store = Store.open(StoreConfig.from_env())
    store.datasets()                                   # what is published, with licence
    store.read_asof("fundamentals", as_of=t, where="original_tag = 'us-gaap:Revenues'")
    store.assemble(recipe, as_of=t)                    # derived dataset (save=False in v0)
    export(table, "out.xlsx")                          # csv | xlsx | parquet by suffix

Security model: the lake is attached read-only, the session cannot touch files outside the lake,
attach databases or change its own settings, and every `where` or recipe must be ONE statement.
`where` and recipe SQL are still trusted caller code: a server exposing this to untrusted input
(the MCP) must pass structured filters, not free text.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq

from myopendata.config import StoreConfig
from myopendata.lake import connect

VIEW_RE = re.compile(r"[a-z_][a-z0-9_]{0,62}")
XLSX_MAX_ROWS = 1_048_575  # Excel's sheet limit minus the header row
RESERVED_VIEW_NAMES = ("lake", "memory", "main", "system", "temp")


class UnknownDataset(KeyError):
    pass


@dataclass(frozen=True)
class Recipe:
    """A derived dataset: inputs are read as of one date, then one DuckDB SQL statement.

    inputs maps a view name used in sql to a published dataset name (latest major unless
    "name_vN" is given). Example: Recipe("leverage", "1.0.0", {"f": "fundamentals"}, "SELECT ...").
    """

    name: str
    version: str
    inputs: dict[str, str]
    sql: str


@dataclass(frozen=True)
class _Dataset:
    name: str
    major: int
    version: str
    kind: str
    licence: str
    natural_key: tuple[str, ...]
    columns: tuple[dict, ...]
    doc: str

    @property
    def column_names(self) -> tuple[str, ...]:
        return ("entity_id",) + tuple(c["name"] for c in self.columns) + ("batch_id",)

    @property
    def table(self) -> str:
        return f"{self.name}_v{self.major}"

    @property
    def fq(self) -> str:
        return f"lake.{'facts' if self.kind == 'fact' else 'derived'}.{self.table}"


class Store:
    def __init__(self, cfg: StoreConfig):
        self.cfg = cfg
        self.con = connect(cfg, read_only=True)
        self.con.execute("SET TimeZone = 'UTC'")
        lake_dir = cfg.lake_data.as_posix().replace("'", "''")
        self.con.execute(f"SET allowed_directories = ['{lake_dir}/']")
        self.con.execute("SET enable_external_access = false")
        self.con.execute("SET lock_configuration = true")

    @classmethod
    def open(cls, cfg: StoreConfig | None = None) -> Store:
        return cls(cfg or StoreConfig.from_env())

    def close(self) -> None:
        self.con.close()

    def __enter__(self) -> Store:
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def _registered(self) -> dict[tuple[str, int], _Dataset]:
        """Datasets as approved in the lake: readable without the producer package installed."""
        rows = (
            self.con.cursor()
            .execute("SELECT name, major, version, definition_json FROM lake.ref.contracts")
            .fetchall()
        )
        out: dict[tuple[str, int], _Dataset] = {}
        for name, major, version, definition in rows:
            d = json.loads(definition)
            cur = out.get((name, major))
            if cur and _semver(cur.version) >= _semver(version):
                continue
            docs = d.get("docs", {})
            cols = tuple(
                {
                    "name": n,
                    "type": t,
                    "nullable": nullable,
                    "allowed": allowed or None,
                    "doc": docs.get("columns", {}).get(n, ""),
                }
                for n, t, nullable, allowed in d["columns"]
            )
            out[(name, major)] = _Dataset(
                name,
                major,
                version,
                d["kind"],
                d.get("licence", ""),
                tuple(d["natural_key"]),
                cols,
                docs.get("dataset", ""),
            )
        return out

    def datasets(self) -> list[dict]:
        tables = {
            (s, t): n
            for s, t, n in self.con.cursor()
            .execute(
                "SELECT schema_name, table_name, estimated_size FROM duckdb_tables() "
                "WHERE database_name = 'lake' AND schema_name IN ('facts', 'derived')"
            )
            .fetchall()
        }
        out = []
        for (_, _), d in sorted(self._registered().items()):
            schema = "facts" if d.kind == "fact" else "derived"
            if (schema, d.table) in tables:
                out.append(
                    {
                        "name": d.name,
                        "major": d.major,
                        "version": d.version,
                        "kind": d.kind,
                        "licence": d.licence,
                        "table": d.table,
                        "rows_estimate": tables[(schema, d.table)],
                    }
                )
        return out

    def _table_columns(self, d: _Dataset) -> set[str]:
        """Columns that exist in the published table (an approved but not yet published MINOR
        may declare more)."""
        schema = "facts" if d.kind == "fact" else "derived"
        rows = (
            self.con.cursor()
            .execute(
                "SELECT column_name FROM duckdb_columns() WHERE database_name = 'lake' "
                "AND schema_name = ? AND table_name = ?",
                [schema, d.table],
            )
            .fetchall()
        )
        return {r[0] for r in rows}

    def describe(self, dataset: str) -> dict:
        """Everything a caller (or an MCP tool listing) needs to query a dataset correctly."""
        d = self._dataset(dataset)
        return {
            "name": d.name,
            "major": d.major,
            "version": d.version,
            "kind": d.kind,
            "licence": d.licence,
            "doc": d.doc,
            "natural_key": list(d.natural_key),
            "columns": [dict(c) for c in d.columns if c["name"] in self._table_columns(d)],
            "added_by_publisher": ["entity_id", "batch_id"],
        }

    def _dataset(self, dataset: str) -> _Dataset:
        reg = self._registered()
        name, sep, major = dataset.rpartition("_v")
        if sep and major.isdigit() and (name, int(major)) in reg:
            return reg[(name, int(major))]
        majors = [k[1] for k in reg if k[0] == dataset]
        if not majors:
            raise UnknownDataset(f"unknown dataset {dataset!r}")
        return reg[(dataset, max(majors))]

    def asof_sql(
        self, dataset: str, as_of: datetime | None = None, *, snapshot: int | None = None
    ) -> str:
        """SQL for the dataset as it was knowable at as_of: per natural key, the latest row
        with known_at <= as_of (ties: the batch published last). snapshot pins a DuckLake
        version (audit: what the store held)."""
        d = self._dataset(dataset)
        as_of = as_of or datetime.now(UTC)
        if as_of.tzinfo is None:
            raise ValueError("as_of must be timezone-aware")
        if snapshot is not None and (isinstance(snapshot, bool) or not isinstance(snapshot, int)):
            raise ValueError("snapshot must be an int DuckLake version")
        at = f" AT (VERSION => {snapshot})" if snapshot is not None else ""
        key = ", ".join(f"f.{k}" for k in d.natural_key)
        ts = as_of.astimezone(UTC).isoformat()
        return (
            f"SELECT * EXCLUDE (_rn) FROM (SELECT f.*, row_number() OVER (PARTITION BY {key} "
            f"ORDER BY f.known_at DESC, b.published_at DESC) AS _rn FROM {d.fq} AS f{at} "
            f"JOIN lake.ref.batches AS b{at} USING (batch_id) "
            f"WHERE f.known_at <= TIMESTAMPTZ '{ts}') WHERE _rn = 1"
        )

    def _one_statement(self, con, sql: str) -> None:
        if len(con.extract_statements(sql)) != 1:
            raise ValueError("exactly one SQL statement expected")

    def read_asof(
        self,
        dataset: str,
        as_of: datetime | None = None,
        *,
        filters: dict | None = None,
        columns: list[str] | None = None,
        limit: int | None = None,
        where: str | None = None,
        snapshot: int | None = None,
    ) -> pa.Table:
        """Rows knowable at as_of. filters = {column: value or list of values}, bound as query
        parameters (safe for untrusted input); columns = projection; limit = row cap.
        where = free SQL for trusted callers only (one expression, no ? placeholders, never
        untrusted input). LIMIT without an ORDER BY returns an arbitrary subset."""
        as_of = as_of or datetime.now(UTC)
        d = self._dataset(dataset)
        sql = self.asof_sql(dataset, as_of, snapshot=snapshot)
        known = set(d.column_names) & self._table_columns(d)
        for col in list(filters or {}) + list(columns or []):
            if col not in known:
                raise ValueError(f"unknown column {col!r} for {d.table}")
        if limit is not None and (
            isinstance(limit, bool) or not isinstance(limit, int) or not 0 <= limit < 2**63
        ):
            raise ValueError("limit must be a non-negative int")
        clauses, params = [], []
        for col, value in (filters or {}).items():
            values = list(value) if isinstance(value, (list, tuple, set)) else [value]
            if not values:
                raise ValueError(f"empty filter list for {col!r}")
            if any(v is None for v in values):
                raise ValueError(f"None in filter for {col!r}: use where for IS NULL")
            clauses.append(f"{col} IN ({', '.join('?' * len(values))})")
            params += values
        con = self.con.cursor()
        try:
            if where:
                self._one_statement(con, f"SELECT 1 WHERE {where}")
                clauses.append(f"({where})")
            sql = f"SELECT {', '.join(columns) if columns else '*'} FROM ({sql})"
            if clauses:
                sql += " WHERE " + " AND ".join(clauses)
            if limit is not None:
                sql += f" LIMIT {limit}"
            t = con.sql(sql, params=params or None).to_arrow_table()
        finally:
            con.close()
        return _stamp(t, as_of, snapshot)

    def assemble(
        self, recipe: Recipe, as_of: datetime | None = None, *, save: bool = False
    ) -> pa.Table:
        if save:
            raise NotImplementedError("save=True ships with the metrics step (plan step 4)")
        as_of = as_of or datetime.now(UTC)
        con = self.con.cursor()  # its own temp views: nothing leaks between calls
        try:
            self._one_statement(con, recipe.sql)
            for view, dataset in recipe.inputs.items():
                if not VIEW_RE.fullmatch(view) or view in RESERVED_VIEW_NAMES:
                    raise ValueError(f"bad recipe input name {view!r}")
                con.execute(f"CREATE TEMP VIEW {view} AS {self.asof_sql(dataset, as_of)}")
            t = con.sql(recipe.sql).to_arrow_table()
        finally:
            con.close()
        return _stamp(t, as_of, None, recipe)


def _semver(v: str) -> tuple[int, ...]:
    return tuple(int(x) for x in v.split("."))


def _stamp(t: pa.Table, as_of: datetime, snapshot: int | None, recipe: Recipe | None = None):
    """Record what the result was computed from, so an answer can be reproduced."""
    meta = {"myopendata.as_of": as_of.astimezone(UTC).isoformat()}
    if snapshot is not None:
        meta["myopendata.snapshot"] = str(snapshot)
    if recipe is not None:
        meta["myopendata.recipe"] = f"{recipe.name}@{recipe.version}"
    return t.replace_schema_metadata({**(t.schema.metadata or {}), **meta})


def _cell(v):
    if isinstance(v, datetime):
        return v.astimezone(UTC).replace(tzinfo=None) if v.tzinfo else v
    if isinstance(v, Decimal):
        return v  # openpyxl writes Decimal as a number
    return v


def export(table: pa.Table, path: str | Path) -> Path:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    if p.suffix == ".csv":
        pcsv.write_csv(table, p)
    elif p.suffix == ".parquet":
        pq.write_table(table, p)
    elif p.suffix == ".xlsx":
        if table.num_rows > XLSX_MAX_ROWS:
            raise ValueError(f"{table.num_rows} rows exceed Excel's limit; export csv or parquet")
        from openpyxl import Workbook

        wb = Workbook(write_only=True)
        ws = wb.create_sheet()
        ws.append(table.column_names)
        for batch in table.to_batches():
            for row in batch.to_pylist():
                ws.append([_cell(v) for v in row.values()])
        wb.save(p)
    else:
        raise ValueError(f"unsupported export format {p.suffix}")
    return p
