"""Read side: the interface the MCP, analyses and strategies code against (spec §2, §6).

store = Store.open(StoreConfig.from_env())
store.datasets()                                   # what is published
store.read_asof("fundamentals", as_of=t, where="concept = 'Debt'")   # look-ahead-free
store.assemble(recipe, as_of=t)                    # derived dataset (save=False in v0)
export(table, "out.xlsx")                          # csv | xlsx | parquet by suffix
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq

from myopendata.config import StoreConfig
from myopendata.contracts.registry import discover_contracts
from myopendata.lake import connect


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


class Store:
    def __init__(self, cfg: StoreConfig):
        self.cfg = cfg
        self.con = connect(cfg, read_only=True)
        self.contracts = discover_contracts()

    @classmethod
    def open(cls, cfg: StoreConfig | None = None) -> Store:
        return cls(cfg or StoreConfig.from_env())

    def close(self) -> None:
        self.con.close()

    def datasets(self) -> list[dict]:
        rows = self.con.execute(
            "SELECT schema_name, table_name, estimated_size FROM duckdb_tables() "
            "WHERE database_name = 'lake' AND schema_name IN ('facts', 'derived') "
            "ORDER BY 1, 2"
        ).fetchall()
        return [{"schema": s, "table": t, "rows_estimate": n} for s, t, n in rows]

    def _contract_for(self, dataset: str):
        if "_v" in dataset and dataset.rsplit("_v", 1)[1].isdigit():
            name, major = dataset.rsplit("_v", 1)
            return self.contracts[(name, int(major))]
        majors = [k[1] for k in self.contracts if k[0] == dataset]
        if not majors:
            raise KeyError(f"unknown dataset {dataset}")
        return self.contracts[(dataset, max(majors))]

    def asof_sql(
        self, dataset: str, as_of: datetime | None = None, *, snapshot: int | None = None
    ) -> str:
        """SQL for the dataset as it was knowable at as_of: per natural key, the latest row
        with known_at <= as_of. snapshot pins a DuckLake version (audit: what the store held)."""
        c = self._contract_for(dataset)
        as_of = as_of or datetime.now(UTC)
        if as_of.tzinfo is None:
            raise ValueError("as_of must be timezone-aware")
        fq = f"lake.{'facts' if c.kind == 'fact' else 'derived'}.{c.table_name}"
        at = f" AT (VERSION => {int(snapshot)})" if snapshot is not None else ""
        key = ", ".join(c.natural_key)
        ts = as_of.astimezone(UTC).isoformat()
        return (
            f"SELECT * EXCLUDE (_rn) FROM (SELECT *, row_number() OVER (PARTITION BY {key} "
            f"ORDER BY known_at DESC, batch_id DESC) AS _rn FROM {fq}{at} "
            f"WHERE known_at <= TIMESTAMPTZ '{ts}') WHERE _rn = 1"
        )

    def read_asof(
        self,
        dataset: str,
        as_of: datetime | None = None,
        *,
        where: str | None = None,
        snapshot: int | None = None,
    ) -> pa.Table:
        sql = self.asof_sql(dataset, as_of, snapshot=snapshot)
        if where:
            sql = f"SELECT * FROM ({sql}) WHERE {where}"
        return self.con.sql(sql).to_arrow_table()

    def assemble(
        self, recipe: Recipe, as_of: datetime | None = None, *, save: bool = False
    ) -> pa.Table:
        if save:
            raise NotImplementedError("save=True ships with the metrics step (plan step 4)")
        for view, dataset in recipe.inputs.items():
            self.con.execute(
                f"CREATE OR REPLACE TEMP VIEW {view} AS {self.asof_sql(dataset, as_of)}"
            )
        return self.con.sql(recipe.sql).to_arrow_table()


def export(table: pa.Table, path: str | Path) -> Path:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    if p.suffix == ".csv":
        pcsv.write_csv(table, p)
    elif p.suffix == ".parquet":
        pq.write_table(table, p)
    elif p.suffix == ".xlsx":
        from openpyxl import Workbook

        wb = Workbook()
        ws = wb.active
        ws.append(table.column_names)
        for row in table.to_pylist():
            ws.append(
                [v.replace(tzinfo=None) if isinstance(v, datetime) else v for v in row.values()]
            )
        wb.save(p)
    else:
        raise ValueError(f"unsupported export format {p.suffix}")
    return p
