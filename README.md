# my-open-data

Bitemporal open-data store for MyOpenFund: dataset **contracts**, a light **writer** for producers,
a **publisher** (the only writer to the lake) and a **reader** for analyses, the MCP and strategies.
Facts are DuckLake tables (Parquet + a Postgres catalogue), read through DuckDB. Every fact carries
`valid_date` (what it is about) and `known_at` (when it became knowable): reads are look-ahead-free.

> This library is a tool. It manages nobody's money and gives no investment advice.

## Install

```
pip install "my-open-data[writer]"     # producers: pyarrow only
pip install "my-open-data[publisher]"  # store operator: DuckDB + DuckLake
pip install "my-open-data[reader]"     # analyses, MCP, strategies
```

## Configuration

| Variable | Meaning |
|---|---|
| `MYOPENDATA_DATA_ROOT` | Holds `landing/` (batches), `lake/` (Parquet), `reports/` |
| `MYOPENDATA_CATALOG` | DuckLake catalogue: `postgres:dbname=... host=... user=...`, or a file path (tests, single user) |
| `MYOPENDATA_DATA_INLINING_ROW_LIMIT` | Default `0`: every batch becomes a Parquet file |
| `MYOPENDATA_KNOWN_AT_FUTURE_TOLERANCE_S` | Clock-skew tolerance, default `300` |

## Reader interface (stable for v0.x)

```python
from datetime import datetime, timezone
from myopendata.reader import Recipe, Store, export

store = Store.open()                                   # config from env
store.datasets()                                       # published tables
t = store.read_asof("fundamentals", datetime(2025, 12, 1, tzinfo=timezone.utc),
                    where="concept = 'LongTermDebt'")  # pyarrow.Table, latest known per key
t0 = store.read_asof("fundamentals", snapshot=42)      # what the store held at DuckLake version 42
r = Recipe("debt_by_source", "1.0.0", {"f": "fundamentals"},
           "SELECT source_code, sum(value) AS debt FROM f GROUP BY 1")
out = store.assemble(r, as_of=datetime.now(timezone.utc))  # save=True arrives with metrics
export(out, "debt.xlsx")                               # .csv | .xlsx | .parquet
```

## Producers

```python
from myopendata.writer import write_batch
ref = write_batch(contract, table, landing_root=data_root / "landing",
                  run_id=run_id, producer="company-corpus", source_inputs=[...])
```

The writer validates the table against its contract and drops an atomic, idempotent batch in
`landing/`. It never touches Postgres. A producer declares its own contracts through the
`myopendata.contracts` entry-point group; standard contracts (`fundamentals`, `entity_observations`)
ship with this library.

## Operator

```
myopendata contracts list
myopendata contracts approve fundamentals 1 --by <name>   # reviewed step, once per major
myopendata publish                                          # exit 0 ok · 1 fatal · 3 degraded
```

`publish` audits every landed batch again, quarantines failures in `landing/_quarantine/` with a
`reason.json`, publishes the rest atomically (one DuckLake snapshot per batch, commit message =
batch id) and writes a run report under `reports/publisher/`.

## Licence

MIT (code). Each dataset carries its own data licence in its contract.
