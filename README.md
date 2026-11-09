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
| `MYOPENDATA_RESOLVING_SCHEMES` | Identifier schemes that may merge entities, default `cik,lei,isin,figi,cb_code,national_id` |
| `MYOPENDATA_SINGLE_VALUED_SCHEMES` | Schemes an entity holds one value of, default `cik,lei,cb_code` |

## Reader interface (stable for v0.x)

```python
from datetime import datetime, timezone
from myopendata.reader import Recipe, Store, export

store = Store.open()                                   # config from env (or `with Store.open() as store:`)
store.datasets()                                       # name, major, version, kind, licence, table
t = store.read_asof("fundamentals", datetime(2025, 12, 1, tzinfo=timezone.utc),
                    where="original_tag = 'us-gaap:Revenues'")  # pyarrow.Table, latest known per key
t1 = store.read_asof("fundamentals", filters={"source_code": ["sec"]},
                     columns=["entity_id", "original_tag", "value"], limit=100)  # bound, safe for servers
store.read_asof("fundamentals", filters={"entity_value": ["0000320193"]}, vintages=True)  # every vintage
store.freshness()                                      # last published_at, batch count
store.describe("fundamentals")                         # columns, docs, natural key, licence
t0 = store.read_asof("fundamentals", snapshot=42)      # what the store held at DuckLake version 42
r = Recipe("debt_by_source", "1.0.0", {"f": "fundamentals"},
           "SELECT source_code, sum(value) AS debt FROM f GROUP BY 1")
out = store.assemble(r, as_of=datetime.now(timezone.utc))  # save=True arrives with metrics
# {"f": "fundamentals:vintages"} gives every row knowable at the date, not the latest per key
export(out, "debt.xlsx")                               # .csv | .xlsx | .parquet
```

**Performance.** Filters on natural-key columns (e.g. `entity_value`) are always pushed explicitly
before the as-of window, so DuckLake reads only the matching files: on a 20-issuer test store a filter
on `entity_value` read 1 fundamentals data file (5 kB) instead of 60 files (139 kB). Filters on other
columns read the whole table. Keep one batch per issuer, and do not merge data files
(`ducklake_merge_adjacent_files`) without measuring: merging issuers into one file defeats the pruning.

## Producers

```python
from myopendata.writer import write_batch
ref = write_batch(contract, table, landing_root=data_root / "landing",
                  run_id=run_id, producer="company-corpus", source_inputs=[...])
```

The writer validates the table against its contract and drops an atomic, idempotent batch in
`landing/`. It never touches Postgres. A producer declares its own contracts through the
`myopendata.contracts` entry-point group; standard contracts (`fundamentals`, `entity_observations`,
`filing_statements`, `filings`) ship with this library.

## Operator

```
myopendata contracts list
myopendata contracts approve fundamentals 3 --by <name>   # reviewed step; refuses conflicting definitions
myopendata contracts approve filing_statements 1 --by <name>   # likewise, for a store that publishes it
myopendata contracts approve filings 1 --by <name>             # likewise
myopendata publish                                          # exit 0 ok · 1 fatal · 3 degraded
```

`publish` audits every landed batch again (file checksum, contract, recomputed batch id),
quarantines failures in `landing/_quarantine/` with a `reason.json`, publishes the rest atomically
(one DuckLake snapshot per batch, commit message = batch id) and writes a run report under
`reports/publisher/`. Design, security model and gotchas: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

Producers may re-emit full history: rows already published with the same values (compared
exactly, so re-emit bit-identical floats) are skipped and counted (`skipped_rows` in the report,
`rows_skipped` in `ref.batches`, where `rows` counts only the inserted rows); the same key and
`known_at` with another value quarantines the batch (`conflicting_republish`). Columns of the
major's first approved definition are compared strictly; columns added by a later minor are compared
only where the published row holds a value (changing it is a conflict), and filling a NULL in one
needs a later `known_at` to be recorded.

## Licence

MIT (code). Each dataset carries its own data licence in its contract.
