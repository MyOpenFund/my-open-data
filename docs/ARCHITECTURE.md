# Architecture

## Flow

```
producer ──writer──▶ landing/<table>/<batch_id>/{data.parquet, batch.json}
                         │
                         ▼  myopendata publish   (the only writer to the lake; one run at a time)
             audit ─▶ quarantine/  (reason.json)          duplicates ─▶ _duplicates/
               │
               ▼  one DuckLake transaction per batch
   lake.facts.<name>_v<major>   lake.ref.{contracts, batches, entity_identifiers, entity_attributes}
               │                commit message = "publish <table> <batch_id>"
               ▼
         reader (read-only, sandboxed): read_asof · assemble · export
```

* **Producers** use `myopendata.writer` (pyarrow only). They never touch Postgres. A batch
  directory appears atomically (written under `.tmp-*`, fsynced, renamed); this needs the landing
  zone on one local or NFS filesystem.
* **The publisher** re-reads every batch and trusts nothing in its manifest: it checks the file
  checksum, re-validates the rows against the approved contract, recomputes the batch id and the
  row hash, then publishes. Failures are quarantined with a `reason.json` and never stop the other
  batches. Existing directories are never overwritten. A file lock (`landing/.publisher.lock`)
  refuses a second concurrent publisher.
* **The reader** attaches the lake read-only, pins the session to UTC, forbids file access outside
  the lake, attaching databases and changing settings, and accepts only single-statement filters
  and recipes. Filters and recipe SQL are still trusted caller code: a server exposing the reader
  to untrusted input must pass structured filters.

## Two clocks

`valid_date` / `known_at` are the business truth: `read_asof(t)` returns, per natural key, the
latest row with `known_at <= t`; on a tie, the batch published last wins. DuckLake snapshots are
the audit trail: `read_asof(..., snapshot=n)` shows what the store held at version `n`. Every
result carries `myopendata.as_of` (and snapshot / recipe) in its schema metadata.

## Contracts

* Common rules (in `contracts/rules.py`): bitemporal columns, provenance (`source_code`,
  `source_doc_id`), natural key containing `valid_date` and `source_code` and never `known_at`,
  mandatory licence and owner, identifier schemes, reserved columns `entity_id` / `batch_id`.
* Standard contracts ship with the library (`fundamentals`, `entity_observations`); producers
  declare their own through the `myopendata.contracts` entry-point group (the spec's working name
  was `myopenfund.datasets`).
* A contract version is publishable once approved (`myopendata contracts approve NAME MAJOR`).
  A strictly additive MINOR (new nullable columns appended, everything else equal, higher version)
  is auto-approved; any other change within a major is refused (`contract_fingerprint_mismatch`).
  The fingerprint covers columns, types, nullability, vocabularies, natural key, checks,
  partitioning and licence.

## Entities (decisions D3, D4)

* Merges happen only on resolving schemes (`cik, lei, isin, figi, cb_code, national_id`,
  configurable). Tickers never merge entities and never key a link; they stay visible in
  `entity_observations`. A subject without a resolving identifier is quarantined.
* Resolution groups all identifiers of a batch first (union-find), so it does not depend on row
  order. It never merges automatically: a group touching two existing entities, or an entity that
  would hold two values of a single-valued scheme (`cik, lei, cb_code`, configurable), is an
  `entity_conflict`.
* `ref.entity_attributes` (name, country) is SCD2, rebuilt from every published observation of
  the touched entities, so a backfill lands in the right interval.

## Exit codes (silent-failure doctrine)

`0` everything published or nothing to do · `3` degraded: some batches quarantined · `1` fatal:
the run could not start, crashed, or a batch hit an infrastructure error. Every run writes
`reports/publisher/<started_at>-<run_id>.json`.

## Gotchas

* DuckDB 2.0: a result read with `fetchone()` blocks the connection; use `fetchall()`.
  `execute(...).arrow()` is gone; use `con.sql(q).to_arrow_table()`.
* DuckDB needs `pytz` to return TIMESTAMPTZ values.
* DuckLake inlines small inserts into the catalogue by default; the store sets
  `DATA_INLINING_ROW_LIMIT 0` so every batch is a Parquet file under `DATA_ROOT`.
* A file catalogue admits one process at a time: no reader while the publisher runs. Use Postgres
  for anything shared.
* pyarrow `and_` / `or_` propagate nulls; row checks use the Kleene variants.

## Not in v0

`assemble(save=True)` and the metrics recipes (consolidated fundamentals, leverage, TTM) arrive
with the metrics step. Production topology is decided before the first production publish.
