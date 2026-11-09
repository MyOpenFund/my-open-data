# Architecture

## Flow

```
producer ──writer──▶ landing/<table>/<batch_id>/{data.parquet, batch.json}
                         │
                         ▼  myopendata publish   (the only writer to the lake; one run at a time)
             audit ─▶ landing/_quarantine/  (reason.json)    replays ─▶ landing/_duplicates/
               │
               ▼  one DuckLake transaction per batch, then the batch moves to landing/_published/
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
  refuses a second concurrent publisher. A batch that hit an infrastructure error stays in place
  and is retried by the next run.
* **Row-level idempotence (fact contracts).** Producers may re-emit their whole history on every
  run. A replayed batch (same `batch_id`) is skipped through the `ref.batches` ledger. A new batch
  is anti-joined on `(natural_key, known_at)` against the published table, scoped to the batch's
  values of the key columns among the entity column pair and the partition columns (never on the
  resolved `entity_id`): identical rows are skipped and counted (`rows_skipped`), new rows are
  published, and a row with the same key and `known_at` but another value quarantines the batch
  (`conflicting_republish:<n>`).
  * Values are compared exactly (null-safe): a producer must re-emit bit-identical values,
    floats included; a recomputed float that differs in its last bit is a conflict.
  * Columns of the major's *first approved definition* (lowest approved version in
    `ref.contracts`) are compared strictly. Columns added by a later MINOR are compared only where
    the published row holds a value: old rows holding NULL in them are still skipped, but changing
    a published value is a conflict. Enriching a NULL in a minor-added column at the same
    `known_at` is skipped and the new value is NOT written; it needs a later `known_at` (a
    restatement) to be recorded. If the first definition is unreadable, the current contract's
    columns are used.
  * `ref.batches.rows` is the number of rows the batch inserted; the batch's total is
    `rows + rows_skipped`. Every audited batch gets its `ref.batches` row, even when it adds no
    row (`rows = 0`): the ledger is the trail of what each producer run submitted and changed.
    Such a batch writes no data file.
* **Recovering a quarantined batch:** fix the cause (approve the contract, fix the producer and
  land a new batch, or for an identity conflict correct the source data), then either land a new
  batch or move the directory back from `_quarantine/<table>/` to `landing/<table>/` and delete
  its `reason.json`. The publisher audits it again from scratch.
* A writer killed mid-write leaves a `landing/<table>/.tmp-*` directory; the publisher ignores
  these, and they can be deleted when no writer is running.
* **The reader** attaches the lake read-only, pins the session to UTC, forbids file access outside
  the lake, attaching databases and changing settings, and accepts only single-statement filters
  and recipes. Filters and recipe SQL are still trusted caller code: a server exposing the reader
  to untrusted input must pass structured filters.

## Two clocks

`valid_date` / `known_at` are the business truth: `read_asof(t)` returns, per natural key, the
latest row with `known_at <= t`. The same key cannot hold two values at one `known_at`: the
publisher quarantines such a batch (`conflicting_republish`), so a correction always carries a
later `known_at`. (The reader still orders ties by publication time, for rows published before
this rule.) DuckLake snapshots are the audit trail: `read_asof(..., snapshot=n)` shows what the store held at version `n`. Every
result carries `myopendata.as_of` (and snapshot / recipe) in its schema metadata.

A recipe input named `name:vintages` (or `name_vN:vintages`) gets every row knowable at the date
instead of the latest per key, for a recipe that compares a value with the one it replaced
(restatements).

## Contracts

* Common rules (in `contracts/rules.py`): bitemporal columns, provenance (`source_code`,
  `source_doc_id`), natural key containing `valid_date` and `source_code` and never `known_at`,
  mandatory licence and owner, identifier schemes, reserved columns `entity_id` / `batch_id`.
* Standard contracts ship with the library (`fundamentals`, `entity_observations`,
  `filing_statements`, `filings`); producers declare
  their own through the `myopendata.contracts` entry-point group (the spec's working name was
  `myopenfund.datasets`).
* `fundamentals` 3.0.0 (the current major) keeps facts as filed, keyed by tag: the natural key uses
  `original_tag` (`ns:Tag`, required), and `filing_ref` (nullable) carries the filing's
  identifier. `frequency` is `quarterly`, `semiannual`, `annual` (durations) or `instant`
  (balance-sheet values, `period_start` null); the row check `instant_period` enforces that
  pairing. The earlier majors were never published to production and no longer ship.
* `filing_statements` 1.0.0 records which tag a filing placed on which primary statement
  (`BS`, `IS`, `CF`), one row per tag per statement per filing, keyed by entity, `filing_ref`,
  `statement`, `tag`, `valid_date` and `source_code`; a tag of that filing absent here was filed in the notes
  only. `known_at` is the filing's own.
* `filings` 1.0.0 records what each filing is: one row per filing with its `form` as filed
  (`10-K`, `10-K/A`, `10-KT`…) and `report_date`, the period it reports on as the source states
  it; `valid_date` is the filing date. Keyed by entity, `filing_ref`, `valid_date` and
  `source_code`. Facts join it on the entity and `filing_ref` (fiscal year-ends, amendments);
  `filing_ref` alone would duplicate rows, as co-registrants share an accession. A filing
  re-listed under another filing date is a second row (the key includes `valid_date`) and readers
  see both; another `form` or `report_date` for the same key and `known_at` quarantines the batch
  (`conflicting_republish`).
* A contract version is publishable once approved (`myopendata contracts approve NAME MAJOR`).
  A strictly additive MINOR (new nullable columns appended, everything else equal, higher version)
  is auto-approved; any other change within a major is refused (`contract_fingerprint_mismatch`).
  The fingerprint covers columns, types, nullability, vocabularies, natural key, checks,
  partitioning, licence and the identity flag. Documentation is stored with the first approval
  of a definition; a doc-only patch does not refresh it in v0.

## Entities (decisions D3, D4)

* Merges happen only on resolving schemes (`cik, lei, isin, figi, cb_code, national_id`,
  configurable). Tickers never merge entities and never key a link; they stay visible in
  `entity_observations`. A subject without a resolving identifier is quarantined.
* Resolution groups all identifiers of a batch first (union-find), so it does not depend on row
  order. It never merges automatically: a group touching two existing entities, or an entity that
  would hold two values of a single-valued scheme (`cik, lei, cb_code`, configurable), is an
  `entity_conflict`.
* `ref.entity_attributes` (name, country) is SCD2, rebuilt from every published observation of
  the touched entities, so a backfill lands in the right interval. It is **current knowledge**,
  not as-of: joining it in a historical read can show a name learnt later. For look-ahead-free
  identity, read `entity_observations` with `read_asof` (an as-of entity reader comes later).
* The policy is configuration: `MYOPENDATA_RESOLVING_SCHEMES` and
  `MYOPENDATA_SINGLE_VALUED_SCHEMES` (comma-separated). Extending the identifier vocabulary
  itself changes the standard contracts' fingerprint, so it ships as a new major of them.
* A contract marks itself as an identity dataset with `resolves_entities=True`; it is published
  before other data in each run.

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

## Reader for servers (MCP)

Use `read_asof(dataset, as_of, filters={col: value | [values]}, columns=[...], limit=n)`: column
names are checked against the dataset, values are bound as query parameters. `describe(dataset)`
returns the columns with types, vocabularies and documentation, the natural key and the licence.
Never forward free text into `where` or a recipe. `vintages=True` returns every vintage knowable at
the date instead of the latest per key; `freshness()` gives the last publication time and batch count.
Filters on natural-key columns are applied before the as-of window, so a filter on `entity_value` reads
only that issuer's files (one batch per issuer = one file); do not merge data files without measuring.

A reader may see the lake's files under another path than the publisher (a publisher on a
server, a reader on a mounted share): set its `MYOPENDATA_DATA_ROOT` to where it sees them.
Read-only sessions attach with DuckLake's `OVERRIDE_DATA_PATH`; the publisher never does, so a
misconfigured publisher fails instead of writing files elsewhere. To run a store on a server (Postgres
catalogue, publisher, backups), see `deploy/nas/README.md`.

## CI

`push` runs on every branch so a branch is tested before its PR exists; `pull_request` runs on
PRs to `main` (a PR from a branch of this repo therefore runs twice). Each run lints, tests on
Python 3.11–3.13 with a Postgres catalogue, builds the package and checks its metadata.

## Not in v0

`assemble(save=True)` and the metrics recipes (consolidated fundamentals, leverage, TTM) arrive
with the metrics step. ntfy notification of degraded or fatal runs belongs to the job runner, as
for the producers. Production topology is decided before the first production publish.
