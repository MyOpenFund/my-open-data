"""Store side: audit landed batches and publish them atomically into DuckLake (spec §6).

The publisher is the only writer to the lake. Never trust a producer: every batch is re-validated
against the approved contract. A bad batch goes to landing/_quarantine/, never into the lake.
"""

from __future__ import annotations

import json
import shutil
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from myopendata.config import StoreConfig
from myopendata.contracts.base import Contract
from myopendata.contracts.registry import discover_contracts
from myopendata.contracts.rules import BatchInvalid, conform
from myopendata.lake import connect
from myopendata.publisher.entities import (
    DEFAULT_RESOLVING_SCHEMES,
    EntityConflict,
    load_links,
    rebuild_attributes,
    resolve_facts,
    resolve_observations,
)
from myopendata.writer.batch import DATA, MANIFEST

EXIT_OK, EXIT_FATAL, EXIT_DEGRADED = 0, 1, 3  # silent-failure doctrine
FIRST = ("entity_observations",)  # published before anything that needs entities


@dataclass
class PublishReport:
    run_id: str
    started_at: str
    published: list[dict] = field(default_factory=list)
    skipped_duplicates: list[str] = field(default_factory=list)
    quarantined: list[dict] = field(default_factory=list)
    fatal: str | None = None
    finished_at: str | None = None

    @property
    def exit_code(self) -> int:
        if self.fatal:
            return EXIT_FATAL
        return EXIT_DEGRADED if self.quarantined else EXIT_OK

    def to_json(self) -> str:
        d = dict(self.__dict__)
        d["exit_code"] = self.exit_code
        return json.dumps(d, indent=2, default=str)


def _definition(c: Contract) -> dict:
    return {
        "columns": [
            [col.name, str(col.type), col.nullable, list(col.allowed or ())] for col in c.columns
        ],
        "natural_key": list(c.natural_key),
        "kind": c.kind,
        "entity_column_pair": list(c.entity_column_pair or ()),
        "checks": list(c.checks),
    }


def approve_contract(cfg: StoreConfig, contract: Contract, approved_by: str) -> None:
    """Marc's reviewed step (spec §6.1): register a contract version as publishable."""
    con = connect(cfg, read_only=False)
    con.execute(
        "INSERT INTO lake.ref.contracts VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            contract.name,
            contract.major,
            contract.version,
            contract.fingerprint(),
            json.dumps(_definition(contract)),
            approved_by,
            datetime.now(UTC),
        ],
    )
    con.close()


def _additive(old: dict, new: dict) -> bool:
    """MINOR rule: same everything, new columns appended and nullable."""
    if any(old[k] != new[k] for k in ("natural_key", "kind", "entity_column_pair", "checks")):
        return False
    n = len(old["columns"])
    return new["columns"][:n] == old["columns"] and all(c[2] for c in new["columns"][n:])


def _approval_state(con, c: Contract) -> str:
    rows = con.execute(
        "SELECT fingerprint, definition_json FROM lake.ref.contracts "
        "WHERE name = ? AND major = ? ORDER BY approved_at",
        [c.name, c.major],
    ).fetchall()
    if not rows:
        return "not_approved"
    if any(fp == c.fingerprint() for fp, _ in rows):
        return "approved"
    if _additive(json.loads(rows[-1][1]), _definition(c)):
        return "auto_minor"
    return "fingerprint_mismatch"


def _ensure_table(con, c: Contract, sample: pa.Table) -> None:
    fq = f"lake.{'facts' if c.kind == 'fact' else 'derived'}.{c.table_name}"
    exists = con.execute(
        "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake' AND schema_name = ? "
        "AND table_name = ?",
        ["facts" if c.kind == "fact" else "derived", c.table_name],
    ).fetchall()[0][0]
    con.register("_sample", sample)
    if not exists:
        con.execute(f"CREATE TABLE {fq} AS SELECT * FROM _sample LIMIT 0")
        if c.partition_by:
            con.execute(f"ALTER TABLE {fq} SET PARTITIONED BY ({', '.join(c.partition_by)})")
    else:
        have = {r[0] for r in con.execute(f"DESCRIBE {fq}").fetchall()}
        for f in sample.schema:
            if f.name not in have:  # additive MINOR change
                typ = con.execute(f"SELECT typeof({f.name}) FROM _sample LIMIT 1").fetchall()[0][0]
                con.execute(f"ALTER TABLE {fq} ADD COLUMN {f.name} {typ}")
    con.unregister("_sample")


def _move(src: Path, root: Path, bucket: str, table: str, bid: str) -> Path:
    dest = root / bucket / table / bid
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        shutil.rmtree(dest)
    shutil.move(str(src), dest)
    return dest


def _pending(landing: Path) -> list[Path]:
    batches = [
        p
        for p in landing.glob("*/*")
        if (p / MANIFEST).exists()
        and not p.parent.name.startswith("_")
        and not p.name.startswith(".tmp-")
    ]

    def order(p: Path):
        m = json.loads((p / MANIFEST).read_text())
        return (0 if m["contract"] in FIRST else 1, m["written_at"], m["batch_id"])

    return sorted(batches, key=order)


def publish_pending(
    cfg: StoreConfig,
    *,
    run_id: str | None = None,
    resolving_schemes: tuple[str, ...] = DEFAULT_RESOLVING_SCHEMES,
    contracts: dict[tuple[str, int], Contract] | None = None,
    now: datetime | None = None,
) -> PublishReport:
    rep = PublishReport(run_id=run_id or uuid.uuid4().hex, started_at=datetime.now(UTC).isoformat())
    try:
        contracts = contracts if contracts is not None else discover_contracts()
        con = connect(cfg, read_only=False)
    except Exception as exc:  # noqa: BLE001 — any failure here is fatal and must be reported
        rep.fatal = f"{type(exc).__name__}: {exc}"
        return _finish(cfg, rep)
    try:
        for path in _pending(cfg.landing):
            _publish_one(cfg, con, contracts, path, rep, resolving_schemes, now)
    except Exception as exc:  # noqa: BLE001 — doctrine: a crash becomes a fatal run report
        rep.fatal = f"{type(exc).__name__}: {exc}"
    finally:
        con.close()
    return _finish(cfg, rep)


def _quarantine(cfg, path, m, reasons, rep):
    dest = _move(path, cfg.landing, "_quarantine", path.parent.name, m["batch_id"])
    (dest / "reason.json").write_text(json.dumps({"reasons": reasons, "run_id": rep.run_id}))
    rep.quarantined.append(
        {"batch_id": m["batch_id"], "contract": m["contract"], "reasons": reasons}
    )


def _publish_one(
    cfg,
    con: duckdb.DuckDBPyConnection,
    contracts,
    path: Path,
    rep: PublishReport,
    resolving_schemes,
    now,
) -> None:
    m = json.loads((path / MANIFEST).read_text())
    bid = m["batch_id"]
    major = int(m["contract_version"].split(".")[0])
    c = contracts.get((m["contract"], major))
    if c is None:
        return _quarantine(cfg, path, m, [f"unknown_contract:{m['contract']}_v{major}"], rep)
    if c.fingerprint() != m["contract_fingerprint"]:
        return _quarantine(cfg, path, m, ["batch_written_with_other_contract_definition"], rep)
    state = _approval_state(con, c)
    if state in ("not_approved", "fingerprint_mismatch"):
        return _quarantine(cfg, path, m, [f"contract_{state}"], rep)
    if con.execute("SELECT count(*) FROM lake.ref.batches WHERE batch_id = ?", [bid]).fetchall()[0][
        0
    ]:
        rep.skipped_duplicates.append(bid)
        _move(path, cfg.landing, "_published", path.parent.name, bid)
        return
    try:
        t = conform(
            c,
            pq.read_table(path / DATA),
            now=now,
            future_tolerance_s=cfg.known_at_future_tolerance_s,
        )
    except BatchInvalid as exc:
        return _quarantine(cfg, path, m, exc.reasons, rep)
    links = load_links(con)
    new_links: list[tuple] = []
    if c.name == "entity_observations":
        try:
            eids, new_links = resolve_observations(t, links, resolving_schemes)
        except EntityConflict as exc:
            return _quarantine(cfg, path, m, [f"entity_conflict:{exc}"], rep)
    elif c.entity_column_pair:
        eids, missing = resolve_facts(t, c.entity_column_pair, links)
        if missing:
            return _quarantine(cfg, path, m, [f"unknown_entity:{missing}"], rep)
    else:
        eids = [None] * t.num_rows
    out = t.add_column(0, "entity_id", pa.array(eids, pa.string()))
    out = out.append_column("batch_id", pa.array([bid] * t.num_rows, pa.string()))
    fq = f"lake.{'facts' if c.kind == 'fact' else 'derived'}.{c.table_name}"
    con.execute("BEGIN")
    try:
        _ensure_table(con, c, out)
        con.register("_batch", out)
        cols = ", ".join(out.column_names)
        con.execute(f"INSERT INTO {fq} ({cols}) SELECT {cols} FROM _batch")
        con.unregister("_batch")
        if new_links:
            con.executemany(
                "INSERT INTO lake.ref.entity_identifiers VALUES (?, ?, ?, ?, ?, ?)",
                [(*lk, bid) for lk in new_links],
            )
        if c.name == "entity_observations":
            rebuild_attributes(con, set(eids))
        if state == "auto_minor":
            con.execute(
                "INSERT INTO lake.ref.contracts VALUES (?, ?, ?, ?, ?, ?, ?)",
                [
                    c.name,
                    c.major,
                    c.version,
                    c.fingerprint(),
                    json.dumps(_definition(c)),
                    "auto-minor",
                    datetime.now(UTC),
                ],
            )
        con.execute(
            "INSERT INTO lake.ref.batches VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                bid,
                c.name,
                c.major,
                c.version,
                m["producer"],
                m["run_id"],
                t.num_rows,
                m["content_sha256"],
                datetime.now(UTC),
            ],
        )
        con.execute(
            "CALL lake.set_commit_message('my-open-data publisher', ?, extra_info => ?)",
            [f"publish {c.table_name} {bid}", json.dumps({"batch_id": bid, "run_id": rep.run_id})],
        )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    _move(path, cfg.landing, "_published", path.parent.name, bid)
    rep.published.append({"batch_id": bid, "contract": c.table_name, "rows": t.num_rows})


def _finish(cfg: StoreConfig, rep: PublishReport) -> PublishReport:
    rep.finished_at = datetime.now(UTC).isoformat()
    out = cfg.data_root / "reports" / "publisher"
    out.mkdir(parents=True, exist_ok=True)
    (out / f"{rep.started_at[:19].replace(':', '')}-{rep.run_id}.json").write_text(rep.to_json())
    return rep
