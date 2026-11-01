"""Store side: audit landed batches and publish them atomically into DuckLake (spec §6).

The publisher is the only writer to the lake. Never trust a producer: every batch is re-read and
re-validated against the approved contract, its batch id and hashes are recomputed, and only then
published. A bad batch goes to landing/_quarantine/ with a reason.json, never into the lake, and
never stops the other batches of the run.
"""

from __future__ import annotations

import fcntl
import json
import re
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
from myopendata.contracts.rules import BatchInvalid, conform, validate_contract
from myopendata.lake import connect
from myopendata.publisher.entities import (
    EntityConflict,
    UnresolvableSubject,
    load_links,
    rebuild_attributes,
    resolve_facts,
    resolve_observations,
)
from myopendata.writer.batch import DATA, MANIFEST, batch_id_for, content_hash, file_sha256

EXIT_OK, EXIT_FATAL, EXIT_DEGRADED = 0, 1, 3  # silent-failure doctrine
BATCH_ID_RE = re.compile(r"[0-9a-f]{32}")
LOCK = ".publisher.lock"
MANIFEST_KEYS = (
    "batch_id",
    "contract",
    "contract_version",
    "contract_fingerprint",
    "producer",
    "run_id",
    "rows",
    "content_sha256",
    "files",
    "written_at",
)


@dataclass
class PublishReport:
    run_id: str
    started_at: str
    published: list[dict] = field(default_factory=list)
    skipped_duplicates: list[str] = field(default_factory=list)
    quarantined: list[dict] = field(default_factory=list)
    errors: list[dict] = field(default_factory=list)  # infrastructure failures on one batch
    fatal: str | None = None
    finished_at: str | None = None

    @property
    def exit_code(self) -> int:
        if self.fatal or self.errors:
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
        "partition_by": list(c.partition_by),
        "licence": c.licence,
        "resolves_entities": c.resolves_entities,
        # documentation travels with the approval so readers can describe a dataset without the
        # producer package installed; never compared (not part of the data definition)
        "docs": {"dataset": c.doc, "columns": {col.name: col.doc for col in c.columns}},
    }


def _semver(v: str) -> tuple[int, ...]:
    return tuple(int(x) for x in v.split("."))


class ApprovalRefused(ValueError):
    pass


def approve_contract(
    cfg: StoreConfig, contract: Contract, approved_by: str, *, force: bool = False
) -> str:
    """The reviewed step (spec §6.1): register a contract version as publishable.

    Returns "approved" (new row) or "already_approved" (no-op). A definition that is neither
    approved nor an additive MINOR of the approved one is refused unless force=True: a major's
    table already holds rows of the approved shape.
    """
    validate_contract(contract)
    con = connect(cfg, read_only=False)
    try:
        state = _approval_state(con, contract)
        if state == "approved":
            return "already_approved"
        if state == "fingerprint_mismatch" and not force:
            raise ApprovalRefused(
                f"{contract.table_name} {contract.version} is not an additive change of the "
                "approved definition; publish it as a new major (or force)"
            )
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
        return "approved"
    finally:
        con.close()


def _additive(old: dict, new: dict) -> bool:
    """MINOR rule (D6): everything else equal, new columns appended and nullable."""
    if any(old.get(k) != new.get(k) for k in new if k not in ("columns", "docs")):
        return False
    n = len(old["columns"])
    return new["columns"][:n] == old["columns"] and all(c[2] for c in new["columns"][n:])


def _approval_state(con, c: Contract) -> str:
    rows = con.execute(
        "SELECT fingerprint, definition_json, version FROM lake.ref.contracts "
        "WHERE name = ? AND major = ? ORDER BY approved_at",
        [c.name, c.major],
    ).fetchall()
    if not rows:
        return "not_approved"
    if any(fp == c.fingerprint() for fp, _, _ in rows):
        return "approved"
    latest = max(rows, key=lambda r: _semver(r[2]))
    if _semver(c.version) > _semver(latest[2]) and _additive(json.loads(latest[1]), _definition(c)):
        return "auto_minor"
    return "fingerprint_mismatch"


def _fq(c: Contract) -> str:
    return f"lake.{'facts' if c.kind == 'fact' else 'derived'}.{c.table_name}"


def _ensure_table(con, c: Contract, sample: pa.Table) -> None:
    schema = "facts" if c.kind == "fact" else "derived"
    exists = con.execute(
        "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake' AND schema_name = ? "
        "AND table_name = ?",
        [schema, c.table_name],
    ).fetchall()[0][0]
    con.register("_sample", sample)
    try:
        if not exists:
            con.execute(f"CREATE TABLE {_fq(c)} AS SELECT * FROM _sample LIMIT 0")
            if c.partition_by:
                con.execute(
                    f"ALTER TABLE {_fq(c)} SET PARTITIONED BY ({', '.join(c.partition_by)})"
                )
            return
        have = {r[0] for r in con.execute(f"DESCRIBE {_fq(c)}").fetchall()}
        types = {r[0]: r[1] for r in con.execute("DESCRIBE _sample").fetchall()}
        for name in sample.column_names:
            if name not in have:  # additive MINOR change
                con.execute(f"ALTER TABLE {_fq(c)} ADD COLUMN {name} {types[name]}")
    finally:
        con.unregister("_sample")


def _move(src: Path, root: Path, bucket: str, table: str, name: str, run_id: str) -> Path:
    """Move a batch directory; never overwrite an existing one (audit copies are kept)."""
    dest = root / bucket / table / name
    if dest.exists():
        dest = dest.with_name(f"{name}-{run_id}")
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(src), dest)
    return dest


def _pending(landing: Path) -> list[Path]:
    """Every candidate batch dir: landing/<table>/<name> outside _buckets and .tmp dirs."""
    out = []
    for table in sorted(landing.iterdir()) if landing.exists() else []:
        if not table.is_dir() or table.name.startswith(("_", ".")):
            continue
        out += [p for p in sorted(table.iterdir()) if p.is_dir() and not p.name.startswith(".tmp-")]
    return out


def _read_manifest(path: Path) -> dict | None:
    try:
        m = json.loads((path / MANIFEST).read_text())
    except (OSError, ValueError):
        return None
    if not isinstance(m, dict) or any(k not in m for k in MANIFEST_KEYS):
        return None
    return m


def publish_pending(
    cfg: StoreConfig,
    *,
    run_id: str | None = None,
    resolving_schemes: tuple[str, ...] | None = None,
    single_valued_schemes: tuple[str, ...] | None = None,
    contracts: dict[tuple[str, int], Contract] | None = None,
    now: datetime | None = None,
) -> PublishReport:
    rep = PublishReport(run_id=run_id or uuid.uuid4().hex, started_at=datetime.now(UTC).isoformat())
    cfg.landing.mkdir(parents=True, exist_ok=True)
    lock = open(cfg.landing / LOCK, "w")  # noqa: SIM115 — held for the whole run
    try:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            rep.fatal = "another publisher is running on this landing zone"
            return _finish(cfg, rep)
        try:
            if contracts is None:
                contracts = discover_contracts()
            for c in contracts.values():
                validate_contract(c)
            con = connect(cfg, read_only=False)
        except Exception as exc:  # noqa: BLE001 — any failure here is fatal and must be reported
            rep.fatal = f"{type(exc).__name__}: {exc}"
            return _finish(cfg, rep)
        try:
            run = _Run(
                cfg,
                con,
                contracts,
                rep,
                resolving_schemes or cfg.resolving_schemes,
                single_valued_schemes or cfg.single_valued_schemes,
                now,
            )
            run.all()
        except Exception as exc:  # noqa: BLE001 — doctrine: a crash becomes a fatal run report
            rep.fatal = f"{type(exc).__name__}: {exc}"
        finally:
            con.close()
        return _finish(cfg, rep)
    finally:
        lock.close()


class _Run:
    def __init__(self, cfg, con, contracts, rep, resolving_schemes, single_valued_schemes, now):
        self.cfg, self.con, self.contracts, self.rep = cfg, con, contracts, rep
        self.resolving, self.single_valued, self.now = resolving_schemes, single_valued_schemes, now
        self.links = load_links(con)
        self.identity = {c.name for c in contracts.values() if c.resolves_entities}

    def all(self) -> None:
        batches = []
        for path in _pending(self.cfg.landing):
            if not BATCH_ID_RE.fullmatch(path.name):
                self.quarantine(path, None, ["bad_batch_dir_name"])
                continue
            m = _read_manifest(path)
            if m is None:
                self.quarantine(path, None, ["manifest_invalid"])
                continue
            first = 0 if m["contract"] in self.identity else 1  # entities before what needs them
            batches.append((first, str(m["written_at"]), path, m))
        for _, _, path, m in sorted(batches, key=lambda b: b[:2] + (b[2].name,)):
            try:
                self.one(path, m)
            except duckdb.Error as exc:  # this batch failed on infrastructure: report, go on
                self.rep.errors.append(
                    {"batch_id": path.name, "error": f"{type(exc).__name__}: {exc}"}
                )

    def quarantine(self, path: Path, m: dict | None, reasons: list[str]) -> None:
        (path / "reason.json").write_text(
            json.dumps({"reasons": reasons, "run_id": self.rep.run_id})
        )
        _move(path, self.cfg.landing, "_quarantine", path.parent.name, path.name, self.rep.run_id)
        self.rep.quarantined.append(
            {"batch_id": path.name, "contract": (m or {}).get("contract"), "reasons": reasons}
        )

    def one(self, path: Path, m: dict) -> None:
        bid = path.name
        try:
            major = _semver(str(m["contract_version"]))[0]
        except ValueError:
            return self.quarantine(path, m, ["manifest_invalid"])
        c = self.contracts.get((m["contract"], major))
        if c is None:
            return self.quarantine(path, m, [f"unknown_contract:{m['contract']}_v{major}"])
        if c.fingerprint() != m["contract_fingerprint"] or c.version != m["contract_version"]:
            return self.quarantine(path, m, ["batch_written_with_other_contract_definition"])
        state = _approval_state(self.con, c)
        if state in ("not_approved", "fingerprint_mismatch"):
            return self.quarantine(path, m, [f"contract_{state}"])
        try:
            expected_sha = m["files"][DATA]["sha256"]
            if file_sha256(path / DATA) != expected_sha:
                return self.quarantine(path, m, ["manifest_mismatch:file_sha256"])
            raw = pq.read_table(path / DATA)
        except (OSError, KeyError, TypeError, pa.ArrowException):
            return self.quarantine(path, m, ["data_unreadable"])
        try:
            t = conform(
                c, raw, now=self.now, future_tolerance_s=self.cfg.known_at_future_tolerance_s
            )
        except BatchInvalid as exc:
            return self.quarantine(path, m, exc.reasons)
        rows_hash = content_hash(t)
        if batch_id_for(c, rows_hash) != bid or m["batch_id"] != bid:
            return self.quarantine(path, m, ["batch_id_mismatch"])
        if m["rows"] != t.num_rows or m["content_sha256"] != rows_hash:
            return self.quarantine(path, m, ["manifest_mismatch:rows_or_content"])
        if self.con.execute(
            "SELECT count(*) FROM lake.ref.batches WHERE batch_id = ?", [bid]
        ).fetchall()[0][0]:
            self.rep.skipped_duplicates.append(bid)
            _move(path, self.cfg.landing, "_duplicates", path.parent.name, bid, self.rep.run_id)
            return None
        new_links: list[tuple] = []
        if c.resolves_entities:
            try:
                eids, new_links = resolve_observations(
                    t, self.links, self.resolving, self.single_valued
                )
            except EntityConflict as exc:
                return self.quarantine(path, m, [f"entity_conflict:{exc}"])
            except UnresolvableSubject as exc:
                return self.quarantine(path, m, [f"subject_not_resolvable:{exc}"])
        elif c.entity_column_pair:
            eids, missing = resolve_facts(t, c.entity_column_pair, self.links)
            if missing:
                return self.quarantine(path, m, [f"unknown_entity:{missing}"])
        else:
            eids = [None] * t.num_rows
        out = t.add_column(0, "entity_id", pa.array(eids, pa.string()))
        out = out.append_column("batch_id", pa.array([bid] * t.num_rows, pa.string()))
        self.commit(c, m, bid, out, eids, new_links, state, rows_hash)
        for eid, scheme, value, _, _ in new_links:
            self.links[(scheme, value)] = eid
        _move(path, self.cfg.landing, "_published", path.parent.name, bid, self.rep.run_id)
        self.rep.published.append({"batch_id": bid, "contract": c.table_name, "rows": t.num_rows})
        return None

    def commit(self, c, m, bid, out, eids, new_links, state, rows_hash) -> None:
        con = self.con
        con.execute("BEGIN")
        try:
            _ensure_table(con, c, out)
            con.register("_batch", out)
            cols = ", ".join(out.column_names)
            con.execute(f"INSERT INTO {_fq(c)} ({cols}) SELECT {cols} FROM _batch")
            con.unregister("_batch")
            if new_links:
                con.executemany(
                    "INSERT INTO lake.ref.entity_identifiers VALUES (?, ?, ?, ?, ?, ?)",
                    [(*lk, bid) for lk in new_links],
                )
            if c.resolves_entities:
                rebuild_attributes(con, set(eids), _fq(c))
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
                    out.num_rows,
                    rows_hash,
                    datetime.now(UTC),
                ],
            )
            con.execute(
                "CALL lake.set_commit_message('my-open-data publisher', ?, extra_info => ?)",
                [
                    f"publish {c.table_name} {bid}",
                    json.dumps({"batch_id": bid, "run_id": self.rep.run_id}),
                ],
            )
            con.execute("COMMIT")
        except BaseException:
            try:
                con.execute("ROLLBACK")
            except duckdb.Error:
                pass  # the original error is the one worth reporting
            raise


def _finish(cfg: StoreConfig, rep: PublishReport) -> PublishReport:
    rep.finished_at = datetime.now(UTC).isoformat()
    out = cfg.data_root / "reports" / "publisher"
    out.mkdir(parents=True, exist_ok=True)
    (out / f"{rep.started_at[:19].replace(':', '')}-{rep.run_id}.json").write_text(rep.to_json())
    return rep
