"""Producer side: validate a table against its contract and drop it as a batch in landing/.

Needs pyarrow only. Never touches Postgres (spec §6). A batch directory appears atomically
(written under a temporary name, fsynced, then renamed), so the publisher never sees half a batch.
Atomic directory rename needs landing_root on one local or NFS filesystem; SMB/CIFS and
object-storage mounts do not guarantee it.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from myopendata.contracts.base import Contract
from myopendata.contracts.rules import conform, validate_contract

MANIFEST = "batch.json"
DATA = "data.parquet"


@dataclass(frozen=True)
class BatchRef:
    batch_id: str
    path: Path
    rows: int
    created: bool  # False when an identical batch was already waiting in landing/


def content_hash(table: pa.Table) -> str:
    """Order-independent hash of the rows: sort by every column, hash the Arrow IPC stream."""
    t = table.replace_schema_metadata(None)
    t = t.sort_by([(n, "ascending") for n in t.column_names])
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, t.schema) as w:
        w.write_table(t)
    return hashlib.sha256(sink.getvalue()).hexdigest()


def batch_id_for(contract: Contract, rows_hash: str) -> str:
    """Same contract version + same rows = same id, so a replayed run is a no-op downstream."""
    raw = f"{contract.name}|{contract.version}|{contract.fingerprint()}|{rows_hash}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _fsync(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_batch(
    contract: Contract,
    table: pa.Table,
    *,
    landing_root: Path,
    run_id: str,
    producer: str,
    source_inputs: list[str] | None = None,
    now: datetime | None = None,
    future_tolerance_s: int = 300,
) -> BatchRef:
    """Validate and write one batch. Raises rules.BatchInvalid on any contract breach."""
    validate_contract(contract)
    t = conform(contract, table, now=now, future_tolerance_s=future_tolerance_s)
    rows_hash = content_hash(t)
    bid = batch_id_for(contract, rows_hash)
    dest = Path(landing_root) / contract.table_name / bid
    if (dest / MANIFEST).exists():
        return BatchRef(bid, dest, t.num_rows, created=False)
    tmp = Path(landing_root) / contract.table_name / f".tmp-{uuid.uuid4().hex}"
    tmp.mkdir(parents=True)
    try:
        pq.write_table(t, tmp / DATA)
        manifest = {
            "batch_id": bid,
            "contract": contract.name,
            "contract_version": contract.version,
            "contract_fingerprint": contract.fingerprint(),
            "producer": producer,
            "run_id": run_id,
            "rows": t.num_rows,
            "content_sha256": rows_hash,
            "files": {
                DATA: {"bytes": (tmp / DATA).stat().st_size, "sha256": file_sha256(tmp / DATA)}
            },
            "source_inputs": sorted(source_inputs or []),
            "written_at": (now or datetime.now(UTC)).isoformat(),
        }
        (tmp / MANIFEST).write_text(json.dumps(manifest, indent=2, sort_keys=True))
        for f in (tmp / DATA, tmp / MANIFEST, tmp):
            _fsync(f)
    except BaseException:
        shutil.rmtree(tmp, ignore_errors=True)
        raise
    try:
        os.rename(tmp, dest)
    except OSError:
        shutil.rmtree(tmp, ignore_errors=True)
        if (dest / MANIFEST).exists():  # a concurrent writer landed the same batch first
            return BatchRef(bid, dest, t.num_rows, created=False)
        raise
    _fsync(dest.parent)
    return BatchRef(bid, dest, t.num_rows, created=True)
