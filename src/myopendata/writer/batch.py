"""Producer side: validate a table against its contract and drop it as a batch in landing/.

Needs pyarrow only. Never touches Postgres (spec §6). A batch directory appears atomically
(written under a temporary name, then renamed), so the publisher never sees half a batch.
"""

from __future__ import annotations

import hashlib
import json
import os
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
    t = table.sort_by([(n, "ascending") for n in table.column_names])
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, t.schema) as w:
        w.write_table(t)
    return hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest()


def batch_id_for(contract: Contract, table: pa.Table) -> str:
    """Same contract version + same rows = same id, so a replayed run is a no-op downstream."""
    raw = f"{contract.name}|{contract.version}|{contract.fingerprint()}|{content_hash(table)}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


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
    bid = batch_id_for(contract, t)
    dest = Path(landing_root) / contract.table_name / bid
    if (dest / MANIFEST).exists():
        return BatchRef(bid, dest, t.num_rows, created=False)
    tmp = Path(landing_root) / contract.table_name / f".tmp-{uuid.uuid4().hex}"
    tmp.mkdir(parents=True)
    pq.write_table(t, tmp / DATA)
    manifest = {
        "batch_id": bid,
        "contract": contract.name,
        "contract_version": contract.version,
        "contract_fingerprint": contract.fingerprint(),
        "producer": producer,
        "run_id": run_id,
        "rows": t.num_rows,
        "content_sha256": content_hash(t),
        "source_inputs": sorted(source_inputs or []),
        "written_at": (now or datetime.now(UTC)).isoformat(),
    }
    (tmp / MANIFEST).write_text(json.dumps(manifest, indent=2, sort_keys=True))
    try:
        os.rename(tmp, dest)
    except OSError:
        # A concurrent writer landed the same batch first: identical content, keep theirs.
        for f in tmp.iterdir():
            f.unlink()
        tmp.rmdir()
        return BatchRef(bid, dest, t.num_rows, created=False)
    return BatchRef(bid, dest, t.num_rows, created=True)
