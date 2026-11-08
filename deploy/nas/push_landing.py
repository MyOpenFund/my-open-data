"""Copy finished my-open-data batches from a local landing zone to a remote landing over SMB.

The writer needs its landing zone on a local or NFS filesystem, and the NAS is reachable from the
Mac over SMB only. So producers write locally, and this script copies each finished batch to
DEST/<table>/.tmp-push-<batch_id> (the publisher ignores ".tmp-" directories), then renames it to
DEST/<table>/<batch_id> on the share, so the publisher never sees half a batch. A batch DEST
already holds, pending or moved by the publisher (_published, _duplicates, _quarantine, with or
without a "-<run_id>" suffix), is not copied again. The publisher re-checks every file's sha256,
so a batch damaged in transit is quarantined, never published. Standard library only.

    python push_landing.py SRC_LANDING DEST_LANDING [--dry-run]

Exit code 0, or 1 when a source batch has no batch.json (incomplete; not copied).
"""

from __future__ import annotations

import argparse
import shutil
import sys
from dataclasses import dataclass, field
from pathlib import Path

MANIFEST = "batch.json"
BUCKETS = ("_published", "_duplicates", "_quarantine")
TMP_PREFIX = ".tmp-push-"


@dataclass
class PushReport:
    copied: list[str] = field(default_factory=list)
    already_there: list[str] = field(default_factory=list)
    incomplete: list[str] = field(default_factory=list)


def finished_batches(src: Path) -> list[Path]:
    """Batch directories under src/<table>/, outside bucket and dot directories."""
    if not src.exists():
        return []
    out: list[Path] = []
    for table in sorted(src.iterdir()):
        if not table.is_dir() or table.name.startswith(("_", ".")):
            continue
        out += [b for b in sorted(table.iterdir()) if b.is_dir() and not b.name.startswith(".")]
    return out


def known_on_dest(dest: Path, table: str, name: str) -> bool:
    if (dest / table / name).exists():
        return True
    for bucket in BUCKETS:
        moved = dest / bucket / table
        if (moved / name).exists() or any(moved.glob(f"{name}-*")):
            return True
    return False


def push(src: Path, dest: Path, *, dry_run: bool = False) -> PushReport:
    rep = PushReport()
    for batch in finished_batches(src):
        table, name = batch.parent.name, batch.name
        key = f"{table}/{name}"
        if not (batch / MANIFEST).is_file():
            rep.incomplete.append(key)
            continue
        if known_on_dest(dest, table, name):
            rep.already_there.append(key)
            continue
        if dry_run:
            rep.copied.append(key)
            continue
        tmp = dest / table / f"{TMP_PREFIX}{name}"
        if tmp.exists():
            shutil.rmtree(tmp)  # left by a push that crashed; only this script uses the prefix
        tmp.parent.mkdir(parents=True, exist_ok=True)
        try:
            shutil.copytree(batch, tmp)
            tmp.rename(dest / table / name)
        except BaseException:
            shutil.rmtree(tmp, ignore_errors=True)
            raise
        rep.copied.append(key)
    return rep


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("src", type=Path, help="local landing zone (MYOPENDATA_DATA_ROOT/landing)")
    p.add_argument("dest", type=Path, help="remote landing zone, e.g. on an SMB mount")
    p.add_argument("--dry-run", action="store_true", help="list what would be copied")
    a = p.parse_args(argv)
    rep = push(a.src, a.dest, dry_run=a.dry_run)
    verb = "would copy" if a.dry_run else "copied"
    print(
        f"{verb} {len(rep.copied)} · already there {len(rep.already_there)} · "
        f"incomplete {len(rep.incomplete)}"
    )
    for key in rep.incomplete:
        print(f"incomplete (no {MANIFEST}): {key}", file=sys.stderr)
    return 1 if rep.incomplete else 0


if __name__ == "__main__":
    sys.exit(main())
