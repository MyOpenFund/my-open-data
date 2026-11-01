"""Store configuration. Nothing host-specific in code: everything comes from env or arguments."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

ENV_DATA_ROOT = "MYOPENDATA_DATA_ROOT"
ENV_CATALOG = "MYOPENDATA_CATALOG"
ENV_INLINING = "MYOPENDATA_DATA_INLINING_ROW_LIMIT"
ENV_FUTURE_TOLERANCE = "MYOPENDATA_KNOWN_AT_FUTURE_TOLERANCE_S"


@dataclass(frozen=True)
class StoreConfig:
    """Where the store lives.

    data_root: holds landing/ (producer batches) and lake/ (DuckLake Parquet files).
    catalog: a DuckLake catalogue spec without the "ducklake:" prefix, e.g.
        "postgres:dbname=lake_catalog host=... user=..." or a file path "/x/meta.ducklake".
        A file catalogue admits one process at a time (no reader while the publisher runs): use it
        for tests and single-user local work only; production uses Postgres.
    data_inlining_row_limit: DuckLake inlines small inserts into the catalogue; 0 forces Parquet
        so every published batch is a file under data_root (auditable, portable).
    known_at_future_tolerance_s: clock-skew tolerance when rejecting known_at in the future.
    """

    data_root: Path
    catalog: str
    data_inlining_row_limit: int = 0
    known_at_future_tolerance_s: int = 300

    @property
    def landing(self) -> Path:
        return self.data_root / "landing"

    @property
    def lake_data(self) -> Path:
        return self.data_root / "lake"

    @classmethod
    def from_env(cls) -> StoreConfig:
        missing = [k for k in (ENV_DATA_ROOT, ENV_CATALOG) if not os.environ.get(k)]
        if missing:
            raise RuntimeError(f"missing environment variables: {', '.join(missing)}")
        return cls(
            data_root=Path(os.environ[ENV_DATA_ROOT]),
            catalog=os.environ[ENV_CATALOG],
            data_inlining_row_limit=int(os.environ.get(ENV_INLINING, "0")),
            known_at_future_tolerance_s=int(os.environ.get(ENV_FUTURE_TOLERANCE, "300")),
        )
