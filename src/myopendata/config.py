"""Store configuration. Nothing host-specific in code: everything comes from env or arguments."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

ENV_DATA_ROOT = "MYOPENDATA_DATA_ROOT"
ENV_CATALOG = "MYOPENDATA_CATALOG"
ENV_INLINING = "MYOPENDATA_DATA_INLINING_ROW_LIMIT"
ENV_FUTURE_TOLERANCE = "MYOPENDATA_KNOWN_AT_FUTURE_TOLERANCE_S"
ENV_RESOLVING = "MYOPENDATA_RESOLVING_SCHEMES"
ENV_SINGLE_VALUED = "MYOPENDATA_SINGLE_VALUED_SCHEMES"

# Schemes trusted to merge identifiers into one entity (decision D3). Tickers are reused over
# time, so they are not in the default and never merge entities.
DEFAULT_RESOLVING_SCHEMES = ("cik", "lei", "isin", "figi", "cb_code", "national_id")
# Schemes an entity holds at most one value of (an issuer has several ISINs/FIGIs, one CIK/LEI).
DEFAULT_SINGLE_VALUED_SCHEMES = ("cik", "lei", "cb_code")


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
    resolving_schemes / single_valued_schemes: entity-resolution policy, see the defaults above.
    """

    data_root: Path
    catalog: str
    data_inlining_row_limit: int = 0
    known_at_future_tolerance_s: int = 300
    resolving_schemes: tuple[str, ...] = DEFAULT_RESOLVING_SCHEMES
    single_valued_schemes: tuple[str, ...] = DEFAULT_SINGLE_VALUED_SCHEMES

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
            resolving_schemes=_schemes(ENV_RESOLVING, DEFAULT_RESOLVING_SCHEMES),
            single_valued_schemes=_schemes(ENV_SINGLE_VALUED, DEFAULT_SINGLE_VALUED_SCHEMES),
        )


def _schemes(var: str, default: tuple[str, ...]) -> tuple[str, ...]:
    raw = os.environ.get(var)
    return tuple(x.strip() for x in raw.split(",") if x.strip()) if raw else default
