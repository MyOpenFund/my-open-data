import os
import uuid
from datetime import UTC, date, datetime
from decimal import Decimal

import pyarrow as pa
import pytest

from myopendata.config import StoreConfig
from myopendata.contracts.standard import ENTITY_OBSERVATIONS, FUNDAMENTALS

NOW = datetime(2026, 11, 1, 12, 0, tzinfo=UTC)


def ts(s: str) -> datetime:
    """Naive ISO string read as UTC; a string with an offset keeps it."""
    d = datetime.fromisoformat(s)
    return d if d.tzinfo else d.replace(tzinfo=UTC)


@pytest.fixture
def cfg(tmp_path):
    return StoreConfig(data_root=tmp_path / "store", catalog=str(tmp_path / "meta.ducklake"))


@pytest.fixture
def pg_cfg(tmp_path):
    dsn = os.environ.get("MYOPENDATA_TEST_PG")
    if not dsn:
        pytest.skip("MYOPENDATA_TEST_PG not set")
    import duckdb

    db = f"t_{uuid.uuid4().hex[:8]}"
    con = duckdb.connect()
    con.execute("INSTALL postgres; LOAD postgres")
    con.execute(f"ATTACH '{dsn}' AS pg (TYPE postgres)")
    con.execute(f"CALL postgres_execute('pg', 'CREATE DATABASE {db}', use_transaction=false)")
    yield StoreConfig(data_root=tmp_path / "store", catalog=f"postgres:{dsn} dbname={db}")
    con.execute(
        f"CALL postgres_execute('pg', 'DROP DATABASE IF EXISTS {db} WITH (FORCE)', "
        "use_transaction=false)"
    )
    con.close()


@pytest.fixture
def approved(cfg):
    from myopendata.publisher import approve_contract

    approve_contract(cfg, ENTITY_OBSERVATIONS, "test")
    approve_contract(cfg, FUNDAMENTALS, "test")
    return cfg


def obs_rows(**over):
    base = dict(
        subject_scheme="cik",
        subject_value="0000789019",
        id_scheme="lei",
        id_value="INR2EJN1ERAN0W5ZP974",
        name="MICROSOFT CORP",
        country="US",
        valid_date=date(2025, 6, 30),
        known_at=ts("2025-07-30T20:00:00"),
        known_at_kind="published",
        source_code="sec",
        source_doc_id=None,
    )
    base.update(over)
    return base


def fund_row(**over):
    base = dict(
        entity_scheme="cik",
        entity_value="0000789019",
        original_tag="us-gaap:LongTermDebt",
        period_start=None,
        frequency="instant",
        fiscal_year=2025,
        basis="consolidated",
        value=Decimal(40152000000),
        unit="USD",
        extraction_method="xbrl_filed",
        confidence=None,
        source_page=None,
        filing_ref="0000789019-25-000001",
        valid_date=date(2025, 6, 30),
        known_at=ts("2025-07-30T20:00:00"),
        known_at_kind="published",
        source_code="sec",
        source_doc_id="doc-10k-2025",
    )
    base.update(over)
    return base


def table(contract, rows):
    return pa.Table.from_pylist(rows, schema=contract.schema)


def statement_row(**over):
    base = dict(
        entity_scheme="cik",
        entity_value="0000789019",
        filing_ref="0000789019-25-000001",
        statement="BS",
        tag="LongTermDebt",
        tag_version="us-gaap/2024",
        plabel="Long-term debt",
        valid_date=date(2025, 6, 30),
        known_at=ts("2025-07-30T20:00:00"),
        known_at_kind="published",
        source_code="sec",
        source_doc_id="doc-10k-2025",
    )
    base.update(over)
    return base


def filing_row(**over):
    base = dict(
        entity_scheme="cik",
        entity_value="0000789019",
        filing_ref="0000789019-25-000001",
        form="10-K",
        report_date=date(2025, 6, 30),
        valid_date=date(2025, 7, 30),
        known_at=ts("2025-07-30T20:00:00"),
        known_at_kind="published",
        source_code="sec",
        source_doc_id="doc-10k-2025",
    )
    base.update(over)
    return base
