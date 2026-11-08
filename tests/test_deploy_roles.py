"""deploy/nas: roles.sql on a real Postgres, and the copy compose.yaml runs at init."""

import os
import shutil
import subprocess
import textwrap
import uuid
from pathlib import Path

import pytest
from conftest import NOW, fund_row, obs_rows
from test_publisher import land

from myopendata.config import StoreConfig
from myopendata.contracts.standard import ENTITY_OBSERVATIONS, FUNDAMENTALS
from myopendata.publisher import approve_contract, publish_pending
from myopendata.reader import Store

DEPLOY = Path(__file__).parents[1] / "deploy" / "nas"
ROLES = DEPLOY / "roles.sql"
BEGIN, END = "-- BEGIN roles.sql", "-- END roles.sql"


def _block(text: str) -> str:
    """The lines between the markers, dedented, trailing spaces dropped."""
    lines = text.splitlines()
    starts = [i for i, ln in enumerate(lines) if ln.strip() == BEGIN]
    ends = [i for i, ln in enumerate(lines) if ln.strip() == END]
    assert len(starts) == 1 and len(ends) == 1 and starts[0] < ends[0]
    body = "\n".join(ln.rstrip() for ln in lines[starts[0] + 1 : ends[0]])
    return textwrap.dedent(body).strip()


def test_compose_runs_exactly_roles_sql():
    """Dockge stacks hold only compose.yaml and .env, so init carries the SQL inline."""
    assert _block((DEPLOY / "compose.yaml").read_text()) == _block(ROLES.read_text())


def _with(dsn: str, **over: str) -> str:
    parts = dict(p.split("=", 1) for p in dsn.split())
    parts.update(over)
    return " ".join(f"{k}={v}" for k, v in parts.items())


def _psql(conninfo: str, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["psql", conninfo, "-X", "-v", "ON_ERROR_STOP=1", *args],
        capture_output=True,
        text=True,
    )


@pytest.fixture
def admin():
    dsn = os.environ.get("MYOPENDATA_TEST_PG")
    if not dsn:
        pytest.skip("MYOPENDATA_TEST_PG not set")
    if shutil.which("psql") is None:
        pytest.skip("psql not installed")
    return dsn


@pytest.mark.postgres
def test_roles_script_publisher_writes_reader_reads_only(admin, tmp_path):
    db = f"lake_{uuid.uuid4().hex[:8]}"
    args = ["-v", f"db={db}", "-v", "publisher_password=pw1", "-v", "reader_password=pw2"]
    for _ in range(2):  # idempotent: init runs it on every start of the stack
        r = _psql(admin, *args, "-f", str(ROLES))
        assert r.returncode == 0, r.stderr
    try:
        pub = StoreConfig(
            data_root=tmp_path / "store",
            catalog="postgres:" + _with(admin, user="lake_publisher", password="pw1", dbname=db),
        )
        approve_contract(pub, ENTITY_OBSERVATIONS, "test")
        approve_contract(pub, FUNDAMENTALS, "test")
        land(pub, ENTITY_OBSERVATIONS, [obs_rows()])
        land(pub, FUNDAMENTALS, [fund_row()])
        rep = publish_pending(pub, now=NOW)
        assert rep.exit_code == 0, rep.to_json()

        reader_info = _with(admin, user="lake_reader", password="pw2", dbname=db)
        s = Store.open(StoreConfig(data_root=tmp_path / "store", catalog="postgres:" + reader_info))
        try:
            assert s.read_asof("fundamentals").num_rows == 1
        finally:
            s.close()
        for sql in ("DELETE FROM ducklake_metadata", "CREATE TABLE public.x (i int)"):
            r = _psql(reader_info, "-c", sql)
            assert r.returncode != 0 and "permission denied" in r.stderr, (sql, r.stderr)
    finally:
        _psql(admin, "-c", f'DROP DATABASE IF EXISTS "{db}" WITH (FORCE)')
