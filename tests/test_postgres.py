"""Same flow against a real Postgres DuckLake catalogue (CI service; skipped without it)."""

import pytest
from conftest import NOW, fund_row, obs_rows
from test_publisher import land

from myopendata.contracts.standard import ENTITY_OBSERVATIONS, FUNDAMENTALS
from myopendata.publisher import approve_contract, publish_pending
from myopendata.reader import Store

pytestmark = pytest.mark.postgres


def test_end_to_end_on_postgres_catalogue(pg_cfg):
    approve_contract(pg_cfg, ENTITY_OBSERVATIONS, "test")
    approve_contract(pg_cfg, FUNDAMENTALS, "test")
    land(pg_cfg, ENTITY_OBSERVATIONS, [obs_rows()])
    land(pg_cfg, FUNDAMENTALS, [fund_row()])
    rep = publish_pending(pg_cfg, now=NOW)
    assert rep.exit_code == 0, rep.to_json()
    s = Store.open(pg_cfg)
    try:
        assert s.read_asof("fundamentals").num_rows == 1
    finally:
        s.close()
