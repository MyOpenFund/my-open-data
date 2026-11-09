from datetime import date
from decimal import Decimal

import pytest
from conftest import NOW, fund_row, obs_rows, ts
from test_publisher import land

from myopendata.contracts.standard import ENTITY_OBSERVATIONS, FUNDAMENTALS
from myopendata.publisher import publish_pending
from myopendata.reader import Store

MSFT, AAPL = "0000789019", "0000320193"


@pytest.fixture
def store(approved):
    land(approved, ENTITY_OBSERVATIONS, [
        obs_rows(),
        obs_rows(subject_value=AAPL, id_value="HWUPKR0MPOU8FGXBT394", name="APPLE INC"),
    ])
    land(approved, FUNDAMENTALS, [
        fund_row(),                                                            # MSFT 10-K
        fund_row(value=Decimal(41000000000), known_at=ts("2025-10-15T00:00:00")),  # MSFT 10-K/A
        fund_row(source_code="ocr-test", extraction_method="ocr", confidence=0.8, source_page=1,
                 value=Decimal(40100000000), known_at=ts("2025-08-15T00:00:00")),  # 2nd source
        fund_row(entity_value=AAPL, filing_ref="0000320193-25-000001",
                 original_tag="us-gaap:Revenues", period_start=date(2024, 9, 29),
                 frequency="annual", valid_date=date(2025, 9, 27), value=Decimal(416161000000),
                 known_at=ts("2025-10-31T20:00:00")),
    ])
    assert publish_pending(approved, now=NOW).exit_code == 0
    s = Store.open(approved)
    yield s
    s.close()


def _rows(t):
    return sorted(t.drop_columns([c for c in ("batch_id",) if c in t.column_names]).to_pylist(),
                  key=lambda r: tuple(str(v) for v in r.values()))


AS_OFS = [ts("2025-07-01T00:00:00"), ts("2025-09-01T00:00:00"), ts("2025-12-01T00:00:00")]


@pytest.mark.parametrize("as_of", AS_OFS)
@pytest.mark.parametrize("vintages", [False, True])
def test_key_filter_inside_window_matches_filtering_outside(store, as_of, vintages):
    pushed = store.read_asof("fundamentals", as_of, filters={"entity_value": [MSFT]},
                             vintages=vintages)
    outside = store.con.execute(
        f"SELECT * FROM ({store.asof_sql('fundamentals', as_of, vintages=vintages)}) "
        "WHERE entity_value = ?", [MSFT]).to_arrow_table()
    assert _rows(pushed) == _rows(outside)


def test_vintages_keep_every_known_at(store):
    t = store.read_asof("fundamentals", ts("2025-12-01T00:00:00"), vintages=True,
                        filters={"entity_value": MSFT, "source_code": "sec"})
    assert sorted(r["value"] for r in t.to_pylist()) == [Decimal(40152000000), Decimal(41000000000)]


def test_vintages_stop_at_as_of(store):
    t = store.read_asof("fundamentals", ts("2025-09-01T00:00:00"), vintages=True,
                        filters={"entity_value": MSFT, "source_code": "sec"})
    assert [r["value"] for r in t.to_pylist()] == [Decimal(40152000000)]


def test_non_key_filter_still_applies_after_the_window(store):
    # unit is not in the natural key: filtering it inside would change which row is latest.
    t = store.read_asof("fundamentals", ts("2025-12-01T00:00:00"), filters={"unit": "USD"})
    assert len(t) == 3


def test_pushed_sql_names_the_key_filter_inside_the_window(store):
    sql = store.asof_sql(
        "fundamentals", ts("2025-12-01T00:00:00"), inner=" AND f.entity_value IN (?)"
    )
    assert sql.index("f.entity_value IN (?)") < sql.index("WHERE _rn = 1")


def test_vintages_rejects_snapshot_bool(store):
    with pytest.raises(ValueError):
        store.read_asof("fundamentals", vintages="yes")
