from decimal import Decimal

import pytest
from conftest import NOW, fund_row, obs_rows, ts
from test_publisher import land

from myopendata.contracts.standard import ENTITY_OBSERVATIONS, FUNDAMENTALS
from myopendata.publisher import publish_pending
from myopendata.reader import Recipe, Store, export


@pytest.fixture
def store(approved):
    land(approved, ENTITY_OBSERVATIONS, [obs_rows()])
    land(
        approved,
        FUNDAMENTALS,
        [
            fund_row(),  # 10-K, 2025-07-30
            fund_row(value=Decimal(41000000000), known_at=ts("2025-10-15T00:00:00")),  # 10-K/A
            fund_row(
                source_code="ocr-test",
                extraction_method="ocr",
                confidence=0.8,
                value=Decimal(40100000000),
                known_at=ts("2025-08-15T00:00:00"),
            ),
        ],
    )
    assert publish_pending(approved, now=NOW).exit_code == 0
    s = Store.open(approved)
    yield s
    s.close()


def _sec_value(t):
    return [r["value"] for r in t.to_pylist() if r["source_code"] == "sec"]


def test_before_publication_nothing_is_known(store):
    assert store.read_asof("fundamentals", ts("2025-07-01T00:00:00")).num_rows == 0


def test_original_value_before_restatement(store):
    t = store.read_asof("fundamentals", ts("2025-09-01T00:00:00"))
    assert _sec_value(t) == [Decimal(40152000000)]


def test_restated_value_after_restatement(store):
    t = store.read_asof("fundamentals", ts("2025-12-01T00:00:00"))
    assert _sec_value(t) == [Decimal(41000000000)]


def test_two_sources_both_visible(store):
    t = store.read_asof("fundamentals", ts("2025-12-01T00:00:00"))
    assert sorted(r["source_code"] for r in t.to_pylist()) == ["ocr-test", "sec"]


def test_where_filter_and_major_suffix(store):
    t = store.read_asof("fundamentals_v1", ts("2025-12-01T00:00:00"), where="source_code = 'sec'")
    assert t.num_rows == 1


def test_naive_as_of_rejected(store):
    from datetime import datetime

    with pytest.raises(ValueError, match="timezone"):
        store.read_asof("fundamentals", datetime(2025, 12, 1))


def test_snapshot_pins_store_state(store, approved):
    before = store.con.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchall()[0][0]
    store.close()  # a file catalogue admits one process at a time; Postgres does not need this
    land(
        approved,
        FUNDAMENTALS,
        [fund_row(concept="Revenue", value=Decimal(281724000000))],
        run_id="r2",
    )
    assert publish_pending(approved, now=NOW).exit_code == 0
    store = Store.open(approved)
    late = ts("2025-12-01T00:00:00")
    assert store.read_asof("fundamentals", late, where="concept = 'Revenue'").num_rows == 1
    pinned = store.read_asof("fundamentals", late, snapshot=before, where="concept = 'Revenue'")
    assert pinned.num_rows == 0


def test_recipe_assembles_as_of(store):
    r = Recipe(
        "debt_by_source",
        "1.0.0",
        {"f": "fundamentals"},
        "SELECT source_code, sum(value) AS debt FROM f GROUP BY 1 ORDER BY 1",
    )
    rows = store.assemble(r, ts("2025-09-01T00:00:00")).to_pylist()
    assert rows == [
        {"source_code": "ocr-test", "debt": Decimal(40100000000)},
        {"source_code": "sec", "debt": Decimal(40152000000)},
    ]


def test_save_true_not_in_v0(store):
    with pytest.raises(NotImplementedError):
        store.assemble(Recipe("x", "1.0.0", {}, "SELECT 1"), save=True)


def test_datasets_lists_published_tables(store):
    names = {d["table"] for d in store.datasets()}
    assert {"fundamentals_v1", "entity_observations_v1"} <= names


def test_reader_is_read_only(store):
    import duckdb

    with pytest.raises(duckdb.Error):
        store.con.execute("DELETE FROM lake.facts.fundamentals_v1")


@pytest.mark.parametrize("suffix", [".csv", ".xlsx", ".parquet"])
def test_export(store, tmp_path, suffix):
    t = store.read_asof("fundamentals", ts("2025-12-01T00:00:00"))
    p = export(t, tmp_path / f"out{suffix}")
    assert p.stat().st_size > 0


def test_entity_master_joins(store):
    t = store.con.execute(
        "SELECT a.name FROM lake.facts.fundamentals_v1 f JOIN lake.ref.entity_attributes a "
        "USING (entity_id) WHERE a.valid_to IS NULL LIMIT 1"
    ).fetchall()
    assert t == [("MICROSOFT CORP",)]
