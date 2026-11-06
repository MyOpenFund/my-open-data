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
                source_page=12,
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
    t = store.read_asof("fundamentals_v3", ts("2025-12-01T00:00:00"), where="source_code = 'sec'")
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
        [fund_row(original_tag="us-gaap:Revenue", value=Decimal(281724000000))],
        run_id="r2",
    )
    assert publish_pending(approved, now=NOW).exit_code == 0
    store = Store.open(approved)
    late = ts("2025-12-01T00:00:00")
    assert (
        store.read_asof("fundamentals", late, where="original_tag = 'us-gaap:Revenue'").num_rows
        == 1
    )
    pinned = store.read_asof(
        "fundamentals", late, snapshot=before, where="original_tag = 'us-gaap:Revenue'"
    )
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


def test_a_recipe_input_can_read_every_vintage(store):
    latest = Recipe(
        "n", "1.0.0", {"f": "fundamentals"}, "SELECT count(*) AS n FROM f WHERE source_code = 'sec'"
    )
    every = Recipe("n", "1.0.0", {"f": "fundamentals:vintages"}, latest.sql)
    at = ts("2025-12-01T00:00:00")
    assert store.assemble(latest, at).to_pylist() == [{"n": 1}]
    assert store.assemble(every, at).to_pylist() == [{"n": 2}]
    assert store.assemble(every, ts("2025-09-01T00:00:00")).to_pylist() == [{"n": 1}]


def test_a_bad_recipe_input_mode_is_refused(store):
    with pytest.raises(ValueError, match="bad recipe input 'fundamentals:other'"):
        store.assemble(Recipe("n", "1.0.0", {"f": "fundamentals:other"}, "SELECT 1"))


def test_save_true_not_in_v0(store):
    with pytest.raises(NotImplementedError):
        store.assemble(Recipe("x", "1.0.0", {}, "SELECT 1"), save=True)


def test_datasets_lists_published_tables(store):
    names = {d["table"] for d in store.datasets()}
    assert {"fundamentals_v3", "entity_observations_v1"} <= names


def test_reader_is_read_only(store):
    import duckdb

    with pytest.raises(duckdb.Error):
        store.con.execute("DELETE FROM lake.facts.fundamentals_v3")


@pytest.mark.parametrize("suffix", [".csv", ".xlsx", ".parquet"])
def test_export(store, tmp_path, suffix):
    t = store.read_asof("fundamentals", ts("2025-12-01T00:00:00"))
    p = export(t, tmp_path / f"out{suffix}")
    assert p.stat().st_size > 0


def test_entity_master_joins(store):
    t = store.con.execute(
        "SELECT a.name FROM lake.facts.fundamentals_v3 f JOIN lake.ref.entity_attributes a "
        "USING (entity_id) WHERE a.valid_to IS NULL LIMIT 1"
    ).fetchall()
    assert t == [("MICROSOFT CORP",)]


def test_same_known_at_correction_is_quarantined_not_applied(approved):
    land(approved, ENTITY_OBSERVATIONS, [obs_rows()])
    land(approved, FUNDAMENTALS, [fund_row(value=Decimal(2))])
    assert publish_pending(approved, now=NOW).exit_code == 0
    land(approved, FUNDAMENTALS, [fund_row(value=Decimal(1))], run_id="r2")  # same known_at
    rep = publish_pending(approved, now=NOW)
    assert rep.exit_code == 3 and rep.quarantined[0]["reasons"] == ["conflicting_republish:1"]
    with Store.open(approved) as s:
        assert _sec_value(s.read_asof("fundamentals")) == [Decimal(2)]


def test_known_at_equal_to_as_of_is_known_and_offsets_are_honoured(store):
    assert store.read_asof("fundamentals", ts("2025-07-30T20:00:00")).num_rows == 1
    assert store.read_asof("fundamentals", ts("2025-07-30T21:59:59+02:00")).num_rows == 0
    assert store.read_asof("fundamentals", ts("2025-07-30T22:00:00+02:00")).num_rows == 1


def test_where_cannot_run_a_second_statement(store):
    with pytest.raises(ValueError, match="one SQL statement"):
        store.read_asof("fundamentals", where="1=1; SELECT 42")


def test_session_cannot_touch_host_files(store, tmp_path):
    import duckdb

    target = tmp_path / "leak.csv"
    with pytest.raises(duckdb.Error):
        store.con.sql(f"COPY (SELECT 42) TO '{target}'").fetchall()
    with pytest.raises(duckdb.Error):
        store.read_asof("fundamentals", where="EXISTS (SELECT * FROM read_csv('/etc/passwd'))")
    with pytest.raises(duckdb.Error):
        store.con.execute("SET enable_external_access = true")
    assert not target.exists()


def test_temp_views_do_not_leak_between_assemble_calls(store):
    store.assemble(Recipe("a", "1.0.0", {"f": "fundamentals"}, "SELECT count(*) FROM f"))
    import duckdb

    with pytest.raises(duckdb.Error):
        store.assemble(Recipe("b", "1.0.0", {}, "SELECT count(*) FROM f"))


@pytest.mark.parametrize("bad", ["lake", "memory", "x;y", "1f"])
def test_recipe_input_names_are_checked(store, bad):
    with pytest.raises(ValueError, match="input name"):
        store.assemble(Recipe("a", "1.0.0", {bad: "fundamentals"}, "SELECT 1"))


def test_recipe_with_two_inputs(store):
    r = Recipe(
        "named",
        "1.0.0",
        {"f": "fundamentals", "e": "entity_observations"},
        "SELECT DISTINCT e.name FROM f JOIN e USING (entity_id)",
    )
    t = store.assemble(r, ts("2025-12-01T00:00:00"))
    assert t.to_pylist() == [{"name": "MICROSOFT CORP"}]
    assert t.schema.metadata[b"myopendata.recipe"] == b"named@1.0.0"


@pytest.mark.parametrize("name", ["nope", "fundamentals_v9"])
def test_unknown_dataset_or_major(store, name):
    from myopendata.reader.api import UnknownDataset

    with pytest.raises(UnknownDataset):
        store.read_asof(name)


def test_bool_snapshot_rejected(store):
    with pytest.raises(ValueError, match="snapshot"):
        store.read_asof("fundamentals", snapshot=True)


def test_result_records_its_as_of(store):
    t = store.read_asof("fundamentals", ts("2025-12-01T00:00:00"))
    assert t.schema.metadata[b"myopendata.as_of"] == b"2025-12-01T00:00:00+00:00"


def test_datasets_carry_name_version_and_licence(store):
    d = {x["name"]: x for x in store.datasets()}
    assert d["fundamentals"]["version"] == "3.0.0" and d["fundamentals"]["licence"]
    assert d["fundamentals"]["table"] == "fundamentals_v3"


def test_xlsx_export_writes_utc_timestamps_and_numbers(store, tmp_path):
    from openpyxl import load_workbook

    t = store.read_asof("fundamentals", ts("2025-09-01T00:00:00"), where="source_code = 'sec'")
    p = export(t, tmp_path / "out.xlsx")
    ws = load_workbook(p).active
    header = [c.value for c in ws[1]]
    row = [c.value for c in ws[2]]
    assert row[header.index("known_at")].isoformat() == "2025-07-30T20:00:00"
    assert float(row[header.index("value")]) == 40152000000


def test_xlsx_row_limit(tmp_path):
    import pyarrow as pa

    from myopendata.reader.api import XLSX_MAX_ROWS

    big = pa.table({"x": pa.array(range(XLSX_MAX_ROWS + 1))})
    with pytest.raises(ValueError, match="Excel"):
        export(big, tmp_path / "big.xlsx")


def test_structured_filters_columns_and_limit(store):
    late = ts("2025-12-01T00:00:00")
    t = store.read_asof(
        "fundamentals",
        late,
        filters={"source_code": ["sec"]},
        columns=["original_tag", "value", "source_code"],
    )
    assert t.column_names == ["original_tag", "value", "source_code"] and t.num_rows == 1
    assert store.read_asof("fundamentals", late, filters={"source_code": "nope"}).num_rows == 0
    assert store.read_asof("fundamentals", late, limit=1).num_rows == 1


def test_filter_values_are_bound_not_interpolated(store):
    t = store.read_asof("fundamentals", filters={"source_code": "sec' OR '1'='1"})
    assert t.num_rows == 0


@pytest.mark.parametrize(
    "kw",
    [
        {"filters": {"nope": 1}},
        {"columns": ["x; DROP"]},
        {"filters": {"original_tag": []}},
        {"limit": -1},
        {"limit": True},
    ],
)
def test_bad_filter_arguments(store, kw):
    with pytest.raises(ValueError):
        store.read_asof("fundamentals", **kw)


def test_describe_carries_columns_docs_and_key(store):
    d = store.describe("fundamentals")
    cols = {c["name"]: c for c in d["columns"]}
    assert d["licence"] and d["doc"].startswith("Reported")
    assert cols["frequency"]["allowed"] == ["annual", "semiannual", "quarterly", "instant"]
    assert "Tag as filed" in cols["original_tag"]["doc"]
    assert d["natural_key"][-1] == "source_code"


def test_none_in_filters_is_refused(store):
    with pytest.raises(ValueError, match="None"):
        store.read_asof("fundamentals", filters={"confidence": None})


def test_huge_limit_is_refused(store):
    with pytest.raises(ValueError, match="limit"):
        store.read_asof("fundamentals", limit=10**30)


def test_approved_but_unpublished_minor_columns_are_not_offered(store, approved):
    import dataclasses

    import pyarrow as pa

    from myopendata.contracts.base import Column
    from myopendata.publisher import approve_contract

    store.close()
    minor = dataclasses.replace(
        FUNDAMENTALS,
        version="3.1.0",
        columns=FUNDAMENTALS.columns + (Column("segment", pa.string()),),
    )
    approve_contract(approved, minor, "reviewer")
    with Store.open(approved) as s:
        assert "segment" not in {c["name"] for c in s.describe("fundamentals")["columns"]}
        with pytest.raises(ValueError, match="unknown column"):
            s.read_asof("fundamentals", columns=["segment"])


def test_a_recipe_joins_facts_with_their_statement_position(approved):
    from conftest import statement_row

    from myopendata.contracts.standard import FILING_STATEMENTS
    from myopendata.publisher import approve_contract

    approve_contract(approved, FILING_STATEMENTS, "test")
    land(approved, ENTITY_OBSERVATIONS, [obs_rows()])
    land(approved, FUNDAMENTALS, [fund_row()])
    land(approved, FILING_STATEMENTS, [statement_row()])
    assert publish_pending(approved, now=NOW).exit_code == 0
    with Store.open(approved) as s:
        rows = s.assemble(
            Recipe(
                "placed",
                "1.0.0",
                {"f": "fundamentals", "p": "filing_statements"},
                "SELECT f.original_tag, p.statement FROM f JOIN p "
                "ON p.filing_ref = f.filing_ref "
                "AND p.tag = split_part(f.original_tag, ':', 2)",
            )
        ).to_pylist()
    assert rows == [{"original_tag": "us-gaap:LongTermDebt", "statement": "BS"}]
