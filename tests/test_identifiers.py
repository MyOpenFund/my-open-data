"""Names interpolated into SQL are quoted identifiers: NAME_RE admits SQL keywords."""

import dataclasses
import json
from datetime import UTC, date, datetime

import duckdb
import pyarrow as pa
import pytest
from conftest import NOW, fund_row, obs_rows, table, ts
from test_publisher import land, q

from myopendata.contracts.base import Column, Contract, ContractError
from myopendata.contracts.rules import NAME_RE, conform, validate_contract
from myopendata.contracts.standard import ENTITY_OBSERVATIONS, FUNDAMENTALS
from myopendata.contracts.standard._common import BITEMPORAL_COLUMNS
from myopendata.lake import connect, qualified, quote_ident
from myopendata.publisher import approve_contract, publish_pending
from myopendata.reader import Recipe, Store
from myopendata.writer import write_batch

KEYWORDS = ("order", "group", "select", "from", "window", "pivot", "default", "union")

# A fact contract whose every own column is a SQL keyword: `order`/`group` in the natural key,
# `group` the partition column, `select` an ordinary column.
KW = Contract(
    name="keywords",
    version="1.0.0",
    kind="fact",
    owner="test",
    licence="test",
    columns=(
        Column("order", pa.string(), nullable=False),
        Column("group", pa.string(), nullable=False),
        Column("select", pa.int64()),
    )
    + BITEMPORAL_COLUMNS,
    natural_key=("order", "group", "valid_date", "source_code"),
    partition_by=("group",),
)
# Its additive MINOR adds a column named `from` (ALTER TABLE ... ADD COLUMN).
KW_MINOR = dataclasses.replace(
    KW, version="1.1.0", columns=KW.columns + (Column("from", pa.int64()),)
)


def kw_row(**over):
    base = {
        "order": "o1",
        "group": "g1",
        "select": 1,
        "valid_date": date(2025, 1, 1),
        "known_at": ts("2025-02-01T00:00:00"),
        "known_at_kind": "published",
        "source_code": "src",
        "source_doc_id": None,
    }
    base.update(over)
    return base


def _land(cfg, contract, rows, run_id):
    return write_batch(
        contract,
        table(contract, rows),
        landing_root=cfg.landing,
        run_id=run_id,
        producer="test",
        now=NOW,
    )


def _publish(cfg, contract):
    return publish_pending(cfg, now=NOW, contracts={(contract.name, contract.major): contract})


@pytest.mark.parametrize("name", KEYWORDS)
def test_name_re_admits_sql_keywords(name):
    """The premise: validation lets keywords through, so every interpolation must quote."""
    assert NAME_RE.fullmatch(name)
    assert duckdb.connect().execute(
        "SELECT count(*) FROM duckdb_keywords() WHERE keyword_name = ?", [name]
    ).fetchall() == [(1,)]


def test_keyword_contract_is_valid():
    validate_contract(KW)
    validate_contract(KW_MINOR)


@pytest.fixture
def kw_store(cfg):
    approve_contract(cfg, KW, "test")
    _land(cfg, KW, [kw_row(), kw_row(order="o2", select=2)], "r1")
    rep = _publish(cfg, KW)
    assert rep.exit_code == 0, rep.to_json()
    return cfg


def test_keyword_columns_publish_partitioned(kw_store):
    assert q(
        kw_store, 'SELECT "order", "group", "select" FROM lake.facts.keywords_v1 ORDER BY 1'
    ) == [
        ("o1", "g1", 1),
        ("o2", "g1", 2),
    ]
    files = [p.as_posix() for p in (kw_store.lake_data / "facts").rglob("*.parquet")]
    assert files and all("group=g1" in f for f in files), files


def test_keyword_columns_republish_is_skipped_and_conflict_is_caught(kw_store):
    # same rows again: the row-level idempotence query compares keyword columns
    _land(kw_store, KW, [kw_row(), kw_row(order="o2", select=2), kw_row(order="o3")], "r2")
    rep = _publish(kw_store, KW)
    assert rep.exit_code == 0, rep.to_json()
    assert rep.skipped_rows == 2 and rep.published[0]["rows"] == 1
    _land(kw_store, KW, [kw_row(select=99)], "r3")
    rep = _publish(kw_store, KW)
    assert rep.quarantined[0]["reasons"] == ["conflicting_republish:1"]


def test_keyword_column_added_by_a_minor(kw_store):
    _land(kw_store, KW_MINOR, [kw_row(order="o4", **{"from": 7})], "r2")
    rep = _publish(kw_store, KW_MINOR)
    assert rep.exit_code == 0, rep.to_json()
    assert q(kw_store, 'SELECT "order", "from" FROM lake.facts.keywords_v1 ORDER BY 1') == [
        ("o1", None),
        ("o2", None),
        ("o4", 7),
    ]


def test_reader_handles_keyword_columns(kw_store):
    with Store.open(kw_store) as s:
        assert s.read_asof("keywords", NOW).num_rows == 2
        assert s.read_asof("keywords", NOW, vintages=True).num_rows == 2
        # `order` is in the natural key (pushed inside the window), `select` is not
        t = s.read_asof("keywords", NOW, filters={"order": "o2"}, columns=["order", "select"])
        assert t.to_pylist() == [{"order": "o2", "select": 2}]
        t = s.read_asof("keywords", NOW, filters={"select": [1]}, columns=["group"], limit=5)
        assert t.to_pylist() == [{"group": "g1"}]
        # free SQL is the caller's: it quotes keyword columns itself
        assert s.read_asof("keywords", NOW, where="\"order\" = 'o1'").num_rows == 1
        assert s.describe("keywords")["natural_key"] == [
            "order",
            "group",
            "valid_date",
            "source_code",
        ]


def test_recipe_view_named_after_a_keyword(kw_store):
    with Store.open(kw_store) as s:
        r = Recipe("kw", "1.0.0", {"select": "keywords"}, 'SELECT sum("select") AS s FROM "select"')
        assert s.assemble(r, NOW).to_pylist() == [{"s": 3}]


# --- the quoting helper ---------------------------------------------------------------------


@pytest.mark.parametrize(
    ("name", "quoted"),
    [
        ("order", '"order"'),
        ("fundamentals_v3", '"fundamentals_v3"'),
        ('a"b', '"a""b"'),
        ('"', '""""'),
        ('x"; DROP TABLE t; --', '"x""; DROP TABLE t; --"'),
        ("Mixed Case", '"Mixed Case"'),
    ],
)
def test_quote_ident_escapes_embedded_quotes(name, quoted):
    assert quote_ident(name) == quoted


@pytest.mark.parametrize("bad", ["", "a\x00b", None, 3, b"order"])
def test_quote_ident_refuses_what_cannot_be_an_identifier(bad):
    with pytest.raises(ValueError):
        quote_ident(bad)


def test_qualified_quotes_every_part():
    assert qualified("lake", "facts", "order") == '"lake"."facts"."order"'


HOSTILE = [
    'a"b',
    'x" FROM t; DROP TABLE t; --',
    "known_at, (SELECT 42) AS injected",
    "x'y",
    "a.b",
    "tab\tname",
    "line\nbreak",
    "Order",
]


@pytest.mark.parametrize("name", HOSTILE)
def test_hostile_identifier_round_trips_through_duckdb(name):
    """Whatever the name, the quoted form names exactly that column and nothing executes."""
    con = duckdb.connect()
    con.execute("CREATE TABLE t (k INTEGER)")
    con.execute(f"CREATE TABLE h ({quote_ident(name)} INTEGER)")
    con.execute(f"INSERT INTO h ({quote_ident(name)}) VALUES (1)")
    assert [r[0] for r in con.execute("DESCRIBE h").fetchall()] == [name]
    assert con.execute(f"SELECT {quote_ident(name)} FROM h").fetchall() == [(1,)]
    assert con.execute(
        "SELECT count(*) FROM duckdb_tables() WHERE table_name = 't'"
    ).fetchall() == [(1,)]


@pytest.mark.parametrize("name", [*HOSTILE, "order\n", "1abc", "a", "a" * 64, "é_col"])
def test_name_re_still_refuses_hostile_names_first(name):
    bad = dataclasses.replace(KW, columns=KW.columns + (Column(name, pa.int64()),))
    with pytest.raises(ContractError):
        validate_contract(bad)
    with pytest.raises(ContractError):
        validate_contract(dataclasses.replace(KW, name=name))


def _hostile_lake(cfg, column):
    """A catalogue whose approved definition and published table carry a column name that never
    went through NAME_RE (a tampered catalogue): the reader must still not run it as SQL."""
    approve_contract(cfg, KW, "test")
    _land(cfg, KW, [kw_row()], "r1")
    assert _publish(cfg, KW).exit_code == 0
    con = connect(cfg, read_only=False)
    try:
        con.execute(f"ALTER TABLE lake.facts.keywords_v1 ADD COLUMN {quote_ident(column)} INTEGER")
        con.execute(f"UPDATE lake.facts.keywords_v1 SET {quote_ident(column)} = 5")
        definition = json.loads(
            con.execute("SELECT definition_json FROM lake.ref.contracts").fetchall()[0][0]
        )
        definition["columns"].append([column, "int32", True, []])
        con.execute(
            "INSERT INTO lake.ref.contracts VALUES ('keywords', 1, '1.9.0', 'x', ?, 't', ?)",
            [json.dumps(definition), datetime.now(UTC)],
        )
    finally:
        con.close()


@pytest.mark.parametrize(
    "column",
    [
        "order, (SELECT count(*) FROM lake.ref.contracts) AS injected",
        'x") AS z, (SELECT 1) AS injected --',
        "limit",
    ],
)
def test_tampered_catalogue_column_is_data_not_sql(cfg, column):
    _hostile_lake(cfg, column)
    with Store.open(cfg) as s:
        t = s.read_asof("keywords", NOW, columns=[column])
        assert t.column_names == [column] and t.to_pylist() == [{column: 5}]
        t = s.read_asof("keywords", NOW, filters={column: 5}, columns=["order"])
        assert t.to_pylist() == [{"order": "o1"}]


# --- compatibility: a lake written by the unquoted SQL of earlier releases -----------------------


def test_lake_created_with_unquoted_sql_keeps_working(cfg):
    """Earlier releases created and filled fact tables with unquoted names. Quoting must address
    the same tables and columns: nothing stored is renamed, and the old lake reads and grows."""
    approve_contract(cfg, ENTITY_OBSERVATIONS, "test")
    approve_contract(cfg, FUNDAMENTALS, "test")
    land(cfg, ENTITY_OBSERVATIONS, [obs_rows()])
    assert publish_pending(cfg, now=NOW).exit_code == 0
    # the fact table as the previous publisher made it: unquoted CREATE, PARTITIONED BY, INSERT
    rows = conform(FUNDAMENTALS, table(FUNDAMENTALS, [fund_row()]), now=NOW)
    eid = q(cfg, "SELECT entity_id FROM lake.ref.entity_identifiers WHERE scheme = 'cik'")[0][0]
    out = rows.add_column(0, "entity_id", pa.array([eid], pa.string()))
    out = out.append_column("batch_id", pa.array(["0" * 32], pa.string()))
    con = connect(cfg, read_only=False)
    try:
        con.register("_sample", out)
        con.execute("CREATE TABLE lake.facts.fundamentals_v3 AS SELECT * FROM _sample LIMIT 0")
        con.execute("ALTER TABLE lake.facts.fundamentals_v3 SET PARTITIONED BY (source_code)")
        cols = ", ".join(out.column_names)
        con.execute(f"INSERT INTO lake.facts.fundamentals_v3 ({cols}) SELECT {cols} FROM _sample")
        con.execute(
            "INSERT INTO lake.ref.batches (batch_id, contract, major, version, producer, run_id, "
            "rows, content_sha256, published_at, rows_skipped) VALUES "
            "(?, 'fundamentals', 3, '3.1.0', 'old', 'r0', 1, 'h', ?, 0)",
            ["0" * 32, datetime(2025, 8, 1, tzinfo=UTC)],
        )
    finally:
        con.close()
    before = q(
        cfg,
        "SELECT column_name FROM duckdb_columns() WHERE table_name = 'fundamentals_v3' "
        "ORDER BY column_index",
    )
    # the new publisher finds the old table, skips the already published row, appends a new one
    land(cfg, FUNDAMENTALS, [fund_row(), fund_row(original_tag="us-gaap:Revenues")], run_id="r2")
    rep = publish_pending(cfg, now=NOW)
    assert rep.exit_code == 0, rep.to_json()
    assert rep.skipped_rows == 1 and rep.published[0]["rows"] == 1
    after = q(
        cfg,
        "SELECT column_name FROM duckdb_columns() WHERE table_name = 'fundamentals_v3' "
        "ORDER BY column_index",
    )
    assert after == before
    assert q(cfg, "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake'") == [(6,)]
    with Store.open(cfg) as s:
        t = s.read_asof("fundamentals", NOW, filters={"entity_value": "0000789019"})
        assert sorted(t["original_tag"].to_pylist()) == ["us-gaap:LongTermDebt", "us-gaap:Revenues"]
