import dataclasses
import json
from datetime import date

import pyarrow as pa
from conftest import NOW, fund_row, obs_rows, table, ts

from myopendata.contracts.base import Column
from myopendata.contracts.standard import ENTITY_OBSERVATIONS, FUNDAMENTALS
from myopendata.lake import connect
from myopendata.publisher import approve_contract, publish_pending
from myopendata.writer import write_batch


def land(cfg, contract, rows, run_id="r1"):
    return write_batch(
        contract,
        table(contract, rows),
        landing_root=cfg.landing,
        run_id=run_id,
        producer="test",
        now=NOW,
    )


def q(cfg, sql):
    con = connect(cfg, read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def test_publishes_entities_then_facts_in_one_run(approved):
    land(approved, FUNDAMENTALS, [fund_row()])  # landed first on purpose
    land(approved, ENTITY_OBSERVATIONS, [obs_rows()])
    rep = publish_pending(approved, now=NOW)
    assert rep.exit_code == 0, rep.to_json()
    assert [p["contract"] for p in rep.published] == ["entity_observations_v1", "fundamentals_v1"]
    (eid,) = q(approved, "SELECT DISTINCT entity_id FROM lake.facts.fundamentals_v1")[0]
    assert eid.startswith("ent_")
    links = q(approved, "SELECT scheme, value FROM lake.ref.entity_identifiers ORDER BY 1")
    assert links == [("cik", "0000789019"), ("lei", "INR2EJN1ERAN0W5ZP974")]
    assert list((approved.landing / "_published" / "fundamentals_v1").iterdir())
    snaps = q(
        approved, "SELECT commit_message FROM lake.snapshots() WHERE commit_message IS NOT NULL"
    )
    assert len(snaps) == 2


def test_files_are_parquet_under_data_root(approved):
    land(approved, ENTITY_OBSERVATIONS, [obs_rows()])
    publish_pending(approved, now=NOW)
    assert list(approved.lake_data.rglob("*.parquet"))


def test_replayed_batch_is_a_noop(approved):
    land(approved, ENTITY_OBSERVATIONS, [obs_rows()])
    publish_pending(approved, now=NOW)
    land(approved, ENTITY_OBSERVATIONS, [obs_rows()], run_id="r2")
    rep = publish_pending(approved, now=NOW)
    assert rep.exit_code == 0 and len(rep.skipped_duplicates) == 1 and not rep.published
    assert q(approved, "SELECT count(*) FROM lake.facts.entity_observations_v1") == [(1,)]


def test_unknown_entity_quarantines_batch(approved):
    land(approved, FUNDAMENTALS, [fund_row(entity_value="0000000001")])
    rep = publish_pending(approved, now=NOW)
    assert rep.exit_code == 3
    assert rep.quarantined[0]["reasons"][0].startswith("unknown_entity")
    qdir = next((approved.landing / "_quarantine" / "fundamentals_v1").iterdir())
    assert json.loads((qdir / "reason.json").read_text())["reasons"]


def test_unapproved_contract_quarantines(cfg):
    land(cfg, ENTITY_OBSERVATIONS, [obs_rows()])
    rep = publish_pending(cfg, now=NOW)
    assert rep.quarantined[0]["reasons"] == ["contract_not_approved"]


def test_entity_conflict_quarantines(approved):
    land(
        approved,
        ENTITY_OBSERVATIONS,
        [
            obs_rows(),
            obs_rows(
                subject_value="0000320193",
                id_scheme="lei",
                id_value="HWUPKR0MPOU8FGXBT394",
                name="APPLE INC",
            ),
        ],
    )
    publish_pending(approved, now=NOW)
    # A third subject claims both companies' LEIs: must not merge two entities silently.
    land(
        approved,
        ENTITY_OBSERVATIONS,
        [
            obs_rows(
                subject_scheme="isin", subject_value="US5949181045", id_value="INR2EJN1ERAN0W5ZP974"
            ),
            obs_rows(
                subject_scheme="isin", subject_value="US5949181045", id_value="HWUPKR0MPOU8FGXBT394"
            ),
        ],
    )
    rep = publish_pending(approved, now=NOW)
    assert rep.quarantined and rep.quarantined[0]["reasons"][0].startswith("entity_conflict")


def test_ticker_never_merges_entities(approved):
    land(
        approved,
        ENTITY_OBSERVATIONS,
        [
            obs_rows(id_scheme="ticker", id_value="X"),
            obs_rows(subject_value="0000320193", id_scheme="ticker", id_value="X", name="OTHER"),
        ],
    )
    rep = publish_pending(approved, now=NOW)
    assert rep.exit_code == 0
    assert q(
        approved, "SELECT count(DISTINCT entity_id) FROM lake.facts.entity_observations_v1"
    ) == [(2,)]


def test_name_change_is_scd2(approved):
    land(
        approved, ENTITY_OBSERVATIONS, [obs_rows(name="FACEBOOK INC", valid_date=date(2021, 6, 30))]
    )
    land(
        approved,
        ENTITY_OBSERVATIONS,
        [
            obs_rows(
                name="META PLATFORMS INC",
                valid_date=date(2022, 6, 30),
                known_at=ts("2022-07-30T00:00:00"),
            )
        ],
        run_id="r2",
    )
    publish_pending(approved, now=NOW)
    rows = q(
        approved, "SELECT name, valid_from, valid_to FROM lake.ref.entity_attributes ORDER BY 2"
    )
    assert rows == [
        ("FACEBOOK INC", date(2021, 6, 30), date(2022, 6, 30)),
        ("META PLATFORMS INC", date(2022, 6, 30), None),
    ]


def test_tampered_batch_is_reaudited(approved):
    ref = land(approved, ENTITY_OBSERVATIONS, [obs_rows()])
    t = table(ENTITY_OBSERVATIONS, [obs_rows(known_at_kind="guessed")])
    import pyarrow.parquet as pq

    pq.write_table(t, ref.path / "data.parquet")  # bypasses the writer
    rep = publish_pending(approved, now=NOW)
    assert "value_not_allowed:known_at_kind:1" in rep.quarantined[0]["reasons"]


def test_changed_contract_definition_quarantines(approved):
    changed = dataclasses.replace(
        FUNDAMENTALS, natural_key=FUNDAMENTALS.natural_key[:-1] + ("source_code", "unit")
    )
    land(approved, ENTITY_OBSERVATIONS, [obs_rows()])
    write_batch(
        changed,
        table(changed, [fund_row()]),
        landing_root=approved.landing,
        run_id="r",
        producer="t",
        now=NOW,
    )
    rep = publish_pending(approved, now=NOW)
    assert rep.quarantined[0]["reasons"] == ["batch_written_with_other_contract_definition"]


def test_additive_minor_is_auto_approved_and_column_added(approved):
    land(approved, ENTITY_OBSERVATIONS, [obs_rows()])
    land(approved, FUNDAMENTALS, [fund_row()])
    publish_pending(approved, now=NOW)
    minor = dataclasses.replace(
        FUNDAMENTALS,
        version="1.1.0",
        columns=FUNDAMENTALS.columns + (Column("segment", pa.string()),),
    )
    write_batch(
        minor,
        table(minor, [fund_row(concept="Revenue", segment="Cloud")]),
        landing_root=approved.landing,
        run_id="r2",
        producer="t",
        now=NOW,
    )
    rep = publish_pending(
        approved,
        now=NOW,
        contracts={("fundamentals", 1): minor, ("entity_observations", 1): ENTITY_OBSERVATIONS},
    )
    assert rep.exit_code == 0, rep.to_json()
    assert q(approved, "SELECT approved_by FROM lake.ref.contracts WHERE version = '1.1.0'") == [
        ("auto-minor",)
    ]
    assert q(approved, "SELECT concept, segment FROM lake.facts.fundamentals_v1 ORDER BY 1") == [
        ("LongTermDebt", None),
        ("Revenue", "Cloud"),
    ]


def test_breaking_change_in_same_major_quarantines(approved):
    breaking = dataclasses.replace(
        FUNDAMENTALS, version="1.1.0", natural_key=FUNDAMENTALS.natural_key + ("unit",)
    )
    land(approved, ENTITY_OBSERVATIONS, [obs_rows()])
    write_batch(
        breaking,
        table(breaking, [fund_row()]),
        landing_root=approved.landing,
        run_id="r",
        producer="t",
        now=NOW,
    )
    rep = publish_pending(
        approved,
        now=NOW,
        contracts={("fundamentals", 1): breaking, ("entity_observations", 1): ENTITY_OBSERVATIONS},
    )
    assert rep.quarantined[0]["reasons"] == ["contract_fingerprint_mismatch"]


def test_backfilled_history_lands_in_the_right_interval(approved):
    land(
        approved,
        ENTITY_OBSERVATIONS,
        [obs_rows(name="META PLATFORMS INC", valid_date=date(2022, 6, 30))],
    )
    publish_pending(approved, now=NOW)
    land(
        approved,
        ENTITY_OBSERVATIONS,
        [
            obs_rows(
                name="FACEBOOK INC",
                valid_date=date(2021, 6, 30),
                known_at=ts("2021-07-30T00:00:00"),
            )
        ],
        run_id="r2",
    )
    publish_pending(approved, now=NOW)
    rows = q(
        approved, "SELECT name, valid_from, valid_to FROM lake.ref.entity_attributes ORDER BY 2"
    )
    assert rows == [
        ("FACEBOOK INC", date(2021, 6, 30), date(2022, 6, 30)),
        ("META PLATFORMS INC", date(2022, 6, 30), None),
    ]


def test_fatal_when_catalogue_unreachable(tmp_path):
    from myopendata.config import StoreConfig

    bad = StoreConfig(data_root=tmp_path, catalog="postgres:host=/nonexistent dbname=x")
    rep = publish_pending(bad, now=NOW)
    assert rep.exit_code == 1 and rep.fatal
    assert list((tmp_path / "reports" / "publisher").glob("*.json"))


def test_approve_records_fingerprint(cfg):
    approve_contract(cfg, FUNDAMENTALS, "marc")
    assert q(cfg, "SELECT fingerprint, approved_by FROM lake.ref.contracts") == [
        (FUNDAMENTALS.fingerprint(), "marc")
    ]
