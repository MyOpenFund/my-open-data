import dataclasses
import json
import shutil
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq
from conftest import NOW, fund_row, obs_rows, table, ts

from myopendata.contracts.base import Column
from myopendata.contracts.standard import ENTITY_OBSERVATIONS, FUNDAMENTALS
from myopendata.lake import connect
from myopendata.publisher import approve_contract, publish_pending
from myopendata.writer import write_batch
from myopendata.writer.batch import file_sha256


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


def test_tampered_file_is_caught_by_its_checksum(approved):
    ref = land(approved, ENTITY_OBSERVATIONS, [obs_rows()])
    t = table(ENTITY_OBSERVATIONS, [obs_rows(name="EVIL CORP")])
    pq.write_table(t, ref.path / "data.parquet")  # bypasses the writer
    rep = publish_pending(approved, now=NOW)
    assert rep.quarantined[0]["reasons"] == ["manifest_mismatch:file_sha256"]


def _forge(ref, rows, contract=ENTITY_OBSERVATIONS, **manifest):
    """Rewrite a landed batch as a forger would: new data, matching file checksum."""
    pq.write_table(table(contract, rows), ref.path / "data.parquet")
    m = json.loads((ref.path / "batch.json").read_text())
    m["files"]["data.parquet"]["sha256"] = file_sha256(ref.path / "data.parquet")
    m.update(manifest)
    (ref.path / "batch.json").write_text(json.dumps(m))


def test_forged_batch_is_reaudited_against_the_contract(approved):
    ref = land(approved, ENTITY_OBSERVATIONS, [obs_rows()])
    _forge(ref, [obs_rows(known_at_kind="guessed")])
    rep = publish_pending(approved, now=NOW)
    assert "value_not_allowed:known_at_kind:1" in rep.quarantined[0]["reasons"]


def test_forged_rows_under_a_published_batch_id_are_not_skipped(approved):
    ref = land(approved, ENTITY_OBSERVATIONS, [obs_rows()])
    publish_pending(approved, now=NOW)
    first = next((approved.landing / "_published" / "entity_observations_v1").iterdir())
    ref2 = land(approved, ENTITY_OBSERVATIONS, [obs_rows(name="OTHER")], run_id="r2")
    _forge(ref2, [obs_rows(name="EVIL CORP")], batch_id=ref.batch_id)
    shutil.move(ref2.path, ref2.path.with_name(ref.batch_id))  # spoof the published id
    rep = publish_pending(approved, now=NOW)
    assert rep.quarantined[0]["reasons"] == ["batch_id_mismatch"] and not rep.skipped_duplicates
    assert json.loads((first / "batch.json").read_text())["run_id"] == "r1"  # audit copy intact


def test_path_traversal_in_batch_dir_or_manifest_is_harmless(approved):
    canary = approved.landing / "canary"
    canary.mkdir(parents=True)
    ref = land(approved, ENTITY_OBSERVATIONS, [obs_rows()])
    m = json.loads((ref.path / "batch.json").read_text())
    m["batch_id"] = "../../canary"
    (ref.path / "batch.json").write_text(json.dumps(m))
    rep = publish_pending(approved, now=NOW)
    assert canary.exists() and rep.quarantined[0]["reasons"] == ["batch_id_mismatch"]


def test_manifest_lies_about_rows_are_quarantined(approved):
    ref = land(approved, ENTITY_OBSERVATIONS, [obs_rows()])
    m = json.loads((ref.path / "batch.json").read_text())
    m["rows"] = 999
    (ref.path / "batch.json").write_text(json.dumps(m))
    rep = publish_pending(approved, now=NOW)
    assert rep.quarantined[0]["reasons"] == ["manifest_mismatch:rows_or_content"]


def test_one_malformed_batch_does_not_stop_the_others(approved):
    bad = approved.landing / "fundamentals_v1" / ("0" * 32)
    bad.mkdir(parents=True)
    (bad / "batch.json").write_text("{not json")
    junk = approved.landing / "fundamentals_v1" / "not-a-batch"
    junk.mkdir()
    land(approved, ENTITY_OBSERVATIONS, [obs_rows()])
    land(approved, FUNDAMENTALS, [fund_row()])
    rep = publish_pending(approved, now=NOW)
    assert rep.exit_code == 3 and len(rep.published) == 2
    assert sorted(r for q in rep.quarantined for r in q["reasons"]) == [
        "bad_batch_dir_name",
        "manifest_invalid",
    ]


def test_ticker_subject_never_merges_entities(approved):
    land(
        approved,
        ENTITY_OBSERVATIONS,
        [
            obs_rows(subject_scheme="ticker", subject_value="X", id_scheme="cik", id_value="1"),
            obs_rows(
                subject_scheme="ticker",
                subject_value="X",
                id_scheme="cik",
                id_value="2",
                valid_date=date(2025, 7, 1),
            ),
        ],
    )
    rep = publish_pending(approved, now=NOW)
    assert rep.quarantined[0]["reasons"][0].startswith("entity_conflict")


def test_ticker_only_subject_is_not_resolvable(approved):
    land(
        approved,
        ENTITY_OBSERVATIONS,
        [obs_rows(subject_scheme="ticker", subject_value="X", id_scheme="ticker", id_value="X")],
    )
    rep = publish_pending(approved, now=NOW)
    assert rep.quarantined[0]["reasons"][0].startswith("subject_not_resolvable")


def test_resolution_does_not_depend_on_row_order():
    from myopendata.publisher.entities import DEFAULT_RESOLVING_SCHEMES, resolve_observations

    rows = [
        obs_rows(
            subject_scheme="isin",
            subject_value="US5949181045",
            id_scheme="lei",
            id_value="INR2EJN1ERAN0W5ZP974",
        ),
        obs_rows(),  # cik subject carrying the same LEI
        obs_rows(
            subject_scheme="isin",
            subject_value="US5949181045",
            id_scheme="figi",
            id_value="BBG000BPH459",
        ),
    ]
    results = []
    for order in (rows, list(reversed(rows)), rows[1:] + rows[:1]):
        eids, links = resolve_observations(
            table(ENTITY_OBSERVATIONS, order), {}, DEFAULT_RESOLVING_SCHEMES
        )
        assert len(set(eids)) == 1
        results.append((eids[0], sorted(lk[:3] for lk in links)))
    assert results[0] == results[1] == results[2]


def test_two_ciks_sharing_one_lei_is_a_conflict_not_a_merge(approved):
    land(
        approved, ENTITY_OBSERVATIONS, [obs_rows(), obs_rows(subject_value="0000320193")]
    )  # same LEI claimed by another CIK
    rep = publish_pending(approved, now=NOW)
    assert rep.quarantined[0]["reasons"][0].startswith("entity_conflict")


def test_second_publisher_is_refused_while_one_runs(approved):
    import fcntl

    approved.landing.mkdir(parents=True, exist_ok=True)
    with open(approved.landing / ".publisher.lock", "w") as held:
        fcntl.flock(held, fcntl.LOCK_EX)
        rep = publish_pending(approved, now=NOW)
    assert rep.exit_code == 1 and "another publisher" in rep.fatal


def test_partitioning_is_applied(approved):
    land(approved, ENTITY_OBSERVATIONS, [obs_rows()])
    land(approved, FUNDAMENTALS, [fund_row()])
    publish_pending(approved, now=NOW)
    files = [p.as_posix() for p in approved.lake_data.rglob("*.parquet")]
    assert any("source_code=sec" in f for f in files), files


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
