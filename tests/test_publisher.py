import dataclasses
import json
import shutil
from datetime import date, timedelta
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from conftest import NOW, fund_row, obs_rows, table, ts

from myopendata.contracts.base import Column, Contract
from myopendata.contracts.standard import ENTITY_OBSERVATIONS, FUNDAMENTALS
from myopendata.contracts.standard._common import BITEMPORAL_COLUMNS
from myopendata.lake import connect
from myopendata.publisher import ApprovalRefused, approve_contract, publish_pending
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
    assert [p["contract"] for p in rep.published] == ["entity_observations_v1", "fundamentals_v3"]
    (eid,) = q(approved, "SELECT DISTINCT entity_id FROM lake.facts.fundamentals_v3")[0]
    assert eid.startswith("ent_")
    links = q(approved, "SELECT scheme, value FROM lake.ref.entity_identifiers ORDER BY 1")
    assert links == [("cik", "0000789019"), ("lei", "INR2EJN1ERAN0W5ZP974")]
    assert list((approved.landing / "_published" / "fundamentals_v3").iterdir())
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
    qdir = next((approved.landing / "_quarantine" / "fundamentals_v3").iterdir())
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
    bad = approved.landing / "fundamentals_v3" / ("0" * 32)
    bad.mkdir(parents=True)
    (bad / "batch.json").write_text("{not json")
    junk = approved.landing / "fundamentals_v3" / "not-a-batch"
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
        version="3.2.0",
        columns=FUNDAMENTALS.columns + (Column("segment", pa.string()),),
    )
    write_batch(
        minor,
        table(minor, [fund_row(original_tag="us-gaap:Revenue", segment="Cloud")]),
        landing_root=approved.landing,
        run_id="r2",
        producer="t",
        now=NOW,
    )
    rep = publish_pending(
        approved,
        now=NOW,
        contracts={("fundamentals", 3): minor, ("entity_observations", 1): ENTITY_OBSERVATIONS},
    )
    assert rep.exit_code == 0, rep.to_json()
    assert q(approved, "SELECT approved_by FROM lake.ref.contracts WHERE version = '3.2.0'") == [
        ("auto-minor",)
    ]
    assert q(
        approved, "SELECT original_tag, segment FROM lake.facts.fundamentals_v3 ORDER BY 1"
    ) == [
        ("us-gaap:LongTermDebt", None),
        ("us-gaap:Revenue", "Cloud"),
    ]


def _with_allowed(contract, column, allowed, version="3.2.0", extra=()):
    cols = tuple(
        dataclasses.replace(c, allowed=allowed) if c.name == column else c for c in contract.columns
    )
    return dataclasses.replace(contract, version=version, columns=cols + tuple(extra))


def _freq(contract=FUNDAMENTALS):
    return next(c for c in contract.columns if c.name == "frequency").allowed


def _publish_under(approved, minor, **row):
    land(approved, ENTITY_OBSERVATIONS, [obs_rows()])
    write_batch(
        minor,
        table(minor, [fund_row(**row)]),
        landing_root=approved.landing,
        run_id="r",
        producer="t",
        now=NOW,
    )
    return publish_pending(
        approved,
        now=NOW,
        contracts={("fundamentals", 3): minor, ("entity_observations", 1): ENTITY_OBSERVATIONS},
    )


def test_appended_vocabulary_value_is_auto_approved_and_publishes(approved):
    minor = _with_allowed(FUNDAMENTALS, "frequency", _freq() + ("ytd12",))
    rep = _publish_under(approved, minor, frequency="ytd12", period_start=date(2025, 1, 1))
    assert rep.exit_code == 0, rep.to_json()
    assert q(approved, "SELECT approved_by FROM lake.ref.contracts WHERE version = '3.2.0'") == [
        ("auto-minor",)
    ]
    assert q(approved, "SELECT frequency FROM lake.facts.fundamentals_v3") == [("ytd12",)]


def test_appended_vocabulary_value_and_appended_column_in_one_minor(approved):
    minor = _with_allowed(
        FUNDAMENTALS,
        "frequency",
        _freq() + ("ytd12",),
        extra=(Column("segment", pa.string()),),
    )
    rep = _publish_under(
        approved, minor, frequency="ytd12", period_start=date(2025, 1, 1), segment="Cloud"
    )
    assert rep.exit_code == 0, rep.to_json()
    assert q(approved, "SELECT approved_by FROM lake.ref.contracts WHERE version = '3.2.0'") == [
        ("auto-minor",)
    ]


def _bad_vocabularies():
    old = _freq()
    return {
        "reordered": tuple(reversed(old + ("ytd12",))),
        "removed_and_added": old[1:] + ("ytd12",),
        "renamed": (old[0] + "_x",) + old[1:],
        "removed": old[:1] + old[2:],
        "tail_shrunk": old[:-1],
        "pure_reorder": tuple(reversed(old)),
    }


@pytest.mark.parametrize(
    "kind",
    ["reordered", "removed_and_added", "renamed", "removed", "tail_shrunk", "pure_reorder"],
)
def test_non_additive_vocabulary_change_is_refused(approved, kind):
    bad = _with_allowed(FUNDAMENTALS, "frequency", _bad_vocabularies()[kind])
    with pytest.raises(ApprovalRefused):
        approve_contract(approved, bad, "x")
    rep = _publish_under(approved, bad)
    assert rep.quarantined[0]["reasons"] == ["contract_fingerprint_mismatch"]


def test_widened_vocabulary_under_a_lower_version_is_refused(approved):
    wide = _with_allowed(FUNDAMENTALS, "frequency", _freq() + ("ytd12",), version="3.0.5")
    with pytest.raises(ApprovalRefused):
        approve_contract(approved, wide, "x")
    rep = _publish_under(approved, wide)
    assert rep.quarantined[0]["reasons"] == ["contract_fingerprint_mismatch"]


def test_vocabulary_cannot_be_added_to_an_open_column_in_a_minor(approved):
    bad = _with_allowed(FUNDAMENTALS, "unit", ("USD", "EUR"))
    with pytest.raises(ApprovalRefused):
        approve_contract(approved, bad, "x")
    rep = _publish_under(approved, bad)
    assert rep.quarantined[0]["reasons"] == ["contract_fingerprint_mismatch"]


def test_appended_vocabulary_value_without_a_higher_version_is_refused(approved):
    same = _with_allowed(FUNDAMENTALS, "frequency", _freq() + ("ytd12",), version="3.1.0")
    with pytest.raises(ApprovalRefused):
        approve_contract(approved, same, "x")
    rep = _publish_under(approved, same, frequency="ytd12", period_start=date(2025, 1, 1))
    assert rep.quarantined[0]["reasons"] == ["contract_fingerprint_mismatch"]


def test_breaking_change_in_same_major_quarantines(approved):
    breaking = dataclasses.replace(
        FUNDAMENTALS, version="3.2.0", natural_key=FUNDAMENTALS.natural_key + ("unit",)
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
        contracts={("fundamentals", 3): breaking, ("entity_observations", 1): ENTITY_OBSERVATIONS},
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
    approve_contract(cfg, FUNDAMENTALS, "reviewer")
    assert q(cfg, "SELECT fingerprint, approved_by FROM lake.ref.contracts") == [
        (FUNDAMENTALS.fingerprint(), "reviewer")
    ]


def test_two_subjects_cannot_attach_two_leis_to_one_entity():
    from myopendata.publisher.entities import (
        DEFAULT_RESOLVING_SCHEMES,
        EntityConflict,
        resolve_observations,
    )

    links = {("cik", "1"): "ent_e", ("isin", "X"): "ent_e"}
    rows = [
        obs_rows(subject_value="1", id_scheme="lei", id_value="LEI_ONE"),
        obs_rows(subject_scheme="isin", subject_value="X", id_scheme="lei", id_value="LEI_TWO"),
    ]
    import pytest

    with pytest.raises(EntityConflict, match="lei"):
        resolve_observations(table(ENTITY_OBSERVATIONS, rows), links, DEFAULT_RESOLVING_SCHEMES)


def test_conflicting_approval_is_refused_and_reapproval_is_a_noop(approved):
    import pytest

    from myopendata.publisher import ApprovalRefused

    assert approve_contract(approved, FUNDAMENTALS, "x") == "already_approved"
    floaty = dataclasses.replace(
        FUNDAMENTALS,
        columns=tuple(
            Column("value", pa.float64(), nullable=False) if c.name == "value" else c
            for c in FUNDAMENTALS.columns
        ),
    )
    with pytest.raises(ApprovalRefused):
        approve_contract(approved, floaty, "x")
    assert q(approved, "SELECT count(*) FROM lake.ref.contracts WHERE name = 'fundamentals'") == [
        (1,)
    ]


def test_infrastructure_error_on_one_batch_is_fatal_but_others_publish(approved, monkeypatch):
    import duckdb

    from myopendata.publisher import publish as pub

    land(approved, ENTITY_OBSERVATIONS, [obs_rows()])
    land(approved, FUNDAMENTALS, [fund_row()])
    real = pub._Run.commit

    def flaky(self, c, *a, **k):
        if c.name == "fundamentals":
            raise duckdb.IOException("disk gone")
        return real(self, c, *a, **k)

    monkeypatch.setattr(pub._Run, "commit", flaky)
    rep = publish_pending(approved, now=NOW)
    assert rep.exit_code == 1 and rep.errors[0]["error"].startswith("IOException")
    assert [p["contract"] for p in rep.published] == ["entity_observations_v1"]
    assert list((approved.landing / "fundamentals_v3").iterdir())  # left in place for a retry


def test_move_never_overwrites(tmp_path):
    from myopendata.publisher.publish import _move

    for content in ("first", "second"):
        src = tmp_path / "src"
        src.mkdir()
        (src / "f").write_text(content)
        _move(src, tmp_path, "_published", "t_v1", "b" * 32, run_id=content)
    kept = sorted(p.name for p in (tmp_path / "_published" / "t_v1").iterdir())
    assert kept == ["b" * 32, "b" * 32 + "-second"]
    assert (tmp_path / "_published" / "t_v1" / ("b" * 32) / "f").read_text() == "first"


def test_manifest_with_wrong_types_is_invalid(approved):
    ref = land(approved, ENTITY_OBSERVATIONS, [obs_rows()])
    m = json.loads((ref.path / "batch.json").read_text())
    m["producer"] = {"name": "x"}
    (ref.path / "batch.json").write_text(json.dumps(m))
    rep = publish_pending(approved, now=NOW)
    assert rep.quarantined[0]["reasons"] == ["manifest_invalid"]


def test_explicit_empty_policy_is_honoured(approved):
    land(approved, ENTITY_OBSERVATIONS, [obs_rows()])
    rep = publish_pending(approved, now=NOW, resolving_schemes=())
    assert rep.quarantined[0]["reasons"][0].startswith("subject_not_resolvable")


def test_old_catalogue_gains_rows_skipped_without_losing_rows(cfg):
    import duckdb

    cfg.lake_data.mkdir(parents=True)
    con = duckdb.connect()
    con.execute("INSTALL ducklake; LOAD ducklake")
    con.execute(
        f"ATTACH 'ducklake:{cfg.catalog}' AS lake (DATA_PATH '{cfg.lake_data.as_posix()}/')"
    )
    con.execute("CREATE SCHEMA lake.ref")
    con.execute(
        "CREATE TABLE lake.ref.batches (batch_id VARCHAR, contract VARCHAR, major INTEGER, "
        "version VARCHAR, producer VARCHAR, run_id VARCHAR, rows BIGINT, "
        "content_sha256 VARCHAR, published_at TIMESTAMPTZ)"
    )
    con.execute(
        "INSERT INTO lake.ref.batches VALUES ('old', 'x', 1, '1.0.0', 'p', 'r', 3, 'h', now())"
    )
    con.close()
    connect(cfg, read_only=False).close()
    assert q(cfg, "SELECT batch_id, rows, rows_skipped FROM lake.ref.batches") == [("old", 3, None)]
    snapshots = q(cfg, "SELECT count(*) FROM lake.snapshots()")
    connect(cfg, read_only=False).close()  # the migration ran once: reconnecting changes nothing
    assert q(cfg, "SELECT count(*) FROM lake.snapshots()") == snapshots
    assert q(cfg, "SELECT batch_id, rows, rows_skipped FROM lake.ref.batches") == [("old", 3, None)]


def test_reconnecting_adds_no_snapshot(cfg):
    connect(cfg, read_only=False).close()
    before = q(cfg, "SELECT count(*) FROM lake.snapshots()")
    connect(cfg, read_only=False).close()
    assert q(cfg, "SELECT count(*) FROM lake.snapshots()") == before


def test_ledger_records_rows_skipped_zero_for_a_new_batch(approved):
    land(approved, ENTITY_OBSERVATIONS, [obs_rows()])
    publish_pending(approved, now=NOW)
    assert q(approved, "SELECT rows, rows_skipped FROM lake.ref.batches") == [(1, 0)]


def _fund_files(cfg):
    return q(
        cfg,
        "SELECT count(*) FROM ducklake_list_files('lake', 'fundamentals_v3', schema => 'facts')",
    )


def test_partly_new_batch_publishes_only_new_rows(approved):
    land(approved, ENTITY_OBSERVATIONS, [obs_rows()])
    land(approved, FUNDAMENTALS, [fund_row()])
    publish_pending(approved, now=NOW)
    land(approved, FUNDAMENTALS, [fund_row(), fund_row(original_tag="us-gaap:Cash")], run_id="r2")
    rep = publish_pending(approved, now=NOW)
    assert rep.exit_code == 0, rep.to_json()
    assert rep.published[0]["rows"] == 1 and rep.published[0]["rows_skipped"] == 1
    assert rep.skipped_rows == 1
    assert q(approved, "SELECT original_tag FROM lake.facts.fundamentals_v3 ORDER BY 1") == [
        ("us-gaap:Cash",),
        ("us-gaap:LongTermDebt",),
    ]


def test_batch_with_no_new_row_gets_a_ledger_row_and_no_data(approved):
    land(approved, ENTITY_OBSERVATIONS, [obs_rows()])
    land(approved, FUNDAMENTALS, [fund_row()])
    land(approved, FUNDAMENTALS, [fund_row(original_tag="us-gaap:Cash")], run_id="r1b")
    publish_pending(approved, now=NOW)
    files = _fund_files(approved)
    union = land(
        approved, FUNDAMENTALS, [fund_row(), fund_row(original_tag="us-gaap:Cash")], run_id="r2"
    )
    rep = publish_pending(approved, now=NOW)
    assert rep.exit_code == 0 and rep.published == [
        {
            "batch_id": union.batch_id,
            "contract": "fundamentals_v3",
            "rows": 0,
            "rows_skipped": 2,
        }
    ]
    assert q(
        approved,
        f"SELECT rows, rows_skipped FROM lake.ref.batches WHERE batch_id = '{union.batch_id}'",
    ) == [(0, 2)]
    assert q(approved, "SELECT count(*) FROM lake.facts.fundamentals_v3") == [(2,)]
    assert _fund_files(approved) == files
    assert (approved.landing / "_published" / "fundamentals_v3" / union.batch_id).is_dir()
    # replaying that same batch again hits the batch_id ledger, not the anti-join
    land(approved, FUNDAMENTALS, [fund_row(), fund_row(original_tag="us-gaap:Cash")], run_id="r3")
    rep = publish_pending(approved, now=NOW)
    assert rep.skipped_duplicates == [union.batch_id] and not rep.published


def test_same_key_and_known_at_with_another_value_is_quarantined(approved):
    land(approved, ENTITY_OBSERVATIONS, [obs_rows()])
    land(approved, FUNDAMENTALS, [fund_row(value=Decimal(2))])
    publish_pending(approved, now=NOW)
    land(
        approved,
        FUNDAMENTALS,
        [fund_row(value=Decimal(1)), fund_row(original_tag="us-gaap:Cash")],
        run_id="r2",
    )
    rep = publish_pending(approved, now=NOW)
    assert rep.exit_code == 3
    assert rep.quarantined[0]["reasons"] == ["conflicting_republish:1"]
    assert q(approved, "SELECT original_tag, value FROM lake.facts.fundamentals_v3") == [
        ("us-gaap:LongTermDebt", Decimal(2))
    ]


def test_conflict_between_two_batches_of_one_run(approved):
    land(approved, ENTITY_OBSERVATIONS, [obs_rows()])
    ref2 = land(approved, FUNDAMENTALS, [fund_row(value=Decimal(2))])
    ref1 = land(approved, FUNDAMENTALS, [fund_row(value=Decimal(1))], run_id="r1b")
    rep = publish_pending(approved, now=NOW)
    assert [p["rows"] for p in rep.published if p["contract"] == "fundamentals_v3"] == [1]
    assert rep.exit_code == 3
    assert len(rep.quarantined) == 1
    assert rep.quarantined[0]["reasons"] == ["conflicting_republish:1"]
    (first,) = (p for p in rep.published if p["contract"] == "fundamentals_v3")
    value = {ref2.batch_id: Decimal(2), ref1.batch_id: Decimal(1)}
    assert first["batch_id"] in value
    (other,) = set(value) - {first["batch_id"]}
    assert rep.quarantined[0]["batch_id"] == other
    expected = value[first["batch_id"]]
    assert q(approved, "SELECT value FROM lake.facts.fundamentals_v3") == [(expected,)]


def test_a_null_column_compares_equal_to_null(approved):
    land(approved, ENTITY_OBSERVATIONS, [obs_rows()])
    land(approved, FUNDAMENTALS, [fund_row(source_doc_id=None)])
    publish_pending(approved, now=NOW)
    land(
        approved,
        FUNDAMENTALS,
        [fund_row(source_doc_id=None), fund_row(original_tag="us-gaap:Cash")],
        run_id="r2",
    )
    rep = publish_pending(approved, now=NOW)
    assert rep.exit_code == 0 and rep.published[0]["rows_skipped"] == 1


def test_null_on_one_side_and_a_value_on_the_other_is_a_conflict(approved):
    land(approved, ENTITY_OBSERVATIONS, [obs_rows()])
    land(approved, FUNDAMENTALS, [fund_row(source_doc_id=None)])
    publish_pending(approved, now=NOW)
    land(approved, FUNDAMENTALS, [fund_row(source_doc_id="doc-10k-2025")], run_id="r2")
    rep = publish_pending(approved, now=NOW)
    assert rep.exit_code == 3
    assert rep.quarantined[0]["reasons"] == ["conflicting_republish:1"]
    assert q(approved, "SELECT source_doc_id FROM lake.facts.fundamentals_v3") == [(None,)]


def test_rows_with_a_null_partition_value_are_still_matched(cfg):
    nullable_part = dataclasses.replace(FUNDAMENTALS, partition_by=("source_doc_id",))
    reg = {(c.name, c.major): c for c in (ENTITY_OBSERVATIONS, nullable_part)}
    approve_contract(cfg, ENTITY_OBSERVATIONS, "test")
    approve_contract(cfg, nullable_part, "test")
    land(cfg, ENTITY_OBSERVATIONS, [obs_rows()])
    land(cfg, nullable_part, [fund_row(source_doc_id=None, value=Decimal(2))])
    publish_pending(cfg, contracts=reg, now=NOW)
    land(
        cfg,
        nullable_part,
        [fund_row(source_doc_id=None, value=Decimal(2)), fund_row(original_tag="us-gaap:Cash")],
        run_id="r2",
    )
    rep = publish_pending(cfg, contracts=reg, now=NOW)
    assert rep.exit_code == 0, rep.to_json()
    assert rep.published[0]["rows"] == 1 and rep.published[0]["rows_skipped"] == 1
    assert q(cfg, "SELECT count(*) FROM lake.facts.fundamentals_v3") == [(2,)]
    land(cfg, nullable_part, [fund_row(source_doc_id=None, value=Decimal(1))], run_id="r3")
    rep = publish_pending(cfg, contracts=reg, now=NOW)
    assert rep.exit_code == 3
    assert rep.quarantined[0]["reasons"] == ["conflicting_republish:1"]


def test_identity_replay_keeps_attributes(approved):
    land(approved, ENTITY_OBSERVATIONS, [obs_rows()])
    publish_pending(approved, now=NOW)
    before = q(approved, "SELECT * FROM lake.ref.entity_attributes")
    land(
        approved,
        ENTITY_OBSERVATIONS,
        [obs_rows(), obs_rows(id_scheme="ticker", id_value="MSFT", name=None, country=None)],
        run_id="r2",
    )
    rep = publish_pending(approved, now=NOW)
    assert rep.exit_code == 0 and rep.published[0]["rows_skipped"] == 1
    assert q(approved, "SELECT * FROM lake.ref.entity_attributes") == before
    assert q(approved, "SELECT count(*) FROM lake.facts.entity_observations_v1") == [(2,)]


def test_other_entities_rows_never_count_as_known(approved):
    land(
        approved,
        ENTITY_OBSERVATIONS,
        [obs_rows(), obs_rows(subject_value="0000320193", id_value="HWUPKR0MPOU8FGXBT394")],
    )
    land(approved, FUNDAMENTALS, [fund_row()])
    publish_pending(approved, now=NOW)
    land(approved, FUNDAMENTALS, [fund_row(entity_value="0000320193")], run_id="r2")
    rep = publish_pending(approved, now=NOW)
    assert rep.published[0]["rows"] == 1 and rep.published[0]["rows_skipped"] == 0


SEGMENTED = dataclasses.replace(
    FUNDAMENTALS,
    version="3.2.0",
    columns=FUNDAMENTALS.columns + (Column("segment", pa.string()),),
)
SEGMENTED_REG = {("fundamentals", 3): SEGMENTED, ("entity_observations", 1): ENTITY_OBSERVATIONS}


def test_full_history_reemitted_under_an_additive_minor_is_never_a_conflict(approved):
    land(approved, ENTITY_OBSERVATIONS, [obs_rows()])
    land(approved, FUNDAMENTALS, [fund_row()])
    publish_pending(approved, now=NOW)
    history = [fund_row(segment="Cloud")]
    # run 2: the producer moves to 2.1.0 and re-emits its history with the new column filled
    land(approved, SEGMENTED, history, run_id="r2")
    rep = publish_pending(approved, now=NOW, contracts=SEGMENTED_REG)
    assert rep.exit_code == 0, rep.to_json()
    assert [(p["rows"], p["rows_skipped"]) for p in rep.published] == [(0, 1)]
    # run 3: one new fact, the table gains the column (old rows hold NULL in it)
    history.append(fund_row(original_tag="us-gaap:Cash", segment="Cloud"))
    land(approved, SEGMENTED, history, run_id="r3")
    rep = publish_pending(approved, now=NOW, contracts=SEGMENTED_REG)
    assert rep.exit_code == 0, rep.to_json()
    assert [(p["rows"], p["rows_skipped"]) for p in rep.published] == [(1, 1)]
    # run 4: same history plus another new fact; the old NULL segment is not a conflict
    history.append(fund_row(original_tag="us-gaap:Debt", segment="Cloud"))
    land(approved, SEGMENTED, history, run_id="r4")
    rep = publish_pending(approved, now=NOW, contracts=SEGMENTED_REG)
    assert rep.exit_code == 0, rep.to_json()
    assert not rep.quarantined
    assert [(p["rows"], p["rows_skipped"]) for p in rep.published] == [(1, 2)]
    assert q(
        approved, "SELECT original_tag, segment FROM lake.facts.fundamentals_v3 ORDER BY 1"
    ) == [
        ("us-gaap:Cash", "Cloud"),
        ("us-gaap:Debt", "Cloud"),
        ("us-gaap:LongTermDebt", None),
    ]


def test_columns_of_the_first_definition_are_still_compared_under_a_minor(approved):
    land(approved, ENTITY_OBSERVATIONS, [obs_rows()])
    land(approved, FUNDAMENTALS, [fund_row(value=Decimal(2))])
    publish_pending(approved, now=NOW)
    land(approved, SEGMENTED, [fund_row(value=Decimal(1), segment="Cloud")], run_id="r2")
    rep = publish_pending(approved, now=NOW, contracts=SEGMENTED_REG)
    assert rep.quarantined[0]["reasons"] == ["conflicting_republish:1"]


def test_a_changed_value_in_a_minor_added_column_is_a_conflict(approved):
    land(approved, ENTITY_OBSERVATIONS, [obs_rows()])
    land(approved, FUNDAMENTALS, [fund_row()])
    publish_pending(approved, now=NOW)
    land(approved, SEGMENTED, [fund_row(original_tag="us-gaap:Cash", segment="Cloud")], run_id="r2")
    rep = publish_pending(approved, now=NOW, contracts=SEGMENTED_REG)
    assert [(p["rows"], p["rows_skipped"]) for p in rep.published] == [(1, 0)]
    land(
        approved, SEGMENTED, [fund_row(original_tag="us-gaap:Cash", segment="OnPrem")], run_id="r3"
    )
    rep = publish_pending(approved, now=NOW, contracts=SEGMENTED_REG)
    assert rep.quarantined[0]["reasons"] == ["conflicting_republish:1"]
    assert q(
        approved,
        "SELECT segment FROM lake.facts.fundamentals_v3 WHERE original_tag = 'us-gaap:Cash'",
    ) == [("Cloud",)]


def test_unreadable_first_definition_falls_back_to_the_current_columns(approved):
    land(approved, ENTITY_OBSERVATIONS, [obs_rows()])
    land(approved, FUNDAMENTALS, [fund_row()])
    publish_pending(approved, now=NOW)
    con = connect(approved, read_only=False)
    con.execute("UPDATE lake.ref.contracts SET definition_json = NULL WHERE name = 'fundamentals'")
    con.close()
    land(approved, FUNDAMENTALS, [fund_row(), fund_row(original_tag="us-gaap:Cash")], run_id="r2")
    rep = publish_pending(approved, now=NOW)
    assert rep.exit_code == 0, rep.to_json()
    assert [(p["rows"], p["rows_skipped"]) for p in rep.published] == [(1, 1)]


def test_lookup_does_not_depend_on_the_resolved_entity_id(approved):
    land(approved, ENTITY_OBSERVATIONS, [obs_rows()])
    land(approved, FUNDAMENTALS, [fund_row(value=Decimal(2))])
    publish_pending(approved, now=NOW)
    con = connect(approved, read_only=False)
    con.execute("UPDATE lake.facts.fundamentals_v3 SET entity_id = 'formerly-resolved-elsewhere'")
    con.close()
    land(approved, FUNDAMENTALS, [fund_row(value=Decimal(1))], run_id="r2")
    rep = publish_pending(approved, now=NOW)
    assert rep.quarantined[0]["reasons"] == ["conflicting_republish:1"]
    land(
        approved,
        FUNDAMENTALS,
        [fund_row(value=Decimal(2)), fund_row(original_tag="us-gaap:Cash")],
        "r3",
    )
    rep = publish_pending(approved, now=NOW)
    assert rep.exit_code == 0, rep.to_json()
    assert [(p["rows"], p["rows_skipped"]) for p in rep.published] == [(1, 1)]


def test_a_partition_column_outside_the_key_never_hides_a_conflict(cfg):
    nullable_part = dataclasses.replace(FUNDAMENTALS, partition_by=("source_doc_id",))
    reg = {(c.name, c.major): c for c in (ENTITY_OBSERVATIONS, nullable_part)}
    approve_contract(cfg, ENTITY_OBSERVATIONS, "test")
    approve_contract(cfg, nullable_part, "test")
    land(cfg, ENTITY_OBSERVATIONS, [obs_rows()])
    land(cfg, nullable_part, [fund_row(source_doc_id=None)])
    publish_pending(cfg, contracts=reg, now=NOW)
    land(cfg, nullable_part, [fund_row(source_doc_id="doc-10k-2025")], run_id="r2")
    rep = publish_pending(cfg, contracts=reg, now=NOW)
    assert rep.quarantined[0]["reasons"] == ["conflicting_republish:1"]
    assert q(cfg, "SELECT count(*) FROM lake.facts.fundamentals_v3") == [(1,)]


RATES = Contract(
    name="rates",
    version="1.0.0",
    kind="fact",
    owner="test",
    licence="test",
    columns=(
        Column("series", pa.string(), nullable=False),
        Column("value", pa.float64()),
        *BITEMPORAL_COLUMNS,
    ),
    natural_key=("series", "valid_date", "source_code"),
)


def rate_row(**over):
    base = dict(
        series="EUR3M",
        value=2.5,
        valid_date=date(2025, 6, 30),
        known_at=ts("2025-07-01T08:00:00"),
        known_at_kind="published",
        source_code="ecb",
        source_doc_id=None,
    )
    base.update(over)
    return base


def test_contract_without_entity_columns_is_idempotent_too(cfg):
    reg = {("rates", 1): RATES}
    approve_contract(cfg, RATES, "test")
    land(cfg, RATES, [rate_row()])
    publish_pending(cfg, contracts=reg, now=NOW)
    land(cfg, RATES, [rate_row(), rate_row(series="USD3M")], run_id="r2")
    rep = publish_pending(cfg, contracts=reg, now=NOW)
    assert rep.exit_code == 0, rep.to_json()
    assert [(p["rows"], p["rows_skipped"]) for p in rep.published] == [(1, 1)]
    land(cfg, RATES, [rate_row(value=2.6)], run_id="r3")
    rep = publish_pending(cfg, contracts=reg, now=NOW)
    assert rep.quarantined[0]["reasons"] == ["conflicting_republish:1"]


def _v300():
    """The 3.0.0 fundamentals definition: frequency vocabulary without ytd9."""
    cols = tuple(
        dataclasses.replace(c, allowed=_freq()[:-1]) if c.name == "frequency" else c
        for c in FUNDAMENTALS.columns
    )
    return dataclasses.replace(FUNDAMENTALS, version="3.0.0", columns=cols)


def test_fundamentals_3_1_0_publishes_ytd9_over_a_published_3_0_0(cfg):
    from myopendata.reader import Store

    old = _v300()
    # anchored to production: main's 3.0.0 fingerprint
    assert old.fingerprint() == "ea77305579a3cdb72cb2804f16284285ddb818d2cad81ddada41417eab07ca9c"
    assert _freq(old) == ("annual", "semiannual", "quarterly", "instant")
    approve_contract(cfg, ENTITY_OBSERVATIONS, "test")
    approve_contract(cfg, old, "test")
    history = [fund_row(), fund_row(original_tag="us-gaap:Cash")]
    land(cfg, ENTITY_OBSERVATIONS, [obs_rows()])
    land(cfg, old, history, run_id="r1")
    reg = {("fundamentals", 3): old, ("entity_observations", 1): ENTITY_OBSERVATIONS}
    assert publish_pending(cfg, now=NOW, contracts=reg).exit_code == 0

    ytd = fund_row(
        original_tag="us-gaap:Revenues",
        frequency="ytd9",
        period_start=date(2025, 6, 30) - timedelta(days=273),
    )
    land(cfg, FUNDAMENTALS, history + [ytd], run_id="r2")
    rep = publish_pending(cfg, now=NOW)
    assert rep.exit_code == 0 and not rep.quarantined, rep.to_json()
    assert [(p["rows"], p["rows_skipped"]) for p in rep.published] == [(1, 2)]
    assert q(cfg, "SELECT approved_by FROM lake.ref.contracts WHERE version = '3.1.0'") == [
        ("auto-minor",)
    ]
    with Store.open(cfg) as s:
        t = s.read_asof("fundamentals", filters={"frequency": "ytd9"})
    assert t.num_rows == 1 and t.column("original_tag").to_pylist() == ["us-gaap:Revenues"]


def test_frequency_outside_the_vocabulary_is_still_refused(approved):
    land(approved, ENTITY_OBSERVATIONS, [obs_rows()])
    ref = land(approved, FUNDAMENTALS, [fund_row()])
    _forge(
        ref,
        [fund_row(frequency="ytd6", period_start=date(2025, 1, 1))],
        contract=FUNDAMENTALS,
    )
    rep = publish_pending(approved, now=NOW)
    assert "value_not_allowed:frequency:1" in rep.quarantined[0]["reasons"]


@pytest.mark.parametrize("catalog", ["cfg", pytest.param("pg_cfg", marks=pytest.mark.postgres)])
def test_first_error_of_a_failed_publish_is_the_one_reported(catalog, request, monkeypatch):
    """A statement failing inside the publishing transaction aborts it; the clean-up that
    follows (unregistering the batch's Arrow view) must not replace that error with DuckDB's
    "Current transaction is aborted". The run goes on: the next batch publishes, and a rerun
    publishes the failed one."""
    from conftest import filing_row

    from myopendata.contracts.standard import FILINGS
    from myopendata.publisher import publish as pub

    cfg = request.getfixturevalue(catalog)
    for contract in (ENTITY_OBSERVATIONS, FUNDAMENTALS, FILINGS):
        approve_contract(cfg, contract, "test")
    land(cfg, ENTITY_OBSERVATIONS, [obs_rows()])
    land(cfg, FUNDAMENTALS, [fund_row()])
    land(cfg, FILINGS, [filing_row()])
    # The first non-identity batch to reach its transaction gets a table in a schema that does
    # not exist: CREATE TABLE fails inside BEGIN. Every later batch sees the real schema.
    real_schema, real_commit = pub._schema, pub._Run.commit
    state = {"target": None, "done": False}

    def schema(c):
        return "no_such_schema" if c.name == state["target"] else real_schema(c)

    def commit(self, c, *a, **k):
        if not state["done"] and not c.resolves_entities:
            state["target"], state["done"] = c.name, True
        try:
            return real_commit(self, c, *a, **k)
        finally:
            state["target"] = None

    monkeypatch.setattr(pub, "_schema", schema)
    monkeypatch.setattr(pub._Run, "commit", commit)
    rep = publish_pending(cfg, now=NOW)
    assert rep.exit_code == 1 and len(rep.errors) == 1, rep.to_json()
    assert rep.errors[0]["error"].startswith("CatalogException"), rep.errors[0]["error"]
    assert "transaction is aborted" not in rep.errors[0]["error"].lower()
    published = {p["contract"] for p in rep.published}
    assert "entity_observations_v1" in published and len(published) == 2  # the other one went on
    failed = rep.errors[0]["batch_id"]
    assert failed in {p.name for p in pub._pending(cfg.landing)}  # left in place for a retry
    monkeypatch.undo()
    rep = publish_pending(cfg, now=NOW)
    assert rep.exit_code == 0 and len(rep.published) == 1, rep.to_json()
    assert rep.published[0]["batch_id"] == failed  # the rerun publishes exactly the failed batch


@pytest.mark.parametrize("catalog", ["cfg", pytest.param("pg_cfg", marks=pytest.mark.postgres)])
def test_facts_behind_a_failed_identity_batch_are_deferred_not_quarantined(
    catalog, request, monkeypatch
):
    """An infrastructure failure on an identity batch leaves its entities unknown for the rest
    of the run. The fact batches that needed them are left pending (reported as errors), not
    quarantined as unknown_entity, so the rerun publishes all of them without a manual move."""
    from conftest import filing_row

    from myopendata.contracts.standard import FILINGS
    from myopendata.publisher import publish as pub

    cfg = request.getfixturevalue(catalog)
    for contract in (ENTITY_OBSERVATIONS, FUNDAMENTALS, FILINGS):
        approve_contract(cfg, contract, "test")
    ids = {
        land(cfg, ENTITY_OBSERVATIONS, [obs_rows()]).batch_id,
        land(cfg, FUNDAMENTALS, [fund_row()]).batch_id,
        land(cfg, FILINGS, [filing_row()]).batch_id,
    }
    real_schema = pub._schema
    monkeypatch.setattr(
        pub, "_schema", lambda c: "no_such_schema" if c.resolves_entities else real_schema(c)
    )
    rep = publish_pending(cfg, now=NOW)
    assert rep.quarantined == [] and rep.published == [], rep.to_json()
    assert rep.exit_code == 1 and {e["batch_id"] for e in rep.errors} == ids, rep.to_json()
    deferred = [e for e in rep.errors if e["error"].startswith("deferred")]
    assert len(deferred) == 2, rep.to_json()
    assert {p.name for p in pub._pending(cfg.landing)} == ids  # all three still pending
    monkeypatch.undo()
    rep = publish_pending(cfg, now=NOW)
    assert rep.exit_code == 0 and len(rep.published) == 3, rep.to_json()


APPLE = dict(subject_value="0000320193", id_value="HWUPKR0MPOU8FGXBT394", name="APPLE INC")


@pytest.mark.parametrize("catalog", ["cfg", pytest.param("pg_cfg", marks=pytest.mark.postgres)])
def test_identity_failure_defers_only_unresolved_facts(catalog, request, monkeypatch):
    """Mixed run behind a failed identity batch: a fact batch whose entity is already known still
    publishes; facts of the failed issuer and of an issuer nobody declared are deferred. The next
    healthy run publishes the issuer and its facts and quarantines the orphan (unknown_entity)."""
    from conftest import filing_row

    from myopendata.contracts.standard import FILINGS
    from myopendata.publisher import publish as pub

    cfg = request.getfixturevalue(catalog)
    for contract in (ENTITY_OBSERVATIONS, FUNDAMENTALS, FILINGS):
        approve_contract(cfg, contract, "test")
    land(cfg, ENTITY_OBSERVATIONS, [obs_rows()])  # Microsoft, published by an earlier run
    assert publish_pending(cfg, now=NOW).exit_code == 0
    apple = land(cfg, ENTITY_OBSERVATIONS, [obs_rows(**APPLE)]).batch_id
    known = land(cfg, FUNDAMENTALS, [fund_row()]).batch_id
    apple_filing = land(
        cfg, FILINGS, [filing_row(entity_value="0000320193", filing_ref="0000320193-25-000001")]
    ).batch_id
    orphan = land(cfg, FUNDAMENTALS, [fund_row(entity_value="0000000001")]).batch_id
    real_schema = pub._schema
    monkeypatch.setattr(
        pub, "_schema", lambda c: "no_such_schema" if c.resolves_entities else real_schema(c)
    )
    rep = publish_pending(cfg, now=NOW)
    assert [p["batch_id"] for p in rep.published] == [known], rep.to_json()
    assert rep.quarantined == [], rep.to_json()
    errors = {e["batch_id"]: e["error"] for e in rep.errors}
    assert set(errors) == {apple, apple_filing, orphan}, rep.to_json()
    assert errors[apple_filing].startswith("deferred") and errors[orphan].startswith("deferred")
    assert not errors[apple].startswith("deferred")
    monkeypatch.undo()
    rep = publish_pending(cfg, now=NOW)
    assert {p["batch_id"] for p in rep.published} == {apple, apple_filing}, rep.to_json()
    assert [q["batch_id"] for q in rep.quarantined] == [orphan], rep.to_json()
    assert rep.quarantined[0]["reasons"] == ["unknown_entity:1"] and rep.exit_code == 3


@pytest.mark.parametrize("catalog", ["cfg", pytest.param("pg_cfg", marks=pytest.mark.postgres)])
def test_a_failed_fact_batch_does_not_defer_orphans(catalog, request, monkeypatch):
    """Only an identity batch's failure defers: after a fact batch fails on infrastructure, a fact
    batch of an unknown issuer is still quarantined as unknown_entity."""
    from conftest import filing_row

    from myopendata.contracts.standard import FILINGS
    from myopendata.publisher import publish as pub

    cfg = request.getfixturevalue(catalog)
    for contract in (ENTITY_OBSERVATIONS, FUNDAMENTALS, FILINGS):
        approve_contract(cfg, contract, "test")
    land(cfg, ENTITY_OBSERVATIONS, [obs_rows()])
    failed = land(cfg, FUNDAMENTALS, [fund_row()]).batch_id
    orphan = write_batch(  # written later, so audited after the failed batch
        FILINGS,
        table(FILINGS, [filing_row(entity_value="0000000001")]),
        landing_root=cfg.landing,
        run_id="r1",
        producer="test",
        now=NOW + timedelta(minutes=1),
    ).batch_id
    real_schema = pub._schema
    monkeypatch.setattr(
        pub, "_schema", lambda c: "no_such_schema" if c.name == "fundamentals" else real_schema(c)
    )
    rep = publish_pending(cfg, now=NOW)
    assert [e["batch_id"] for e in rep.errors] == [failed], rep.to_json()
    assert not rep.errors[0]["error"].startswith("deferred")
    assert [q["batch_id"] for q in rep.quarantined] == [orphan], rep.to_json()
    assert rep.quarantined[0]["reasons"] == ["unknown_entity:1"]
    assert [p["contract"] for p in rep.published] == ["entity_observations_v1"]


@pytest.mark.parametrize("catalog", ["cfg", pytest.param("pg_cfg", marks=pytest.mark.postgres)])
def test_an_interrupt_with_a_failing_rollback_stays_an_interrupt(catalog, request, monkeypatch):
    """Ctrl-C inside a batch's transaction, then a failing ROLLBACK: the interrupt propagates
    as itself (never RollbackFailed or a batch error), the batches stay pending, and the rerun
    publishes them."""
    import duckdb
    from conftest import filing_row

    from myopendata.contracts.standard import FILINGS
    from myopendata.publisher import publish as pub

    cfg = request.getfixturevalue(catalog)
    for contract in (ENTITY_OBSERVATIONS, FUNDAMENTALS, FILINGS):
        approve_contract(cfg, contract, "test")
    land(cfg, ENTITY_OBSERVATIONS, [obs_rows()])
    facts = {
        land(cfg, FUNDAMENTALS, [fund_row()]).batch_id,
        land(cfg, FILINGS, [filing_row()]).batch_id,
    }

    class NoRollback:
        def __init__(self, con):
            self._con = con

        def __getattr__(self, name):
            return getattr(self._con, name)

        def execute(self, sql, *a, **k):
            if sql == "ROLLBACK":
                raise duckdb.ConnectionException("connection lost")
            return self._con.execute(sql, *a, **k)

    real_connect, real_ensure = pub.connect, pub._ensure_table

    def ensure(con, c, sample):
        if not c.resolves_entities:
            raise KeyboardInterrupt
        return real_ensure(con, c, sample)

    monkeypatch.setattr(pub, "connect", lambda *a, **k: NoRollback(real_connect(*a, **k)))
    monkeypatch.setattr(pub, "_ensure_table", ensure)
    with pytest.raises(KeyboardInterrupt):
        publish_pending(cfg, now=NOW)
    assert {p.name for p in pub._pending(cfg.landing)} == facts
    monkeypatch.undo()
    rep = publish_pending(cfg, now=NOW)
    assert rep.exit_code == 0 and {p["batch_id"] for p in rep.published} == facts, rep.to_json()


@pytest.mark.postgres
def test_a_commit_conflict_on_the_identity_batch_is_a_batch_error_and_defers_facts(
    pg_cfg, monkeypatch
):
    """A failed COMMIT ends the transaction (DuckDB 2.0 / DuckLake): the ROLLBACK that follows
    has nothing to roll back. That is the commonest infrastructure failure (a commit conflict),
    so it must stay a per-batch error, never a fatal run: here another writer creates the
    identity table just before the publisher's COMMIT; the facts behind it are deferred and the
    rerun publishes all three batches. Postgres only: a file catalogue refuses a second writer
    (ResourceInUseException), so it cannot reach a commit conflict."""
    from conftest import filing_row

    from myopendata.contracts.standard import FILINGS
    from myopendata.publisher import publish as pub

    cfg = pg_cfg
    for contract in (ENTITY_OBSERVATIONS, FUNDAMENTALS, FILINGS):
        approve_contract(cfg, contract, "test")
    ids = {
        land(cfg, ENTITY_OBSERVATIONS, [obs_rows()]).batch_id,
        land(cfg, FUNDAMENTALS, [fund_row()]).batch_id,
        land(cfg, FILINGS, [filing_row()]).batch_id,
    }
    real_connect, real_commit = pub.connect, pub._Run.commit
    state = {"rival": None}  # (contract, rows) to create concurrently before the next COMMIT

    class Raced:
        def __init__(self, con):
            self._con = con

        def __getattr__(self, name):
            return getattr(self._con, name)

        def execute(self, sql, *a, **k):
            if sql == "COMMIT" and state["rival"]:
                c, out = state["rival"]
                state["rival"] = None
                other = real_connect(cfg, read_only=False)
                try:
                    other.execute("BEGIN")
                    pub._ensure_table(other, c, out)
                    other.execute("COMMIT")
                finally:
                    other.close()
            return self._con.execute(sql, *a, **k)

    def commit(self, c, m, bid, out, *a, **k):
        if c.resolves_entities:
            state["rival"] = (c, out)
        return real_commit(self, c, m, bid, out, *a, **k)

    monkeypatch.setattr(pub, "connect", lambda *a, **k: Raced(real_connect(*a, **k)))
    monkeypatch.setattr(pub._Run, "commit", commit)
    rep = publish_pending(cfg, now=NOW)
    assert rep.fatal is None and rep.exit_code == 1, rep.to_json()
    assert rep.published == [] and rep.quarantined == [], rep.to_json()
    assert {e["batch_id"] for e in rep.errors} == ids, rep.to_json()
    deferred = [e for e in rep.errors if e["error"].startswith("deferred")]
    (failed,) = [e for e in rep.errors if e not in deferred]
    assert len(deferred) == 2 and "Transaction conflict" in failed["error"], rep.to_json()
    assert {p.name for p in pub._pending(cfg.landing)} == ids
    monkeypatch.undo()
    rep = publish_pending(cfg, now=NOW)
    assert rep.exit_code == 0 and len(rep.published) == 3, rep.to_json()


@pytest.mark.parametrize("catalog", ["cfg", pytest.param("pg_cfg", marks=pytest.mark.postgres)])
def test_a_failed_rollback_is_fatal_and_stops_the_run(catalog, request, monkeypatch):
    """When ROLLBACK itself fails while a transaction is open, it may still be open: the run must
    stop with a fatal report naming both errors, not go on publishing on that connection."""
    import duckdb
    from conftest import filing_row

    from myopendata.contracts.standard import FILINGS
    from myopendata.publisher import publish as pub

    cfg = request.getfixturevalue(catalog)
    for contract in (ENTITY_OBSERVATIONS, FUNDAMENTALS, FILINGS):
        approve_contract(cfg, contract, "test")
    land(cfg, ENTITY_OBSERVATIONS, [obs_rows()])
    facts = {
        land(cfg, FUNDAMENTALS, [fund_row()]).batch_id,
        land(cfg, FILINGS, [filing_row()]).batch_id,
    }

    class NoRollback:
        def __init__(self, con):
            self._con = con

        def __getattr__(self, name):
            return getattr(self._con, name)

        def execute(self, sql, *a, **k):
            if sql == "ROLLBACK":
                raise duckdb.ConnectionException("connection lost")
            return self._con.execute(sql, *a, **k)

    real_connect, real_schema = pub.connect, pub._schema
    monkeypatch.setattr(pub, "connect", lambda *a, **k: NoRollback(real_connect(*a, **k)))
    monkeypatch.setattr(
        pub, "_schema", lambda c: real_schema(c) if c.resolves_entities else "no_such_schema"
    )
    rep = publish_pending(cfg, now=NOW)
    assert rep.exit_code == 1 and rep.fatal, rep.to_json()
    assert "ROLLBACK failed" in rep.fatal and "connection lost" in rep.fatal, rep.fatal
    assert "CatalogException" in rep.fatal, rep.fatal  # the original error is kept
    assert [p["contract"] for p in rep.published] == ["entity_observations_v1"], rep.to_json()
    assert rep.errors == [] and rep.quarantined == [], rep.to_json()  # the run stopped there
    assert {p.name for p in pub._pending(cfg.landing)} == facts
    monkeypatch.undo()
    rep = publish_pending(cfg, now=NOW)
    assert rep.exit_code == 0 and len(rep.published) == 2, rep.to_json()


def test_unregister_keeps_the_original_error_but_raises_on_its_own():
    import duckdb

    from myopendata.lake import registered

    con = duckdb.connect()
    con.execute("BEGIN")
    with pytest.raises(duckdb.CatalogException):
        with registered(con, "_t", pa.table({"a": [1]})):
            con.execute("CREATE TABLE no_such_schema.t AS SELECT * FROM _t")
    con.execute("ROLLBACK")
    with pytest.raises(duckdb.CatalogException):  # the ROLLBACK dropped the view
        con.execute("SELECT * FROM _t")
    with registered(con, "_t", pa.table({"a": [1]})):
        assert con.execute("SELECT a FROM _t").fetchall() == [(1,)]
    with pytest.raises(duckdb.CatalogException):  # unregistered on a clean exit
        con.execute("SELECT * FROM _t")


def test_registered_unregisters_after_a_non_duckdb_error_and_an_early_return():
    import duckdb

    from myopendata.lake import registered

    con = duckdb.connect()
    with pytest.raises(KeyError):
        with registered(con, "_t", pa.table({"a": [1]})):
            raise KeyError("not a DuckDB error")
    with pytest.raises(duckdb.CatalogException):
        con.execute("SELECT * FROM _t")

    def early():
        with registered(con, "_t", pa.table({"a": [1]})):
            return con.execute("SELECT a FROM _t").fetchall()

    assert early() == [(1,)]
    with pytest.raises(duckdb.CatalogException):
        con.execute("SELECT * FROM _t")
