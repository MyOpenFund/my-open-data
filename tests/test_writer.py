import json
from datetime import timedelta

import pyarrow as pa
import pytest
from conftest import NOW, fund_row, table, ts

from myopendata.contracts.rules import BatchInvalid
from myopendata.contracts.standard import FUNDAMENTALS
from myopendata.writer import write_batch


def _write(tmp_path, rows, **kw):
    return write_batch(
        FUNDAMENTALS,
        table(FUNDAMENTALS, rows),
        landing_root=tmp_path,
        run_id="r1",
        producer="test",
        now=NOW,
        **kw,
    )


def test_batch_lands_with_manifest(tmp_path):
    ref = _write(tmp_path, [fund_row()], source_inputs=["companyfacts/CIK0000789019.json"])
    m = json.loads((ref.path / "batch.json").read_text())
    assert ref.created and m["rows"] == 1 and m["contract_version"] == "1.0.0"
    assert ref.path.parent.name == "fundamentals_v1"
    assert not list(tmp_path.glob("*/.tmp-*"))


def test_same_rows_in_any_order_give_same_batch(tmp_path):
    a = _write(tmp_path, [fund_row(), fund_row(concept="Revenue")])
    b = _write(tmp_path, [fund_row(concept="Revenue"), fund_row()])
    assert a.batch_id == b.batch_id and b.created is False


def test_duplicate_natural_key_rejected(tmp_path):
    with pytest.raises(BatchInvalid, match="duplicate_natural_key"):
        _write(tmp_path, [fund_row(), fund_row(value=1)])


def test_restatement_is_not_a_duplicate(tmp_path):
    _write(tmp_path, [fund_row(), fund_row(value=1, known_at=ts("2025-09-01T00:00:00"))])


def test_two_sources_same_fact_is_not_a_duplicate(tmp_path):
    _write(
        tmp_path,
        [fund_row(), fund_row(source_code="ocr-test", extraction_method="ocr", confidence=0.9)],
    )


def test_ocr_without_confidence_rejected(tmp_path):
    with pytest.raises(BatchInvalid, match="ocr_confidence"):
        _write(tmp_path, [fund_row(extraction_method="ocr", source_code="ocr-test")])


def test_known_at_in_future_rejected(tmp_path):
    with pytest.raises(BatchInvalid, match="known_at_in_future"):
        _write(tmp_path, [fund_row(known_at=NOW + timedelta(days=1))])


def test_naive_known_at_rejected(tmp_path):
    t = table(FUNDAMENTALS, [fund_row()])
    i = t.schema.get_field_index("known_at")
    t = t.set_column(i, "known_at", t["known_at"].cast(pa.timestamp("us")))
    with pytest.raises(BatchInvalid, match="known_at_without_timezone"):
        write_batch(FUNDAMENTALS, t, landing_root=tmp_path, run_id="r", producer="t", now=NOW)


def test_value_outside_vocabulary_rejected(tmp_path):
    with pytest.raises(BatchInvalid, match="value_not_allowed:frequency"):
        _write(tmp_path, [fund_row(frequency="ttm")])


def test_unknown_and_missing_columns_rejected(tmp_path):
    t = (
        table(FUNDAMENTALS, [fund_row()])
        .drop_columns(["unit"])
        .append_column("extra", pa.array([1]))
    )
    with pytest.raises(BatchInvalid) as e:
        write_batch(FUNDAMENTALS, t, landing_root=tmp_path, run_id="r", producer="t", now=NOW)
    assert any(r.startswith("unknown_columns") for r in e.value.reasons)
    assert any(r.startswith("missing_columns") for r in e.value.reasons)


def test_empty_batch_rejected(tmp_path):
    with pytest.raises(BatchInvalid, match="empty_batch"):
        _write(tmp_path, [])


def test_non_race_rename_error_is_raised(tmp_path):
    from myopendata.contracts.rules import conform
    from myopendata.writer.batch import batch_id_for, content_hash

    t = conform(FUNDAMENTALS, table(FUNDAMENTALS, [fund_row()]), now=NOW)
    bid = batch_id_for(FUNDAMENTALS, content_hash(t))
    (tmp_path / "fundamentals_v1").mkdir()
    (tmp_path / "fundamentals_v1" / bid).write_text("not a batch")
    with pytest.raises(OSError):
        _write(tmp_path, [fund_row()])
    assert not list(tmp_path.glob("*/.tmp-*"))


def test_failed_write_leaves_no_tmp_dir(tmp_path, monkeypatch):
    import myopendata.writer.batch as b

    def boom(*a, **k):
        raise OSError("disk full")

    monkeypatch.setattr(b.pq, "write_table", boom)
    with pytest.raises(OSError, match="disk full"):
        _write(tmp_path, [fund_row()])
    assert not list(tmp_path.glob("*/.tmp-*"))


def test_manifest_hashes_match_the_file(tmp_path):
    import pyarrow.parquet as pq

    from myopendata.writer.batch import content_hash, file_sha256

    ref = _write(tmp_path, [fund_row(), fund_row(concept="Revenue")], source_inputs=["b", "a"])
    m = json.loads((ref.path / "batch.json").read_text())
    assert m["source_inputs"] == ["a", "b"]
    assert m["files"]["data.parquet"]["sha256"] == file_sha256(ref.path / "data.parquet")
    assert m["content_sha256"] == content_hash(pq.read_table(ref.path / "data.parquet"))


def test_hash_ignores_chunking_column_order_and_metadata():
    from myopendata.contracts.rules import conform
    from myopendata.writer.batch import content_hash

    t = conform(FUNDAMENTALS, table(FUNDAMENTALS, [fund_row(), fund_row(concept="R")]), now=NOW)
    chunked = pa.concat_tables([t.slice(0, 1), t.slice(1)])
    permuted = conform(FUNDAMENTALS, t.select(list(reversed(t.column_names))), now=NOW)
    meta = t.replace_schema_metadata({b"pandas": b"{}"})
    assert content_hash(t) == content_hash(chunked) == content_hash(permuted) == content_hash(meta)


def test_other_contract_version_gives_other_batch(tmp_path):
    import dataclasses

    a = _write(tmp_path, [fund_row()])
    other = dataclasses.replace(FUNDAMENTALS, version="1.0.1")
    b = write_batch(
        other, table(other, [fund_row()]), landing_root=tmp_path, run_id="r", producer="t", now=NOW
    )
    assert a.batch_id != b.batch_id


def test_default_clock_is_used_when_now_is_omitted(tmp_path):
    ref = write_batch(
        FUNDAMENTALS,
        table(FUNDAMENTALS, [fund_row()]),
        landing_root=tmp_path,
        run_id="r",
        producer="t",
    )
    assert json.loads((ref.path / "batch.json").read_text())["written_at"]
