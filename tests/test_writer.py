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
