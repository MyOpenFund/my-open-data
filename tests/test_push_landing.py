"""deploy/nas/push_landing.py: local landing -> NAS landing, one atomic rename per batch."""

import importlib.util
import sys
from pathlib import Path

import pytest
from conftest import NOW, fund_row, obs_rows
from test_publisher import land

from myopendata.config import StoreConfig
from myopendata.contracts.standard import ENTITY_OBSERVATIONS, FUNDAMENTALS
from myopendata.publisher import approve_contract, publish_pending

_PATH = Path(__file__).parents[1] / "deploy" / "nas" / "push_landing.py"
_spec = importlib.util.spec_from_file_location("push_landing", _PATH)
pl = importlib.util.module_from_spec(_spec)
sys.modules["push_landing"] = pl  # dataclasses resolves the module by name at class creation
_spec.loader.exec_module(pl)


@pytest.fixture
def local(tmp_path):
    return StoreConfig(data_root=tmp_path / "mac", catalog=str(tmp_path / "unused.ducklake"))


@pytest.fixture
def nas(tmp_path):
    cfg = StoreConfig(data_root=tmp_path / "nas", catalog=str(tmp_path / "nas.ducklake"))
    approve_contract(cfg, ENTITY_OBSERVATIONS, "test")
    approve_contract(cfg, FUNDAMENTALS, "test")
    cfg.landing.mkdir(parents=True, exist_ok=True)
    return cfg


def _files(d: Path) -> dict[str, bytes]:
    return {p.name: p.read_bytes() for p in sorted(d.iterdir())}


def test_copies_a_finished_batch_byte_for_byte(local, nas):
    ref = land(local, FUNDAMENTALS, [fund_row()])
    rep = pl.push(local.landing, nas.landing)
    dest = nas.landing / ref.path.parent.name / ref.path.name
    assert rep.copied == [f"{ref.path.parent.name}/{ref.path.name}"]
    assert _files(dest) == _files(ref.path)
    assert not list(dest.parent.glob(".tmp-*"))


def test_ignores_writer_temp_dirs_and_bucket_dirs(local, nas):
    land(local, FUNDAMENTALS, [fund_row()])
    table = next(p for p in local.landing.iterdir() if p.is_dir())
    (table / ".tmp-abc").mkdir()
    (local.landing / "_published" / table.name / "x").mkdir(parents=True)
    rep = pl.push(local.landing, nas.landing)
    assert len(rep.copied) == 1
    assert not (nas.landing / table.name / ".tmp-abc").exists()
    assert not (nas.landing / "_published").exists()


def test_second_push_copies_nothing(local, nas):
    land(local, FUNDAMENTALS, [fund_row()])
    pl.push(local.landing, nas.landing)
    rep = pl.push(local.landing, nas.landing)
    assert rep.copied == [] and len(rep.already_there) == 1


@pytest.mark.parametrize("bucket", ["_published", "_duplicates", "_quarantine"])
@pytest.mark.parametrize("suffix", ["", "-run2"])
def test_skips_a_batch_the_nas_already_moved(local, nas, bucket, suffix):
    ref = land(local, FUNDAMENTALS, [fund_row()])
    (nas.landing / bucket / ref.path.parent.name / f"{ref.path.name}{suffix}").mkdir(parents=True)
    rep = pl.push(local.landing, nas.landing)
    assert rep.copied == [] and rep.already_there == [f"{ref.path.parent.name}/{ref.path.name}"]
    assert not (nas.landing / ref.path.parent.name / ref.path.name).exists()


def test_dry_run_lists_but_copies_nothing(local, nas):
    land(local, FUNDAMENTALS, [fund_row()])
    rep = pl.push(local.landing, nas.landing, dry_run=True)
    assert len(rep.copied) == 1
    assert not any(nas.landing.rglob("batch.json"))


def test_failed_copy_leaves_neither_temp_nor_batch(local, nas, monkeypatch):
    ref = land(local, FUNDAMENTALS, [fund_row()])

    def broken_copyfile(src, dst):
        Path(dst).write_bytes(b"half")
        raise OSError("share went away")

    monkeypatch.setattr(pl.shutil, "copyfile", broken_copyfile)
    with pytest.raises(OSError):
        pl.push(local.landing, nas.landing)
    table_dir = nas.landing / ref.path.parent.name
    assert not (table_dir / ref.path.name).exists()
    assert not list(table_dir.glob(".tmp-*"))


def test_replaces_a_stale_temp_left_by_a_crashed_push(local, nas):
    ref = land(local, FUNDAMENTALS, [fund_row()])
    stale = nas.landing / ref.path.parent.name / f".tmp-push-{ref.path.name}"
    stale.mkdir(parents=True)
    (stale / "data.parquet").write_bytes(b"half")
    pl.push(local.landing, nas.landing)
    assert _files(nas.landing / ref.path.parent.name / ref.path.name) == _files(ref.path)
    assert not stale.exists()


def test_incomplete_batch_is_reported_not_copied(local, nas, capsys):
    ref = land(local, FUNDAMENTALS, [fund_row()])
    (ref.path / "batch.json").unlink()
    rep = pl.push(local.landing, nas.landing)
    assert rep.copied == [] and rep.incomplete == [f"{ref.path.parent.name}/{ref.path.name}"]
    assert pl.main([str(local.landing), str(nas.landing)]) == 1
    assert "incomplete 1" in capsys.readouterr().out


def test_pushed_batches_publish_on_the_other_side(local, nas):
    land(local, ENTITY_OBSERVATIONS, [obs_rows()])
    land(local, FUNDAMENTALS, [fund_row()])
    assert pl.main([str(local.landing), str(nas.landing)]) == 0
    rep = publish_pending(nas, now=NOW)
    assert rep.exit_code == 0 and len(rep.published) == 2, rep.to_json()


def test_missing_source_is_an_empty_push(tmp_path):
    dest = tmp_path / "dest"
    dest.mkdir()
    rep = pl.push(tmp_path / "nothing", dest)
    assert (rep.copied, rep.already_there, rep.incomplete) == ([], [], [])
    assert list(dest.iterdir()) == []


def test_missing_destination_is_refused(local, tmp_path, capsys):
    land(local, FUNDAMENTALS, [fund_row()])
    gone = tmp_path / "unmounted" / "landing"  # e.g. an SMB share that is not mounted
    with pytest.raises(FileNotFoundError):
        pl.push(local.landing, gone)
    assert pl.main([str(local.landing), str(gone)]) == 2
    assert "not a directory" in capsys.readouterr().err
    assert not gone.parent.exists()
