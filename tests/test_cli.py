import json

from conftest import NOW, obs_rows
from test_publisher import land

from myopendata.cli import main
from myopendata.contracts.standard import ENTITY_OBSERVATIONS


def _env(monkeypatch, cfg):
    monkeypatch.setenv("MYOPENDATA_DATA_ROOT", str(cfg.data_root))
    monkeypatch.setenv("MYOPENDATA_CATALOG", cfg.catalog)


def test_contracts_list(capsys):
    assert main(["contracts", "list"]) == 0
    out = capsys.readouterr().out
    assert "fundamentals_v1\t1.0.0" in out and "entity_observations_v1" in out


def test_approve_then_publish_exit_codes(cfg, monkeypatch, capsys):
    _env(monkeypatch, cfg)
    land(cfg, ENTITY_OBSERVATIONS, [obs_rows(known_at=NOW)])
    assert main(["publish"]) == 3  # not approved yet → quarantined → degraded
    assert main(["contracts", "approve", "entity_observations", "1", "--by", "marc"]) == 0
    land(cfg, ENTITY_OBSERVATIONS, [obs_rows()])
    capsys.readouterr()
    assert main(["publish"]) == 0
    assert json.loads(capsys.readouterr().out)["published"]


def test_approve_unknown_contract(cfg, monkeypatch):
    _env(monkeypatch, cfg)
    assert main(["contracts", "approve", "nope", "1", "--by", "marc"]) == 1


def test_missing_env_is_an_error(monkeypatch):
    monkeypatch.delenv("MYOPENDATA_DATA_ROOT", raising=False)
    monkeypatch.delenv("MYOPENDATA_CATALOG", raising=False)
    import pytest

    with pytest.raises(RuntimeError, match="MYOPENDATA_DATA_ROOT"):
        main(["publish"])
