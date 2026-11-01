import dataclasses

import pyarrow as pa
import pytest

from myopendata.contracts.base import Column
from myopendata.contracts.registry import discover_contracts
from myopendata.contracts.rules import ContractError, validate_contract
from myopendata.contracts.standard import ENTITY_OBSERVATIONS, FUNDAMENTALS


def test_standard_contracts_are_discovered_via_entry_point():
    found = discover_contracts()
    assert ("fundamentals", 1) in found and ("entity_observations", 1) in found


@pytest.mark.parametrize("c", [FUNDAMENTALS, ENTITY_OBSERVATIONS])
def test_standard_contracts_pass_common_rules(c):
    validate_contract(c)


def _without(c, name):
    return dataclasses.replace(c, columns=tuple(x for x in c.columns if x.name != name))


@pytest.mark.parametrize(
    "col", ["valid_date", "known_at", "known_at_kind", "source_code", "source_doc_id"]
)
def test_missing_common_column_is_rejected(col):
    with pytest.raises(ContractError, match=col):
        validate_contract(_without(FUNDAMENTALS, col))


def test_naive_known_at_is_rejected():
    cols = tuple(
        Column("known_at", pa.timestamp("us"), nullable=False) if c.name == "known_at" else c
        for c in FUNDAMENTALS.columns
    )
    with pytest.raises(ContractError, match="known_at"):
        validate_contract(dataclasses.replace(FUNDAMENTALS, columns=cols))


def test_known_at_in_natural_key_is_rejected():
    c = dataclasses.replace(FUNDAMENTALS, natural_key=FUNDAMENTALS.natural_key + ("known_at",))
    with pytest.raises(ContractError, match="known_at"):
        validate_contract(c)


def test_natural_key_without_source_is_rejected():
    nk = tuple(k for k in FUNDAMENTALS.natural_key if k != "source_code")
    with pytest.raises(ContractError, match="source_code"):
        validate_contract(dataclasses.replace(FUNDAMENTALS, natural_key=nk))


def test_empty_licence_is_rejected():
    with pytest.raises(ContractError, match="licence"):
        validate_contract(dataclasses.replace(FUNDAMENTALS, licence=" "))


def test_declaring_entity_id_is_rejected():
    c = dataclasses.replace(
        FUNDAMENTALS, columns=FUNDAMENTALS.columns + (Column("entity_id", pa.string()),)
    )
    with pytest.raises(ContractError, match="entity_id"):
        validate_contract(c)


def test_fingerprint_ignores_docs_but_not_schema():
    doc_only = dataclasses.replace(FUNDAMENTALS, doc="other words")
    assert doc_only.fingerprint() == FUNDAMENTALS.fingerprint()
    more = dataclasses.replace(
        FUNDAMENTALS, columns=FUNDAMENTALS.columns + (Column("note", pa.string()),)
    )
    assert more.fingerprint() != FUNDAMENTALS.fingerprint()


def test_duplicate_definitions_of_one_version_are_rejected():
    clash = dataclasses.replace(
        FUNDAMENTALS,
        owner="someone-else",
        columns=FUNDAMENTALS.columns + (Column("note", pa.string()),),
    )
    with pytest.raises(ContractError, match="declared twice"):
        discover_contracts([clash])


def _bad(**kw):
    return dataclasses.replace(FUNDAMENTALS, **kw)


@pytest.mark.parametrize(
    "contract, match",
    [
        (_bad(name="Fundamentals"), "name"),
        (_bad(name="fundamentals\n"), "name"),
        (_bad(version="1.0"), "version"),
        (_bad(version="1.0.0\n"), "version"),
        (_bad(kind="view"), "kind"),
        (_bad(owner=""), "owner"),
        (_bad(natural_key=FUNDAMENTALS.natural_key + ("concept",)), "duplicate natural_key"),
        (_bad(natural_key=FUNDAMENTALS.natural_key + ("confidence",)), "non-nullable"),
        (_bad(partition_by=("nope",)), "partition_by"),
        (_bad(checks=("nope",)), "unknown check"),
        (_bad(columns=FUNDAMENTALS.columns + (Column("batch_id", pa.string()),)), "batch_id"),
        (
            _bad(columns=FUNDAMENTALS.columns + (Column("nn", pa.int32(), allowed=("1",)),)),
            "allowed",
        ),
    ],
)
def test_bad_contracts_are_rejected(contract, match):
    with pytest.raises(ContractError, match=match):
        validate_contract(contract)


def test_check_without_its_columns_is_rejected():
    c = _without(FUNDAMENTALS, "source_page")
    with pytest.raises(ContractError, match="needs column source_page"):
        validate_contract(c)


def test_entity_scheme_outside_common_schemes_is_rejected():
    cols = tuple(
        Column("entity_scheme", pa.string(), nullable=False, allowed=("my_scheme",))
        if c.name == "entity_scheme"
        else c
        for c in FUNDAMENTALS.columns
    )
    with pytest.raises(ContractError, match="entity scheme"):
        validate_contract(_bad(columns=cols))


@pytest.mark.parametrize(
    "change",
    [
        {"licence": "CC-BY-4.0"},
        {"partition_by": ()},
    ],
)
def test_fingerprint_covers_terms_and_layout(change):
    assert _bad(**change).fingerprint() != FUNDAMENTALS.fingerprint()


def test_same_definition_several_patches_keeps_highest():
    found = discover_contracts([_bad(version="1.0.7")])
    assert found[("fundamentals", 1)].version == "1.0.7"


def test_identity_dataset_needs_identity_columns():
    c = dataclasses.replace(FUNDAMENTALS, resolves_entities=True)
    with pytest.raises(ContractError, match="identity dataset"):
        validate_contract(c)


def test_nested_columns_are_rejected():
    c = _bad(columns=FUNDAMENTALS.columns + (Column("tags", pa.list_(pa.string())),))
    with pytest.raises(ContractError, match="nested"):
        validate_contract(c)


def test_resolution_policy_comes_from_env(monkeypatch, tmp_path):
    from myopendata.config import StoreConfig

    monkeypatch.setenv("MYOPENDATA_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("MYOPENDATA_CATALOG", str(tmp_path / "m.ducklake"))
    monkeypatch.setenv("MYOPENDATA_RESOLVING_SCHEMES", "cik, lei")
    cfg = StoreConfig.from_env()
    assert cfg.resolving_schemes == ("cik", "lei")
    assert cfg.single_valued_schemes == ("cik", "lei", "cb_code")


@pytest.mark.parametrize(
    "var, value",
    [
        ("MYOPENDATA_RESOLVING_SCHEMES", "CIK"),
        ("MYOPENDATA_RESOLVING_SCHEMES", ","),
        ("MYOPENDATA_DATA_INLINING_ROW_LIMIT", "x"),
    ],
)
def test_bad_env_values_are_clear_errors(monkeypatch, tmp_path, var, value):
    from myopendata.config import StoreConfig

    monkeypatch.setenv("MYOPENDATA_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("MYOPENDATA_CATALOG", str(tmp_path / "m.ducklake"))
    monkeypatch.setenv(var, value)
    with pytest.raises(RuntimeError, match=var):
        StoreConfig.from_env()
