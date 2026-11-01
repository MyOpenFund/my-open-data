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
