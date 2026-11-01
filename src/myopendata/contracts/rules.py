"""Common rules every contract must satisfy, and the row checks shared by writer and publisher."""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pyarrow.compute as pc

from myopendata.contracts.base import Contract, ContractError

NAME_RE = re.compile(r"^[a-z][a-z0-9_]{1,62}$")
SEMVER_RE = re.compile(r"^\d+\.\d+\.\d+$")
KINDS = ("fact", "derived")
KNOWN_AT_KINDS = ("published", "collected")
# Identifier schemes the publisher can resolve (common rule, spec §6.1). The standard contracts
# list them as their `allowed` vocabulary, so extending this tuple changes their fingerprint:
# it ships as a new MAJOR of those contracts, never in place.
ID_SCHEMES = ("cik", "lei", "isin", "figi", "ticker", "cb_code", "national_id")
IDENTITY_COLUMNS = ("subject_scheme", "subject_value", "id_scheme", "id_value", "name", "country")
# Columns the publisher adds to every published row.
RESERVED = ("entity_id", "batch_id")

# Columns every fact contract carries, with their required arrow types.
BITEMPORAL = {
    "valid_date": pa.date32(),
    "known_at": pa.timestamp("us", tz="UTC"),
    "known_at_kind": pa.string(),
    "source_code": pa.string(),
}
PROVENANCE_OPTIONAL = {"source_doc_id": pa.string()}


def validate_contract(c: Contract) -> None:
    """Raise ContractError if the contract breaks a common rule."""
    if not NAME_RE.fullmatch(c.name):
        raise ContractError(f"{c.name}: name must match {NAME_RE.pattern}")
    if not SEMVER_RE.fullmatch(c.version):
        raise ContractError(f"{c.name}: version must be MAJOR.MINOR.PATCH, got {c.version!r}")
    if c.kind not in KINDS:
        raise ContractError(f"{c.name}: kind must be one of {KINDS}")
    if not c.licence.strip():
        raise ContractError(f"{c.name}: licence is mandatory")
    if not c.owner.strip():
        raise ContractError(f"{c.name}: owner is mandatory")
    names = [col.name for col in c.columns]
    if len(names) != len(set(names)):
        raise ContractError(f"{c.name}: duplicate column names")
    for col in c.columns:
        if not NAME_RE.fullmatch(col.name):
            raise ContractError(f"{c.name}.{col.name}: bad column name")
        if col.name in RESERVED:
            raise ContractError(f"{c.name}: {col.name} is assigned by the publisher, not declared")
        if col.allowed is not None and (col.type != pa.string() or not col.allowed):
            raise ContractError(f"{c.name}.{col.name}: allowed needs a non-empty string column")
    for req, typ in {**BITEMPORAL, **PROVENANCE_OPTIONAL}.items():
        if req not in names:
            raise ContractError(f"{c.name}: missing common column {req}")
        col = c.column(req)
        if col.type != typ:
            raise ContractError(f"{c.name}.{req}: must be {typ}, got {col.type}")
        if req in BITEMPORAL and col.nullable:
            raise ContractError(f"{c.name}.{req}: must be non-nullable")
    if set(c.column("known_at_kind").allowed or ()) != set(KNOWN_AT_KINDS):
        raise ContractError(f"{c.name}.known_at_kind: allowed must be {KNOWN_AT_KINDS}")
    if "known_at" in c.natural_key:
        raise ContractError(f"{c.name}: known_at must not be in natural_key (it versions the key)")
    for k in ("valid_date", "source_code"):
        if k not in c.natural_key:
            raise ContractError(f"{c.name}: natural_key must include {k}")
    if len(set(c.natural_key)) != len(c.natural_key):
        raise ContractError(f"{c.name}: duplicate natural_key columns")
    for k in c.natural_key:
        if k not in names:
            raise ContractError(f"{c.name}: natural_key column {k} not declared")
        if c.column(k).nullable:
            raise ContractError(f"{c.name}: natural_key column {k} must be non-nullable")
    if c.entity_column_pair:
        for k in c.entity_column_pair:
            if k not in names or c.column(k).nullable:
                raise ContractError(f"{c.name}: entity column {k} must be declared non-nullable")
        schemes = c.column(c.entity_column_pair[0]).allowed
        if not schemes or not set(schemes) <= set(ID_SCHEMES):
            raise ContractError(f"{c.name}: entity scheme column must allow only {ID_SCHEMES}")
    if c.resolves_entities:
        for k in IDENTITY_COLUMNS:
            if k not in names:
                raise ContractError(f"{c.name}: an identity dataset needs column {k}")
        if c.entity_column_pair != ("subject_scheme", "subject_value"):
            raise ContractError(f"{c.name}: identity datasets resolve on the subject columns")
        if not set(c.column("id_scheme").allowed or ("?",)) <= set(ID_SCHEMES):
            raise ContractError(f"{c.name}: id_scheme must allow only {ID_SCHEMES}")
        for k in IDENTITY_COLUMNS:
            if c.column(k).type != pa.string():
                raise ContractError(f"{c.name}.{k}: identity columns are strings")
    for col in c.columns:
        if pa.types.is_nested(col.type):
            raise ContractError(f"{c.name}.{col.name}: nested types are not supported")
    for k in c.partition_by:
        if k not in names:
            raise ContractError(f"{c.name}: partition_by column {k} not declared")
    for chk in c.checks:
        if chk not in ROW_CHECKS:
            raise ContractError(f"{c.name}: unknown check {chk}")
        for k in ROW_CHECKS[chk][0]:
            if k not in names:
                raise ContractError(f"{c.name}: check {chk} needs column {k}")


class BatchInvalid(ValueError):
    """A batch breaks its contract. reasons: list of short machine-readable strings."""

    def __init__(self, reasons: list[str]):
        super().__init__("; ".join(reasons))
        self.reasons = reasons


def _count(mask) -> int:
    return pc.sum(pc.cast(pc.fill_null(mask, False), pa.int64())).as_py() or 0


def _check_ocr_confidence(t: pa.Table) -> list[str]:
    """OCR rows need confidence, source_doc_id and source_page; any confidence is within 0..1."""
    is_ocr = pc.equal(t["extraction_method"], "ocr")
    conf = t["confidence"]
    out = []
    n = _count(pc.or_kleene(pc.less(conf, 0), pc.greater(conf, 1)))
    if n:
        out.append(f"confidence_out_of_range:{n}")
    for col in ("confidence", "source_doc_id", "source_page"):
        n = _count(pc.and_kleene(is_ocr, pc.is_null(t[col])))
        if n:
            out.append(f"ocr_lineage_missing:{col}:{n}")
    return out


# name -> (columns the check needs, check)
ROW_CHECKS = {
    "ocr_confidence": (
        ("extraction_method", "confidence", "source_doc_id", "source_page"),
        _check_ocr_confidence,
    ),
}


def conform(
    contract: Contract,
    table: pa.Table,
    *,
    now: datetime | None = None,
    future_tolerance_s: int = 300,
) -> pa.Table:
    """Cast table to the contract schema and run every row check. Raises BatchInvalid."""
    reasons: list[str] = []
    if len(set(table.column_names)) != len(table.column_names):
        raise BatchInvalid(["duplicate_columns"])
    expected = [c.name for c in contract.columns]
    extra = sorted(set(table.column_names) - set(expected))
    missing = sorted(set(expected) - set(table.column_names))
    if extra:
        reasons.append(f"unknown_columns:{','.join(extra)}")
    if missing:
        reasons.append(f"missing_columns:{','.join(missing)}")
    if reasons:
        raise BatchInvalid(reasons)
    if table.num_rows == 0:
        raise BatchInvalid(["empty_batch"])
    kt = table.schema.field("known_at").type
    if not pa.types.is_timestamp(kt):
        raise BatchInvalid(["wrong_type:known_at"])  # e.g. epoch integers: never guess the unit
    if kt.tz is None:
        raise BatchInvalid(["known_at_without_timezone"])
    if not pa.types.is_date(table.schema.field("valid_date").type):
        raise BatchInvalid(["wrong_type:valid_date"])
    for col in contract.columns:
        n = table[col.name].null_count
        if not col.nullable and n:
            reasons.append(f"nulls_in_required:{col.name}:{n}")
        if pa.types.is_decimal(col.type) and pa.types.is_floating(
            table.schema.field(col.name).type
        ):
            reasons.append(f"float_for_decimal:{col.name}")
    if reasons:
        raise BatchInvalid(reasons)
    try:
        table = table.select(expected).cast(contract.schema, safe=True)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError, ValueError) as exc:
        raise BatchInvalid([f"type_cast_failed:{exc}"]) from exc
    for col in contract.columns:
        arr = table[col.name]
        if col.allowed is not None:
            ok = pc.is_in(arr, value_set=pa.array(col.allowed, type=pa.string()))
            n = pc.sum(pc.cast(pc.invert(pc.fill_null(ok, True)), pa.int64())).as_py() or 0
            if n:
                reasons.append(f"value_not_allowed:{col.name}:{n}")
    now = now or datetime.now(UTC)
    limit = pa.scalar(
        now + timedelta(seconds=future_tolerance_s), type=pa.timestamp("us", tz="UTC")
    )
    n_future = pc.sum(pc.cast(pc.greater(table["known_at"], limit), pa.int64())).as_py() or 0
    if n_future:
        reasons.append(f"known_at_in_future:{n_future}")
    key = list(contract.natural_key) + ["known_at"]
    dup = table.group_by(key).aggregate([([], "count_all")])
    n_dup = pc.sum(pc.cast(pc.greater(dup["count_all"], 1), pa.int64())).as_py() or 0
    if n_dup:
        reasons.append(f"duplicate_natural_key:{n_dup}")
    for chk in contract.checks:
        reasons.extend(ROW_CHECKS[chk][1](table))
    if reasons:
        raise BatchInvalid(reasons)
    return table
