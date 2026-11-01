import pyarrow as pa

from myopendata.contracts.base import Column
from myopendata.contracts.rules import ID_SCHEMES, KNOWN_AT_KINDS

__all__ = ["BITEMPORAL_COLUMNS", "ID_SCHEMES"]

BITEMPORAL_COLUMNS = (
    Column(
        "valid_date",
        pa.date32(),
        nullable=False,
        doc="Date the fact is about (period end, quote date).",
    ),
    Column(
        "known_at",
        pa.timestamp("us", tz="UTC"),
        nullable=False,
        doc="When the fact became knowable: publication time if known, else collection time.",
    ),
    Column("known_at_kind", pa.string(), nullable=False, allowed=KNOWN_AT_KINDS),
    Column(
        "source_code", pa.string(), nullable=False, doc="Producer's source code (sec, esef, ...)."
    ),
    Column("source_doc_id", pa.string(), doc="vault documents.doc_id the fact was extracted from."),
)
