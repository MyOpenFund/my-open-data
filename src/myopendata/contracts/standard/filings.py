"""Standard contract: what each filing is, its form and the period it reports on."""

import dataclasses

import pyarrow as pa

from myopendata.contracts.base import Column, Contract
from myopendata.contracts.standard._common import BITEMPORAL_COLUMNS, ID_SCHEMES

FILINGS = Contract(
    name="filings",
    version="1.0.0",
    kind="fact",
    owner="my-open-data",
    licence="per-source; see source_code",
    doc=(
        "One row per filing a producer knows of: its form and the period it reports on, as "
        "filed at the source. valid_date is the filing date. Facts join on the entity and "
        "filing_ref."
    ),
    columns=(
        Column("entity_scheme", pa.string(), nullable=False, allowed=ID_SCHEMES),
        Column("entity_value", pa.string(), nullable=False),
        Column(
            "filing_ref",
            pa.string(),
            nullable=False,
            doc="The filing's identifier at its source (SEC accession), as in fundamentals. "
            "Co-registrants share an accession: join on the entity and filing_ref.",
        ),
        Column(
            "form",
            pa.string(),
            nullable=False,
            doc="Form as filed at the source, amendments included (10-K, 10-K/A, 10-KT).",
        ),
        Column(
            "report_date",
            pa.date32(),
            doc="The period the filing reports on, as stated by the source (SEC reportDate).",
        ),
        *(
            dataclasses.replace(c, doc="Filing date: the day the source received the filing.")
            if c.name == "valid_date"
            else c
            for c in BITEMPORAL_COLUMNS
        ),
    ),
    natural_key=(
        "entity_scheme",
        "entity_value",
        "filing_ref",
        "valid_date",
        "source_code",
    ),
    entity_column_pair=("entity_scheme", "entity_value"),
    partition_by=("source_code",),
)
