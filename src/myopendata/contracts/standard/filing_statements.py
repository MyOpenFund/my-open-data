"""Standard contract: which tags a filing placed on which primary statement."""

import pyarrow as pa

from myopendata.contracts.base import Column, Contract
from myopendata.contracts.standard._common import BITEMPORAL_COLUMNS, ID_SCHEMES

STATEMENTS = ("BS", "IS", "CF")

FILING_STATEMENTS = Contract(
    name="filing_statements",
    version="1.0.0",
    kind="fact",
    owner="my-open-data",
    licence="per-source; see source_code",
    doc=(
        "Which tag appears on which primary statement of one filing. One row per tag per "
        "statement per filing. A tag absent here for a filing listed here was filed in the "
        "notes only. known_at is the filing's own: a position is known on filing day."
    ),
    columns=(
        Column("entity_scheme", pa.string(), nullable=False, allowed=ID_SCHEMES),
        Column("entity_value", pa.string(), nullable=False),
        Column(
            "filing_ref",
            pa.string(),
            nullable=False,
            doc="The filing's identifier at its source (SEC accession).",
        ),
        Column(
            "statement",
            pa.string(),
            nullable=False,
            allowed=STATEMENTS,
            doc="BS balance sheet, IS income statement, CF cash flow statement.",
        ),
        Column("tag", pa.string(), nullable=False, doc="Tag as filed, without namespace."),
        Column(
            "tag_version",
            pa.string(),
            doc="Taxonomy version as filed; equal to the filing's accession for a custom tag.",
        ),
        Column("plabel", pa.string(), doc="The filer's label for this line."),
        *BITEMPORAL_COLUMNS,
    ),
    natural_key=(
        "entity_scheme",
        "entity_value",
        "filing_ref",
        "statement",
        "tag",
        "valid_date",
        "source_code",
    ),
    entity_column_pair=("entity_scheme", "entity_value"),
    partition_by=("source_code",),
)
