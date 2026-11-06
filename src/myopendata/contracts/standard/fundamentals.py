import pyarrow as pa

from myopendata.contracts.base import Column, Contract
from myopendata.contracts.standard._common import BITEMPORAL_COLUMNS, ID_SCHEMES

EXTRACTION_METHODS = ("xbrl_filed", "xbrl_inline", "ocr", "vendor_api", "register_api", "manual")
# frequency = the period the value covers. Durations: quarterly (~3 months), semiannual
# (~6 months, incl. US 10-Q six-month year-to-date), annual (~12 months). instant = a
# balance-sheet value at valid_date (period_start null), one key per balance date whatever the
# filing frequency of the source or region (2.0.0).
FREQUENCIES = ("annual", "semiannual", "quarterly", "instant")
BASES = ("consolidated", "parent")

FUNDAMENTALS = Contract(
    name="fundamentals",
    version="3.0.0",
    kind="fact",
    owner="my-open-data",
    licence="per-source; see source_code",
    doc=(
        "Reported (filed) financial facts as filed, one row per entity/tag/period/basis/source. "
        "Which tag answers which concept is a versioned reader recipe, never a column here. "
        "Multi-source by design (spec v3 §8): two sources that disagree are two rows; "
        "a restatement is the same key with a later known_at. "
        "Computed metrics are recipes, never rows here."
    ),
    columns=(
        Column("entity_scheme", pa.string(), nullable=False, allowed=ID_SCHEMES),
        Column("entity_value", pa.string(), nullable=False),
        Column(
            "original_tag",
            pa.string(),
            nullable=False,
            doc="Tag as filed, 'namespace:Tag' (us-gaap:Revenues).",
        ),
        Column(
            "period_start",
            pa.date32(),
            doc="First day of a duration; null exactly when frequency = instant.",
        ),
        Column(
            "frequency",
            pa.string(),
            nullable=False,
            allowed=FREQUENCIES,
            doc="Period covered: quarterly, semiannual, annual (durations) or instant.",
        ),
        Column("fiscal_year", pa.int32()),
        Column("basis", pa.string(), nullable=False, allowed=BASES),
        Column("value", pa.decimal128(38, 6), nullable=False),
        Column(
            "unit",
            pa.string(),
            nullable=False,
            doc="As filed: ISO 4217 currency, 'shares', per share e.g. 'USD/shares', or 'pure'.",
        ),
        Column("extraction_method", pa.string(), nullable=False, allowed=EXTRACTION_METHODS),
        Column("confidence", pa.float64(), doc="0..1; required when extraction_method = ocr."),
        Column("source_page", pa.int32(), doc="Page in source_doc_id (OCR)."),
        Column(
            "filing_ref",
            pa.string(),
            doc="The filing's identifier at its source (SEC accession), as filed.",
        ),
        *BITEMPORAL_COLUMNS,
    ),
    natural_key=(
        "entity_scheme",
        "entity_value",
        "original_tag",
        "valid_date",
        "frequency",
        "basis",
        "source_code",
    ),
    entity_column_pair=("entity_scheme", "entity_value"),
    partition_by=("source_code",),
    checks=("ocr_confidence", "instant_period"),
)
