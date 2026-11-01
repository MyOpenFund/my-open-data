import pyarrow as pa

from myopendata.contracts.base import Column, Contract
from myopendata.contracts.standard._common import BITEMPORAL_COLUMNS, ID_SCHEMES

EXTRACTION_METHODS = ("xbrl_filed", "xbrl_inline", "ocr", "vendor_api", "register_api", "manual")
FREQUENCIES = ("annual", "semiannual", "quarterly")
BASES = ("consolidated", "parent")

FUNDAMENTALS = Contract(
    name="fundamentals",
    version="1.0.0",
    kind="fact",
    owner="my-open-data",
    licence="per-source; see source_code",
    doc=(
        "Reported (filed) financial facts, one row per entity/concept/period/basis/source. "
        "Multi-source by design (spec v3 §8): two sources that disagree are two rows; "
        "a restatement "
        "is the same key with a later known_at. Computed metrics are recipes, never rows here."
    ),
    columns=(
        Column("entity_scheme", pa.string(), nullable=False, allowed=ID_SCHEMES),
        Column("entity_value", pa.string(), nullable=False),
        Column(
            "concept", pa.string(), nullable=False, doc="Normalised concept (us-gaap/ifrs mapping)."
        ),
        Column("original_tag", pa.string(), doc="Tag or label as filed."),
        Column("period_start", pa.date32(), doc="Null for instant (balance-sheet) facts."),
        Column("frequency", pa.string(), nullable=False, allowed=FREQUENCIES),
        Column("fiscal_year", pa.int32()),
        Column("basis", pa.string(), nullable=False, allowed=BASES),
        Column("value", pa.decimal128(38, 6), nullable=False),
        Column("unit", pa.string(), nullable=False, doc="ISO 4217 currency, 'shares' or 'pure'."),
        Column("extraction_method", pa.string(), nullable=False, allowed=EXTRACTION_METHODS),
        Column("confidence", pa.float64(), doc="0..1; required when extraction_method = ocr."),
        Column("source_page", pa.int32(), doc="Page in source_doc_id (OCR)."),
        *BITEMPORAL_COLUMNS,
    ),
    natural_key=(
        "entity_scheme",
        "entity_value",
        "concept",
        "valid_date",
        "frequency",
        "basis",
        "source_code",
    ),
    entity_column_pair=("entity_scheme", "entity_value"),
    partition_by=("source_code",),
    checks=("ocr_confidence",),
)
