import pyarrow as pa

from myopendata.contracts.base import Column, Contract
from myopendata.contracts.standard._common import BITEMPORAL_COLUMNS, ID_SCHEMES

ENTITY_OBSERVATIONS = Contract(
    name="entity_observations",
    version="1.0.0",
    kind="fact",
    owner="my-open-data",
    licence="per-source; see source_code",
    doc=(
        "One row = one identifier a producer observed for one subject entity, as of valid_date. "
        "The subject is named by the producer's own primary identifier (subject_scheme/value); "
        "the publisher links every identifier of a subject to one stable entity_id."
    ),
    columns=(
        Column("subject_scheme", pa.string(), nullable=False, allowed=ID_SCHEMES),
        Column("subject_value", pa.string(), nullable=False),
        Column("id_scheme", pa.string(), nullable=False, allowed=ID_SCHEMES),
        Column("id_value", pa.string(), nullable=False),
        Column("name", pa.string()),
        Column("country", pa.string(), doc="ISO 3166-1 alpha-2."),
        *BITEMPORAL_COLUMNS,
    ),
    natural_key=(
        "subject_scheme",
        "subject_value",
        "id_scheme",
        "id_value",
        "valid_date",
        "source_code",
    ),
    entity_column_pair=("subject_scheme", "subject_value"),
    resolves_entities=True,
)
