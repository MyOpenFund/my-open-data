"""Dataset contracts: the one interface between producers and the store."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field

import pyarrow as pa


class ContractError(ValueError):
    """A contract breaks the common rules."""


@dataclass(frozen=True)
class Column:
    name: str
    type: pa.DataType
    nullable: bool = True
    allowed: tuple[str, ...] | None = None  # closed vocabulary for string columns
    doc: str = ""


@dataclass(frozen=True)
class Contract:
    """A versioned dataset definition.

    kind: "fact" (published from producers) or "derived" (saved recipe output).
    version: semver "MAJOR.MINOR.PATCH". MINOR may only add nullable columns; anything else is a
        new MAJOR, which is a new table (<name>_v<major>) and needs a fresh approval.
    natural_key: columns that identify one fact for one source; uniqueness is on
        natural_key + known_at (a restatement = same key, later known_at).
    entity_column_pair: (scheme column, value column) the publisher resolves to entity_id,
        or None for datasets that are not about an entity.
    resolves_entities: True for an identity dataset (shape of entity_observations): the publisher
        builds identifier links and SCD2 attributes from it, and publishes it before other data.
    """

    name: str
    version: str
    kind: str
    owner: str
    licence: str
    columns: tuple[Column, ...]
    natural_key: tuple[str, ...]
    entity_column_pair: tuple[str, str] | None = None
    partition_by: tuple[str, ...] = ()
    doc: str = ""
    checks: tuple[str, ...] = field(default=())  # names of extra row checks, see rules.ROW_CHECKS
    resolves_entities: bool = False

    @property
    def major(self) -> int:
        return int(self.version.split(".")[0])

    @property
    def table_name(self) -> str:
        return f"{self.name}_v{self.major}"

    @property
    def schema(self) -> pa.Schema:
        return pa.schema([pa.field(c.name, c.type, nullable=c.nullable) for c in self.columns])

    def column(self, name: str) -> Column:
        for c in self.columns:
            if c.name == name:
                return c
        raise KeyError(name)

    def fingerprint(self) -> str:
        """Stable hash of everything that defines the data or its terms (not doc strings)."""
        payload = {
            "licence": self.licence,
            "partition_by": list(self.partition_by),
            "name": self.name,
            "major": self.major,
            "kind": self.kind,
            "columns": [
                [c.name, str(c.type), c.nullable, list(c.allowed or ())] for c in self.columns
            ],
            "natural_key": list(self.natural_key),
            "entity_column_pair": list(self.entity_column_pair or ()),
            "checks": list(self.checks),
            "resolves_entities": self.resolves_entities,
        }
        raw = json.dumps(payload, sort_keys=True).encode()
        return hashlib.sha256(raw).hexdigest()
