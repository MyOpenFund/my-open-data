"""Discover contracts through the "myopendata.contracts" entry-point group (spec §6.1)."""

from __future__ import annotations

from importlib.metadata import entry_points

from myopendata.contracts.base import Contract, ContractError
from myopendata.contracts.rules import validate_contract

GROUP = "myopendata.contracts"


def discover_contracts(extra: list[Contract] | None = None) -> dict[tuple[str, int], Contract]:
    """Return {(name, major): contract} for every installed contract, validated.

    Each entry point is a callable returning a list of Contract. Two different definitions of the
    same (name, major) are an error: a dataset version has exactly one owner.
    """
    found: list[Contract] = []
    for ep in entry_points(group=GROUP):
        found.extend(ep.load()())
    found.extend(extra or [])
    out: dict[tuple[str, int], Contract] = {}
    for c in found:
        validate_contract(c)
        key = (c.name, c.major)
        if key in out and out[key].fingerprint() != c.fingerprint():
            raise ContractError(f"{c.name} v{c.major} declared twice with different definitions")
        if key not in out or _semver(c.version) > _semver(out[key].version):
            out[key] = c  # same definition, several patch versions: keep the highest
    return out


def _semver(v: str) -> tuple[int, ...]:
    return tuple(int(x) for x in v.split("."))
