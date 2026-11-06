"""Standard contracts owned by the library: domains several producers feed (spec §8)."""

from myopendata.contracts.standard.entity_observations import ENTITY_OBSERVATIONS
from myopendata.contracts.standard.filing_statements import FILING_STATEMENTS
from myopendata.contracts.standard.fundamentals import FUNDAMENTALS


def contracts():
    return [ENTITY_OBSERVATIONS, FILING_STATEMENTS, FUNDAMENTALS]
