"""Entity resolution: raw identifiers in, one stable entity_id out (spec §6)."""

from __future__ import annotations

import hashlib

import duckdb
import pyarrow as pa

# Schemes trusted to identify an entity across sources. Tickers are recorded but never used to
# merge entities (they get reused). Configurable: pass resolving_schemes to the publisher.
DEFAULT_RESOLVING_SCHEMES = ("cik", "lei", "isin", "figi", "cb_code", "national_id")


class EntityConflict(ValueError):
    pass


def new_entity_id(scheme: str, value: str) -> str:
    return "ent_" + hashlib.sha256(f"{scheme}:{value}".encode()).hexdigest()[:16]


def load_links(con: duckdb.DuckDBPyConnection) -> dict[tuple[str, str], str]:
    rows = con.execute(
        "SELECT scheme, value, entity_id FROM lake.ref.entity_identifiers"
    ).fetchall()
    return {(s, v): e for s, v, e in rows}


def resolve_observations(
    obs: pa.Table, links: dict[tuple[str, str], str], resolving_schemes: tuple[str, ...]
) -> tuple[list[str], list[tuple]]:
    """Return (entity_id per row, new links [(entity_id, scheme, value, source_code, known_at)]).

    Raises EntityConflict when one subject's identifiers already belong to two entities.
    """
    links = dict(links)
    rows = obs.to_pylist()
    by_subject: dict[tuple[str, str], list[dict]] = {}
    for r in rows:
        by_subject.setdefault((r["subject_scheme"], r["subject_value"]), []).append(r)
    subject_entity: dict[tuple[str, str], str] = {}
    new_links: list[tuple] = []
    for subj, rs in sorted(by_subject.items()):
        ids = {subj} | {
            (r["id_scheme"], r["id_value"]) for r in rs if r["id_scheme"] in resolving_schemes
        }
        known = {links[i] for i in ids if i in links}
        if len(known) > 1:
            raise EntityConflict(f"subject {subj[0]}:{subj[1]} maps to entities {sorted(known)}")
        eid = known.pop() if known else new_entity_id(*subj)
        subject_entity[subj] = eid
        first = min(rs, key=lambda r: r["known_at"])
        for i in sorted(ids):
            if i not in links:
                links[i] = eid
                new_links.append((eid, i[0], i[1], first["source_code"], first["known_at"]))
    return [subject_entity[(r["subject_scheme"], r["subject_value"])] for r in rows], new_links


def resolve_facts(
    table: pa.Table, pair: tuple[str, str], links: dict[tuple[str, str], str]
) -> tuple[list[str | None], int]:
    schemes, values = table[pair[0]].to_pylist(), table[pair[1]].to_pylist()
    out = [links.get((s, v)) for s, v in zip(schemes, values, strict=True)]
    return out, sum(1 for e in out if e is None)


def rebuild_attributes(con: duckdb.DuckDBPyConnection, entity_ids: set[str]) -> None:
    """SCD2 on (name, country), rebuilt from every published observation of each entity.

    Order-independent: a backfill of older history lands in the right interval. Per valid_date
    the latest known_at wins, ties broken by source_code. Consecutive equal values collapse.
    Must run inside the publishing transaction, after the batch is inserted.
    """
    for eid in sorted(entity_ids):
        rows = con.execute(
            "SELECT valid_date, name, country, known_at, source_code, batch_id FROM ("
            " SELECT *, row_number() OVER (PARTITION BY valid_date"
            "   ORDER BY known_at DESC, source_code) AS rn"
            " FROM lake.facts.entity_observations_v1"
            " WHERE entity_id = ? AND (name IS NOT NULL OR country IS NOT NULL))"
            " WHERE rn = 1 ORDER BY valid_date",
            [eid],
        ).fetchall()
        intervals: list[list] = []
        for vd, name, country, known_at, src, bid in rows:
            if intervals and (intervals[-1][1], intervals[-1][2]) == (name, country):
                continue
            if intervals:
                intervals[-1][4] = vd
            intervals.append([eid, name, country, vd, None, known_at, src, bid])
        con.execute("DELETE FROM lake.ref.entity_attributes WHERE entity_id = ?", [eid])
        if intervals:
            con.executemany(
                "INSERT INTO lake.ref.entity_attributes VALUES (?, ?, ?, ?, ?, ?, ?, ?)", intervals
            )
