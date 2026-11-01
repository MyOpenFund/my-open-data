"""Entity resolution: raw identifiers in, one stable entity_id out (spec §6, decision D3)."""

from __future__ import annotations

import hashlib

import duckdb
import pyarrow as pa

from myopendata.config import DEFAULT_RESOLVING_SCHEMES, DEFAULT_SINGLE_VALUED_SCHEMES

# Non-resolving schemes (tickers) never merge entities and never key the identifier links; they
# stay visible in the identity dataset itself. Policy comes from StoreConfig.
__all__ = ["DEFAULT_RESOLVING_SCHEMES", "DEFAULT_SINGLE_VALUED_SCHEMES"]


class EntityConflict(ValueError):
    pass


class UnresolvableSubject(ValueError):
    pass


def new_entity_id(scheme: str, value: str) -> str:
    return "ent_" + hashlib.sha256(f"{scheme}:{value}".encode()).hexdigest()[:16]


def load_links(con: duckdb.DuckDBPyConnection) -> dict[tuple[str, str], str]:
    rows = con.execute(
        "SELECT scheme, value, entity_id FROM lake.ref.entity_identifiers"
    ).fetchall()
    return {(s, v): e for s, v, e in rows}


def resolve_observations(
    obs: pa.Table,
    links: dict[tuple[str, str], str],
    resolving_schemes: tuple[str, ...],
    single_valued_schemes: tuple[str, ...] = DEFAULT_SINGLE_VALUED_SCHEMES,
) -> tuple[list[str], list[tuple]]:
    """Return (entity_id per row, new links [(entity_id, scheme, value, source_code, known_at)]).

    All resolving identifiers of the batch are grouped first (union-find), so the result does
    not depend on row order. A group touching two existing entities raises EntityConflict (no
    automatic merge), and so does an entity that would end up with two values of a
    single-valued scheme; a subject with no resolving identifier raises UnresolvableSubject.
    """
    rows = obs.to_pylist()
    parent: dict[tuple[str, str], tuple[str, str]] = {}

    def find(x):
        while parent.setdefault(x, x) != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    subject_ids: dict[tuple[str, str], set[tuple[str, str]]] = {}
    first_seen: dict[tuple[str, str], dict] = {}
    for r in rows:
        subj = (r["subject_scheme"], r["subject_value"])
        ids = subject_ids.setdefault(subj, set())
        for i in (subj, (r["id_scheme"], r["id_value"])):
            if i[0] in resolving_schemes:
                ids.add(i)
                if i not in first_seen or r["known_at"] < first_seen[i]["known_at"]:
                    first_seen[i] = r
    for subj, ids in subject_ids.items():
        if not ids:
            raise UnresolvableSubject(f"subject {subj[0]}:{subj[1]} has no resolving identifier")
        ids = sorted(ids)
        find(ids[0])
        for i in ids[1:]:
            parent[find(i)] = find(ids[0])
    groups: dict[tuple[str, str], list[tuple[str, str]]] = {}
    for i in parent:
        groups.setdefault(find(i), []).append(i)
    held_by: dict[str, set[tuple[str, str]]] = {}
    for i, e in links.items():
        held_by.setdefault(e, set()).add(i)
    entity_of: dict[tuple[str, str], str] = {}
    new_links: list[tuple] = []
    for members in groups.values():
        members.sort()
        known = {links[i] for i in members if i in links}
        if len(known) > 1:
            raise EntityConflict(f"identifiers {members} map to entities {sorted(known)}")
        eid = known.pop() if known else new_entity_id(*members[0])
        held = held_by.get(eid, set()) | set(members)
        held_by[eid] = held  # later groups of this batch see what this one attached
        for scheme in single_valued_schemes:
            values = sorted(v for sch, v in held if sch == scheme)
            if len(values) > 1:
                raise EntityConflict(f"entity {eid} would hold {scheme} values {values}")
        for i in members:
            entity_of[i] = eid
            if i not in links:
                r = first_seen[i]
                new_links.append((eid, i[0], i[1], r["source_code"], r["known_at"]))
    out = []
    for r in rows:
        subj = (r["subject_scheme"], r["subject_value"])
        out.append(entity_of[min(subject_ids[subj])])
    return out, new_links


def resolve_facts(
    table: pa.Table, pair: tuple[str, str], links: dict[tuple[str, str], str]
) -> tuple[list[str | None], int]:
    schemes, values = table[pair[0]].to_pylist(), table[pair[1]].to_pylist()
    out = [links.get((s, v)) for s, v in zip(schemes, values, strict=True)]
    return out, sum(1 for e in out if e is None)


def rebuild_attributes(
    con: duckdb.DuckDBPyConnection, entity_ids: set[str], observations_table: str
) -> None:
    """SCD2 on (name, country), rebuilt from every published observation of the given entities.

    Order-independent: a backfill of older history lands in the right interval (decision D4).
    Per valid_date, each attribute takes the latest known non-null value (ties: source_code).
    Consecutive equal values collapse. Runs inside the publishing transaction, after the insert.
    """
    if not entity_ids:
        return
    con.register("_eids", pa.table({"entity_id": sorted(entity_ids)}))
    rows = con.execute(
        "SELECT entity_id, valid_date,"
        " arg_max(name, (known_at, source_code)) FILTER (WHERE name IS NOT NULL),"
        " arg_max(country, (known_at, source_code)) FILTER (WHERE country IS NOT NULL),"
        " max(known_at), arg_max(source_code, known_at), arg_max(batch_id, known_at)"
        f" FROM {observations_table} WHERE entity_id IN (SELECT entity_id FROM _eids)"
        " AND (name IS NOT NULL OR country IS NOT NULL)"
        " GROUP BY entity_id, valid_date ORDER BY entity_id, valid_date"
    ).fetchall()
    con.unregister("_eids")
    intervals: list[list] = []
    for eid, vd, name, country, known_at, src, bid in rows:
        last = intervals[-1] if intervals and intervals[-1][0] == eid else None
        if last and (last[1], last[2]) == (name, country):
            continue
        if last:
            last[4] = vd
        intervals.append([eid, name, country, vd, None, known_at, src, bid])
    con.executemany(
        "DELETE FROM lake.ref.entity_attributes WHERE entity_id = ?", [[e] for e in entity_ids]
    )
    if intervals:
        con.executemany(
            "INSERT INTO lake.ref.entity_attributes VALUES (?, ?, ?, ?, ?, ?, ?, ?)", intervals
        )
