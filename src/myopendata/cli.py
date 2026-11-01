"""myopendata CLI: contracts list|approve, publish."""

from __future__ import annotations

import argparse
import sys

from myopendata.config import StoreConfig
from myopendata.contracts.registry import discover_contracts


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="myopendata")
    sub = p.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("contracts")
    csub = c.add_subparsers(dest="action", required=True)
    csub.add_parser("list")
    ap = csub.add_parser("approve")
    ap.add_argument("name")
    ap.add_argument("major", type=int)
    ap.add_argument("--by", required=True)
    ap.add_argument(
        "--force",
        action="store_true",
        help="approve a non-additive change within a major (rarely right)",
    )
    sub.add_parser("publish")
    a = p.parse_args(argv)

    if a.cmd == "contracts" and a.action == "list":
        for (name, major), ct in sorted(discover_contracts().items()):
            print(f"{name}_v{major}\t{ct.version}\t{ct.owner}\t{ct.fingerprint()[:12]}")
        return 0
    try:
        cfg = StoreConfig.from_env()
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    if a.cmd == "contracts" and a.action == "approve":
        from myopendata.publisher import approve_contract

        ct = discover_contracts().get((a.name, a.major))
        if ct is None:
            print(f"error: unknown contract {a.name}_v{a.major}", file=sys.stderr)
            return 1
        from myopendata.publisher import ApprovalRefused

        try:
            state = approve_contract(cfg, ct, a.by, force=a.force)
        except ApprovalRefused as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        print(f"{state} {a.name}_v{a.major} {ct.version} {ct.fingerprint()[:12]}")
        return 0
    from myopendata.publisher import publish_pending

    rep = publish_pending(cfg)
    print(rep.to_json())
    return rep.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
