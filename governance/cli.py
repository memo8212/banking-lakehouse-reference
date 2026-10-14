"""Governance CLI: render/apply access + Delta Sharing as code.

    python -m governance access [--out governance/generated/access.sql] [--execute]
    python -m governance share  [--out governance/generated/sharing.sql]
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from governance.access import apply_access, load_access, render_access_sql
from governance.sharing import apply_sharing, load_sharing, render_sharing_sql, share_manifest


def cmd_access(args) -> int:
    access = load_access(args.file) if args.file else load_access()
    sql = render_access_sql(access)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(sql, encoding="utf-8")
    print(sql)
    print(f"-> {out}")
    if args.execute:
        from lakehouse.cli import make_spark
        spark = make_spark("governance_access")
        n = apply_access(spark, access)
        spark.stop()
        print(f"applied {n} statements")
    return 0


def cmd_share(args) -> int:
    sharing = load_sharing(args.file) if args.file else load_sharing()
    sql = render_sharing_sql(sharing)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(sql, encoding="utf-8")
    manifest = share_manifest(sharing)
    (out.parent / "sharing_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    print(sql)
    print("manifest:", json.dumps(manifest, sort_keys=True))
    print(f"-> {out}")
    if args.execute:
        from lakehouse.cli import make_spark
        spark = make_spark("governance_share")
        n = apply_sharing(spark, sharing)
        spark.stop()
        print(f"applied {n} statements")
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="governance", description="Governance as code")
    sub = ap.add_subparsers(dest="cmd", required=True)

    a = sub.add_parser("access", help="render/apply grants, tags, retention, masks")
    a.add_argument("--file", default=None)
    a.add_argument("--out", default="governance/generated/access.sql")
    a.add_argument("--execute", action="store_true")
    a.set_defaults(func=cmd_access)

    s = sub.add_parser("share", help="render Delta Sharing SQL + manifest")
    s.add_argument("--file", default=None)
    s.add_argument("--out", default="governance/generated/sharing.sql")
    s.add_argument("--execute", action="store_true")
    s.set_defaults(func=cmd_share)

    args = ap.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
