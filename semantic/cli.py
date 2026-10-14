"""Semantic layer CLI.

    python -m semantic metrics
    python -m semantic sql total_card_spend --by segment
    python -m semantic genie --out semantic/generated/genie_space.json
    python -m semantic ask "what is card spend by segment?" --run
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from semantic.ask import answer
from semantic.execute import execute
from semantic.genie import genie_space
from semantic.model import load_model, render_sql


def cmd_metrics(args) -> int:
    model = load_model()
    print(f"Semantic model — {len(model.metrics)} metrics (catalog {model.catalog}):\n")
    for m in model.metrics.values():
        print(f"- {m.name}: {m.description}")
        print(f"    table={m.table} measure={m.measure} dims={m.dimensions}")
    return 0


def cmd_sql(args) -> int:
    model = load_model()
    if args.metric not in model.metrics:
        print(f"unknown metric '{args.metric}'. Try: {', '.join(model.metrics)}")
        return 2
    print(render_sql(model, model.get(args.metric), group_by=args.by))
    return 0


def cmd_genie(args) -> int:
    model = load_model()
    space = genie_space(model)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(space, indent=2, sort_keys=True), encoding="utf-8")
    print(f"Genie space '{space['name']}': {len(space['trusted_tables'])} trusted tables, "
          f"{len(space['sample_questions'])} sample questions, {len(space['sql'])} metric SQL snippets")
    print(f"-> {out}")
    return 0


def cmd_ask(args) -> int:
    model = load_model()
    res = answer(args.question, model, group_by=args.by)
    print(f"Q: {res['question']}")
    print(f"metric: {res['metric']}  (source={res['source']}, score={res['score']})")
    if not res["metric"]:
        print("No matching metric. Try rephrasing or see `python -m semantic metrics`.")
        return 1
    print("\nSQL:\n" + res["sql"])
    if args.run:
        gb = args.by or res.get("group_by")
        df = execute(args.storage, model, model.get(res["metric"]), group_by=gb)
        print("\nResult:\n" + (df.to_string(index=False) if not df.empty
                               else "(no data — run `python -m lakehouse run` first)"))
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="semantic", description="Semantic layer + Genie space")
    sub = ap.add_subparsers(dest="cmd", required=True)

    m = sub.add_parser("metrics", help="list metrics")
    m.set_defaults(func=cmd_metrics)

    s = sub.add_parser("sql", help="render a metric's SQL")
    s.add_argument("metric")
    s.add_argument("--by", default=None, help="group-by dimension")
    s.set_defaults(func=cmd_sql)

    g = sub.add_parser("genie", help="generate a Genie space config")
    g.add_argument("--out", default="semantic/generated/genie_space.json")
    g.set_defaults(func=cmd_genie)

    a = sub.add_parser("ask", help="answer a natural-language question")
    a.add_argument("question")
    a.add_argument("--by", default=None)
    a.add_argument("--run", action="store_true", help="execute against local gold")
    a.add_argument("--storage", default="lakehouse_storage")
    a.set_defaults(func=cmd_ask)

    args = ap.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
