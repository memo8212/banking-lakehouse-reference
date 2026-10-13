"""CLI: run the banking lakehouse medallion locally (Spark + Delta).

    python -m lakehouse run           # bronze → silver → gold → reconcile
    python -m lakehouse reconcile     # reconciliation only
"""
from __future__ import annotations

import argparse
from pathlib import Path

from lakehouse.config import Env
from lakehouse.reconcile import reconcile, report_md


def make_spark(app: str = "bank_lakehouse"):
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession
    builder = (SparkSession.builder.master("local[*]").appName(app)
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog",
                       "org.apache.spark.sql.delta.catalog.DeltaCatalog")
               .config("spark.sql.shuffle.partitions", "8")
               .config("spark.ui.enabled", "false"))
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def cmd_run(args) -> int:
    from lakehouse.pipeline import run
    env = Env(catalog=args.catalog)
    spark = make_spark()
    result = run(spark, env)
    out = Path(env.recon)
    out.mkdir(parents=True, exist_ok=True)
    (out / "reconciliation_report.md").write_text(report_md(result["recon"]), encoding="utf-8")

    print("Bronze:", result["bronze"])
    print("Silver:", {k: (v.get("good_rows"), v.get("rejected_rows")) for k, v in result["silver"].items()})
    print("Gold:", result["gold"])
    passed = sum(1 for r in result["recon"] if r["status"] == "PASSED")
    print(f"Reconcile: {passed}/{len(result['recon'])} passed")
    print(report_md(result["recon"]))
    print(f"\n-> {out / 'reconciliation_report.md'}")
    spark.stop()
    return 0 if passed == len(result["recon"]) else 1


def cmd_reconcile(args) -> int:
    env = Env(catalog=args.catalog)
    spark = make_spark()
    results = reconcile(spark, env)
    print(report_md(results))
    spark.stop()
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="lakehouse",
                                 description="Banking lakehouse reference pipeline")
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="run bronze -> silver -> gold -> reconcile")
    r.add_argument("--catalog", default="bank_dev")
    r.set_defaults(func=cmd_run)
    rc = sub.add_parser("reconcile", help="reconciliation only")
    rc.add_argument("--catalog", default="bank_dev")
    rc.set_defaults(func=cmd_reconcile)
    args = ap.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
