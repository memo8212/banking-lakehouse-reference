"""Data contracts: a feed's schema, types, PII and quality rules.

A contract is a small YAML file describing one source feed. It drives:
  * schema validation (expected columns + types),
  * data-quality rules (not-null, allowed values, ranges),
  * PII handling (which columns to mask/tokenize in Silver),
  * the target layer/table and primary key.

Loaded by `load_contracts()`; validated with `validate(df, contract)`.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

CONTRACTS_DIR = Path(__file__).resolve().parent.parent / "contracts"

_TYPE = {"string": "string", "int": "int", "long": "bigint", "double": "double",
         "decimal": "decimal", "date": "date", "timestamp": "timestamp", "boolean": "boolean"}


@dataclass
class Contract:
    name: str
    source: str
    target_layer: str
    target_table: str
    primary_key: str
    columns: dict = field(default_factory=dict)      # name -> type
    not_null: list = field(default_factory=list)
    allowed: dict = field(default_factory=dict)      # column -> [values]
    pii: dict = field(default_factory=dict)          # column -> mask | tokenize

    @property
    def target(self) -> str:
        return f"{self.target_layer}/{self.target_table}"


def _yaml():
    import yaml
    return yaml


def load_contracts(directory: str | Path = CONTRACTS_DIR) -> dict[str, Contract]:
    y = _yaml()
    out: dict[str, Contract] = {}
    for path in sorted(Path(directory).glob("*.contract.yaml")):
        doc = y.safe_load(path.read_text())
        c = Contract(
            name=doc["name"], source=doc["source"],
            target_layer=doc.get("target_layer", "silver"),
            target_table=doc.get("target_table", doc["name"]),
            primary_key=doc["primary_key"],
            columns={c["name"]: _TYPE.get(c.get("type", "string"), "string")
                     for c in doc.get("columns", [])},
            not_null=doc.get("not_null", []),
            allowed=doc.get("allowed", {}),
            pii=doc.get("pii", {}),
        )
        out[c.name] = c
    return out


def validate(df, contract: Contract) -> dict:
    """Return a validation report {missing, extra, type_errors, nulls, allowed}."""
    present = set(df.columns)
    expected = set(contract.columns)
    missing = sorted(expected - present)
    extra = sorted(present - expected)

    report = {"missing_columns": missing, "extra_columns": extra,
              "nulls": {}, "bad_values": {}, "rows": df.count()}

    from pyspark.sql import functions as F
    for col in contract.not_null:
        if col in present:
            n = df.filter(F.col(col).isNull()).count()
            if n:
                report["nulls"][col] = n
    for col, allowed in contract.allowed.items():
        if col in present:
            n = df.filter(~F.col(col).isin(*allowed) | F.col(col).isNull()).count()
            if n:
                report["bad_values"][col] = n
    report["passed"] = not missing and not report["nulls"] and not report["bad_values"]
    return report
