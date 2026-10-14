"""Load the semantic model and render metric SQL (deterministic)."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
METRICS = HERE / "metrics.yaml"


@dataclass
class Metric:
    name: str
    description: str
    table: str
    measure: str
    dimensions: list = field(default_factory=list)
    synonyms: list = field(default_factory=list)
    question: str = ""
    filter: str = ""

    def full_table(self, catalog: str) -> str:
        return f"{catalog}.{self.table}"


@dataclass
class Model:
    catalog: str
    metrics: dict = field(default_factory=dict)

    def get(self, name: str) -> Metric:
        return self.metrics[name]


def load_model(path: str | Path = METRICS) -> Model:
    doc = yaml.safe_load(Path(path).read_text())
    catalog = doc.get("catalog", "bank_dev")
    metrics = {}
    for m in doc.get("metrics", []):
        metrics[m["name"]] = Metric(
            name=m["name"], description=m.get("description", ""),
            table=m["table"], measure=m["measure"],
            dimensions=m.get("dimensions", []), synonyms=[s.lower() for s in m.get("synonyms", [])],
            question=m.get("question", ""), filter=m.get("filter", ""),
        )
    return Model(catalog=catalog, metrics=metrics)


def render_sql(model: Model, metric: Metric, group_by: str | None = None,
               limit: int = 1000) -> str:
    """Render a metric as SQL (single table + measure [+ optional group by])."""
    if group_by and group_by not in metric.dimensions:
        raise ValueError(f"'{group_by}' is not a dimension of metric '{metric.name}' "
                         f"(allowed: {metric.dimensions})")
    select = f"{metric.measure} AS {metric.name}"
    if group_by:
        select = f"{group_by}, {select}"
    sql = [f"SELECT {select}", f"FROM   {metric.full_table(model.catalog)}"]
    if metric.filter:
        sql.append(f"WHERE  {metric.filter}")
    if group_by:
        sql.append(f"GROUP  BY {group_by}")
        sql.append("ORDER  BY 2 DESC")
    sql.append(f"LIMIT  {limit}")
    return "\n".join(sql) + ";"
