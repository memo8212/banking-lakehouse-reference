"""Map a natural-language question to a metric (Genie-like, offline-first).

Heuristic keyword/synonym scoring; an LLM is used only if configured via env
(OPENAI_API_KEY / DATABRICKS_HOST+TOKEN) and always returns a metric name.
"""
from __future__ import annotations

import json
import os
import re
import urllib.request

from semantic.model import Model, render_sql


def _score(question: str, metric) -> float:
    q = question.lower()

    def has(term: str) -> bool:
        return re.search(rf"\b{re.escape(term)}\b", q) is not None

    s = 0.0
    for syn in metric.synonyms:
        if has(syn):
            s += len(syn.split())          # phrase match weighs more
    for w in metric.name.split("_"):
        if has(w):
            s += 1.0
    for w in re.findall(r"\w+", metric.description.lower()):
        if len(w) > 3 and has(w):
            s += 0.5
    return s


def match(question: str, model: Model):
    best, best_score = None, 0.0
    for m in model.metrics.values():
        sc = _score(question, m)
        if sc > best_score:
            best, best_score = m, sc
    return best, best_score


def _llm_metric(question: str, model: Model) -> str | None:
    if not (os.environ.get("OPENAI_API_KEY")
            or (os.environ.get("DATABRICKS_HOST") and os.environ.get("DATABRICKS_TOKEN"))):
        return None
    names = list(model.metrics)
    prompt = ("Pick the single best metric name for this question.\n"
              f"Question: {question}\nMetrics: {names}\nReply with ONLY the name.")
    try:
        if os.environ.get("OPENAI_API_KEY"):
            base = os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1").rstrip("/")
            url = base + "/chat/completions"
            model_name = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
            headers = {"Authorization": f"Bearer {os.environ['OPENAI_API_KEY']}",
                       "Content-Type": "application/json"}
        else:
            host = os.environ["DATABRICKS_HOST"].rstrip("/")
            model_name = os.environ.get("DATABRICKS_MODEL", "databricks-meta-llama-3-1-70b-instruct")
            url = f"{host}/serving-endpoints/{model_name}/invocations"
            headers = {"Authorization": f"Bearer {os.environ['DATABRICKS_TOKEN']}",
                       "Content-Type": "application/json"}
        body = json.dumps({"model": model_name, "temperature": 0, "max_tokens": 16,
                           "messages": [{"role": "user", "content": prompt}]}).encode()
        req = urllib.request.Request(url, data=body, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=20) as resp:  # noqa: S310
            text = json.loads(resp.read().decode())["choices"][0]["message"]["content"]
        name = text.strip().strip(".\"' ")
        return name if name in model.metrics else None
    except Exception:  # noqa: BLE001
        return None


def answer(question: str, model: Model, group_by: str | None = None) -> dict:
    llm = _llm_metric(question, model)
    metric, score = match(question, model)
    source = "heuristic"
    if llm:
        metric, source, score = model.metrics[llm], "llm", 1.0
    if metric is None:
        return {"question": question, "metric": None, "sql": None,
                "source": source, "score": 0.0}
    if group_by is None:                       # infer "by <dimension>" from the text
        q = question.lower()
        group_by = next((d for d in metric.dimensions if d.lower() in q), None)
    return {"question": question, "metric": metric.name, "source": source,
            "score": round(score, 2), "group_by": group_by,
            "sql": render_sql(model, metric, group_by=group_by)}
