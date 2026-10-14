"""Apply the semantic model to a Databricks AI/BI Genie space.

    python -m semantic.apply_genie --warehouse-id <id>            # create
    python -m semantic.apply_genie --space-id <id> --warehouse-id <id>   # update

Best-effort: builds a Genie space payload from the semantic model and calls the
SDK (`w.genie.create_space` / `update_space`). If the SDK/API isn't available
(e.g. local), it writes `semantic/generated/genie_space.json`, which you can paste
into / reference from the Genie UI.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from semantic.genie import genie_space
from semantic.model import load_model


def build_payload(space: dict) -> dict:
    """A Genie space payload derived from the semantic model (names + grounding)."""
    return {
        "version": 1,
        "display_name": space["name"],
        "description": "Banking gold metrics (semantic layer)",
        "instructions": {"text_instructions": [{"content": space["instructions"]}]},
        "config": {"sample_questions": [{"question": [q]} for q in space["sample_questions"]]},
        "data_sources": {"tables": [{"identifier": t} for t in space["trusted_tables"]]},
        "metrics": space["metrics"],
        "sql": space["sql"],
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Create/update a Genie space from the semantic layer")
    ap.add_argument("--warehouse-id", default=None, help="SQL warehouse id for Genie")
    ap.add_argument("--space-id", default=None, help="existing Genie space id (to update)")
    ap.add_argument("--name", default="bank_gold_genie")
    ap.add_argument("--out", default="semantic/generated/genie_space.json")
    args = ap.parse_args()

    space = genie_space(load_model(), name=args.name)
    payload = build_payload(space)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    print(f"Genie payload: {len(space['trusted_tables'])} tables, "
          f"{len(space['sample_questions'])} questions -> {out}")

    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        serialized = json.dumps(payload)
        if args.space_id:
            w.genie.update_space(space_id=args.space_id, serialized_space=serialized)
            print(f"[genie] updated space {args.space_id}")
        else:
            resp = w.genie.create_space(warehouse_id=args.warehouse_id,
                                        serialized_space=serialized)
            print(f"[genie] created space: {getattr(resp, 'space_id', resp)}")
        return 0
    except Exception as e:  # noqa: BLE001
        print(f"[genie] SDK/API not available here ({type(e).__name__}). "
              f"Apply {out} via the Databricks Genie UI or the API.")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
