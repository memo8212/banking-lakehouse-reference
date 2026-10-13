"""Banking lakehouse configuration — the ONLY place names/paths live."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent


@dataclass
class Env:
    catalog: str = "bank_dev"
    storage: str = str(BASE / "lakehouse_storage")
    source_dir: str = str(BASE / "data")

    def layer(self, name: str) -> str:
        return f"{self.storage}/{name}"

    def table(self, layer: str, table: str) -> str:
        return f"{self.storage}/{layer}/{table}"

    def uc(self, schema: str, table: str) -> str:
        return f"{self.catalog}.{schema}.{table}"

    # layer paths
    @property
    def landing(self) -> str:
        return self.layer("landing")

    @property
    def bronze(self) -> str:
        return self.layer("bronze")

    @property
    def silver(self) -> str:
        return self.layer("silver")

    @property
    def gold(self) -> str:
        return self.layer("gold")

    @property
    def recon(self) -> str:
        return self.layer("recon")
