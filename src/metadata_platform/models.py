"""Typed validation for pipeline contracts."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .naming import require_safe_name


class SpecValidationError(ValueError):
    """Raised when a pipeline contract violates platform policy."""


ALLOWED_SOURCE_TYPES = {"postgres_cdc", "adls_batch", "kafka_stream"}
ALLOWED_MODES = {"merge", "append", "overwrite_partition"}
ALLOWED_TYPES = {"string", "integer", "long", "decimal", "date", "timestamp", "boolean"}
ALLOWED_CLASSIFICATIONS = {"public", "internal", "confidential", "restricted"}


@dataclass(frozen=True)
class Column:
    name: str
    data_type: str
    nullable: bool
    classification: str


@dataclass(frozen=True)
class PipelineSpec:
    name: str
    owner: str
    schedule: str
    source: dict[str, Any]
    target: dict[str, Any]
    columns: tuple[Column, ...]
    quality: tuple[dict[str, Any], ...]
    dependencies: tuple[str, ...]
    sla_minutes: int
    description: str

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "PipelineSpec":
        try:
            name = require_safe_name(raw["name"], "name")
            owner = str(raw["owner"]).strip()
            schedule = str(raw["schedule"]).strip()
            source = dict(raw["source"])
            target = dict(raw["target"])
            raw_columns = list(raw["columns"])
        except (KeyError, TypeError, ValueError) as exc:
            raise SpecValidationError(f"missing or invalid required field: {exc}") from exc

        if not owner or "@" not in owner:
            raise SpecValidationError("owner must be a team email")
        if not schedule:
            raise SpecValidationError("schedule cannot be empty")
        if source.get("type") not in ALLOWED_SOURCE_TYPES:
            raise SpecValidationError(f"unsupported source.type: {source.get('type')}")
        if target.get("mode") not in ALLOWED_MODES:
            raise SpecValidationError(f"unsupported target.mode: {target.get('mode')}")

        for required in ("catalog", "schema", "table"):
            if not target.get(required):
                raise SpecValidationError(f"target.{required} is required")

        columns: list[Column] = []
        names: set[str] = set()
        for item in raw_columns:
            try:
                column_name = require_safe_name(item["name"], "column.name")
                data_type = item["type"]
                nullable = item["nullable"]
                classification = item["classification"]
            except (KeyError, TypeError, ValueError) as exc:
                raise SpecValidationError(f"invalid column: {exc}") from exc
            if column_name in names:
                raise SpecValidationError(f"duplicate column: {column_name}")
            if data_type not in ALLOWED_TYPES:
                raise SpecValidationError(f"unsupported type for {column_name}: {data_type}")
            if not isinstance(nullable, bool):
                raise SpecValidationError(f"nullable must be boolean for {column_name}")
            if classification not in ALLOWED_CLASSIFICATIONS:
                raise SpecValidationError(f"invalid classification for {column_name}")
            names.add(column_name)
            columns.append(Column(column_name, data_type, nullable, classification))

        keys = tuple(target.get("keys", []))
        if target["mode"] == "merge" and not keys:
            raise SpecValidationError("merge targets require target.keys")
        if not set(keys).issubset(names):
            raise SpecValidationError("every target key must be declared as a column")
        if any(column.nullable for column in columns if column.name in keys):
            raise SpecValidationError("target key columns cannot be nullable")

        dependencies = tuple(raw.get("dependencies", []))
        try:
            dependencies = tuple(require_safe_name(value, "dependency") for value in dependencies)
        except ValueError as exc:
            raise SpecValidationError(str(exc)) from exc

        sla = raw.get("sla_minutes", 60)
        if not isinstance(sla, int) or sla <= 0:
            raise SpecValidationError("sla_minutes must be a positive integer")

        quality = tuple(raw.get("quality", []))
        for rule in quality:
            if not isinstance(rule, dict) or not rule.get("type"):
                raise SpecValidationError("each quality rule requires a type")
            if rule.get("column") and rule["column"] not in names:
                raise SpecValidationError(f"quality rule references unknown column: {rule['column']}")

        return cls(
            name=name,
            owner=owner,
            schedule=schedule,
            source=source,
            target=target,
            columns=tuple(columns),
            quality=quality,
            dependencies=dependencies,
            sla_minutes=sla,
            description=str(raw.get("description", "")).strip(),
        )


def load_spec(path: str | Path) -> PipelineSpec:
    with Path(path).open(encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)
    if not isinstance(raw, dict):
        raise SpecValidationError("pipeline specification must be a YAML mapping")
    return PipelineSpec.from_dict(raw)

