"""Schema-compatibility analysis used by CI release gates."""

from dataclasses import dataclass

from .models import PipelineSpec


@dataclass(frozen=True)
class CompatibilityReport:
    compatible: bool
    breaking_changes: tuple[str, ...]
    compatible_changes: tuple[str, ...]


def compare_specs(old: PipelineSpec, new: PipelineSpec) -> CompatibilityReport:
    breaking: list[str] = []
    compatible: list[str] = []
    old_columns = {column.name: column for column in old.columns}
    new_columns = {column.name: column for column in new.columns}

    for name, old_column in old_columns.items():
        if name not in new_columns:
            breaking.append(f"removed column: {name}")
            continue
        new_column = new_columns[name]
        if old_column.data_type != new_column.data_type:
            breaking.append(
                f"changed type: {name} ({old_column.data_type} -> {new_column.data_type})"
            )
        if old_column.nullable and not new_column.nullable:
            breaking.append(f"made column required: {name}")

    for name, column in new_columns.items():
        if name not in old_columns:
            message = f"added {'nullable' if column.nullable else 'required'} column: {name}"
            (compatible if column.nullable else breaking).append(message)

    if tuple(old.target.get("keys", [])) != tuple(new.target.get("keys", [])):
        breaking.append("changed target keys")
    if old.target.get("mode") != new.target.get("mode"):
        breaking.append("changed target write mode")

    return CompatibilityReport(not breaking, tuple(breaking), tuple(compatible))

