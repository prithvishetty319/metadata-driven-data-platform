import tempfile
import unittest
from pathlib import Path

import yaml

from metadata_platform import (
    PipelineRegistry,
    PipelineSpec,
    SpecValidationError,
    compare_specs,
    compile_platform,
)


def valid_raw(name="sample_pipeline"):
    return {
        "name": name,
        "owner": "data-platform@company.example",
        "schedule": "0 * * * *",
        "source": {"type": "adls_batch", "path": "abfss://landing/sample"},
        "target": {"catalog": "main", "schema": "silver", "table": name, "mode": "merge", "keys": ["record_id"]},
        "columns": [
            {"name": "record_id", "type": "string", "nullable": False, "classification": "restricted"},
            {"name": "value_text", "type": "string", "nullable": True, "classification": "internal"},
        ],
        "quality": [{"type": "not_null", "column": "record_id"}],
        "dependencies": [],
        "sla_minutes": 30,
    }


class CompilerTests(unittest.TestCase):
    def test_example_registry_has_three_pipelines(self):
        registry = PipelineRegistry.from_directory("pipelines")
        self.assertEqual(3, len(registry.specs))
        order = [spec.name for spec in registry.deployment_order()]
        self.assertLess(order.index("members_cdc"), order.index("claims_daily"))

    def test_invalid_name_is_rejected(self):
        raw = valid_raw("Unsafe-Name")
        with self.assertRaises(SpecValidationError):
            PipelineSpec.from_dict(raw)

    def test_merge_requires_keys(self):
        raw = valid_raw()
        raw["target"]["keys"] = []
        with self.assertRaises(SpecValidationError):
            PipelineSpec.from_dict(raw)

    def test_unknown_quality_column_is_rejected(self):
        raw = valid_raw()
        raw["quality"] = [{"type": "not_null", "column": "missing"}]
        with self.assertRaises(SpecValidationError):
            PipelineSpec.from_dict(raw)

    def test_dependency_cycle_is_rejected(self):
        first = valid_raw("first_pipeline")
        second = valid_raw("second_pipeline")
        first["dependencies"] = ["second_pipeline"]
        second["dependencies"] = ["first_pipeline"]
        registry = PipelineRegistry([PipelineSpec.from_dict(first), PipelineSpec.from_dict(second)])
        with self.assertRaises(SpecValidationError):
            registry.deployment_order()

    def test_unknown_dependency_is_rejected(self):
        raw = valid_raw()
        raw["dependencies"] = ["missing_pipeline"]
        with self.assertRaises(SpecValidationError):
            PipelineRegistry([PipelineSpec.from_dict(raw)])

    def test_nullable_addition_is_compatible(self):
        old = PipelineSpec.from_dict(valid_raw())
        new_raw = valid_raw()
        new_raw["columns"].append(
            {"name": "optional_note", "type": "string", "nullable": True, "classification": "internal"}
        )
        report = compare_specs(old, PipelineSpec.from_dict(new_raw))
        self.assertTrue(report.compatible)
        self.assertIn("added nullable column: optional_note", report.compatible_changes)

    def test_removed_column_is_breaking(self):
        old = PipelineSpec.from_dict(valid_raw())
        new_raw = valid_raw()
        new_raw["columns"] = new_raw["columns"][:1]
        report = compare_specs(old, PipelineSpec.from_dict(new_raw))
        self.assertFalse(report.compatible)
        self.assertIn("removed column: value_text", report.breaking_changes)

    def test_compile_is_deterministic_and_complete(self):
        registry = PipelineRegistry([PipelineSpec.from_dict(valid_raw())])
        with tempfile.TemporaryDirectory() as first_dir, tempfile.TemporaryDirectory() as second_dir:
            first = compile_platform(registry, first_dir)
            second = compile_platform(registry, second_dir)
            self.assertEqual(first["manifest_sha256"], second["manifest_sha256"])
            self.assertEqual(6, len(first["files"]))
            self.assertTrue((Path(first_dir) / "airflow" / "sample_pipeline_dag.py").exists())

    def test_yaml_round_trip_fixture(self):
        raw = valid_raw()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "sample.yml"
            path.write_text(yaml.safe_dump(raw), encoding="utf-8")
            registry = PipelineRegistry.from_directory(directory)
        self.assertIn("sample_pipeline", registry.specs)


if __name__ == "__main__":
    unittest.main()

