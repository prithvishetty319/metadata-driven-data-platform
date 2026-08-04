"""Deterministic compiler for data-platform deployment assets."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import yaml

from .models import PipelineSpec
from .registry import PipelineRegistry


def _write(path: Path, content: str) -> dict[str, str]:
    normalized = content.rstrip() + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(normalized, encoding="utf-8")
    return {
        "path": path.as_posix(),
        "sha256": hashlib.sha256(normalized.encode()).hexdigest(),
    }


def _airflow(spec: PipelineSpec) -> str:
    upstream = repr(list(spec.dependencies))
    return f'''"""Generated DAG for {spec.name}. Do not edit directly."""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

PIPELINE = "{spec.name}"
UPSTREAM_PIPELINES = {upstream}

def submit_spark_job(**context):
    """Replace with the organization's Databricks/EMR submission adapter."""
    print({{"pipeline": PIPELINE, "run_id": context.get("run_id")}})

with DAG(
    dag_id="mdp_{spec.name}",
    schedule="{spec.schedule}",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={{"owner": "{spec.owner}", "retries": 2,
                   "retry_delay": timedelta(minutes=5)}},
    tags=["metadata-driven", "generated"],
) as dag:
    run_pipeline = PythonOperator(
        task_id="submit_spark_job",
        python_callable=submit_spark_job,
    )
'''


def _spark(spec: PipelineSpec) -> str:
    keys = repr(list(spec.target.get("keys", [])))
    columns = repr([column.name for column in spec.columns])
    target = ".".join(spec.target[key] for key in ("catalog", "schema", "table"))
    return f'''"""Generated Spark entry point for {spec.name}."""
from pyspark.sql import SparkSession, functions as F

PIPELINE_NAME = "{spec.name}"
SOURCE_TYPE = "{spec.source['type']}"
TARGET_TABLE = "{target}"
WRITE_MODE = "{spec.target['mode']}"
KEYS = {keys}
COLUMNS = {columns}

def enforce_contract(frame):
    missing = sorted(set(COLUMNS) - set(frame.columns))
    if missing:
        raise ValueError(f"missing contract columns: {{missing}}")
    return frame.select(*COLUMNS).withColumn("_loaded_at", F.current_timestamp())

def run(spark: SparkSession, source_path: str):
    frame = enforce_contract(spark.read.format("delta").load(source_path))
    if WRITE_MODE == "merge":
        from delta.tables import DeltaTable
        target = DeltaTable.forName(spark, TARGET_TABLE)
        condition = " AND ".join([f"target.{{key}} = source.{{key}}" for key in KEYS])
        (target.alias("target").merge(frame.alias("source"), condition)
         .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    else:
        frame.write.format("delta").mode("append").saveAsTable(TARGET_TABLE)

if __name__ == "__main__":
    session = SparkSession.builder.appName(PIPELINE_NAME).getOrCreate()
    run(session, "{{{{ source_path }}}}")
'''


def _dbt_model(spec: PipelineSpec) -> str:
    names = ",\n    ".join(column.name for column in spec.columns)
    materialized = "incremental" if spec.target["mode"] in {"merge", "append"} else "table"
    unique_key = list(spec.target.get("keys", []))
    return f"""{{{{ config(materialized='{materialized}', unique_key={unique_key!r}) }}}}

select
    {names}
from {{{{ source('platform_raw', '{spec.name}') }}}}
{{% if is_incremental() %}}
where _loaded_at > (select coalesce(max(_loaded_at), '1900-01-01') from {{{{ this }}}})
{{% endif %}}
"""


def _dbt_schema(spec: PipelineSpec) -> str:
    columns: list[dict[str, Any]] = []
    keys = set(spec.target.get("keys", []))
    for column in spec.columns:
        tests = []
        if not column.nullable:
            tests.append("not_null")
        if column.name in keys and len(keys) == 1:
            tests.append("unique")
        columns.append(
            {
                "name": column.name,
                "description": f"Classification: {column.classification}",
                "data_tests": tests,
            }
        )
    return yaml.safe_dump(
        {"version": 2, "models": [{"name": spec.name, "description": spec.description, "columns": columns}]},
        sort_keys=False,
    )


def _lineage(spec: PipelineSpec) -> str:
    source_name = spec.source.get("table") or spec.source.get("topic") or spec.source.get("path")
    target = ".".join(spec.target[key] for key in ("catalog", "schema", "table"))
    payload = {
        "eventType": "COMPLETE",
        "job": {"namespace": "metadata-platform", "name": spec.name},
        "inputs": [{"namespace": spec.source["type"], "name": source_name}],
        "outputs": [{"namespace": "delta", "name": target}],
        "ownership": {"owner": spec.owner},
        "columnLineage": {column.name: column.name for column in spec.columns},
    }
    return json.dumps(payload, indent=2, sort_keys=True)


def _prometheus(spec: PipelineSpec) -> str:
    rules = [
        {
            "alert": f"{spec.name}_SlaBreach",
            "expr": f'mdp_pipeline_duration_minutes{{pipeline="{spec.name}"}} > {spec.sla_minutes}',
            "for": "5m",
            "labels": {"severity": "critical", "owner": spec.owner},
            "annotations": {"summary": f"{spec.name} exceeded its {spec.sla_minutes}-minute SLA"},
        },
        {
            "alert": f"{spec.name}_QualityFailure",
            "expr": f'mdp_quality_success{{pipeline="{spec.name}"}} == 0',
            "for": "2m",
            "labels": {"severity": "warning", "owner": spec.owner},
        },
    ]
    return yaml.safe_dump({"groups": [{"name": f"mdp_{spec.name}", "rules": rules}]}, sort_keys=False)


def compile_spec(spec: PipelineSpec, output: str | Path) -> list[dict[str, str]]:
    root = Path(output)
    files = [
        _write(root / "airflow" / f"{spec.name}_dag.py", _airflow(spec)),
        _write(root / "spark" / f"{spec.name}_job.py", _spark(spec)),
        _write(root / "dbt" / "models" / f"{spec.name}.sql", _dbt_model(spec)),
        _write(root / "dbt" / "models" / f"{spec.name}.yml", _dbt_schema(spec)),
        _write(root / "lineage" / f"{spec.name}.json", _lineage(spec)),
        _write(root / "monitoring" / f"{spec.name}.yml", _prometheus(spec)),
    ]
    return files


def compile_platform(registry: PipelineRegistry, output: str | Path) -> dict[str, Any]:
    root = Path(output)
    files: list[dict[str, str]] = []
    order = registry.deployment_order()
    for spec in order:
        files.extend(compile_spec(spec, root))

    # Store paths relative to the compilation root so manifests are reproducible
    # across developer machines, CI runners, and artifact staging directories.
    for item in files:
        item["path"] = Path(item["path"]).relative_to(root).as_posix()

    manifest_core = {
        "compiler_version": "1.0.0",
        "deployment_order": [spec.name for spec in order],
        "pipelines": len(order),
        "files": sorted(files, key=lambda item: item["path"]),
    }
    manifest_core["manifest_sha256"] = hashlib.sha256(
        json.dumps(manifest_core, sort_keys=True).encode()
    ).hexdigest()
    _write(root / "manifest.json", json.dumps(manifest_core, indent=2, sort_keys=True))
    return manifest_core
