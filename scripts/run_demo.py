#!/usr/bin/env python3
"""Compile, syntax-check, and prove deterministic generation."""

import json
import py_compile
import tempfile
from pathlib import Path

from metadata_platform import PipelineRegistry, compile_platform


def main() -> None:
    registry = PipelineRegistry.from_directory("pipelines")
    manifest = compile_platform(registry, "artifacts/generated")

    generated_python = sorted(Path("artifacts/generated").rglob("*.py"))
    for path in generated_python:
        py_compile.compile(str(path), doraise=True)

    with tempfile.TemporaryDirectory() as temporary:
        second = compile_platform(registry, temporary)
    deterministic = manifest["manifest_sha256"] == second["manifest_sha256"]
    if not deterministic:
        raise RuntimeError("compilation is not deterministic")

    summary = {
        "pipelines_compiled": manifest["pipelines"],
        "generated_assets": len(manifest["files"]),
        "generated_python_checked": len(generated_python),
        "deployment_order": manifest["deployment_order"],
        "deterministic": deterministic,
        "manifest_sha256": manifest["manifest_sha256"],
    }
    Path("artifacts").mkdir(exist_ok=True)
    Path("artifacts/demo_summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()

