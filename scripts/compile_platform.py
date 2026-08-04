#!/usr/bin/env python3
"""Compile an entire specification registry."""

import argparse
import json

from metadata_platform import PipelineRegistry, compile_platform


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--spec-dir", default="pipelines")
    parser.add_argument("--output", default="artifacts/generated")
    args = parser.parse_args()
    registry = PipelineRegistry.from_directory(args.spec_dir)
    print(json.dumps(compile_platform(registry, args.output), indent=2))


if __name__ == "__main__":
    main()

