"""Pipeline registry and dependency-graph validation."""

from __future__ import annotations

from pathlib import Path

from .models import PipelineSpec, SpecValidationError, load_spec


class PipelineRegistry:
    def __init__(self, specs: list[PipelineSpec]):
        self.specs = {spec.name: spec for spec in specs}
        if len(self.specs) != len(specs):
            raise SpecValidationError("pipeline names must be unique")
        for spec in specs:
            missing = set(spec.dependencies) - self.specs.keys()
            if missing:
                raise SpecValidationError(f"{spec.name} has unknown dependencies: {sorted(missing)}")

    @classmethod
    def from_directory(cls, directory: str | Path) -> "PipelineRegistry":
        paths = sorted(Path(directory).glob("*.yml")) + sorted(Path(directory).glob("*.yaml"))
        if not paths:
            raise SpecValidationError(f"no YAML pipeline specifications found in {directory}")
        return cls([load_spec(path) for path in paths])

    def deployment_order(self) -> list[PipelineSpec]:
        temporary: set[str] = set()
        permanent: set[str] = set()
        ordered: list[PipelineSpec] = []

        def visit(name: str) -> None:
            if name in permanent:
                return
            if name in temporary:
                raise SpecValidationError(f"dependency cycle detected at {name}")
            temporary.add(name)
            for dependency in sorted(self.specs[name].dependencies):
                visit(dependency)
            temporary.remove(name)
            permanent.add(name)
            ordered.append(self.specs[name])

        for name in sorted(self.specs):
            visit(name)
        return ordered

