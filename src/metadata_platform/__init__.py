"""Metadata-driven data-platform compiler."""

from .compiler import compile_platform, compile_spec
from .diff import CompatibilityReport, compare_specs
from .models import PipelineSpec, SpecValidationError, load_spec
from .registry import PipelineRegistry

__all__ = [
    "CompatibilityReport",
    "PipelineRegistry",
    "PipelineSpec",
    "SpecValidationError",
    "compare_specs",
    "compile_platform",
    "compile_spec",
    "load_spec",
]

