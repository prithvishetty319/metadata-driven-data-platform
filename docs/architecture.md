# Architecture decisions

## Control plane versus data plane

The compiler is a control-plane service. It validates metadata and produces immutable deployment assets; it does not move production data itself. Generated Airflow and Spark entry points belong to the data plane.

## Deterministic compilation

Every generated asset is normalized and SHA-256 hashed. The manifest includes the ordered file inventory and its own content-derived checksum. Timestamps are excluded so identical inputs remain identical across builds.

## Policy enforcement

Validation happens before generation. Names must be safe identifiers, merge targets require non-null keys, referenced quality columns must exist, classifications must be approved, dependencies must resolve, and cycles are rejected.

## Extension model

Add another generator function for a new deployment target, then include its artifact in `compile_spec`. The contract and registry stay independent of cloud vendor SDKs.

