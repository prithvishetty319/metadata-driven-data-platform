# Onboard a pipeline

1. Copy one specification from `pipelines/` and choose a safe, unique name.
2. Declare ownership, SLA, source, target, contract columns, classification, quality rules, and dependencies.
3. Run `make test` and `make demo` locally.
4. Review the generated manifest and assets under `artifacts/generated/`.
5. Open a pull request. CODEOWNERS and CI should require platform and domain-owner approval in a production organization.
6. The release job publishes the immutable generated artifact and manifest.

Do not manually edit generated files. Change the YAML contract or compiler instead.

