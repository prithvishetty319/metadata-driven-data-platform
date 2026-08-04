# Governance model

- **Domain owner:** owns meaning, classifications, and consumer expectations.
- **Platform owner:** owns compiler policy, templates, runtime adapters, and reliability.
- **Security:** approves restricted-data handling and retention controls.
- **CI gate:** rejects invalid contracts, dependency cycles, non-deterministic output, syntax failures, and breaking changes without an explicit migration plan.
- **Auditability:** the release stores the source contract, commit SHA, generated assets, and manifest checksum together.

For production, add a schema-registry lookup and policy-as-code checks for encryption, data residency, retention, and least-privilege grants.

