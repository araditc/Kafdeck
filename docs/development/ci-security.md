# CI and security automation

## Current checks

The always-on [quality gate](../../.github/workflows/quality-gate.yml) validates repository structure, immutable action SHA pins, Conventional Commit subjects and DCO trailers. It builds/tests the .NET solution, locked Terraform provider and React frontend, and runs W82 budget-verifier negative cases. PR validation uses the exact head; branch protection and unresolved conversations remain authoritative for merge.

Path-filtered workflows add [dependency review](../../.github/workflows/dependency-review.yml), [CodeQL](../../.github/workflows/codeql.yml) for C#, JavaScript/TypeScript and Go, [container builds](../../.github/workflows/container.yml), and [Kafka compatibility](../../.github/workflows/kafka-compatibility.yml). The workflow matrix defines tested Kafka/security cells; a smoke test or compatible wire protocol does not establish blanket provider support. See [compatibility](../../README.md#kafka-compatibility).

## Release and integration evidence

[Release supply-chain checks](../../.github/workflows/release-supply-chain.yml) build one source-bound candidate, prove isolated startup, generate an SPDX SBOM and scan that exact image. Native/OCI/Helm and identity/recovery acceptance have separate producers. The final v0.9 full-distribution manifest was explicitly owner-authorized and Sigstore-signed; official readiness and publication verified the selected source, media and accepted evidence. See [release standards](release-standards.md) and the [published record](../implementation/evidence/v0.9-release-published-20261112.json).

These controls exist now; a workflow's presence is not a claim that every current optional run passes. Required failures block merge/publication. Diagnose other failures against their actual logs and applicable admission policy; do not infer a vulnerability solely from a failed job or silently remove a required check. Reuse successful source-bound acceptance evidence where the release procedure permits it; documentation edits do not justify another load/soak run.

## Security posture

Workflows use least-privilege permissions, immutable third-party action SHAs and concurrency cancellation where safe. Only explicitly guarded signing/publication jobs receive signing or registry-write authority. Dependency/license findings and SAST results require triage; passing unit tests do not replace those controls.

Native GitHub secret scanning, push protection and private vulnerability reporting remain conditional on repository/organization availability and settings. Do not describe them as universally active without checking settings. No workflow may print credentials or Kafka payloads. Follow [repository enforcement](repository-enforcement.md) and [SECURITY.md](../../SECURITY.md).
