# CI and Security Automation

## Principles

- Workflows run with least-privilege `GITHUB_TOKEN` permissions.
- Third-party actions are pinned to immutable full commit SHAs.
- Workflow concurrency cancels obsolete runs where safe.
- CI validates the exact pull-request head, not an assumed local state.
- A passing unit test suite never substitutes for security, dependency, or integration controls.

## Quality gate

`quality-gate` is the always-on baseline check. It validates:

- required governance/repository files,
- immutable SHA pinning of external actions,
- Conventional Commit subjects,
- DCO `Signed-off-by` trailers,
- backend build/tests once .NET projects exist,
- frontend build/tests once the React application exists.

Implementation-specific commands become mandatory the same PR that introduces the relevant scaffold.

## Dependency review

Pull requests are reviewed for newly introduced vulnerable dependencies and prohibited strong-copyleft licenses. License exceptions require explicit governance review.

## CodeQL

CodeQL is configured for C# and JavaScript/TypeScript and activates when source paths exist/change. SAST findings are triaged as security findings, not ordinary lint warnings.

## Kafka smoke environment

The project uses the official Apache Kafka JVM image pinned to `apache/kafka:4.3.1` for the Phase 0 local/CI smoke environment. The smoke test validates startup and basic topic administration on a KRaft broker. The compatibility matrix expands in later phases.

## Secret scanning

GitHub native secret scanning and push protection are the preferred repository controls when available. Independent secret-scanning automation may be added if organization settings cannot provide equivalent coverage. No workflow may print secrets for debugging.

## Future release controls

Before the first distributable release, CI must add:

- CycloneDX SBOM generation,
- OCI image vulnerability scanning,
- container/artifact signing with Sigstore/Cosign,
- provenance generation aligned with the approved SLSA target,
- release reproducibility and upgrade validation.
