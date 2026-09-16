# Contributing to Kafdeck

Thank you for contributing to Kafdeck.

Kafdeck is designed as a secure, operations-first open-source control plane for Apache Kafka. Contributions are welcome when they preserve the project's safety, reliability, vendor-neutrality, and maintainability principles.

## Before contributing

Read:
- `GOVERNANCE.md`
- `docs/architecture/architecture.md`
- `docs/development/engineering-standards.md`
- `docs/development/testing-standards.md`
- `docs/development/git-workflow.md`
- `SECURITY.md`

## Official language

Engineering documentation, PR descriptions, ADRs/RFCs, maintainership-oriented code comments, and release notes are written in English.

## Contribution workflow

1. Start from the latest `main`.
2. Create a short-lived branch (`feature/`, `fix/`, `docs/`, `refactor/`, `security/`, or `chore/`).
3. Keep the change narrowly scoped.
4. Add or update tests.
5. Update documentation for behavior, configuration, security, or compatibility changes.
6. Run required local checks.
7. Open a pull request using the project template.
8. Address review findings without hiding or bypassing required checks.
9. Revalidate the exact head before merge.

## Commit convention

Kafdeck uses Conventional Commits. Typical prefixes:
- `feat:`
- `fix:`
- `refactor:`
- `perf:`
- `test:`
- `docs:`
- `build:`
- `ci:`
- `chore:`
- `security:`

Example:

`feat(topics): add topic configuration explorer`

## DCO

By contributing, you certify the Developer Certificate of Origin (DCO). Commits must be signed off where repository automation requires it, using a `Signed-off-by` trailer that reflects the contributor identity.

## Architecture changes

Do not introduce a major architectural decision inside an implementation PR without the required ADR/RFC.

Examples include:
- changing the architecture style,
- introducing a new persistence technology,
- leaking infrastructure/vendor types into core modules,
- introducing a new public API compatibility commitment,
- changing authentication/authorization semantics,
- changing supported Kafka tiers.

## Security changes

Authentication, authorization, secrets, payload visibility, audit, crypto/TLS, dependency-security controls, and destructive Kafka operations are security-sensitive areas and require explicit security review.

## Definition of Done

A feature is not done merely because it works. Where applicable, Done includes:
- implementation,
- tests,
- security controls,
- observability,
- error handling,
- accessibility,
- documentation,
- compatibility/migration consideration.
