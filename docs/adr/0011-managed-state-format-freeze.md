# ADR-0011 — Freeze the managed identity/connection file representation

- Status: Proposed for G06 PR admission; exact-head CI, CODEOWNER review and merge are recorded in [#469](https://github.com/araditc/Kafdeck/issues/469). Acceptance takes effect on that admission.
- Date: 2026-11-13
- Related: [G06 contract and evidence](../implementation/v1.0-g06-state-responsiveness.md), [ADR-0004](0004-persistence.md), [ADR-0006](0006-durable-mutation-operations.md).

## Context

Managed users and registered connections live in a private JSON file outside the mutation database. Integration credentials depend on the original Data Protection key ring and application context. A database migration alone cannot establish compatibility. Silently ignoring newer JSON fields permits an older writer to discard information.

## Decision

Freeze the legacy v0.9 record representation plus the already admitted optional G01 provisioning receipts. Add no envelope, new persistent store or automatic migration in this slice. Reject unknown fields recursively, duplicate properties, invalid JSON and wrong-issuer state; validate at startup and before overwriting an existing file. Keep atomic write/flush/rename and existing exclusive operation leases.

Preserve default Data Protection behavior. Permit explicit selection of an existing absolute private key directory and optional matching application discriminator for verified restores. Preserve Windows current-user DPAPI protection for newly generated keys; do not promise cross-account/machine portability. Missing or mismatched decryption material denies credential use without deleting saved state.

Restore the managed file, consistent database backup, matching decryptable key ring/context and identity configuration together. Rollback means that backup plus the original binary/configuration. A future format change needs a separately reviewed migration/compatibility decision and evidence; this freeze does not permit downgrading live newer state.

## Consequences

Newer files fail closed instead of being silently truncated. Existing v0.9 files require no rewrite merely to read or upgrade. Operators must preserve encryption context as well as database bytes. Actual released-to-development-candidate tests on both databases close G06; final signed-RC/media binding and publication remain separate readiness gates.
