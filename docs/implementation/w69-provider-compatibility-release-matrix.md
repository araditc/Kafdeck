# W69 — Provider compatibility and release matrix

Status: **candidate release checklist; not published**.

## Gates
1. Backend W68 runtime ownership, import/read/plan/apply and exact versioned API schema admitted.
2. Terraform Plugin Framework provider implemented with typed resources and safe state upgrade.
3. Acceptance fixtures: Kafdeck + Keycloak OIDC + PostgreSQL HA + actual permitted NotificationSubscription lifecycle.
4. Tests: first install/import, drift, plan/apply, refresh, PendingApproval, delete boundary, HA failover, retries, credential rotation, state migration and secret redaction.
5. Platform CI: supported Linux/Windows/macOS and Terraform compatibility versions explicitly pinned.
6. Security: no provider direct Kafka, DB, raw HTTP proxy or arbitrary code; CodeQL/static analysis, dependency review and signed distribution artifacts.
7. Documentation: install, required permissions, import recipe, state security, upgrade, troubleshooting and known unsupported families.
8. Release: version/tag/provider registry publication only with independent owner approval and W69 issue evidence.

A successful planning PR is not a Terraform provider release.

## Mandatory negative lifecycle and recovery acceptance

End-to-end acceptance MUST include real backend `AwaitingApproval` and `ExecutionUnknown` states. Preserve the durable operation ID across Terraform process restart and resume *observation only*, never blind redispatch. Neither state may be reported as synchronous success or final Terraform state. Verify no duplicate protected effect after a lost response, and a distinct-principal approval path.

Run denied/cross-scope fixtures: missing exact read permission, missing `declarative.apply`, missing underlying mutation grant, foreign deployment or registered source/owner, and replay of another principal's stored plan. Assert zero protected-data disclosure, zero unauthorized provider effect and unchanged prior Terraform state. All acceptance scenarios must be exercised against admitted versioned W68 APIs; documentation alone cannot satisfy this gate.

## Typed drift and fail-closed read acceptance

Run actual provider acceptance fixtures for `Unknown`, `Inaccessible`, `Conflict` and `Unsupported` returned inside successful HTTP responses, not only network errors. Each case MUST generate an error-severity diagnostic, preserve prior Terraform state, and avoid a state delete, import, plan/apply or destructive effect. Also assert `Missing` cannot become unowned auto-adoption, and 401/403 never map to 404. Run stale ownership-generation, expired revision and incomplete evidence cases with identical fail-closed results.
