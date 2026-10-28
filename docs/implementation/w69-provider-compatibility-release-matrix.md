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
