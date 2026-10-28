# W69 — Read / refresh / drift contract

Status: **candidate / runtime blocked by W68**.

Refresh calls only a typed, versioned Kafdeck read API with the caller's token. Never query brokers or persistence directly.
Interpret statuses exactly: InSync, OutOfSync, Missing, Unknown, Inaccessible, Conflict, Unsupported. Missing is not permission to import, adopt or recreate. On 401/403, invalid identity, timeout, stale evidence, or partial response return a Terraform diagnostic, not a successful state delete.
Keep resource scope, owner/source generation, CAS revision and desired SHA256 fingerprint in the state projection. Secret references remain write-only/sensitive. Redact bearer tokens, raw message payloads and provider credentials from diagnostics and logs.

## Acceptance
- Exact scope read under per-resource RBAC.
- 403 never becomes 404; unavailable never becomes Missing.
- Stale generation / revision causes re-plan, never auto-adoption.
- State upgrade and drift parity tests against accepted API fixtures.
- Retry is bounded and only for safe idempotent reads.

**No working provider read is claimed until real W68 API fixtures exist.**

## Explicit Terraform state mapping

`InSync`: retain canonical state with fresh backend revision.
`OutOfSync`: expose a typed drift diagnostic and require a new governed plan; do not mutate during refresh.
`Missing`: preserve prior Terraform state and report a diagnostic until authoritative, authorized ownership/lifecycle reconciliation proves deletion. Never infer adopt/recreate.
`Unknown`, `Inaccessible`, `Conflict`, `Unsupported`: **error-severity diagnostic, preserve prior state, no state removal and no mutation** even when HTTP 200 carries the typed status.
HTTP 401/403, stale timestamp, absent owner generation, malformed body, timeout or inconsistent scoped read are also error diagnostics retaining prior state. Negative tests must assert each status separately; do not convert denied or unavailable into a normal 404.
