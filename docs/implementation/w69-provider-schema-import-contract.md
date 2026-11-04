# W69 — Typed Terraform provider schema and import contract

Status: **implemented integration candidate / publication not performed**. G3 W68 runtime is accepted in PR #418. The actual protocol-6 provider, typed resource/data source, exact import, schema upgrade and governed effect boundary are implemented in `providers/terraform`; final G4 real-runtime evidence and protected acceptance are tracked in the G4 ledger.

## Scope
- Provider uses a dedicated Kafdeck HTTPS API client; no Kafka broker, provider HTTP proxy, or database connection.
- Initial resource family: `NotificationSubscription` only with the admitted versioned backend capability; all other families stay unregistered.
- Resource ID is the server-issued immutable deployment/scope/kind/id tuple; import requires exact backend read and matching tenant authorization.
- Config: server URL, token supplied from secure environment, optional CA trust and a bounded 15-second request timeout; never put bearer tokens into state.
- Per-resource attributes: `deployment_id`, `resource_id`, `revision`, `source_revision`, `operation_id` (the admitted scope is exactly `Deployment`, with scope ID equal to deployment ID); typed destination references, not secret material.
- Import must fail closed for missing, inaccessible, unowned, conflicted or unsupported resources.
- Provider schema version is pinned to a declared backend API contract; no generic REST object or arbitrary Kafka configuration resource.

## Admission tests required
Schema acceptance, exact-scope import, non-secret state snapshots, duplicate import conflicts, unsupported family rejection, tenant isolation, backwards compatibility.

The installed provider uses the default-off backend runtime only when operators explicitly enable and authorize it.

## Mandatory typed subscription attributes and import authority

Required/optional configuration follows the admitted W68 `NotificationSubscription` schema: exact `destinationRef`, `state` (only supported lifecycle transitions), and canonical sorted/normalized `eventClasses` and `eventTypes` typed collections. `deployment_id` and `resource_id` are exact identity inputs; scope is fixed to Deployment/deployment_id and checked against the response; `revision`, `operation_id`, ownership generation are backend-computed read-only values; the supplied source_revision is the exact canonical desired fingerprint. Server-normalized values must not be guessed or accepted without canonical readback.

Import of an existing managed subscription is **not adoption or transfer**: require fresh authoritative exact-resource read, registered owner/source identity and generation binding to the authenticated Terraform principal, plus separately admitted import permission and no competing owner/source claim. The caller must hold exact read permission and the governed import/adopt authority for the existing binding; otherwise return a diagnostic without changing state. Stale/unavailable storage yields `Unknown`, terminal `Retired` yields `Conflict`, and both reject import. Missing, Inaccessible and Unsupported also reject import; no state erasure or implicit adoption.

Acceptance includes owner mismatch, competing source, stale registration, `Retired`, uncertain read, absent import grant, invalid event filter and attempted writes to computed fields.
