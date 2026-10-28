# W69 — Typed Terraform provider schema and import contract

Status: **candidate / not release admitted**. Prerequisite: W68 owner registry and governed versioned lifecycle APIs.

## Scope
- Provider uses a dedicated Kafdeck HTTPS API client; no Kafka broker, provider HTTP proxy, or database connection.
- Initial resource family: `NotificationSubscription` only after the backend advertises `declarativelyOwnable=true` and a versioned schema.
- Resource ID is the server-issued immutable deployment/scope/kind/id tuple; import requires exact backend read and matching tenant authorization.
- Config: server URL, token supplied from secure environment, optional CA trust and request timeout; never put bearer tokens into state.
- Per-resource attributes: `deployment_id`, `scope_kind`, `scope_id`, `resource_id`, `revision`, `desired_fingerprint`, `operation_id`; typed destination references, not secret material.
- Import must fail closed for missing, inaccessible, unowned, conflicted or unsupported resources.
- Provider schema version is pinned to a declared backend API contract; no generic REST object or arbitrary Kafka configuration resource.

## Admission tests required
Schema acceptance, exact-scope import, non-secret state snapshots, duplicate import conflicts, unsupported family rejection, tenant isolation, backwards compatibility.

**No provider is enabled by this contract alone.**
