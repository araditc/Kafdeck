# Managed browser API contract (G03)

Status: **experimental, dashboard-coupled**. This classifies the existing managed onboarding/users/connection routes; it does not declare a stable third-party API, CLI/provider parity, a v1.0 release or broader owner permissions. Existing versioned public contracts are unchanged.

The canonical [OpenAPI 3.1 catalog](openapi-managed-browser.json) contains 20 operations on 14 paths. Its `1.0.0-experimental.2` is the document revision, not the product version. In enabled OIDC deployments, authenticated `SystemRead` can read `/api/v1/openapi/managed-browser.json`; that document includes only operations actually mapped in the process. The static catalog describes all conditional families. Neither document reveals configured targets, users or secrets.

## Availability, identity and transport

- Setup/users require `ManagedIdentity.Enabled` with its existing dedicated Keycloak/HTTPS requirements. Registered HTTP services additionally require their runtime to be present. Kafka target/test routes require OIDC plus `ConnectionWorkspace.Enabled`; target registration also requires the current managed owner. These routes are absent in local/token modes; they are not alternative logins.
- Browser sign-in is the existing Keycloak/OIDC flow. The session cookie is `Kafdeck.Session`. These route names are not admitted to the separate non-browser Bearer/CLI allowlist; upstream Schema Registry/Connect/ksqlDB bearer credentials are distinct from operator authentication.
- Every POST/PUT keeps the existing CSRF requirement: `X-Kafdeck-CSRF` plus matching `Kafdeck.Antiforgery` cookie. Authenticated clients obtain a request token from `/api/v1/auth/csrf`; anonymous setup obtains `csrf` from GET setup. Reacquire after identity changes. Missing/mismatched tokens produce 403 `urn:kafdeck:problem:antiforgery-validation-failed`.
- The OpenAPI `x-declared-body-limit-bytes` records the existing endpoint metadata (8192 setup; 16384 user writes; 1024 recovery; 393216 Kafka probes/registration; 32768 service settings/catalog writes). It does not introduce a new transport-enforcement guarantee. Hosting/proxy limits may reject earlier with 413; handler field limits/preconditions are recorded separately. Unknown JSON fields currently are ignored, not rejected as a closed schema. Malformed/binding failures may have an empty HTTP 400 body before handler execution.

## Operation families

| Methods / path under `/api/v1` | Authority and successful result |
| --- | --- |
| GET, POST `/setup` | Anonymous state/CSRF; valid host-issued one-use setup code plus CSRF for creation. State 200; first owner 204 followed by separate browser login |
| GET, POST `/identity/users` | Current managed owner; redacted listing 200; new or exactly reconciled ordinary user 201 |
| POST `/identity/users/provisioning/{operationId}/recover` | Current owner and original requester; positively bound original account 201; no request DTO needed |
| PUT `/identity/users/{id}` | Current owner, ordinary users only; updated public user 200 |
| GET, POST `/connections/targets` | Browser `ClusterRead` filters listing; current managed owner registers/reuses a target. Both return 200, not 201 |
| POST `/connections/test` | Browser operator for an authorized target; owner for a new target. Bounded observation 200; no registration |
| GET, PUT `/connections/{clusterId}/{service}` | Current owner, registered target; redacted settings 200 / tested-and-saved settings 200 |
| POST `/connections/{clusterId}/{service}/test` | Current owner; tested candidate 200 with `connected: true`, no save |
| POST `/connections/{clusterId}/ksqldb/streams`, `/tables` | Owner, configured service, current policy/cluster preconditions; confirmed 201, unconfirmed 202 |

`service` above expands to three **literal** slugs: `schema-registry`, `kafka-connect`, `ksqldb`. No generic service proxy or new provider operation is admitted. Topic/consumer/schema/Connect management and ksqlDB SELECT use their retained families; this catalog does not duplicate or rename them. Each concrete method/path has a unique operation ID, DTO schema, successful response, path parameters, security, declared body limit and handler problem categories in OpenAPI.

## Failure, retries and secret handling

- Problems returned by these handlers use `application/problem+json`, `type: urn:kafdeck:problem:<category>`, `title` and `status`; service errors also expose `code`/`detail`. Middleware/binding/transport can return empty responses before handlers, so clients must inspect status before parsing JSON. Unknown 5xx never proves no effect. OpenAPI lists explicit handler categories without pretending all adapter/infrastructure codes form a frozen enum.
- Setup `available` is not proof a valid unexpired host code exists. `recovery` never reopens first-owner creation automatically. The first owner cannot be updated via ordinary-user APIs.
- Ordinary-user creation persists request intent before dispatch. Optional nonempty `requestId` binds exact requester/email/role/cluster material. HTTP 409 `identity-provisioning-pending` includes `operationId`; use original-request recovery, which never resubmits an unknown creation or resets a password. Do not adopt an existing email. See [private Keycloak attribute and recovery](../operator/managed-identity-setup.md#safe-ordinary-user-provisioning-and-recovery-main-after-v09). Audit recovery is at least once under stable operation correlation. Capacity is 1000 users plus pending slots and 2048 retained provisioning receipts.
- Disabling a user revokes locally **before** contacting Keycloak. A provider 502 can coexist with a completed local revocation. Reload current state after uncertainty. Enabling commits locally after provider success; no cross-system exactly-once update guarantee is introduced.
- Kafka registration deduplicates protocol plus case-insensitive bootstrap set; the display name and SASL mechanism are not a replacement identity. New and existing target success both return 200. Secrets never enter saved target JSON or registration audit; saved target presence does not guarantee an active credential lease. The existing in-memory lease expires after 30 minutes and restart requires credentials again. CIDR/advertised broker checks, 256-target limit, verified TLS and retained KAF-02 deployment-tested-egress condition remain unchanged. Secured registered-target success now requires actual credential retention. HTTP 409 `connection-credential-capacity` includes `clusterId` and `targetSaved: true`: the target definition exists, but no usable lease was retained. Reload/reuse the saved target and retry after expiry; do not duplicate registration. Other admitted leases remain usable; capacity remains 128. See [G06/E01/Q01 stabilization](../implementation/v1.0-g06-state-responsiveness.md).
- Service test/save uses optimistic `revision` (null/omitted only when unconfigured), checked before provider IO and again under the state lock on save. A stale value yields 409. Changing URL/auth requires credentials again; blank/omitted username and secret may retain the prior protected value only for unchanged normalized URL/auth. `none` clears credentials. Public responses never include username, secret or protected credential. Save returns a fresh opaque revision; reload it before retrying a lost acknowledgement.
- ksqlDB creation accepts typed fields, not arbitrary SQL. Partitions/replicas are paired; tables require a supported primary key and streams forbid one. Governance/policy and fresh provider Kafka-cluster identity checks remain mandatory. A 202 `unconfirmed` means an effect may exist; inspect the catalog before retrying. These routes have no request-ID or `Idempotency-Key` guarantee.

## Compatibility checks and evidence boundary

The canonical JSON is embedded in the API assembly; discovery selects from actual endpoint inventory metadata. Focused tests compare every mapped method/name/request DTO/CSRF/declared limit and conditional deployment subset, resolve references and check actual response shapes. Real loopback HTTP exercises setup states, binding/authority failures, Kafka observations and service test/save/credential retention/stale-revision semantics. Provider/Kafka fixtures and synthetic identities are explicitly controlled; these tests are not new human-login, Kafka deployment, W82 or release evidence. G01/G02 successful tests are reused, not manually rerun.

This closes G03 classification/contract work after its PR is admitted. Remaining G04 coverage, G05/G06, Q01 and full v1.0 contract/support/RC gates in [#469](https://github.com/araditc/Kafdeck/issues/469) remain open.

Local validation on .NET SDK 10.0.404: 15 distinct focused cases passed with zero failures/skips (initial 13, two additional wire cases, then only the three strengthened inventory cases). Build outputs used the authorized temporary C workspace after D ran out of space; no source/build flag or dependency version was changed for this. The checks are controlled loopback/source-contract evidence, not new provider-release acceptance.
