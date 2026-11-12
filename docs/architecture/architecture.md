# Kafdeck architecture

Status: **Gate 0 baseline retained; current v0.9 overview below**. The [ADR index](../adr/README.md), [runtime contracts](v0.9-governance-contracts.md) and [release record](../implementation/evidence/v0.9-release-published-20261112.json) distinguish original decisions from released acceptance.

## 1. Architecture style

Kafdeck is a **modular monolith** with strict internal boundaries. This optimizes OSS deployment simplicity (`docker run` / a small Compose deployment) while preserving the option to extract components later if operational evidence justifies it.

Distributed/microservice architecture is not permitted without a future RFC/ADR demonstrating a concrete need.

## 2. Technology baseline

- Backend: ASP.NET Core on .NET 10 LTS
- Frontend: React + TypeScript
- Kafka client: Confluent.Kafka behind Kafdeck-owned ports/adapters
- API: REST with OpenAPI 3.1 contract
- Problem responses: RFC 9457 Problem Details semantics
- Observability: OpenTelemetry
- Persistence: provider abstraction; SQLite for simple standalone deployments and PostgreSQL as the production-oriented provider
- Deployment: OCI-compatible container; Kubernetes is optional rather than required

## 3. Dependency direction

Conceptual dependency flow:

`UI -> HTTP/API -> Application -> Domain -> Ports <- Infrastructure Adapters`

Rules:
- Domain and application layers do not depend on Kafka client types, database ORM types, HTTP framework types, or vendor-specific SDKs.
- Infrastructure implements ports owned by inner layers.
- Kafka DTO/client types may not escape the Kafka adapter boundary.
- UI contracts are explicit API contracts, not direct database or Kafka representations.
- Cross-module interaction must use module contracts; one module may not update another module's persistence directly.

## 4. Current runtime boundaries

`Kafdeck.Api` hosts HTTP, server-side OIDC sessions, managed identity/connection endpoints and composition. `Kafdeck.Core` owns shared contracts. Modules cover Clusters, Topics, Records, Consumers, Administration, Schemas, Connect and Generator. Infrastructure implements typed Kafka, ecosystem, persistence, configuration, security and SerDe adapters. Governance catalog/policy/requests/attribution retain the shared effect boundary; CLI, Terraform and MCP call authenticated backend contracts, not Kafka directly.

For managed installations, a host-issued one-time setup code creates the first owner through a dedicated Keycloak provisioning client. Subsequent sign-in uses Keycloak OIDC. Owner-registered Kafka definitions persist in private managed state; Kafka credentials use a bounded in-memory lease, while optional HTTP-service credentials are Data Protection-protected. Registration tests targets before adding them to dashboard cluster selection. See [identity setup](../operator/managed-identity-setup.md) and [dashboard](../operator/v0.9-dashboard-guide.md).

Durable mutation/governance state uses SQLite standalone or the admitted PostgreSQL persistence. Managed identity state, the matching Keycloak realm, Data Protection keys, database backups and outside-database deployment generation/recovery anchors have separate persistence/recovery roles. Restoring a database does not silently readmit old effect authority. [Recovery contract](../operations/v0.9-distribution-recovery.md)

OCI, native API/UI/CLI/MCP and Helm media are released. The shipped Helm topology has one application replica in Standalone mode; external PostgreSQL database HA does not establish multi-application Helm HA. Exact platform limits and accepted media are in the release record.

## 5. Provider boundaries

Provider-specific capabilities must be represented through interfaces/capabilities such as:
- Kafka administration/metadata port
- Record-reading/producing port
- Schema Registry port
- Kafka Connect port
- Secret provider
- Persistence provider
- Identity provider

Capability detection is preferred to assuming every Kafka-compatible platform exposes identical behavior.

## 6. Kafka compatibility

Kafdeck is designed for KRaft-era Kafka. It has no direct ZooKeeper integration.

Compatibility policy:
- Tier 1: Apache Kafka releases that are currently supported by the Apache Kafka project and explicitly present in the Kafdeck CI matrix.
- Tier 2: Kafka 3.9 latest patch compatibility where practical during the early product lifecycle.
- Older releases: unsupported unless a future compatibility RFC accepts them.
- ZooKeeper-mode clusters may only be interacted with through normal broker/client APIs when those APIs remain compatible; Kafdeck does not connect to ZooKeeper.

## 7. Read/write posture

Read-only remains the default. v0.5-v0.9 introduced explicitly enabled, typed and authorized mutation families backed by durable state and capability checks. Metadata read, record read/export, sign-in and ownership labels remain separate from effect authority. The [administration guide](../operator/v0.5-safe-administration-guide.md) describes the governed operation pipeline. Registered-target Stream/Table creation is a separate owner-only path and is blocked by an active governance generation or enabled policy; the SELECT editor never accepts arbitrary DDL/DML.

## 8. Operation model

Every mutation ultimately follows:

`Request -> Authentication -> Authorization -> Capability Check -> Validation -> Risk Classification -> Preview -> Confirmation/Approval -> Execute -> Verify -> Audit`

Operation risk levels:
- LOW
- MODERATE
- HIGH
- CRITICAL

The implementation may optimize presentation for low-risk operations, but may not bypass authentication, authorization, capability, validation, preview, confirmation/approval, verification, or audit rules.

Beginning with v0.5, mutation capability requires durable Kafdeck operation state. Read-only operation remains database-optional; mutation mode fails closed when supported persistence is unavailable. CRITICAL operations require durable evidence from a distinct eligible approver.

## 9. Resilience and Kafka load protection

All Kafka I/O must support, where applicable:
- cancellation,
- explicit timeout,
- bounded concurrency,
- transient-only retry,
- exponential backoff and jitter,
- per-cluster bulkheads,
- pagination/bounded fetches,
- caching with explicit freshness semantics,
- protection from request storms.

N+1 Kafka Admin requests caused by a UI rendering pattern are an architecture defect.

## 10. State and payload handling

Kafdeck stores only application state required for configuration, identity/access, audit, and approved operational features.

Kafka message payloads are not persisted by default. Record browsing is ephemeral and bounded.

## 11. Observability

OpenTelemetry instrumentation is an architectural requirement from the foundation phase. Components should expose actionable traces/metrics/logs without leaking secrets or record payloads.

## 12. Architecture-change control

A change to architecture style, core dependency direction, supported persistence model, compatibility policy, public API policy, or trust boundary requires an ADR/RFC as defined in `GOVERNANCE.md`.
