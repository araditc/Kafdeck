# Released v0.9 capability availability

This is the current availability guide. [ROADMAP.md](../../ROADMAP.md) records chronology/future direction; the [Gate 2 capability map](capability-map.md) preserves the original target inventory. A historical TARGET/DEFER label is not current support. Availability is bounded by actual configuration, provider capabilities, authorization and the [released evidence](../implementation/evidence/v0.9-release-published-20261112.json).

| Capability | Current posture and boundary |
| --- | --- |
| Kafka connectivity | Implemented: static profiles and owner-registered, tested targets; PLAINTEXT/TLS/SASL and scoped switching. Exact authentication/provider profiles only; OAUTHBEARER/Kerberos/cloud IAM adapters are not implied |
| Identity and users | Implemented: Keycloak first-owner setup with a local one-time code, no initial email verification, managed users/cluster assignments; external OIDC/RBAC remains a separate configuration path |
| Topics and records | Implemented: metadata/configuration, bounded masked read/export, governed lifecycle and publish/purge/jobs; read-only default and separate payload rights |
| Consumers | Implemented: members/assignments/offsets/lag, diagnostics and governed offset/group actions; missing metrics remain unknown |
| Schema Registry | Implemented for admitted Confluent-compatible and tested Karapace profiles: versions/content/references/diff/compatibility and authorized lifecycle. Apicurio remains Unsupported pending a distinct adapter |
| SerDe | Implemented bounded CBOR/XML/MessagePack tooling; not a general runtime plugin engine |
| Connect | Implemented: configured profiles and registered endpoints, status/tasks/configuration and governed lifecycle; optional bounded restart, no unbounded self-healing |
| ksqlDB | Implemented: catalog, bounded SELECT with earliest/new-message selection. Separate registered-target Stream/Table forms require owner and matching cluster identity, and are blocked by active governance/policy; arbitrary SQL/DML remains blocked |
| Streams and lineage | Partial/provider-dependent: registered application/topology/metric evidence; inferred lineage grants no rights and missing telemetry is not fabricated |
| Fleet/security/provider operations | Partial/capability-driven: supported typed primitives only. No generic broker maintenance operation is invented to hide an unsupported provider capability |
| Telemetry, history, SLO, data quality and notifiers | Implemented within admitted metric/record/transport contracts. Missing/stale measurements remain explicit; no universal throughput or cost promise |
| Catalog, requests, policy and attribution | Implemented: scoped catalog/history, immutable request decisions, effect-time policy and measured attribution/rates. Catalog/approval does not grant execution rights; attribution is not billing/payment |
| CLI, Terraform, MCP and GitOps | Implemented admitted finite families and shared backend governance. Terraform 0.8.0 and MCP 0.9.0 ship in v0.9 media. No blanket API-family parity, Terraform destroy or public package-feed claim |
| Deployment and recovery | Implemented signed offline/native/OCI/Helm media, tested upgrades/restores and generation fencing. Helm has one application replica; external database HA is separate |
| Accessibility and locales | English/Persian RTL, keyboard/focus and accepted accessibility observations; original automated/manual evidence remains distinct |
| Public runtime plugins, embedded shell, gateway | Public dynamic plugin SDK remains deferred; unrestricted shell/provider proxy and a data-plane gateway are not core capabilities |

Use the [installation guide](../operator/v0.9-installation-guide.md), [first owner guide](../operator/managed-identity-setup.md) and [dashboard walkthrough](../operator/v0.9-dashboard-guide.md) for actual use. Per-release tests do not grant blanket provider/CNI support; KAF-02 requires tested deployment egress.
