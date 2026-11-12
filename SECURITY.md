# Kafdeck Security Policy

Security is a core product property of Kafdeck because the application can observe and, when explicitly enabled and authorized, administer security- and availability-sensitive Kafka resources.

## Supported versions

Before v1.0, security fixes are guaranteed only for the latest development line unless a release note states otherwise. The current published release is [v0.9](docs/releases/v0.9.md); main may advance for maintenance. This does not add an LTS/backport or response-time promise. A formal supported-version table will be established before v1.0.

## Reporting a vulnerability

Do not publish exploitable security vulnerabilities in a public issue. Use GitHub private vulnerability reporting when enabled for this repository, or another private channel explicitly published by the maintainers.

A useful report includes:
- affected version/commit,
- impact,
- prerequisites,
- reproduction steps,
- proof of concept where safe,
- suggested mitigation if known.

## Security baseline

Kafdeck targets:
- OWASP ASVS 5.0 Level 2 as the general application-security baseline, with stronger controls for administrative/authentication paths where justified,
- OWASP Top 10 and OWASP API Security risks as threat inputs,
- OpenSSF OSPS Baseline controls for open-source project security,
- SLSA-aligned provenance and build-integrity practices,
- SBOM generation for releases.

## Non-negotiable security rules

1. Authorization is deny-by-default.
2. Topic metadata permission and Kafka record-payload permission are separate capabilities.
3. Secrets are never logged, exposed to the browser unnecessarily, committed to source, or stored in plaintext persistence.
4. Kafka payloads are not persisted by Kafdeck by default.
5. Mutating operations are risk-classified, authorized, validated, and audited.
6. Critical/destructive operations require stronger confirmation controls.
7. Audit data excludes secrets and record payloads.
8. TLS verification is enabled by default; insecure overrides must be explicit and clearly identified.
9. Authentication/authorization failures fail closed.
10. Security-relevant configuration changes are observable and auditable.

## Secret classes

Examples include:
- Kafka SASL passwords,
- TLS private keys,
- OIDC client secrets,
- Schema Registry credentials,
- Kafka Connect credentials,
- application signing/encryption material.

Static secret references use environment variables or absolute private mounted files. Helm can consume existing Kubernetes Secrets; do not embed real secrets in chart values or commits. Managed HTTP-service credentials use ASP.NET Data Protection and require persistent matching keys; registered Kafka credentials have a bounded in-memory lease. Follow [managed state/key persistence](docs/operator/managed-identity-setup.md) and the [Helm guide](deploy/helm/kafdeck/README.md). A general Vault integration is not implied.

## Supply-chain controls

Current repository/release controls include the following, with native secret scanning/push protection conditional on repository settings and service availability:
- immutable full-SHA action pins and digest-pinned application images,
- least-privilege workflow permissions,
- dependency scanning,
- secret scanning,
- SAST,
- container scanning,
- SBOM generation,
- signed release/container artifacts,
- provenance generation,
- controlled dependency-license policy.

## Security review triggers

Explicit security review is required for changes to:
- authentication,
- authorization/RBAC,
- session management,
- cryptography/TLS,
- secret storage,
- record browsing/export,
- audit logging,
- destructive/admin Kafka operations,
- plugin/provider loading,
- dependency execution or build pipeline,
- network trust boundaries.

## Released boundaries and evidence

Use the [threat-model index](docs/architecture/threat-model.md), [final v0.9 independent review](docs/security/v0.9-w78-independent-review.md) and [release evidence](docs/implementation/evidence/v0.9-release-published-20261112.json). KAF-02 is accepted only with tested host/CNI egress in the actual deployment: Kafka can advertise another address after registration, so input CIDRs alone are insufficient. The tested K3s/kube-router direct-IP result does not admit every CNI or Service DNAT topology. Security targets and scans do not constitute a compliance certification or proof of universal provider support.
