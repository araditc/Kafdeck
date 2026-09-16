# Kafdeck Security Policy

Security is a core product property of Kafdeck because the application can observe and eventually administer security- and availability-sensitive Kafka resources.

## Supported versions

Before v1.0, security fixes are guaranteed only for the latest development line unless a release note states otherwise. A formal supported-version table will be established before v1.0.

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

Secret handling must go through an abstraction suitable for standalone secrets initially and external providers such as Kubernetes Secrets or Vault in future approved phases.

## Supply-chain controls

Required direction for CI/release hardening includes:
- pinned action versions/digests where practical,
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
