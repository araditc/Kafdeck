# Threat Model Baseline

This is the root threat-model index. Feature-specific threat models may live beside the relevant architecture/RFC documents but must link back here.

## Assets

Primary assets include:
- Kafka administrative capability,
- Kafka record data,
- Kafka cluster credentials,
- user identities and sessions,
- authorization policies,
- audit integrity,
- Kafdeck persistence/configuration,
- release/build integrity.

## Primary threat areas

- unauthorized record disclosure,
- privilege escalation from metadata read to payload/admin access,
- destructive Kafka operations,
- credential/secret leakage,
- SSRF or arbitrary network reach through cluster configuration,
- denial of service against Kafka caused by Kafdeck polling/browsing,
- malicious/crafted Kafka payload rendering,
- browser-side injection through record/topic/schema content,
- audit tampering or omission,
- compromised dependencies/build workflows,
- insecure TLS or identity-provider downgrade,
- cross-cluster authorization confusion.

## Required analysis for new capabilities

For every new trust boundary or security-sensitive subsystem, document:
1. assets/data handled,
2. actors and trust assumptions,
3. entry points,
4. abuse/failure scenarios,
5. mitigations,
6. residual risk,
7. security tests/observability.

STRIDE may be used as a checklist but is not mandatory notation.

## v0.1 W02 implementation delta — configuration and deployment access

### Assets and entry points

W02 introduces immutable operator-defined cluster profiles, external secret references, process startup validation, and the minimum browser-to-Kafdeck deployment access-token boundary. The relevant entry points are application configuration, mounted secret files/environment variables, the HTTP listener, and the `X-Kafdeck-Access-Token` request header.

### Implemented mitigations

- loopback-only HTTP binding is the default;
- any non-loopback binding fails startup validation unless an `env:` or absolute mounted `file:` access-token reference is configured;
- secret reference locators are not publicly serializable and secret values expose no serializable value property;
- startup diagnostics contain only safe structural metadata and never secret locators or resolved secret material;
- deployment-token comparison uses `CryptographicOperations.FixedTimeEquals`;
- failed deployment-token attempts are bounded per remote client with a bounded in-memory tracker and return `429` after the configured failure window is exhausted;
- rejected requests never echo expected or supplied access tokens;
- TLS-backed Kafka profiles cannot disable server-certificate verification;
- mTLS profiles require certificate/key references as a pair;
- SASL and TLS configuration must agree with the selected transport/security protocol;
- insecure Kafka transport profiles remain explicit and generate a startup warning rather than being silently inferred or downgraded.

### Residual risk

The v0.1 deployment token is a deployment boundary, not user identity or fine-grained authorization. Distributed/shared rate limiting, OIDC, RBAC, session management, audit policy, and richer credential-provider integrations remain later-roadmap controls. Operators must still protect process environment access, mounted secret files, reverse-proxy logs, and the network path to remote HTTP listeners.

### Verification

Automated tests cover remote-bind fail-closed behavior, invalid secret references, secret/redaction serialization, exact token comparison, bounded failed attempts, response non-disclosure, and rejection of TLS verification downgrade.
