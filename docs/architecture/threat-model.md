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
