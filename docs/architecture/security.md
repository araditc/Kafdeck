# Security Architecture Baseline

## Security objective

Kafdeck must remain safe when connected to production Kafka clusters that may contain sensitive data and availability-critical workloads.

## Trust boundaries

At minimum, designs must consider:
- Browser/user to Kafdeck API
- Kafdeck to Kafka clusters
- Kafdeck to identity providers
- Kafdeck to persistence
- Kafdeck to secret providers
- Kafdeck to Schema Registry/Kafka Connect
- CI/build system to release artifacts

Each new integration must document authentication, authorization, secret flow, network trust, data exposure, and failure behavior.

## Authorization model

Authorization is capability-based and deny-by-default.

Metadata and payload access are separate. Examples:
- `topic.read` does not imply `record.read`.
- `consumer.read` does not imply `consumer.offset.reset`.
- `acl.read` does not imply `acl.manage`.

Authorization checks belong in the application operation boundary and may not rely solely on hidden UI controls.

## Administrative operations

Mutations require:
- authenticated identity,
- explicit permission,
- target validation,
- risk classification,
- confirmation appropriate to risk,
- normalized audit record.

High/critical operations should support a dry-run or impact preview when technically meaningful.

## Audit model

Audit events should capture:
- actor identity,
- timestamp,
- target cluster/resource,
- operation,
- normalized before/after state when safe and available,
- result,
- correlation/request identifier,
- relevant client/session context,
- optional operator reason.

Audit must not include:
- passwords/tokens/private keys,
- full Kafka record payloads,
- unrelated sensitive headers.

## Record-data exposure

Record browsing must be:
- separately authorized,
- bounded,
- maskable,
- non-persistent by default,
- protected from accidental bulk export.

## Threat modeling

A lightweight threat-model update is required for every new trust boundary or security-sensitive subsystem. STRIDE may be used as an analysis aid, but the project requirement is the documented threat/mitigation outcome rather than a specific notation.

## Fail-safe behavior

Authentication/authorization uncertainty fails closed. Security-provider outages must not silently downgrade access. Insecure TLS bypass, if ever supported for development, must be explicit, visible, and unsuitable as a default.
