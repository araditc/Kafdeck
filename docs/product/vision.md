# Product Vision

## Positioning

**Kafdeck — The Open Source Kafka Control Plane**

Kafdeck is a secure, vendor-neutral, operations-first control plane for Apache Kafka. It aims to provide a single operational surface for visibility, diagnosis, administration, security management, and ecosystem integration without forcing operators into a vendor-specific control plane.

## Primary users

- Kafka/platform operators
- SRE and DevOps teams
- Backend and event-platform developers
- Security/platform administrators
- Organizations operating Kafka on-premise or in restricted/air-gapped environments

## Core value

Kafdeck should reduce the operational cost and risk of understanding and managing Kafka while remaining safe enough for regulated and high-throughput environments.

It is intentionally broader than a message browser or topic UI. The long-term product boundary includes cluster exploration, consumer diagnostics, record inspection, safe mutations, RBAC/audit, Kafka ACL administration, Schema Registry, Kafka Connect, and observability.

## Differentiation principles

### Safe administration
Administrative actions are modeled as operations with authorization, validation, risk classification, preview where feasible, confirmation, execution, and audit.

### Security as core OSS functionality
RBAC, masking, audit, and separation of metadata access from record access are product fundamentals rather than afterthoughts.

### Air-gap first
A normal Kafdeck deployment must not require an external SaaS, cloud account, or Internet connectivity.

### Vendor neutral
The domain model reflects Apache Kafka semantics. Vendor integrations are adapters and may not redefine the core.

### Operationally responsible
Kafdeck must protect the Kafka clusters it observes. Expensive scans, unbounded tails, N+1 admin calls, aggressive polling, and uncontrolled concurrency are product defects.

## Explicit non-goals for early releases

- Replacing Kafka itself or acting as a message broker
- Persisting user Kafka payloads as an application database
- Performing automatic destructive remediation without explicit operator authorization
- Coupling core functionality to one managed Kafka vendor
- Requiring Kubernetes for basic deployment
