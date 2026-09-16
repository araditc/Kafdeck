# Product Vision

## Positioning

**Kafdeck — The Open Source Kafka Control Plane**

Kafdeck is an Apache-2.0, secure, vendor-neutral, operations-first control plane for Apache Kafka and tested Kafka-compatible services. It provides a single operational surface for visibility, diagnosis, governed administration, security, ecosystem integration and automation without forcing operators into a vendor-specific control plane or a mandatory traffic-path proxy.

## Primary users

- Kafka/platform operators
- SRE and DevOps teams
- Backend and event-platform developers
- Security/platform administrators
- Data governance and audit teams
- Organizations operating Kafka on-premise or in restricted/air-gapped environments
- Regulated organizations that require explainable, reviewable administrative actions

## Core value

Kafdeck should reduce the operational cost and risk of understanding and managing Kafka while remaining safe enough for regulated and high-throughput environments.

It is intentionally broader than a message browser or topic UI. The long-term product boundary includes cluster/fleet exploration, consumer diagnostics, governed record inspection, safe mutations, identity/RBAC/masking/audit, Kafka security administration, Schema Registry, Kafka Connect, streaming ecosystem integration, observability, GitOps/API automation and governance workflows.

## Differentiation principles

### Governed safe operations
Administrative actions are first-class operations with authorization, validation, risk classification, preview, confirmation/approval, execution verification and audit. The same pipeline applies to UI users, REST clients, CLI, Terraform and AI/MCP agents.

### Security and governance as Apache-2.0 product fundamentals
RBAC, server-side masking, audit, read-only modes and metadata-vs-payload separation are core product goals rather than optional architectural afterthoughts. Wire-path masking is a different architecture and, if ever added, belongs to an optional separately governed gateway.

### Out-of-band by default
Core Kafdeck must not sit in the producer/consumer data path. Observability/governance must not introduce a new Kafka availability dependency merely to use the UI.

### Zero-write observation
Read-only Kafdeck must be able to operate without creating internal Kafka topics or obtaining Kafka write permissions for its own persistence.

### Air-gap first
A normal Kafdeck deployment must not require an external SaaS, cloud account or Internet connectivity.

### Vendor neutral, capability explicit
The domain model reflects Apache Kafka semantics. Vendor integrations are adapters. Managed-service differences and partial compatibility are surfaced as explicit capabilities rather than hidden behind optimistic compatibility claims.

### Operationally responsible
Expensive scans, unbounded tails, N+1 admin calls, aggressive polling, uncontrolled retries/concurrency and high-cardinality telemetry are product defects.

### API-first automation
UI, API, CLI, Terraform, GitOps and MCP use shared application contracts and authorization. Automation cannot become a back door around safety controls.

### Measured lightweight deployment
Kafdeck targets a low operational footprint through .NET, bounded state and single-container/self-contained distribution. Resource targets are release benchmarks, not an unmeasured promise such as an unconditional sub-100-MB requirement.

### Explainability over opaque scores
Health, anomaly, lineage, access and automated recommendations show the evidence used to derive conclusions. Unknown/partial information is not silently converted into a confident score.

## Explicit non-goals for early releases

- Replacing Kafka itself or acting as a message broker
- Requiring a proxy/gateway in the Kafka data path
- Persisting user Kafka payloads as an application database by default
- Using the managed Kafka cluster as Kafdeck's mandatory state database
- Performing automatic destructive remediation without authorization and policy
- Coupling core functionality to one managed Kafka vendor
- Requiring Kubernetes for basic deployment
- Providing an unrestricted shell/terminal inside the web application
- Running arbitrary untrusted JavaScript on the Kafdeck server for record filtering
- Claiming compatibility, search throughput or memory usage without repeatable evidence
