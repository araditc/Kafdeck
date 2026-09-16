# ADR-0002: Technology Stack Baseline

- Status: Accepted
- Date: 2026-09-16
- Gate: 0

## Decision

Kafdeck uses:
- ASP.NET Core / .NET 10 LTS for backend services,
- React + TypeScript for the web application,
- Confluent.Kafka as the initial Kafka client implementation behind Kafdeck-owned ports/adapters,
- OpenAPI 3.1 for HTTP API description,
- RFC 9457-style Problem Details for API errors,
- OpenTelemetry for traces, metrics, and logs.

## Rationale

The combination provides a mature server runtime, strong asynchronous/concurrency support, broad contributor familiarity on the frontend, and a well-established Kafka client while preserving the ability to replace/provider-specialize infrastructure through internal abstractions.

## Consequences

The `Confluent.Kafka` API is an infrastructure detail and may not become a domain contract. Frontend/backend integration must use explicit HTTP contracts rather than shared database/Kafka representations.
