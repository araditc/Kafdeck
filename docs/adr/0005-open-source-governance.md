# ADR-0005: Open-Source License and Contribution Attestation

- Status: Accepted
- Date: 2026-09-16
- Gate: 0

## Decision

Kafdeck uses the Apache License 2.0 and Developer Certificate of Origin (DCO) contribution attestation model.

A Contributor License Agreement is not required in the initial governance model.

## Rationale

Apache-2.0 is permissive, includes an explicit patent grant, and is well aligned with infrastructure/open-source ecosystems. DCO provides lightweight contributor attestation with less contribution friction than a bespoke CLA.

## Consequences

Repository automation should validate DCO sign-off once contribution CI is implemented. Dependency licenses must comply with the project license policy in `GOVERNANCE.md`.
