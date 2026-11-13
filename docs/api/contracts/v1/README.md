# Explicit v1.0 contract baseline

Owner-accepted [RFC-0010](../../../rfcs/0010-v1.0-contract-stability-support.md) governs this finite selection. It becomes a Stable guarantee at v1.0 publication, with independently versioned components retaining their selection. This directory does not announce v1.0 or change released v0.9 media.

- [api.json](api.json): compiled maximal route identities and inferred request/typed governance response schemas; canonical OpenAPI document fingerprints and complete operation identities; explicit Stable/Experimental selection and Internal exclusions; authoritative API/shared producers.
- [cli.json](cli.json): compiled command/flag usage, shared named reads, exit codes and command/output/error producers.
- [mcp.json](mcp.json): protocol, tool names, closed input schemas, effect-enable classification and producers.
- [operator.json](operator.json): qualified and receiver-relative config accesses, models/enums, default/validation binders, selected chart values/schema/templates and independently versioned Terraform producers.

Run the bounded checks with `dotnet test tests/Kafdeck.Architecture.Tests --filter FullyQualifiedName~V1ContractBaselineTests` using the repository's locked restore/build conventions. Normal tests only compare. To prepare a proposed baseline, set `KAFDECK_CONTRACT_EXPORT_DIRECTORY` to a separate output directory, run the same checks, inspect the exported JSON, review compatibility and copy only explicitly selected files. Exporting does not make a failed comparison pass. Do not set that variable in ordinary CI.

Fingerprints are conservative review tripwires: a benign implementation/help change may require a reviewed baseline update without being a major-version break. They do not replace request execution, supported-deployment evidence or authorization tests. Scope, reproduction and reused evidence are in the [inventory](../../../implementation/v1.0-contract-inventory.md); final release gates remain in the [checklist](../../../releases/v1.0-readiness-checklist.md).
