# Documentation reconciliation - 2026-11-13

This closes the 25 findings from the repository-wide documentation audit of main `210aeb28143fb0d8b36ba498d45e5a8e06115fb8`. It changes documentation and the related release-identity test; it does not change product behavior or create new runtime acceptance evidence.

| Audit ID | Finding | Resolution |
| --- | --- | --- |
| DOC-01 | README status/table | [README](../../README.md) now links Roadmap without the historical capability table. |
| DOC-02 | Release identity assertions | Historical candidate inputs remain validated separately from current publication metadata and operator links; no obsolete README prose is required. |
| DOC-03 | Primary onboarding | [Getting started](../../README.md#getting-started) follows install, one-time code, first owner, Keycloak, tested Kafka and users. [Static Token evaluation](../operator/static-token-evaluation.md) is separate. |
| DOC-04 | Streaming capability wording | README distinguishes the SELECT editor from owner-only registered Stream/Table forms and their governance/cluster-identity gates. |
| DOC-05 | Acceptance strategy | [Test strategy](../testing/v0.9-test-strategy.md) points to completed acceptance and retained numerical evidence. |
| DOC-06 | Product scope status | [Product journeys](../product/v0.9-governance-enterprise-hardening.md) retain the full accepted scope and identify release completion. |
| DOC-07 | Catalog/policy status | [Runtime contract](../api/v0.9-catalog-policy-runtime.md) no longer describes W78/W82 as future gates. |
| DOC-08 | MCP installation | [MCP](../operator/v0.8-mcp.md) distinguishes current 0.9.0 packages from the retained v0.8 API; no NuGet-feed publication claim. |
| DOC-09 | Terraform installation | [Provider guide](../../providers/terraform/README.md) describes verified release ZIPs and filesystem mirrors, retaining 0.8.0 and not claiming public Registry publication. |
| DOC-10 | Target vs availability | [Current availability](../product/current-capabilities.md) is separate from the preserved [Gate 2 map](../product/capability-map.md). |
| DOC-11 | Local development | [Guide](local-development.md) documents actual listener keys, empty defaults, content/web roots and integrated UI. |
| DOC-12 | CI/security automation | [Guide](ci-security.md) names implemented workflows and differentiates controls from actual check outcomes. |
| DOC-13 | Dependency policy | [Policy](dependency-policy.md) reflects current NuGet/npm/Go/Actions automation, locks and image pins. |
| DOC-14 | Release standards | [Standards](release-standards.md) describe released signed media, source/publisher identities, verification and demonstrated limits. |
| DOC-15 | Architecture | [Overview](../architecture/architecture.md) includes current modules, identity, registered connections and durable/recovery boundaries. |
| DOC-16 | Threat model | [Index](../architecture/threat-model.md) links security deltas through v0.9 and preserves the historical v0.1 analysis. |
| DOC-17 | Decision indexes | [RFC](../rfcs/README.md) and [ADR](../adr/README.md) indexes are complete; additive notes distinguish original proposal status from released scope without rewriting decisions. |
| DOC-18 | Governance API design | [Contract](../api/v0.9-governance-contract.md) identifies the original route sketch and points to actual runtime/OpenAPI contracts. |
| DOC-19 | Threat validation status | [Contract](../security/v0.9-threat-validation.md) links completed independent review and retained conditional risk. |
| DOC-20 | Support | [Support](../../SUPPORT.md) links current channels, templates, diagnostics and private vulnerability reporting guidance without a new SLA. |
| DOC-21 | Security posture | [Policy](../../SECURITY.md) reflects implemented administration/secrets/supply-chain controls and preserves the pre-v1 support policy. |
| DOC-22 | Gate 9 | [Decision](../decisions/gate-9-v0.9-governance-hardening.md) labels original admission conditions as met and retains owner/publication history. |
| DOC-23 | Execution plan | [Plan](../implementation/v0.9-full-scope-delivery-execution-plan.md#final-closure-ledger) closes V9-3/4/5, labels intermediate inputs historical and provides a final W72-W82 ledger. |
| DOC-24 | Frozen report source links | [Additive errata](../releases/v0.9-evidence-errata.md) maps three omitted sources to a checksummed retained supplement outside signed payloads. |
| DOC-25 | Prepublication vs installation | [Signing](../releases/v0.9-prepublication-signing.md), [readiness](../releases/v0.9-release-readiness.md) and [publication](../../publication/v09/README.md) identify the completed maintainer procedure and direct operators to installation. |

## Preservation and verification

Original decision dates/statuses and observations are retained; additions explain current disposition. Signed W80/W81 publication directories, their indexes/JSON receipts, the original release-input metadata, release asset hashes and immutable v0.9 tag are unchanged. The source supplement is explicitly outside the original signature.

Local validation checks relative paths/heading anchors, UTF-8, whitespace and preservation of frozen evidence. The three known missing links inside the immutable report remain physically unchanged and are resolved for readers by the separate errata mapping. The applicable required CI validates the changed release-identity test and repository merge gate. Successful product load/soak, recovery and distribution acceptance is not rerun for these prose changes.
