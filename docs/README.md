# Kafdeck documentation

Current stable release: [v0.9](releases/v0.9.md), published 2026-11-12. Start with the current guides below. Older numbered runbooks remain applicable where linked; their release dates and historical evidence are preserved.

| Task | Guide |
| --- | --- |
| Install Docker/Podman, native archives or Helm; verify signed media | [v0.9 installation](operator/v0.9-installation-guide.md) |
| Configure Keycloak, issue/renew setup code, create first owner, manage users | [Managed identity](operator/managed-identity-setup.md) |
| Register/test Kafka, select clusters, work with topics/consumers and integrations | [v0.9 dashboard](operator/v0.9-dashboard-guide.md) |
| English/Persian UI and accessibility | [Localization/accessibility](development/v0.9-localization-accessibility.md) |
| Governed write previews, risk and distinct-person approvals | [Administration](operator/v0.5-safe-administration-guide.md) |
| Human/service CLI, Terraform and MCP | [CLI human OIDC](operator/v0.8-cli-human-oidc-runbook.md), [declarative CLI](operator/v0.8-declarative-cli.md), [MCP](operator/v0.8-mcp.md) |
| Upgrade, backup, restore, generation admission and air-gap | [Distribution/recovery](operations/v0.9-distribution-recovery.md) |
| Catalog, policy, requests and measured attribution | [Catalog/policy](api/v0.9-catalog-policy-runtime.md), [requests/attribution](api/v0.9-requests-attribution-runtime.md) |
| Security findings and deployment-tested egress limits | [W78 disposition](security/v0.9-w78-independent-review.md) |
| Measured load/soak methodology and accepted limits | [W82 budgets](performance/v0.9-budget-protocol.md) |
| Exact released source, signatures and asset hashes | [Release evidence](implementation/evidence/v0.9-release-published-20261112.json) |
| Current capability availability | [Released v0.9 matrix](product/current-capabilities.md) |
| Architecture and project direction | [Architecture](architecture/), [ADRs](adr/), [RFCs](rfcs/), [Roadmap](../ROADMAP.md) |

Development scaffolds/laboratories under `deploy/dev` are isolated fixtures. They are not turnkey production deployments, and historical candidate reports are not replacements for the final v0.9 release evidence.

## Reading historical documents

Versioned RFCs/ADRs, Gate decisions and intermediate test reports retain original dates, statuses and observed results. Use the [final closure ledger](implementation/v0.9-full-scope-delivery-execution-plan.md#final-closure-ledger) for completed v0.9 acceptance. Signed W80/W81 evidence is immutable; corrections are additive in the [errata/source supplement](releases/v0.9-evidence-errata.md). A versioned API/runbook filename can remain v0.8 while applying to current 0.9 packages; do not change wire versions or the independently versioned Terraform provider to match the app.
