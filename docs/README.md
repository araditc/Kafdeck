# Kafdeck documentation

Current stable release: [v1.1.0](releases/v1.1.md). Start with the current guides below. Older numbered runbooks remain applicable where linked; their release dates and historical evidence are preserved.

The [final publication record](releases/v1.0.md) and [completed checklist](releases/v1.0-readiness-checklist.md) record 1.0.0 acceptance. The [support matrix](operator/v1.0-support-matrix.md) defines supported deployments. The [dated status](releases/v1.0-status-20261117.md) and [security admission packet](releases/v1.0-security-support-admission.md) retain the source-bound decisions.

| Task | Guide |
| --- | --- |
| Build/use the single-file offline Windows EXE (development supplement, English/Persian) | [Graphical setup](operator/windows-graphical-setup.md), [Persian RTL](operator/windows-graphical-setup.fa.md) |
| Install 1.1.0 offline on a fresh Windows Server and create the first administrator | [English offline installer guide](operator/windows-offline-install.md), [Persian RTL](operator/windows-offline-install.fa.md) |
| Upgrade an existing Windows Services installation offline | [PowerShell upgrade](operator/windows-offline-upgrade.md), [Persian RTL](operator/windows-offline-upgrade.fa.md) |
| Install the Java 21 Kafka monitoring agent, use Docker/Windows, build its source | [Java agent guide](operator/kafka-agent.md), [agent source](../tools/kafka-agent/) |
| Install Docker/Podman, native archives or Helm; verify signed media | [v1.1 installation and upgrade](operator/v1.1-installation-guide.md) |
| Configure Keycloak, issue/renew setup code, create first owner, manage users | [Managed identity](operator/managed-identity-setup.md) |
| Register/test Kafka, select clusters, work with topics/consumers and integrations | [v0.9 dashboard](operator/v0.9-dashboard-guide.md) |
| Monitor brokers, investigate slow requests and set alerts in the dashboard | [Kafka monitoring](operator/kafka-monitoring.md) |
| English/Persian UI and accessibility | [Localization/accessibility](development/v0.9-localization-accessibility.md) |
| Governed write previews, risk and distinct-person approvals | [Administration](operator/v0.5-safe-administration-guide.md) |
| Human/service CLI, Terraform and MCP | [CLI human OIDC](operator/v0.8-cli-human-oidc-runbook.md), [declarative CLI](operator/v0.8-declarative-cli.md), [MCP](operator/v0.8-mcp.md) |
| Upgrade, backup, restore, generation admission and air-gap | [Distribution/recovery](operations/v0.9-distribution-recovery.md) |
| Catalog, policy, requests and measured attribution | [Catalog/policy](api/v0.9-catalog-policy-runtime.md), [requests/attribution](api/v0.9-requests-attribution-runtime.md) |
| Security findings and deployment-tested egress limits | [W78 disposition](security/v0.9-w78-independent-review.md) |
| Measured load/soak methodology and accepted limits | [W82 budgets](performance/v0.9-budget-protocol.md) |
| Exact released source, signatures and asset hashes | [v1.1.0 release and evidence](releases/v1.1.md), [historical v1.0 evidence](implementation/evidence/v1.0-release-published-20261117.json) |
| v1.0 stabilization | [Bounded checklist](releases/v1.0-readiness-checklist.md), [code-gap audit](implementation/v1.0-code-gap-audit.md), [accepted compatibility/support policy](operator/v1.0-compatibility-support.md), [contract inventory](implementation/v1.0-contract-inventory.md), [RC installation/upgrade admission](releases/v1.0-rc-admission.md) |
| Current capability availability | [v1.1 capability overview](../README.md#feature-matrix), [release capability matrix](product/current-capabilities.md) |
| Architecture and project direction | [Architecture](architecture/), [ADRs](adr/), [RFCs](rfcs/), [Roadmap](../ROADMAP.md) |
| Fleet walkthrough for Linux/systemd and Docker Compose | [Fleet quickstart](operator/v1.1-fleet-quickstart.md) |
| v1.1 SSH hosts and verified broker/service mappings | [Broker host registration](operator/v1.1-broker-host-registration.md) |
| v1.1 bounded service logs from Brokers, Fleet and Monitoring | [Broker service logs](operator/v1.1-broker-service-logs.md) |
| v1.1 offline agent install and upgrade for a verified broker | [Offline broker agent](operator/v1.1-offline-broker-agent.md) |

Development scaffolds/laboratories under `deploy/dev` are isolated fixtures. They are not turnkey production deployments, and historical candidate reports are not replacements for the release evidence linked above.

## Reading historical documents

Versioned RFCs/ADRs, Gate decisions and intermediate test reports retain original dates, statuses and observed results. Use the [final closure ledger](implementation/v0.9-full-scope-delivery-execution-plan.md#final-closure-ledger) for completed v0.9 acceptance. Signed W80/W81 evidence is immutable; corrections are additive in the [errata/source supplement](releases/v0.9-evidence-errata.md). A versioned API/runbook filename can remain v0.8 while applying to current packages; do not change wire versions or the independently versioned Terraform provider to match the app.

## Managed browser API reference

The [experimental managed-browser contract](api/managed-browser-contract.md) and [OpenAPI catalog](api/openapi-managed-browser.json) document first-owner setup, ordinary users, Kafka targets and registered service settings without changing the retained versioned public API families.

## Fleet dashboard onboarding in newer source builds

- [Encrypted credentials and guided Linux host preparation](operator/fleet-dashboard-onboarding.md)
- [راهنمای فارسی راست‌به‌چپ](operator/fleet-dashboard-onboarding.fa.md)
- [RFC-0012: accepted design and security/storage boundary](rfcs/0012-fleet-dashboard-onboarding.md)
