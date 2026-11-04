# Kafdeck Terraform provider 0.8.0 candidate

This is an executable Terraform Plugin Framework **v1.19.0**, protocol **6**
provider for the admitted `kafdeck.io/v0.8` NotificationSubscription API. The
provider address is `registry.terraform.io/araditc/kafdeck`. Registry publication
and signing are separate owner publication gates; this repository builds local
installable packages without publishing a tag or release.

Build with pinned Go **1.27.3** and test with Terraform **1.14.9**. Protocol 6
requires Terraform 1.0 or later; only versions actually exercised by acceptance
are claimed tested. The pinned Go module graph and `go.sum` are committed.
The transitive gRPC transport is explicitly pinned to **1.85.0**, with its
required patched module graph, to resolve the dependency-review advisories;
provider protocol and backend authorization contracts remain unchanged.
HashiCorp's [framework documentation](https://developer.hashicorp.com/terraform/plugin/framework)
and [provider-server contract](https://developer.hashicorp.com/terraform/plugin/framework/provider-servers)
define the SDK and protocol choice.

## Install and operate offline

Run `pwsh scripts/build.ps1 -Go /absolute/path/to/go`. This tests and builds
Linux amd64/arm64, Windows amd64 and macOS amd64/arm64 binaries, distribution ZIPs,
SHA256SUMS and module build information in ignored `dist/`. Cross compilation
alone is not proof that acceptance ran on every platform. Verify the ZIP hash
against your approved internally signed delivery manifest before extracting.
For example, put the Windows executable in:

```
MIRROR/registry.terraform.io/araditc/kafdeck/0.8.0/windows_amd64/terraform-provider-kafdeck_v0.8.0.exe
```

Use a Terraform CLI configuration (`TF_CLI_CONFIG_FILE`) containing:

```hcl
provider_installation {
  filesystem_mirror {
    path    = "/absolute/path/to/MIRROR"
    include = ["registry.terraform.io/araditc/kafdeck"]
  }
}
```

No direct fallback is needed for an air-gapped installation. Generate/check in
the environment's `.terraform.lock.hcl` against your approved mirror and keep
the signed ZIP checksums with the delivery packet. The executable contains its
module versions (`go version -m BINARY`); `go mod verify` checks dependency cache
integrity. Do not treat this as a registry signature or release approval.

## Exact typed resource and data source

Set `KAFDECK_ENDPOINT` and a current `KAFDECK_TOKEN` in the process environment.
Use a dedicated registered service principal with exact declarative read/apply
and underlying notification permissions, or a real authorized human token.
Independent CRITICAL approval always belongs to an eligible distinct human.
`KAFDECK_CA_FILE` adds a trusted PEM CA; TLS verification cannot be disabled.
HTTPS origins are required except numeric loopback HTTP for local deployments.
Redirects never forward credentials. Tokens and API error bodies are not printed.
Prefer environment credentials: the optional sensitive `token` provider argument
still places a token in Terraform configuration if you choose to supply it there.

```hcl
terraform {
  required_providers {
    kafdeck = { source = "araditc/kafdeck", version = "0.8.0" }
  }
}
provider "kafdeck" {}
resource "kafdeck_notification_subscription" "alerts" {
  deployment_id         = "deployment-a"
  owner_id              = "ops-owner"
  source_ref            = "git-ops"
  source_revision       = "CANONICAL_MANIFEST_SHA256"
  resource_id           = "alerts"
  destination_ref       = "server-owned-destination"
  subscription_state    = "Active"
  event_classes         = ["Operational"]
  event_types           = []
  apply_plan_id          = "IMMUTABLE_BACKEND_OPERATION_UUID"
  apply_plan_fingerprint = "BACKEND_PREVIEW_SHA256"
}
```

Use the same attributes for the `kafdeck_notification_subscription` **data
source**, omitting the optional `apply_plan_id` and `apply_plan_fingerprint`.
Inputs are a closed typed family, not raw JSON/provider routes. There is no
shell, SQL, broker, credential material, source advancement or generic HTTP tool.
The computed identity is `deployment/NotificationSubscription/resource`; revision,
ownership/source/deployment generations, drift and `operation_id` are computed.
The backend must return current managed authority and exact source binding.

`terraform plan` remains advisory. Create and independently review a backend
immutable plan with the shipped CLI/API, explicitly confirm it and obtain
required approval, then supply its exact ID and fingerprint above. Terraform
never creates confirmation/approval evidence and never issues an automatic
request requiring a human decision. Apply verifies every typed field against
the saved plan and calls only the governed apply endpoint. The backend retains
current authorization, source revision, policy, CAS, fencing and expiry checks.

`AwaitingConfirmation`, `AwaitingApproval`, `ExecutionUnknown`, failure and
transport ambiguity produce **error diagnostics**, not final successful state.
The durable operation ID is already in configuration (`apply_plan_id`) and the
diagnostic before dispatch; keep it across Terraform process restart. Observe
and reconcile that original operation using the shipped CLI/API. Never replace
it to retry an unknown effect. A saved `AppliedVerified` plan has its existing positive receipt reverified through the backend recover endpoint under current Apply authority and deployment generation, then is read back,
without redispatch. There is no automatic retry, unknown-effect recovery, approval or rollback.

For a legitimate source-content update, advance the pin explicitly through the
SourceManage API and obtain a fresh independently approved plan. Because the old
state's source pin is now stale, perform the explicitly reviewed update with
`terraform plan -refresh=false -out=update.tfplan` then `terraform apply
update.tfplan`. This skips only the stale old-source refresh; Apply still checks
the newly sealed exact plan and all live backend guards. Ordinary refresh stays
fail-closed on a stale source. Never use this to bypass a failed authority gate.

## Import, refresh, upgrade and destroy

Import existing **managed** ownership after an explicit independently reviewed
backend adopt/import operation has completed. Terraform import is neither
adoption nor transfer. The import ID is bounded JSON containing exactly the
resource's snake_case inputs above, with `event_classes`/`event_types` arrays and
the completed operation's `apply_plan_id`/`apply_plan_fingerprint`. For example,
store it in a protected local file and pass its content to `terraform import
kafdeck_notification_subscription.alerts ID`. The provider requires the exact
completed plan to be readable by this principal and calls read-only
`/api/v1/declarative/import-check`, requiring explicit Adopt plus exact read and
registered owner/source/controller binding. No plan, source, adoption or effect
is created during import. Competing/unowned/Retired/denied/stale input is rejected.

Resource refresh updates normalized current fields only on authoritative managed
InSync responses. OutOfSync returns a drift error and retains the sealed desired
state; overwriting desired fields while retaining the original source hash would
invalidate the next refresh. The read-only data source also returns an OutOfSync
diagnostic without rewriting its configured desired content. Missing, Unknown, Inaccessible, Conflict, Unsupported,
401/403, absent generation, stale timestamps and malformed responses are errors;
prior state is retained and never removed or implicitly recreated. Changed
ownership/source/deployment generations require explicit reconciliation.

Resource schema version is **1**. An explicit version-0 unpublished candidate
upgrade preserves the same complete typed state and all authority/operation
bindings without network or effects. A legacy state missing generations is
rejected instead of inventing authority. Unknown/future versions have no
upgrader and fail closed. Keep encrypted, access-controlled state backups;
state contains non-secret resource references and operation proofs.

Backend NotificationSubscription does not admit delete/retire/relinquish. Both
destroy planning and direct Delete return errors and retain Terraform state.
No irreversible destroy is advertised as supported. Other resource families
remain unregistered until their own backend lifecycle is admitted.

Acceptance is exercised through the actual Terraform binary, filesystem mirror,
provider executable and real Keycloak-authenticated Kafdeck API. HTTP contract
unit tests additionally prove each truth status, cross-scope and incomplete
evidence, pending/unknown outcomes and zero redispatch. See the G4 acceptance
ledger for exact run hashes and the limits of each proof.
