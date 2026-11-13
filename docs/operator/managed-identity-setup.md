# First administrator, Keycloak sign-in and user management (v0.9)

Start with the [released installation guide](v0.9-installation-guide.md). This flow is for one managed installation, with a dedicated Keycloak realm, persistent local identity state and HTTPS. Keycloak authenticates people; the Kafka connection form configures brokers separately. No deployment token is entered into this sign-in flow.

## 1. Prepare Keycloak and HTTPS

1. Create a dedicated realm such as `kafdeck`; the `master` realm is rejected. Give the realm an HTTPS issuer reachable by both the browser and Kafdeck, for example `https://idp.example.test/realms/kafdeck`. Install trusted certificates, including the browser-facing Kafdeck hostname/IP in its certificate SAN. Follow [Keycloak TLS configuration](https://www.keycloak.org/server/enabletls).
2. Keep public user self-registration **off**. Keep **Verify email** off for initial setup. With no SMTP configured, do not enable email-dependent required actions or promise email-based password recovery. The first administrator's email is an account identifier; creation does not verify ownership of that mailbox.
3. Create an OIDC browser client, for example `kafdeck-browser`, with Standard Flow / Authorization Code and PKCE S256. This example uses client authentication and a server-side client secret. Register the exact Kafdeck origin and its `/signin-oidc` callback, for example `https://kafdeck.example.test:8080/signin-oidc`, and `/signout-callback-oidc` at that same origin for the OIDC post-logout callback, plus the intended signed-out return origin. Do not use unrestricted redirect wildcards.
4. Create a **separate** confidential service client, `kafdeck-user-management`, with service accounts/client credentials enabled. Grant its service account the dedicated realm's `realm-management` / `manage-users` permission needed to create and update users. Store its secret in a private server file. This is a provisioning identity, not the browser login identity or a Kafka credential.
5. Confirm discovery/JWKS/token/Admin REST connectivity from Kafdeck. A private issuer CA can be supplied using `Kafdeck:Deployment:Oidc:TrustedCaPemFile`; the browser must trust that issuer separately.

The first Kafdeck administrator is not a Keycloak server administrator. Maintain a separate, secured Keycloak administrative recovery path.

## 2. Configure persistent Kafdeck state

Use [the complete example](examples/v0.9-managed-appsettings.json) as `appsettings.Production.json`. Replace the issuer, client IDs, private file paths and approved network with real deployment values. `192.0.2.0/24` is a documentation-only example, not a working Kafka network.

The important settings are:

| Setting | Meaning |
| --- | --- |
| `Kafdeck:Deployment:AccessMode = Oidc` | Server-side OIDC sessions |
| `Kafdeck:Deployment:ListenUrls` | HTTPS endpoints; managed identity requires HTTPS even for loopback |
| `Kafdeck:ManagedIdentity:Enabled = true` | Enable first-owner provisioning and managed users |
| `Kafdeck:ManagedIdentity:StateFile` | Absolute, persistent, private state file; create its parent directory first |
| `Kafdeck:ManagedIdentity:ServiceClientId` / `ServiceSecretFile` | Dedicated Keycloak provisioning client and absolute secret-file path |
| `Kafdeck:ConnectionWorkspace:Enabled = true` | Enable the dashboard Kafka connection workspace |
| `Kafdeck:ConnectionWorkspace:RegistrationNetworks` | Explicit approved broker/service CIDRs; an empty list does not admit arbitrary new targets |

Do not combine managed identity with `Authorization:SubjectBindings` or `GroupBindings`. Externally managed OIDC uses the [separate RBAC configuration](v0.2-oidc-rbac-guide.md), not this first-owner wizard.

Only the runtime/installer should write the state directory. Keep its issuer unchanged across restart and back up the state with the matching Keycloak realm, application databases and ASP.NET Core Data Protection key ring. The state contains managed users, saved connection definitions and protected integration credentials. Preserve the runtime's key storage on a persistent private volume; otherwise a replacement container may be unable to decrypt saved credentials. See [Microsoft's Data Protection persistence guidance](https://learn.microsoft.com/en-us/aspnet/core/security/data-protection/configuration/default-settings?view=aspnetcore-10.0). There is no Kafdeck-specific key-ring-path setting in this release.

On Windows, replace Linux absolute paths with absolute Windows paths and grant the runtime account the appropriate directory/secret ACLs. Use the same service identity, content root and environment for setup-code issuance and normal execution.

## 3. Issue a one-time setup code

Start Kafdeck with the configured HTTPS certificate. From the installed application directory, use the same configuration/content root as the running API:

```sh
dotnet Kafdeck.Api.dll --contentRoot /etc/kafdeck --issue-setup-code
```

For a self-contained archive, use `./Kafdeck.Api --contentRoot /etc/kafdeck --issue-setup-code` on Unix or `.\Kafdeck.Api.exe --contentRoot C:\Kafdeck\config --issue-setup-code` in PowerShell. For the running release container:

```sh
docker exec kafdeck dotnet /app/Kafdeck.Api.dll --contentRoot /app --issue-setup-code
```

The command prints a **32-character one-time code**; keep it private. It expires after **15 minutes**. Issuing another code replaces the previous one. A configured HTTPS listener alone does not issue a code automatically.

Open the Kafdeck origin, enter the code, email and password in the initial setup form. Passwords must contain 12–128 characters, without control characters, and must also satisfy the Keycloak realm's password policy. Setup creates the permanent `owner` account without sending email or marking that address verified. Then sign in through Keycloak with that account.

After the first owner exists, setup closes and the command refuses to issue another code. Refreshing a code is only for an uninitialized installation; it does not reset an existing owner. If provisioning has an unknown outcome, the installation locks setup for host-side recovery. Inspect the realm and matching state/backup; do not delete the identity state or recreate the owner to bypass the lock.

## 4. Add Kafka, then create users

As the owner, open **Kafka connections**, enter a new target, **Test connection**, then save/open it. Follow the [connection and dashboard walkthrough](v0.9-dashboard-guide.md). Registration tests actual Kafka metadata before persisting the target. The target appears in the cluster selector and the dashboard opens **Topics**.

Open **Manage users** and create each user with their email, temporary password, role and assigned clusters:

| Role in the UI | Stored role | Scope |
| --- | --- | --- |
| Viewer | `viewer` | Cluster/broker/topic/schema observations for assigned clusters |
| Operator | `operator` | Also configuration, consumer, Connect and ksqlDB observations; payload read/export is not implied |
| Cluster manager | `manager` | Assigned registered-target administration when governed mutation configuration permits it |
| Initial administrator | `owner` | Installation users/target registration; separate Kafka ACLs and mutation controls still apply |

New users change their temporary password on their first Keycloak sign-in. Disabling a user or changing access invalidates their older managed sessions. The first owner cannot be disabled or have its role changed through this page. A managed role never grants Kafka broker ACLs, bypasses masking, enables an unconfigured mutation family, or replaces distinct-person approval for a critical operation.

## Safe ordinary-user provisioning and recovery (main after v0.9)

Before creating additional users with this implementation, a Keycloak realm administrator must add the user-profile attribute below using the realm's User Profile settings. Keep its view/edit permissions **admin only**, single-valued, and optional. Do not mark it required for ordinary users or expose it through a token mapper. Merge this attribute into the existing profile; do not replace the other profile definitions.

```json
{
  "name": "kafdeckProvisioningId",
  "multivalued": false,
  "permissions": { "view": ["admin"], "edit": ["admin"] }
}
```

Kafdeck checks the profile before dispatch/recovery; a missing or user-editable proof attribute fails closed with `identity-provisioning-profile`. Its existing provisioning client needs `manage-users` and can read the profile; **do not grant it `manage-realm`**. Profile configuration remains an administrator task. This does not change initial-owner setup or require email verification.

Creating an ordinary user writes a private durable request intent before contacting Keycloak. A response timeout or local storage failure leaves an explicit pending operation. In **Manage users**, use **Recover user creation** for that original request. Recovery checks the exact private request proof and account identity and does not send another create request or reset a password. A re-entered password on an exact retry does not replace the original temporary password. An unrelated existing email account is never adopted.

The owner-only API exposes pending operation IDs in `GET /api/v1/identity/users` and requires CSRF for `POST /api/v1/identity/users/provisioning/{operationId}/recover`. No proof, password or provider token is returned. Changed material/current authority or uncertain/missing provider evidence fails closed. If recovery cannot prove an outcome, inspect/fence the installation with the Keycloak administrator; do not delete its request intent, adopt an account by email alone or blindly repeat dispatch.

Preserve the managed state file and matching realm/key material together. New nullable receipt fields read legacy v0.9 state; do not run an older writer against the new state. Rollback uses the verified matching pre-change backup and binary. The receipt log is bounded to 2,048 retained requests, with capacity reserved within the existing 1,000-user limit. Audit delivery is at least once with a stable request correlation ID; a crash after audit delivery can repeat that same correlated event. No exactly-once logging claim is made.

## Troubleshooting

| Symptom | Action |
| --- | --- |
| Setup code expired | Run the issuance command again for the same uninitialized state; use the newest code within 15 minutes |
| API is healthy but setup/dashboard HTML is missing | For native execution with a separate configuration directory, pass `--webroot` pointing at the extracted application's `wwwroot` |
| Setup page absent | Check `ManagedIdentity.Enabled`, OIDC mode, HTTPS and whether the installation is already initialized |
| `identity-provider` / provisioning failure | Check the dedicated realm, service client secret, `manage-users`, TLS trust, connectivity and realm password policy |
| Login succeeds but users/clusters are absent | Use the matching issuer/state/realm; confirm the current role and cluster assignments |
| New target is rejected | Configure approved CIDRs and runtime egress; use reachable broker IPs/ports, not arbitrary unapproved destinations |
| Saved integration cannot decrypt after restart | Restore the matching Data Protection key ring or re-enter its credentials; preserve keys and state on future restarts |
| Owner lost password and SMTP is absent | Use the secured Keycloak administration recovery procedure; do not delete Kafdeck's identity state |

## Browser API reference

See the [experimental managed-browser contract](../api/managed-browser-contract.md) for exact user/connection request and response shapes, CSRF, conditional availability, recovery and revision semantics. Authenticated discovery is `/api/v1/openapi/managed-browser.json`; this is not a separate Bearer/CLI management surface.

## Managed-state upgrade and key backups (main after v0.9)

The [G06 format-freeze and recovery contract](../implementation/v1.0-g06-state-responsiveness.md#g06-format-freeze-and-recoverable-encryption-context) covers the managed JSON separately from the mutation database. Back up both with the matching Data Protection key ring, original content-root/application discriminator, issuer/client configuration and material-digest key. Stop writers and verify a private restored copy before switching binaries. Unknown/duplicate/truncated/newer state is rejected at startup and cannot be overwritten by ordinary setup/user/connection writes. Do not delete state to recover an installation.

By default the existing ASP.NET key-ring selection is retained. To use an explicit restored ring, configure `Kafdeck:DataProtection:KeyDirectory` to its existing absolute private directory. Optional `Kafdeck:DataProtection:ApplicationName` must match the original context; otherwise retain the original content root. Windows DPAPI-protected keys require the original compatible account/machine context. Merely copying a key XML file does not make it decryptable elsewhere. Rollback restores verified backed-up data/configuration and the original binary together, rather than running an old binary against a newer live file.

Secured Kafka leases remain in memory for 30 minutes, with capacity 128. HTTP 409 `connection-credential-capacity` means the target definition exists but credentials were not retained; reconnect to that saved target after capacity becomes available. Existing admitted leases stay usable. Restart still requires Kafka credentials again. Browser reads have a 15-second deadline; a timed-out read may be retried explicitly. Never interpret a write timeout as proof no effect occurred.
