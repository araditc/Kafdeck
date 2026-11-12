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
