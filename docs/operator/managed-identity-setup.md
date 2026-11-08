# Managed Keycloak setup

For a new standalone installation, use a dedicated Keycloak realm and enable
`Kafdeck:ManagedIdentity`. Configure HTTPS for the Kafdeck listeners and issuer,
with a trusted certificate for each. Keep self-registration off. During initial
setup, leave email verification and email-based password reset disabled until
the realm has working email and an appropriate recovery policy. Configure a
confidential Keycloak service client with client credentials and permission to
manage users in this realm. Keep the browser OIDC client
separate. The service client secret belongs in a private server file.

```json
{
  "Kafdeck": {
    "ManagedIdentity": {
      "Enabled": true,
      "StateFile": "/var/lib/kafdeck/identity-state.json",
      "ServiceClientId": "kafdeck-user-management",
      "ServiceSecretFile": "/run/secrets/kafdeck-management-secret"
    }
  }
}
```

Set up the state directory before launching Kafdeck. Use a persistent local
filesystem and restrict write access to the application and installer. Back up
this file together with the matching Keycloak realm. The managed mode is for a
single installation with one local state directory. Externally managed OIDC
installations use the usual subject or group bindings instead.

From the host, use the same content root and configuration as the running API:

```sh
dotnet Kafdeck.Api.dll --contentRoot /etc/kafdeck --issue-setup-code
```

Enter the code on the setup page with the first administrator's email and
password. Each issuance replaces the previous code and expires after 15 minutes.
Once setup succeeds, the CLI will refuse to issue another code. If provisioning
stops with an unknown outcome, the installation locks setup for server-side
recovery. Inspect the realm and restore matching state from backup if available.
Do not delete the setup state as a password recovery mechanism.

After signing in, the first administrator can create users, choose a viewer or
operator role, assign individual clusters, and disable or re-enable accounts.
New users change their temporary password on their first Keycloak sign-in.
The first administrator is protected from disabling or changing their role
through the user management page. Managed roles grant bounded read access to
Kafka. Governed mutations use their separate authorization policy.

The owner can register Kafka targets from the connection workspace. Set
`Kafdeck:ConnectionWorkspace:RegistrationNetworks` to the approved CIDR
prefixes, for example `["192.168.100.0/24"]`. For new targets, enter each
bootstrap broker as an IP address and port. A successful Kafka metadata probe
is required before saving. Kafdeck keeps the name, broker list, protocol, and
SASL mechanism in the identity state file. Enter credentials again when
opening a connection that requires them. The owner can grant users access to
saved targets in Manage users. Saving or opening a connection selects it in the
dashboard and opens Topics. Saved targets also appear in the dashboard cluster
selector. A secured target uses the current sign-in's in-memory credentials
for up to 30 minutes. Return to the connection form and re-test it if those
credentials expire.

When governed mutations are configured, the owner and users assigned the
Cluster manager role can administer topics and consumer groups on registered
targets in their scope. Open a topic to see its partitions and live Kafka
configuration. The Messages view supports bounded reads and masked exports
without committing consumer group offsets. Publish message previews the
exact payload before sending and shows the broker's partition and offset
when acknowledged. If dispatch is uncertain, check the operation status
before sending again. Critical operations such as deleting a topic or
purging records require approval from a different authorized account.

Only allow registration on networks with trusted Kafka brokers. Kafka metadata
can advertise additional broker addresses. The connection test rejects advertised
broker addresses outside the registration networks. Use network egress controls
on the Kafdeck host to constrain connections if brokers change those addresses
after registration.

Direct ksqlDB Stream and Table creation is available to the installation owner
on a managed connection only when governance is inactive and ksqlDB reports the
same Kafka cluster ID. Governed deployments keep direct catalog writes disabled.
