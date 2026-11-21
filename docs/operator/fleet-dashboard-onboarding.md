# Set up Linux broker hosts from Fleet

This workflow requires a source build containing dashboard Fleet onboarding. The immutable 1.1.0 release predates it and continues to use the [manual host preparation guide](v1.1-broker-host-registration.md). Use an HTTPS Kafdeck installation, its managed installation-owner account and a working Kafka connection. Ordinary users cannot manage host secrets.

## 1. Enable host management

Select your Kafka cluster and open **Fleet → Broker hosts → Fleet settings and SSH credentials → Access and networks**. Enable host management, enter allowed host CIDRs, and save. Use a specific `/32` for one IPv4 server, for example `192.168.100.33/32`. These settings take effect immediately. Grant log-reading and agent-installation permissions to the required managed users here.

## 2. Inspect a host

Open **Prepare a Linux host from the dashboard**. Enter a name, literal IP, SSH port, temporary administrative username and either its password or private key/passphrase. A non-root account needs sudo permission; supply its sudo password if required. Verify the SHA256 host fingerprint through a trusted console or administrator and enter it in the form. A fingerprint obtained from an unverified network scan alone does not establish trust.

Choose **Test SSH and inspect services**. The fixed setup program reads running services and compares their broker IDs and Kafka cluster ID with the selected connection. Docker is not needed for Linux/systemd. Compose discovery uses actual project/service labels and one running container per selected service. Pick only the intended services. A host can run several brokers.

Administrative credentials remain in the form between inspection and preparation. Kafdeck does not save them in its database, audit or browser storage. They are cleared after a preparation response. Close the form/page when abandoning setup.

## 3. Prepare access and the agent

Enter an HTTPS ingestion URL reachable **from the brokers**, ending in `/monitoring/ingest`. A broker on a different server cannot use Kafdeck's `127.0.0.1` URL. Select the sampling interval and an authorized UTC maintenance window, up to eight hours. For private/self-signed TLS, choose **Use this Kafdeck server certificate** for a locally configured Kestrel certificate, or enter the public CA PEM for your reverse proxy. Never paste a private TLS key.

Review the bundled package version and SHA256 and confirm preparation. Kafdeck generates separate restricted discovery and lifecycle SSH keys, encrypts them, enrolls a monitoring source for each selected broker, and transfers its fixed helper and offline JAR. The host receives forced-command accounts, root-owned catalogs, protected token files and launch preparation. Systemd gets a service drop-in; Compose gets an overlay containing the additional JVM option and a read-only agent-directory mount. Existing original Compose files are retained. Installing these prerequisites needs existing OpenSSH, Python 3 and sudo on the Linux host; this is not an OS package manager.

Preparation does **not** restart Kafka. An incomplete or uncertain result remains visible under **Pending host setup operations**. Check that same operation first. A verified receipt allows recovery without administrative credentials; a partial setup can be resumed by entering the same temporary access and explicitly confirming. Clearing an unused reservation never claims that an agent was installed.

Existing nonstandard agent paths or conflicting launch configurations are blocked rather than overwritten. For an already prepared 1.1 host, add its restricted credentials under **SSH credentials**, register and bind its existing approved service, and select its existing enrolled monitoring source below. The [manual lifecycle guide](v1.1-offline-broker-agent.md) describes its host-side catalog requirements.

## 4. Install, verify and select the next broker

In **Offline broker agent**, choose the staged version and **Preview agent change**. Review identity, package, affected partitions, replica safety and maintenance window. Confirm the individual broker restart and apply. Use **Check agent operation** until both Kafka health and a fresh JVM measurement are verified. A single-replica partition or insufficient surviving ISR blocks a restart; do not bypass that block.

For a host with several brokers, open **Select the broker for the next agent change**, choose a verified binding and its source/credential, and save. The wizard-created credential and source are suggested automatically. Finish or reconcile a pending agent change before switching its target. Verify charts under **Monitoring**. Log access uses **Brokers → Broker service logs**; Kafka configuration uses **Fleet → Dynamic broker settings** and its own permissions.

## Replace or revoke access

Under **Fleet settings and SSH credentials → SSH credentials**, select an existing alias and enter a replacement password/key to rotate it. Secrets are write-only: saved private keys, passwords and passphrases cannot be displayed or downloaded. Confirm **Revoke credential** to erase it from Kafdeck and prevent further use. Removing the host-side public key is a separate Linux administration step; an independently copied private key is not remotely destroyed by database revocation. A replacement key must already be authorized on its Linux account.

## Backup, restore and upgrade

Stop Kafdeck for a consistent cold backup. Back up managed identity state, its old sidecars, monitoring state, application Data Protection keys, and the **entire** `<StateFile>.fleet` directory. This includes the encrypted SQLite file and the separate Fleet key ring. Losing the key ring makes SSH secrets unreadable; copying only the database intentionally does not decrypt them.

On Windows restore under the same service account and machine because Fleet keys use DPAPI. Moving to another machine/account requires a deliberate credential reprovisioning procedure; do not copy keys and assume DPAPI is portable. On Linux preserve ownership and modes: the Fleet directory/key ring is owner-only, and the wrapping PFX is `0600`. Encrypt backups containing both data and keys. Keep the existing application Data Protection keys needed for protected pending agent plans as well.

The offline Windows upgrade takes a cold snapshot of the installed `state` tree, which includes a normally located Fleet directory. Do not put managed state or keys outside the upgrade's admitted backup paths. Existing file/environment credential references continue to work; database aliases override matching references, including revoked aliases. Keep historical sidecars until the upgrade/rollback window has closed. New Fleet changes are authoritative in the database and are not synchronized back into the old sidecar.
