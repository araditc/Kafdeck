# Offline Windows Server installation (monitoring preview)

This preview includes Kafdeck, Microsoft OpenJDK **21.0.13**, PostgreSQL **17.12**, Keycloak **26.8.1**, WinSW **2.12.0**, the Microsoft Visual C++ runtime installer, and the Kafka monitoring agent. No existing identity server, Java installation, .NET installation or Internet connection is required on the target server. The API is self-contained. This unsigned monitoring preview has its own download and does not replace the signed stable v0.9 distribution.

Download the ZIP and its `.sha256` file from the [monitoring preview release](https://github.com/araditc/Kafdeck/releases/tag/monitoring-preview-20261115). Keep both files together. The checksum file is for verifying the outer ZIP; the installer verifies the extracted files against `bundle-manifest.json`.

## Requirements

- Windows Server 2019/2022, x64, with Windows PowerShell 5.1 and Administrator access.
- At least 4 GB free disk space for installation; allow additional space for monitoring history, logs and backups.
- A stable private IPv4 address assigned to the server, reachable from browsers and Kafka brokers.
- Free ports: **8080** (dashboard), **8443** (Keycloak), **15432** (private PostgreSQL).
- The Windows password of the account running the installer. A Windows Hello PIN is not a service password. Use a controlled installation account: Kafdeck and Keycloak retain this identity for DPAPI and service logon.

## Install

1. Copy the ZIP and checksum to the server. Verify the hash against the accompanying file:

   ```powershell
   Get-FileHash C:\Downloads\kafdeck-offline-win-x64.zip -Algorithm SHA256
   Get-Content C:\Downloads\kafdeck-offline-win-x64.zip.sha256
   ```

2. After verifying the source and hash, unblock the downloaded ZIP in its Properties if Windows marked it as downloaded. Extract into a **new, empty** directory:

   ```powershell
   Expand-Archive -LiteralPath C:\Downloads\kafdeck-offline-win-x64.zip -DestinationPath C:\Kafdeck-Setup
   ```

3. Open **Windows PowerShell → Run as administrator** and run:

   ```powershell
   Set-Location C:\Kafdeck-Setup
   .\Install-Kafdeck.ps1
   ```

   If scripts are disabled, review the scripts and use `Set-ExecutionPolicy -Scope Process RemoteSigned` for this window. Organization Group Policy restrictions require the Windows administrator to authorize execution.

4. Enter this server's IPv4 address, the Kafka host IPv4 addresses (comma-separated, without ports), and the Windows password when prompted. The account name is selected automatically; password entry is masked. The default installation directory is `C:\Kafdeck`, separate from the extracted medium. Use an ASCII installation path without spaces.
5. If the Visual C++ installer requests a reboot, restart Windows and run the **same extracted installer** again with the same Windows account. Keep the installation directory. The receipt allows the same package/account/path to resume.
6. Wait for PostgreSQL, Keycloak and the dashboard to become ready. The first Keycloak start can take several minutes.

Ports can be selected for a fresh install, for example `./Install-Kafdeck.ps1 -IdentityPort 18443`. An existing installation resumes with its recorded ports. Never stop an unrelated process merely to free an installer port.

The script refuses existing foreign services, tasks or a nonempty unknown target directory. It does not migrate an older Scheduled Task installation or overwrite an existing installation with another package. Do not extract a different package over an in-progress installation.

## Create the first administrator

At completion the installer prints the actual URL and a one-time setup code valid for **15 minutes**:

```text
https://YOUR-SERVER-IP:8080/
```

The URL and code are also in the private file `C:\Kafdeck\secrets\FIRST-ADMIN.txt`. Open the URL on the server, enter the setup code, administrator email and a password of at least 12 characters. **Email verification is not required.** Then sign in through Keycloak. Initial setup closes after the first owner is created.

To renew an expired code before creating the first owner, run as the installation account:

```powershell
C:\Kafdeck\scripts\New-SetupCode.ps1
```

This command does not reset an existing owner. Manage subsequent users through **Manage users** in the dashboard. The permanent Keycloak recovery account is separate; display its details locally with `C:\Kafdeck\scripts\Show-RecoveryInfo.ps1` and keep them private. The temporary Keycloak bootstrap account is removed after verifying the recovery account.

## Services and reboot verification

All three components are registered with Windows Service Control Manager:

| Service | Startup | Runs as | Dependency |
| --- | --- | --- | --- |
| Kafdeck-PostgreSQL | Automatic | NetworkService | — |
| Kafdeck-Keycloak | Automatic (Delayed Start) | Installation account | Kafdeck-PostgreSQL |
| Kafdeck | Automatic (Delayed Start) | Installation account | Kafdeck-Keycloak |

The two application services use the bundled WinSW wrapper. The installer grants Log on as a service; organization policies that deny it must be resolved by the Windows administrator. SCM holds the service password; it is not written to XML or command arguments. Closing PowerShell or signing out does not stop installed services. Recovery actions restart failed processes. Keycloak waits for an authenticated PostgreSQL connection; Kafdeck waits for identity discovery readiness.

After restarting Windows, run:

```powershell
Get-Service Kafdeck-PostgreSQL,Kafdeck-Keycloak,Kafdeck
C:\Kafdeck\scripts\Test-InstalledServices.ps1 -RequireReboot
```

The check verifies a reboot occurred after service registration, automatic/running services, dependencies, delayed startup and both HTTPS endpoints. For manual shutdown stop Kafdeck, then Keycloak, then PostgreSQL. Start them in reverse order. Use `services.msc` for normal administration. If the Windows password changes, update **Properties → Log On** for Kafdeck and Kafdeck-Keycloak. Changing the account requires a deliberate DPAPI/key migration.

## Connect browsers, Kafka and monitoring

The installer creates a private CA and a server certificate, trusts the CA on this server, and exports public copies under `C:\Kafdeck\public`. For another browser machine, securely copy **only** `server-public.cer`, verify its origin and import it with Administrator access:

```powershell
Import-Certificate -FilePath C:\Path\server-public.cer -CertStoreLocation Cert:\LocalMachine\Root
```

Use the same server IP configured during installation. The server leaf certificate lasts one year; plan renewal. This is an internal-network deployment, not a public Internet certificate setup. Never distribute PFX files, DPAPI files or the secrets directory.

After sign-in open **Kafka connections**, enter the broker addresses and security settings, test and save. The saved cluster is selectable in the dashboard. Configure Schema Registry, Connect or ksqlDB separately for that cluster when needed. Open **Monitoring**, enable collection and create one agent source per broker. Follow the [complete Java agent guide](kafka-agent.md) to install the bundled JAR on broker JVMs. Installing the Windows package does not install remote broker agents.

PostgreSQL listens only on `127.0.0.1:15432`. Inbound firewall rules permit dashboard/identity HTTPS from LocalSubnet and the supplied Kafka addresses. Other client networks require deliberate firewall configuration. Deployment-tested outbound egress restrictions remain necessary: broker-advertised addresses can differ from initial bootstrap addresses (KAF-02).

## Troubleshooting and backups

- **Package verification failed:** compare ZIP hash, extract again into a new empty folder; never edit the manifest or disable verification. The outer `.sha256` does not belong inside the extracted package.
- **Account prompt on an older package returns nothing:** use the same extracted package and explicitly provide a masked current-account credential:

  ```powershell
  $account = [Security.Principal.WindowsIdentity]::GetCurrent().Name
  $password = Read-Host "Windows password for $account" -AsSecureString
  $credential = [PSCredential]::new($account, $password)
  .\Install-Kafdeck.ps1 -Credential $credential
  ```

- **Reboot required:** reboot and resume with the same package. Do not delete its receipt.
- **Service will not start:** inspect `C:\Kafdeck\logs`, `services.msc` and Event Viewer → Windows Logs → System. Check account password, Log on as a service policy, ports and dependencies. Wrapper stdout/stderr logs rotate by size.
- **Verify media without installing:** run `./Install-Kafdeck.ps1 -VerifyOnly`.

Back up **config, secrets, state** and a consistent backup of the Keycloak PostgreSQL database together. Data Protection keys are in `state\keys`. Current-user DPAPI binds secrets to the installation account and machine; copying directories alone is not a supported cross-machine migration. Preserve the database and state when troubleshooting.

## Validation record

Automated evidence shipped in the ZIP covers real Java/PostgreSQL/Keycloak startup, TLS, first-owner creation without email verification, setup closure and temporary-admin removal. Focused checks cover WinSW parsing, registration parameters and masked account-bound credential input. A user reported successful installation and execution on Windows Server on **2026-11-15**, after the required runtime reboot. This is operator-reported acceptance; a full server reboot of all three installed services was not independently observed. Use the post-reboot check above on each target deployment.

See also the [Persian RTL guide](windows-offline-install.fa.md).
