# Offline Windows upgrade

Obtain the supplementary `kafdeck-offline-1.1.0-win-x64.zip` and its matching `.zip.sha256`. This package supports a fresh installation or an in-place application upgrade of a completed **Kafdeck 1.0.0 Windows Services** installation with the bundled vendor stack.

## Run one command

Copy the files to the server, compare `Get-FileHash` with the downloaded checksum, and extract into a fresh directory such as `C:\Kafdeck-Setup-1.1.0`. Open **Windows PowerShell as administrator**, using the same Windows account used for installation, and run

```powershell
C:\Kafdeck-Setup-1.1.0\Upgrade-Kafdeck.ps1
```

For an installation in another directory, supply `-InstallRoot D:\Kafdeck`. Use `-VerifyOnly` to check eligibility before scheduling downtime. The script uses the existing service credentials and requires no password prompt. The dashboard will be unavailable while the upgrade runs.

## What the script does

1. Verifies every package file and the existing installation receipt, Windows identity, service paths and supported vendor stack. Acquires an exclusive installation lock.
2. Checks available space and records the existing first-administrator state. The application uses the existing configured address and ports.
3. Disables automatic startup temporarily, then stops Kafdeck, Keycloak and PostgreSQL in dependency order.
4. Copies application files, configuration, secrets, Data Protection keys, monitoring history, identity PostgreSQL files, runtime files and service definitions into a private cold backup. Verifies the entire backup by SHA256 before changing application files.
5. Replaces the application, operator scripts, documentation and agent distribution with the new package. The bundled agent can subsequently be deployed to broker JVMs using the agent guide.
6. Restores automatic startup, starts PostgreSQL, Keycloak and Kafdeck, then verifies HTTPS, identity discovery, the first-owner state and the installed application hashes.
7. Records the installed package/version and prints the dashboard URL and backup directory.

The script updates Kafdeck to **1.1.0** with the existing **Java 21.0.13, PostgreSQL 17 and Keycloak 26.8.1** installation. Vendor major-version migrations need a release-specific migration implementation. The script checks eligibility before stopping services. Customized external databases or state outside the installation directory require a corresponding backup/migration implementation.

## Failure and power loss

A failed upgrade stops the services, restores the verified cold snapshot including application state and databases, and starts the original version. It returns an error explaining that recovery completed. If recovery itself fails, preserve the installation and the printed backup, resolve the reported operating-system error, and run the **same command** again.

After a power loss or interrupted upgrade, run the same command under the original account. Its journal recovers the original installation first. It then asks you to rerun the command to apply the new package. An already completed upgrade checks the installed version without creating another backup. A concurrent upgrade is rejected.

Backups are kept under `C:\Kafdeck\backups\upgrade-<id>`. Retain them until you have signed in and checked your saved clusters, users and monitoring history. Backup files include private credentials and database contents and retain restricted ACLs. Current-user DPAPI requires the original Windows machine/account for recovery.

## Verify after upgrading

```powershell
Get-Service Kafdeck-PostgreSQL,Kafdeck-Keycloak,Kafdeck
C:\Kafdeck\scripts\Test-InstalledServices.ps1
```

Open the URL printed by the script, sign in with the existing administrator and select the existing cluster. After your next planned server reboot, run `Test-InstalledServices.ps1 -RequireReboot` to verify automatic startup. Keep the saved package, checksum and backup together with your operational records.

For a fresh server use `Install-Kafdeck.ps1` and the [installation guide](windows-offline-install.md). The [Java agent guide](kafka-agent.md) covers broker JVM installation. The original signed native manifest and signature are included in `native-signature/`. The supplementary Windows installer has its own checksum inventory. Its scripts and bundled third-party runtimes are outside the native distribution's Sigstore signature scope.

## A previous package reports another installation or account

Windows can report a service path using different slash separators from the installer, or a local service account as `.` followed by the username. The corrected package compares equivalent paths and resolves the local account through the computer name before checking its SID.

If an earlier 1.1.0 package reports `Service Kafdeck belongs to another installation/account`, obtain the corrected ZIP and matching checksum, extract into a fresh folder and run `Upgrade-Kafdeck.ps1 -VerifyOnly` as the original installation account. Then run `Upgrade-Kafdeck.ps1`. The corrected error identifies an actual path or account mismatch. Preserve the original installation and service identity when investigating a mismatch.

## Bundled Microsoft Java version

The included Microsoft JDK reports `JAVA_VERSION="21.0.13.0.2"`. The corrected upgrade accepts this exact vendor revision as well as the previously admitted `21.0.13`. If an earlier package rejects the installed bundled stack, extract the corrected package into a fresh folder and run `Upgrade-Kafdeck.ps1 -VerifyOnly`. Other unsupported Java versions, a different PostgreSQL data major, and a missing Keycloak 26.8.1 core remain rejected with component-specific diagnostics.
