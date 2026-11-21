# Single-file offline Windows setup

The source-built graphical installer is a supplement to the signed native Kafdeck distribution. It is **not yet an officially published release asset**. Do not replace a published asset or assume this installer contains unreleased Fleet features. The embedded manifest records the actual runtime version and source separately from the installer source.

Publisher metadata: **Arad ITC** · [arad-itc.com](https://arad-itc.com) · [info@arad-itc.org](mailto:info@arad-itc.org).

## Install on an empty Windows Server

1. Obtain the single `Kafdeck.Setup.exe` from your trusted distributor. Compare its SHA256 against the independently obtained build/release evidence. A checksum detects changed bytes; it does not authenticate an unknown download. Do not bypass a Windows security warning for an unverified file.
2. Sign in using the Windows installation account and double-click the EXE. Approve Windows elevation for that account. Windows Server 2019 or newer, x64, is required. No .NET runtime or separate Java/identity/database installation is required.
3. Choose **English** (default) or **فارسی**. Persian pages use right-to-left layout. Read the Apache 2.0 agreement and the third-party notices; accepting them is required to continue. The English upstream license text is authoritative.
4. Choose **Install / resume / restore retained services**. Use a separate application directory such as `C:\Kafdeck`. The existing engine requires an ASCII path without spaces. Enter this server's stable private IPv4 address, the Kafka hosts' IPv4 addresses without ports, and permitted browser networks. Choose three distinct unused ports: dashboard 8080, Keycloak 8443, local-only PostgreSQL 15432 by default.
5. Enter the current Windows account's password, not a Windows Hello PIN or a Kafdeck password. This account owns the Windows Services and DPAPI-encrypted secrets; retain it for subsequent maintenance. The password goes through a private stdin pipe and is not written into setup configuration or process arguments.
6. Review and start. Java, PostgreSQL, Keycloak, Kafdeck and the required Windows runtime are inside the EXE. Setup verifies the embedded archive and each payload file. The first Keycloak start may take several minutes. Do not terminate setup or reboot while it is working.
7. If a prerequisite requests a reboot, restart Windows, then run **the same EXE** with the same account and directory. Setup resumes from its receipt. It never reboots Windows automatically or stores the Windows password for unattended resume.
8. On success, the final page shows the dashboard address, e.g. `https://192.168.100.36:8080/`. If the first administrator does not exist, it also shows the one-time setup code, valid for 15 minutes after issuance. Create the administrator there, then sign in through Keycloak. No email verification is required for the first owner. Existing administrators are preserved and the final page offers sign-in instead.

The code also remains in the private `C:\Kafdeck\secrets\FIRST-ADMIN.txt`. If an unused code expires, use the existing `scripts\New-SetupCode.ps1` with the installation account; do not reset the database. On another browser machine, review/import `public\server-public.cer` using your organization’s certificate procedure. Production deployments should supply an appropriate trusted certificate.

All three services start automatically after reboot. The application appears in **Windows Settings → Apps → Installed apps** with publisher **Arad ITC**. PostgreSQL listens on loopback only. Configure Kafka authentication, clusters and ecosystem endpoints from the dashboard as described in the [dashboard guide](v0.9-dashboard-guide.md).

## Upgrade using the same EXE workflow

Run the **new version's** single EXE using the original Windows account, accept the agreement, select **Upgrade existing installation**, and keep the original directory. No Windows password is collected for an upgrade. Review and start; no manual ZIP extraction or PowerShell commands are necessary.

The established offline upgrade engine checks ownership, supported vendor-stack compatibility and free disk space, stops services, creates and verifies a cold backup, replaces application/installer files, restarts services and checks health and first-owner state. A failure uses its retained backup and recovery journal. Users, connection settings, monitoring history, databases, DPAPI keys and vendor runtimes are retained. Downgrades and unsupported vendor migrations are refused. The GUI does not override these checks. See [offline recovery details](windows-offline-upgrade.md).

## Remove services, retain data, or restore

Choose **Uninstall** from Installed apps, or run the EXE and select **Remove services (retain files and all data)**. It validates every service's executable path and account before the first stop, then stops/unregisters only the three owned services. The application directory, binaries, vendor runtimes, users, databases, secrets, keys, logs, backups and certificate remain. A partial removal can be resumed by choosing removal again.

To restore service registration, run the **exact same media** with the same account and directory, choose **Install / resume**, and enter the Windows password. This reuses the retained database/configuration; it does not create a replacement administrator. Restore services before upgrading to a different package. Full data destruction is deliberately outside the GUI.

## Disk space, cache and support

Allow at least **6 GB free for a new installation**, plus data/history capacity. Upgrades require enough room for the engine's verified cold backup (it checks before stopping services). Setup keeps its verified medium and maintenance EXE under `%ProgramFiles%\Arad ITC\Kafdeck Setup\<payload-sha256>`. This cache is private to the installation account, Administrators and SYSTEM. Do not delete the current cache while the application is registered: it supplies the Installed apps uninstaller and reboot/resume path. Older caches can be archived after confirming they are not referenced by Installed apps and no pending installation/upgrade depends on them.

An interrupted extraction is staged separately and never executed as verified media. A hash/inventory failure stops setup. Do not edit files inside an extracted/signed package. Failure messages intentionally exclude secrets; preserve `logs` and any recovery journal, then contact **info@arad-itc.org**.

The EXE's Company/Publisher metadata does not constitute an Authenticode signature. Current development builds are **unsigned**. A trusted code-signing certificate and release verification are required before claiming a verified Arad ITC publisher. Native Sigstore signatures retain their original scope; they do not sign this supplementary EXE.

## Reproducible build and acceptance

On a connected build host with .NET 10 SDK, Python 3.11+ and Pillow:

```powershell
python scripts/package/build-windows-setup.py --media C:\Inputs\kafdeck-offline-1.1.0-win-x64.zip --expected-sha256 <independently-verified-sha256> --output artifacts\windows-setup-build
```

The operator needs only `publish\Kafdeck.Setup.exe`. `build-evidence.json` and its outer checksum remain distribution/audit evidence. Building refreshes installer scripts/docs while preserving the admitted native/runtime/vendor bytes; it records the installer source and whether those build inputs have local modifications. It never relabels the native runtime as a new feature release. Do not distribute development output as an accepted stable release.

Release acceptance includes both language/layout checks, rejected license acceptance, corrupt payload/path rejection, no credential leakage, clean installation, reboot/resume, existing-installation upgrade/recovery and service removal/restore on an isolated elevated Windows environment. Reuse existing successful runtime tests for unchanged native bits; report any unexecuted elevated acceptance explicitly.
