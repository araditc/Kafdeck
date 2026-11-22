# RFC-0013: single-file offline Windows setup

- Status: **Implemented; isolated Windows acceptance recorded; publication pending**
- Date: 2026-11-21
- Owner decision: [approval log](../decisions/approval-log.md#windows-graphical-offline-setup--2026-11-21)
- Scope: supplementary installer frontend, no native release relabeling or vendor-stack migration

## Accepted behavior

Ship one self-contained Windows Forms EXE embedding the verified existing offline medium (Kafdeck, Java 21, PostgreSQL, Keycloak and agent). Reuse the established PowerShell 5.1 install/upgrade/recovery engine. The target server requires no downloads or preinstalled .NET runtime.

Use **Arad ITC**, the existing Kafdeck logo, `https://arad-itc.com` and `info@arad-itc.org`. Offer English (default) and Persian with RTL layout. Display unmodified Apache 2.0 text and third-party notices with mandatory acceptance; add no proprietary restrictions or SLA. Record acceptance timestamp, account SID, language and package identity privately.

UAC and the original account/DPAPI boundary remain. Collect the current Windows password only for new/resumed installation; send structured input through an anonymous stdin pipe to a fixed worker. Never persist it or put it in command arguments. Never expose native error records, credentials or setup codes in generic output/logs.

Keep the existing application layout and all account/path/version/vendor-stack guards. Upgrade retains the verified cold backup, health checks and recovery journal. A recovered interrupted upgrade must be reported as recovery, not as successful application of the new package. Present a reboot-required page for prerequisite exit 3010; the user reruns the same EXE after reboot and supplies credentials again. No automatic reboot or persisted Windows password.

Register Installed apps metadata and the private cached maintenance EXE. Removal admits every service path/account before stopping any, journals intent, and unregisters only the owned three services. Retain all binaries, runtimes, configuration, users, databases, secrets, keys, logs and backups. Resume partial removal safely. Restoring services requires the exact same medium/account and reuses retained state. Refuse removal during a pending upgrade. No data-purge option.

## Package trust

Require an explicitly admitted input SHA256 and verify its complete inventory. Refresh only scripts/docs and preserve original signed native/vendor bytes. Record native source separately from installer source and modified-input status. Embed archive hash/length. Refuse unsafe Windows paths, aliases, links, duplicate/unlisted files and oversized archives before executing anything.

Cache only beneath protected Program Files directories with explicit installation-account/Administrators/SYSTEM ACLs. Refuse untrusted ownership, writable ACLs and reparse points. Extract into unique staging, validate then atomically promote. Retain the current cache for maintenance/reboot resume.

Company metadata is not Authenticode identity. A verified-publisher claim requires a real trusted code-signing certificate; do not substitute a self-signed certificate. The native Sigstore signature does not sign the EXE. New signing/publication is a separate release decision.

## Acceptance

- Verify one distributable EXE, logo/company/contact metadata, English default, both languages/RTL and mandatory agreement.
- Reject changed payloads, unsafe paths, unlisted files and insecure executable caches; demonstrate no credential handoff on disk/command line.
- On isolated elevated Windows, test installation, reboot/resume, automatic service start, first administrator, retained-state upgrade/recovery and service removal/restore.
- Reuse successful unchanged runtime evidence. Report unexecuted elevated checks honestly. Specialist review and protected-main checks remain required.

## Recorded implementation evidence

[Elevated Windows acceptance](../implementation/evidence/windows-graphical-setup-acceptance-20261122.json) records the exact tested baseline, EXE identity and scope. The production worker and real vendor services passed installation, first-owner creation, automatic dependency restart, cold-backup update and retained-state service restoration. Physical OS reboot was not performed; prerequisite exit 3010 was injected. A subsequent review-driven recovery test reproduces an absent live configuration receipt and verifies journal-account refusal, rollback recovery and explicit reapplication; its exact-head result is tracked in PR #515. New release signing/publication remains separate.

[Recovery follow-up acceptance](../implementation/evidence/windows-graphical-setup-recovery-20261122.json) passed on the production worker: a missing live config receipt was recovered from the verified snapshot, a foreign journal account was rejected without effects, recovery was reported separately, and the next explicit upgrade reapplied successfully. The original baseline evidence is retained.
