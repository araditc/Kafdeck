# W79 accessibility and localization evidence

Status: producer authored; execution and actual assistive-technology acceptance remain open. This document does not claim WCAG conformance or a screen-reader pass.

The target is [WCAG 2.2 AA](https://www.w3.org/TR/WCAG22/), with the U1–U4 scenarios in [the acceptance strategy](v0.9-test-strategy.md). English is authoritative. Persian translation quality requires community review as well as technical RTL checks.

## Producers and evidence boundaries

`src/frontend/scripts/v09-accessibility-live-browser.cjs` takes four arguments: loopback API origin, private Keycloak laboratory manifest, scenario JSON and output evidence JSON. It uses the compiled product, genuine OIDC cookie sessions for two distinct humans, actual backend operations and durable reads. Authentication, the underlying immutable topic-effect preview and its exact kernel confirmation are fixture preparation. Every product workflow control uses keyboard input; target locators observe focus and do not click, focus, fill or select controls.

Each English and Persian context independently creates and reads a catalog record/history, drafts and submits a service request, receives a self-decision denial, obtains a distinct human decision and explicitly revises the material. It checks literal escaped catalog text, immutable material across language switching and the absence of mutating HTTP calls from changing locale. Axe scans desktop, 320 CSS pixel mobile and durable unknown screens. Axe rule results, incomplete results, keyboard focus styles, screenshots, Chromium version, source SHA and compiled asset hashes are recorded. An axe pass covers the evaluated rules only. Incomplete rules still require human review.

Scenario fields are `clusterId`, `topicName`, `handshakeDirectory`, `requestExpirySeconds` and optional `manualHandoffDirectory`. The root live fixture prepares an actual ACK-loss recovery case. For each locale, the browser writes `catalog-applied-<locale>.json` with the applied operation ID, hash and record ID. The fixture independently verifies a durable commit receipt before changing that operation's execution state to unknown and writing `catalog-applied-<locale>-ready.json`. This is an explicit post-commit ACK-loss fixture, not a broker outage or an API response mock. The browser reads the actual unknown operation, verifies that dispatch and replacement-key controls are blocked, and invokes positive-receipt recovery using the same operation ID. The fixture must independently verify unchanged history/receipt counts and repeat recovery idempotence.

For expiry, the browser writes `request-submitted-<locale>.json` containing the actual request ID, linked operation ID and UTC expiry. The fixture waits until that real expiry and writes `request-submitted-<locale>-ready.json`. The browser then reads `Expired`, verifies that the linked effect cannot execute, prepares a new immutable operation and explicitly revises the request. No clock, expiry or runtime projection is faked by the browser.

The scripts print only closed phase labels and numeric HTTP status on failures. Identity fields, passwords, cookie/token material, full URLs and raw exceptions must never enter public evidence. Screenshots are restricted to the authenticated application; no authentication-page screenshot is captured. Fixtures and handoffs are fresh directories, and stale completion files cause failure.

## Actual screen-reader procedure

Native discovery found `C:\Windows\System32\Narrator.exe`, file version `10.0.26100.8972`. NVDA was not found at its standard installation paths and was not running. Availability alone is not a passed screen-reader test.

Read-only host metadata reports Windows NT `10.0.26300`, x64, registry display version `26H2`, build revision `9818`. The registry product-name string reports `Windows 10 Pro`; retain the numeric build rather than infer the marketed Windows release from that string. CIM operating-system discovery was unavailable to this process, so these fields come from runtime/registry reads and executable version metadata.

Microsoft documents Narrator's speech recap and live transcription at [the Narrator guide](https://support.microsoft.com/en-us/accessibility/windows/narrator/complete-guide-to-narrator). `Insert+Alt+X` opens the last 500 spoken strings and live output. Observing this actual Narrator output is distinct from inspecting the application's browser accessibility tree. It does not establish the quality of audible Persian pronunciation or replace review by a proficient Persian screen-reader user.

For the headed run, set `KAFDECK_W79_HEADED=1` and provide `manualHandoffDirectory`. After genuine authentication and authorized cluster selection, the producer pauses before product actions. Only the designated UI operator starts Narrator, selects the dedicated Kafdeck Chromium window and observes the speech recap. Personal browser windows and authentication windows are excluded. The producer pauses again after catalog, submitted, approved, expired, replanned, unknown and recovered stages in both languages. Files are `ready-<locale>-<stage>.json` and `resume-<locale>-<stage>.json`; they contain no secrets. Pauses do not extend production preview or approval expiry. A timed-out or expired flow must be repeated with fresh material.

At each stage, preserve the actual AT/browser/OS versions, operator, UTC time, keys used, expected announcement, observed speech excerpt and issue outcome in [the manual matrix](v09-w79-manual-matrix.md). Verify headings/landmarks, labels/role/state, status/error announcement, reading order, keyboard escape and focus after asynchronous updates. Check disabled controls in both browse mode and keyboard focus order. A speech recap screenshot or text excerpt is actual output evidence; a DOM/AX snapshot alone is not.

Before stopping Narrator, return the dedicated browser to English, verify that no authentication window is active and stop only the AT session started for this run. Retain no speech from unrelated apps. Record cleanup and any untested pronunciation, touch, platform or AT combination explicitly.

## Acceptance rule

The automated driver deliberately records `manualScreenReader.status=not-observed` and leaves screen-reader acceptance missing. A separately observed manual record may close its named cells; the automated producer must never silently turn that status into a pass. W79 remains open while any required actual flow, manual contrast/focus/mobile review, translation review or tool/environment evidence is absent or failed. Browser-synthetic keyboard input, native keyboard input and actual screen-reader output must retain their individual provenance.
