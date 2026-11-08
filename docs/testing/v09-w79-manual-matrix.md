# W79 actual keyboard and assistive-technology issue log

Status: the 2026-11-08 headed run recorded all 18 English and Persian observations and both native window returns on `c0b6f82e551cafdf27954e99409bc3a2caafac37`. The assembled record includes owner approval for human contrast. Runtime and automated results are in the [evidence ledger](v09-w79-accessibility-evidence.md). Earlier incomplete attempts remain historical.

The earlier Release pilot reached ten catalog keyboard controls and observed zero new catalog speech. The November 8 run verified native window control in a separate preflight before starting the authenticated journeys. See the [earlier native attempt](v09-w79-accessibility-evidence.md#earlier-native-attempt).

| Cell | English expected evidence | Persian RTL expected evidence | Result / artifact / issue |
|---|---|---|---|
| U1 catalog | Keyboard create/preview/confirm/execute, literal text and immutable history; spoken labels, exact material and result | Same actual controls and material with RTL reading order and isolated machine IDs | Both catalog stages recorded |
| U1 request submission | Draft and submit exact bound operation; requester/decision separation announced | Same actual submit flow; English machine codes and UTC representation preserved | Both submissions recorded |
| U1 distinct human approval | Separate genuine human session; self-decision denial; exact revision approval and disabled effect gates | Same actual decision flow and denial; localized role/label/state | Both independent reviews and decisions recorded |
| U1 expiry/replan | Actual expiry announced; linked effect blocked; fresh explicit revision with old decisions cleared | Same actual expiry and replan; no stale approval transfer | Both expiries and revisions recorded |
| U1 unknown/recovery | Durable ACK-loss unknown; no blind redispatch/new key; same-ID positive receipt recovery; no duplicate history | Same actual unknown/recovery with readable invariant material | Both recoveries recorded |
| U2 locale/fallback | Authoritative English labels; missing/unsupported translation fallback; no workflow state reset or mutation | Persian resources, `lang=fa`, `dir=rtl`; labels and long mixed-script IDs remain readable | Runtime fallback and locale invariance recorded |
| U3 keyboard/focus | Logical Tab/Shift+Tab order, visible focus, no trap, clear focus after asynchronous actions; screenshot review | Logical RTL reading order with consistent keyboard sequence and visible focus | Both native returns preserved the original input and focus |
| U3 semantics/AT | Actual screen reader: headings/landmarks, controls and state, errors and dynamic status; actual spoken output retained | Actual screen reader on same flows; Persian pronunciation limits and reviewer proficiency recorded | All 18 NVDA stages captured |
| U4 contrast | Human review of text, disabled/hover/focus/error states and nontext boundaries with measured colors | Same visual states in Persian | Human contrast approved by the project owner |
| U4 mobile/reflow | 320 CSS pixel viewport, 400% equivalent desktop width, zoom/text-spacing and focus not obscured; scrollable machine material remains usable | Same widths/spacing with RTL and mixed identifiers; no document-wide horizontal overflow | Captured layouts and focus reviewed |
| U4 motion/preferences | Reduced motion, forced colors/high contrast and input targets checked where applicable | Same preference checks in RTL | Browser-emulated preferences recorded |

Run metadata must include UTC start/end, source SHA and dirty tree, compiled asset hashes, OS/build, browser/version, AT executable/version, voice/language and operator. Store safe excerpts only. User-authentication fields, tokens, private URLs and unrelated app speech are excluded.

Issue entries use: `cell; locale; exact trigger/keys; expected; actual; severity; artifact; fix/source; retest`. An automated axe incomplete result becomes a named review entry until resolved. Do not mark human translation review or audible pronunciation passed from static resource coverage or a speech transcription alone.
