# D1 actual signed OCI runtime supplement

Application source is unchanged protected main `01c7553425fbc61ba4a7a21918265c562086ba02`. Exact official full-distribution manifest SHA256: `b63864259a8a5e1c3c9d8dc3312d35a56e82749d62e2f6a05b88112f98af29ba`; successful signing producer42984770583, signed artifact13839715488, ZIP SHA256 `7100754a28ebc405a8ca9739412341a3e13587f6e9514882ab547f2e62b8c295`. Owner-reviewed trust root: `0ba58f6c09c271f664f0f8c1fa62e2065844ca89a0397c0178ed5188f6096724`.

The fixture author/executor was Codex `/root/w78_independent`; this is executed fixture evidence, not an independent review of its own harness. No product/image rebuild, main change, registry mutation or W82 rerun occurred. The frozen W80 owner proposal remains untouched. Fixture branch: `codex/v09-oci-d1`.

## Executed native cells

| Native runner | Accepted job / immutable fixture source | Artifact / verified ZIP SHA256 |
|---|---|---|
| Ubuntu24.04 x86_64 | [Run42996452390/job130441628118](https://github.com/araditc/Kafdeck/actions/runs/42996452390/job/130441628118), successful selected amd64 job; driver `72c5d23a2bac2f8057f936df73537fbea25dbf7c` | 13844770244; `b4938f4a846b5a9d637428ecf3f5b508aab28c538ef95da2383496ec87595080` |
| Ubuntu24.04 aarch64 | [Run42997914364/job130446509830](https://github.com/araditc/Kafdeck/actions/runs/42997914364/job/130446509830), successful ARM-only run; driver `31cfa25ce6178843230073a12ff4939a1e02fdc7` | 13844816863; `f6e9161ea6cc66088dd66ec2e33b5d11ed1170f08607b8a1a64b4f08658167d1` |

The first listed wrapper run was **overall failed because its ARM job failed**; only its actually successful x64 job is accepted. Subsequent runs targeted only the remaining ARM cell, preserving x64 evidence. No emulation or build-only cell is counted.

Each runner authenticated the official signing run/artifact/source/ZIP, independently pinned the historical trust-root digest, verified the official workflow certificate signature, and recomputed the signed media before loading the matching OCI archive. A fresh vendor-default root response is also recorded observationally; it never substitutes for the independent owner pin. In particular the copied field `independentDefaultSigstoreRootCompared` describes a performed comparison; exact match/equality is reported separately in `trust-root-observation.json`.

| Actual OCI | Signed archive SHA256 | Config digest |
|---|---|---|
| amd64 | `9b06341c69ef254700ec5c0c12d969ebf0c5f5302eb3e40de2587fb1165232ae` | `sha256:8b87c333d701f77511de2745f7e22c4cd4519aac296d30db28e983bba8ea7624` |
| arm64 | `7b2b5e6304a224ec2b198c70c11c03aa96cf9ce5bb2a2bc0ca6088fb4953fbd3` | `sha256:2128b2c2c1c08187fd4f0c3f64fdfa6e7847a1b8ed531967a655bbe12bc8b216` |

## Actual observations on both architectures

- The running server was the **signed OCI API/UI**, not the extracted native API apphost. A same-image sleep anchor held a network-none namespace across app process replacements. Keycloak, real Kafka and signed native CLI/MCP fixtures joined that namespace from the host. Loopback was the only interface; no IPv4/IPv6 default route existed.
- The API container used UID1654, readonly root, dropped ALL capabilities, no-new-privileges, writable SQLite state bind and bounded tmpfs. Dependencies were staged before isolation; runtime downloaded none. Host Java21, Keycloak26.8.1 archive digest and Kafka4.3.1 distribution identities are retained.
- Genuine Keycloak HTTPS private_key_jwt service authentication succeeded through the OCI API for capabilities, actual Kafka topic metadata and the exact fixture record. Unauthenticated read returned401. Signed MCP observed the same actual broker metadata. There is no browser-human, business-approval or product-mutation claim.
- Actual app initialization created persisted governance, catalog and request SQLite schemas. Stop/start of the same OCI app preserved generation/revision/schema and authenticated reading of the exact record succeeded again.
- With the original writer stopped, SQLite backup/restore passed integrity_check and preserved app schema/generation. The **same actual OCI** performed maintenance-mode next-generation admission without HTTP/workers, rejected the old generation, then started on the restored database with the new generation and read the exact Kafka record again. Schema hash stayed unchanged; generation revision changed1→2. This is initialized app state, not an unknown-effect Kafka replay fixture.

ARM negative-test nuance: the actual stale process logged `GovernanceGenerationException` before HTTP but `docker start --attach` did not finish within the earlier bound. The final fixture starts detached, observes that exact exception and absence of an HTTP listener, explicitly removes/fences the stale container, then resumes the new generation. Its report truthfully records `staleContainerRequiredForcedCleanup: true`; autonomous clean exit is not claimed. Both actual positive startup/restore paths succeeded.

Owned app/anchor containers and JVM fixtures are bounded and cleaned by finally/trap; runners are disposable. Early failed attempts remain in branch history: a brittle current-vendor-root byte comparison, root-owned SQLite WAL/SHM caused by Python connections not explicitly closed, and ARM attached-fatal-stream timeout. Fixture corrections changed no signed product bytes and did not disable generation enforcement.

## Evidence and limits

[amd64 identity](amd64/oci-identity.json), [amd64 journey](amd64/journey.json), [ARM identity](arm64/oci-identity.json), [ARM journey](arm64/journey.json), verified downloaded ZIPs, exact fixture-source ZIPs and [driver hashes](driver-hashes.json) are retained. The image-identity records additionally bind the individual runtime fixture files to each driver SHA.

This closes the demonstrated **actual OCI identity/broker/SQLite persistence/restart/restore** gap for both native architectures. It does not independently demonstrate a released-v0.8 OCI-container→v0.9 OCI-container upgrade or PostgreSQL inside this new OCI lane. W80 official-old-OCI-binary→signed-native upgrade/rollback and PostgreSQL physical restore remain separately named evidence. The parent release review must preserve that distinction when mapping the D1 row; this supplement alone does not declare the entire distribution matrix or publication ready.
