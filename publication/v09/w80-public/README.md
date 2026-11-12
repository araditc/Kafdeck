# W80 signed-media acceptance review packet

This is a proposal awaiting owner adoption, not an executed readiness decision or publication authorization. `owner-w80-packet.sha256` identifies its exact JSON bytes. Product source is protected main `01c7553425fbc61ba4a7a21918265c562086ba02`; manifest is `b63864259a8a5e1c3c9d8dc3312d35a56e82749d62e2f6a05b88112f98af29ba`.

Actual official v0.8 OCI binary upgrade and stable reopen passed for SQLite and PostgreSQL in run 42988318558. Verified pre-upgrade backup rollback to that official old binary passed for SQLite there and PostgreSQL in the separate bounded run 42990085696. Only the fixture probe was compiled; old and new products were not rebuilt in those jobs.

The Windows recovery lane uses a local build of the immutable old tag as its stress fixture, and the exact signed Windows candidate. It separately proves SQLite interruption at a real exclusive-write-lock migration boundary, retry, backup and generation fencing; PostgreSQL 17.11 proves interruption after one migration commit, physical base backup and named-target WAL replay, rejected quarantined writes, both original-node fences, stale-generation refusal and fresh-generation startup. Original unknown snapshots remain unknown; this is not an assertion of a reconciled Kafka effect or automatic replay.

Run 42986123750 separately proves the genuine Keycloak service-authenticated operator journey with signed installed API/CLI/MCP and actual Kafka metadata/fixture-message reads in a loopback-only namespace without default routes. Human browser/business approvals and dispatched product mutations are not claimed by that offline fixture; those broader integrated journeys retain their accepted main evidence.

The packet snapshots 29 exact fixture scripts plus raw JSON, immutable driver/run/artifact identities and SHA256 inventories. Windows and CI hosts are explicitly distinct. Full media was checked by official preparation and signed-run offline installation; Windows verified the official signature and selected archive before executing. See `acceptance-scope.json` for the precise boundaries and independent root pin. W82 load/soak tests were not repeated.
