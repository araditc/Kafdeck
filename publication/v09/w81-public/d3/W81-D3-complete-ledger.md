# W81 D3 signed Helm acceptance ledger

Both admitted chart profiles passed on the frozen application source `01c7553425fbc61ba4a7a21918265c562086ba02`. The tests installed the official signed chart and image bytes with a genuine HTTPS Keycloak, TLS Kafka, and the signed native CLI. Fixture driver `da6bfbd17084526f917e0f05a2eb5025d96c00cc` changed only test tooling. This folder retains verified producer ZIPs, every public JSON/NDJSON result, exact tested driver sources, the conditional NetworkPolicy boundary evidence, and a reproducible evidence verifier.

| Identity | Value |
| --- | --- |
| Official signed producer | `42984770583`, artifact `13839715488` |
| Signed artifact ZIP SHA256 | `7100754a28ebc405a8ca9739412341a3e13587f6e9514882ab547f2e62b8c295` |
| Full signed manifest SHA256 | `b63864259a8a5e1c3c9d8dc3312d35a56e82749d62e2f6a05b88112f98af29ba` |
| Independent Sigstore root SHA256 | `0ba58f6c09c271f664f0f8c1fa62e2065844ca89a0397c0178ed5188f6096724` |
| Exact signed chart SHA256 | `3e403f55c185ca0eb855ee96347d52b27e15e7729f2f139a4aee3702b3d851d1` |
| Signed amd64 image config | `sha256:8b87c333d701f77511de2745f7e22c4cd4519aac296d30db28e983bba8ea7624` |
| Actual registry and Pod image manifest | `sha256:a5379cd7eb20bdbf0ad3ec7e12bba3071d939155dc98ae7b9a587e206f3b1049` |
| Functional runtime | Kind `v0.31.0`, Kubernetes `v1.34.3`, Helm `v3.21.4+g813176c`, native Linux amd64 |
| Identity and broker | Keycloak `26.8.1` HTTPS/private_key_jwt, Apache Kafka `4.3.1` TLS |
| External PostgreSQL | `16.15 (Ubuntu 16.15-1.pgdg24.04+2)`, separate native primary/standby processes, SCRAM fixture owner, synchronous `remote_apply` |

The signed media and independent trust root were verified before any installation. Both evidence ZIP hashes were independently checked against GitHub's artifact digest, and every extracted file was compared with the ZIP payload. The actual Pod records bind distinct Pod UIDs and the runtime image ID to the exact deployed manifest. The application and chart were installed from signed archives without rebuilding either artifact.

| Admitted profile | Real run | Producer ZIP SHA256 | Final lifecycle report SHA256 |
| --- | --- | --- | --- |
| SQLite, one API Pod, `Standalone` | [43018140685](https://github.com/araditc/Kafdeck/actions/runs/43018140685) | `9b4b599782c6dc20a9350eaf79fea681f23b75ff5bb3f32c0189b1ad7c6923f3` | `cf20a97a77b00bc1dc7403e498183699b964a63f276ae18d3a636286050ec816` |
| PostgreSQL, one API Pod, `Standalone`, external database HA | [43018144998](https://github.com/araditc/Kafdeck/actions/runs/43018144998) | `f29b5ab720a230601b431a77ebd98be136bea0b8623e965877768aef0896b17a` | `e991ab81c1ba570abf53444384aa7a488b03cbe8cb0c251e1e0a0f47393c2976` |

Each ready phase rejected an unauthenticated operator read with HTTP 401, then authenticated through the genuine Keycloak and observed broker metadata and the exact seeded Kafka record through the signed CLI and deployed API. API and identity certificates and Kafka's server certificate were verified against the private fixture CA. Records were seeded directly in the isolated Kafka fixture.

| Actual case | SQLite | External PostgreSQL HA | Retained proof |
| --- | --- | --- | --- |
| Install with real authentication and exact Kafka record | Passed | Passed | `journeys.ndjson`, `runtime-pods.ndjson` |
| Upgrade forces an actual replacement Pod and retains application state | Passed | Passed | `state-initial.json` equals `state-upgraded.json` |
| Deliberately failed image rollout and atomic rollback | Passed | Passed | Failed revision 3, deployed rollback revision 4 in `helm-history.json`, authenticated rollback read |
| Uninstall/reinstall retains the original claim and application state | Passed | Passed | Actual PVC UID assertion and equal `state-reinstalled.json` |
| External database primary fencing and standby promotion | Applicable to SQLite's single-writer lifecycle | Passed | `pg-primary-fence.json`, synchronous replay evidence, equal `state-failover.json`, authenticated message after promotion |
| Restore with all application Pods fenced to zero | Passed | Passed | `restore-fence.json`, restored database digest |
| Restored state starts quarantined and serves no ready HTTP endpoint | Passed | Passed | `restored-unadmitted` row in `journeys.ndjson` |
| Offline owning generation admission followed by authenticated reads | Passed | Passed | `state-admitted.json` revision 2 and admission receipt match exact restore/fence hashes |
| Owned Kind node, registry and real fixture processes removed | Passed | Passed | Successful final explicit absence checks and `ownedKindRegistryFixturesRemoved` |

The persisted state in these chart rows is the application's real schema ledger and governance generation, initialized by the running application. Every schema row and the original generation were equal across upgrade, rollback, reinstall and database promotion. Offline admission used the actual `AdoGovernanceGenerationGate` from the signed application assemblies. Its receipt records the original and fresh generations and exact fencing/restore evidence hashes. Five SQLite and six PostgreSQL ready stages returned the same respective fixture message. The intentionally quarantined stage served no ready HTTP endpoint.

The Helm upgrade here is a same-artifact chart rollout with changed resources. Actual released-v0.8 migration and populated mutation-history recovery remain bound to the separately completed W80 and OCI acceptance evidence for this same signed application source. The external PostgreSQL chart row exercised its real stable TLS endpoint, physical synchronous database promotion and restored application generation. The admitted chart keeps one API replica and `Standalone` mutation execution in both profiles.

## Conditional NetworkPolicy boundary transfer

Kind provides the functional chart lifecycle runtime in these two runs. The separately owner-accepted policy boundary remains the actual K3s `v1.34.13+k3s1`, flannel `host-gw`, kube-router `v2.11.1-k3s2` proof at source `8f4f8b228f91ef6b51e9d679b09a47463a40154c`. The exact `deploy/helm/kafdeck/templates/network-policy.yaml` bytes are identical at the tested and frozen sources, SHA256 `b4f57d1cfbe5e606bd318fffc9e4e3748dc62192dc3ace788906e23e2712b021`.

That actual chart Pod ran as UID 1654. Its approved broker still returned TLS metadata after the advertised destination changed. The unrestricted control reached the outside destination with two SYN packets and a persisted record at offset 0. Applying the restricted chart policy to the same Pod yielded zero outside SYN packets, `NotPersisted`, and an unchanged broker end offset of 1. The retained machine result has SHA256 `dc07ba6a6114b570b4a222a87115309586cf4e519a9c821d79c5cee4fdd02220`. The accepted report has SHA256 `c0fc9419167fb8c0c837e15bf5078af1ecaa1547af3de4b2c2e0f600e8ca6e8f`.

This transfer covers the unchanged policy on the tested direct-IP route and admitted enforcing CNI. Service DNAT and other CNIs retain their separate-admission requirement. The current Kind runs make no new NetworkPolicy enforcement certification. The accepted boundary report, raw captures, CNI rules and cleanup record are retained in `conditional-network-boundary/`.

## Retained failed fixture attempts

Runs `43015929325` and `43016559452` stopped before chart installation because the driver obtained an empty gateway from Docker IPAM, which produced an invalid certificate SAN. Runs `43017426062` and `43017575955` then reached genuine TLS fixtures but stopped before chart installation because the driver guessed the wrong signed-chart directory. The tested driver resolves the gateway from the actual node route and resolves the chart path from the verified signed manifest. These were fixture failures. The successful cases above retain their own immutable producer identities.

`verification.json` records all checked case assertions. `index.json` hashes the entire public evidence set. No credentials, private keys, tokens, raw database backups, product rebuilds or publication are included.
