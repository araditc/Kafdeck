# Kafdeck 1.1.0-rc.1

This pre-release introduces broker host management in Fleet. The signed build identifies as `1.1.0-rc.1` and uses source `ba60c2b0bd53d3928040e351673a8c07ffd2e658`. Terraform provider packages remain at `0.8.0`.

- Register SSH hosts and verify which service or container runs each broker.
- Read bounded broker logs and correlate them with monitoring.
- Install or upgrade an approved offline Java agent on a verified broker.
- Preview and apply supported dynamic Kafka settings, with readback and restore.
- Resume an interrupted rollout from its saved stage and confirm broker health and fresh metrics before moving to the next broker.

Start with the [Fleet operator guide](https://github.com/araditc/Kafdeck/blob/ba60c2b0bd53d3928040e351673a8c07ffd2e658/docs/operator/v1.1-fleet-operational-recovery.md). Acceptance covered [Linux with systemd](https://github.com/araditc/Kafdeck/actions/runs/44743543323) and the approved isolated Docker Compose pilot on the Arad Office host. [Rollout acceptance scope](https://github.com/araditc/Kafdeck/issues/489).

Native archive installation passed on five platforms. An isolated Windows x64 test also upgraded and restored data from stable `1.0.0` with SQLite and PostgreSQL, retaining Keycloak identities, registered Kafka targets and stored credentials.

## Download and verify

The full-distribution ZIP contains `manifest.json` and the 43-file signed media inventory. Download `manifest.sigstore.json` and `trusted-root.json` alongside it. Individual media have the `media__` prefix. `SHA256SUMS` covers all downloadable Release files except the checksum file itself.

Verify the manifest with Cosign 3.0.6 from the directory containing the three files.

```sh
cosign verify-blob --offline \
  --bundle manifest.sigstore.json --trusted-root trusted-root.json \
  --certificate-identity 'https://github.com/araditc/Kafdeck/.github/workflows/v1-candidate-media.yml@refs/heads/main' \
  --certificate-oidc-issuer 'https://token.actions.githubusercontent.com' \
  --certificate-github-workflow-sha 459e9d7c055e7def6a52dc803d853cbb97a41885 manifest.json
```

The trusted root SHA256 is `0ba58f6c09c271f664f0f8c1fa62e2065844ca89a0397c0178ed5188f6096724`. Verify the extracted media against the approved digest with `scripts/release/v1_media.py verify` from the selected source. The [signing run](https://github.com/araditc/Kafdeck/actions/runs/44766054199) recorded verification with networking disabled. GHCR access follows the existing package permissions.
