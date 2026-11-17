# Kafdeck 1.0.0

Kafdeck provides a Kafka control plane with managed onboarding, Keycloak sign-in, saved cluster connections, topic and consumer operations, Schema Registry, Kafka Connect, ksqlDB, and integrated monitoring.

This stable distribution uses product source `0617a87ddc176b8255ca1452fc53c097008d9a51`. API/UI, CLI, MCP and Helm identify as `1.0.0`. The Terraform provider retains version `0.8.0`.

## Installation and operations

- [Installation and first administrator](https://github.com/araditc/Kafdeck/tree/0617a87ddc176b8255ca1452fc53c097008d9a51/docs/operator)
- [Native installation, upgrade and restore evidence](https://github.com/araditc/Kafdeck/blob/0617a87ddc176b8255ca1452fc53c097008d9a51/docs/releases/v1.0-rc-admission.md)
- [Security and supported configurations](https://github.com/araditc/Kafdeck/blob/0617a87ddc176b8255ca1452fc53c097008d9a51/docs/releases/v1.0-security-support-admission.md)
- [Stable and experimental contract policy](https://github.com/araditc/Kafdeck/blob/0617a87ddc176b8255ca1452fc53c097008d9a51/docs/rfcs/0010-v1.0-contract-stability-support.md)

## Distribution and verification

The full-distribution ZIP contains `manifest.json` and the selected media tree. Download `manifest.sigstore.json` and `trusted-root.json` alongside it. The manifest binds all 43 selected files, including native API/UI, CLI and MCP archives for five platforms, two OCI archives, Helm, Terraform and integration packages. Individually downloadable media use `media__` followed by the manifest path with `/` replaced by `__`.

Verify the manifest with Cosign 3.0.6 using the following command after extracting the full distribution and placing the signature files in that directory.

```sh
cosign verify-blob --offline \
  --bundle manifest.sigstore.json --trusted-root trusted-root.json \
  --certificate-identity 'https://github.com/araditc/Kafdeck/.github/workflows/v1-candidate-media.yml@refs/heads/main' \
  --certificate-oidc-issuer 'https://token.actions.githubusercontent.com' \
  --certificate-github-workflow-sha 0617a87ddc176b8255ca1452fc53c097008d9a51 manifest.json
```

Trusted root SHA256 is `0ba58f6c09c271f664f0f8c1fa62e2065844ca89a0397c0178ed5188f6096724`. The signed manifest SHA256 is `0cefda57cdef623f96739439b426a60321727ffebff2c1e4509feccc5a3f75bc`. Use `scripts/release/v1_media.py verify` from the reviewed source to check the extracted media against this manifest. `SHA256SUMS` covers the downloadable Release files. The registry transport inventory records the index, architecture manifests, signed configuration hashes and layer hashes. GHCR access follows the existing package permissions.

## Acceptance and limits

All 18 main workflows passed on the selected product source. Installed native versions, both OCI architectures, Helm lifecycle and integration packages were verified. Existing operator, upgrade/restore and load evidence retain their original source identities. KAF-02 requires tested deployment egress. Monitoring-enabled workloads require their own capacity measurements. [Signed media and isolated verification](https://github.com/araditc/Kafdeck/actions/runs/44134707888).
