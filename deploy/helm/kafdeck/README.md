# Kafdeck Helm v0.9

This chart packages the Kafdeck API and bundled UI as one TLS-serving
workload. Real signed-chart lifecycle acceptance passed on Kubernetes 1.34.3 / Helm 3.21.4 with real Keycloak and Kafka. See the [released installation guide](../../../docs/operator/v0.9-installation-guide.md) and [final evidence](../../../docs/implementation/evidence/v0.9-release-published-20261112.json). The current
release uses one replica for both SQLite and PostgreSQL because the browser
OIDC ticket store lives in process memory. A restarted pod requires a fresh
login. PostgreSQL multi-replica admission is separate work.

Supply these existing resources in the release namespace:

- A TLS Secret with keys `tls.pfx` and `tls-password`. The certificate must be
  valid for `publicHost`. The service terminates TLS on the pod and exposes a
  ClusterIP on port 443. Configure TLS passthrough or verified HTTPS backends
  on the operator-owned gateway. Register
  `https://<publicHost>/signin-oidc` as the redirect URI with the IdP.
- An operator ConfigMap with `appsettings.Production.json`. Use it for RBAC,
  the OIDC client secret and trust references, Kafka SASL and TLS references,
  and for enabling the applicable governance and persistence capabilities.
  Keep credentials outside the ConfigMap. The chart owns the listen URL,
  first Kafka profile's identity and endpoints, and the mutation-store
  provider and mode. Leave these chart-owned keys out of the ConfigMap,
  including additional entries in the same arrays.
- A credentials Secret mounted at `/run/kafdeck-credentials`. Reference
  credential files in the ConfigMap with the application's documented
  `file:` syntax. PostgreSQL requires a `postgres-connection` key in this
  Secret. Its connection string must enforce verification of the database
  server certificate. Supply a trusted OIDC CA PEM path if the issuer uses a
  private CA. See the [OIDC and RBAC guide](../../../docs/operator/v0.2-oidc-rbac-guide.md).
- A PVC for writable state, owned outside the Helm release. Use a storage
  class that supports the chart's group ownership and appropriate locking for
  SQLite. Back it up before upgrades. The chart uses a Recreate rollout and
  leaves the claim intact on uninstall. Follow the
  [upgrade and recovery plan](../../../docs/operations/v0.9-distribution-recovery.md)
  before changing schemas or restoring data.
- For private image registries, an existing pull Secret named by
  `image.pullSecret`. Images are selected by digest.

Set `network.ingressNamespaces` to the namespace of the trusted gateway or
local acceptance client. List the CIDR and TCP port of every advertised
Kafka broker and the external IdP in `network.egress`. Add the PostgreSQL
endpoint for that persistence provider. The policy requires an enforcing
CNI and permits cluster DNS using the default `kube-dns` pod label in
`kube-system`. Verify effective egress after service DNAT in your CNI and
omit unused destinations. The workload has no Kubernetes API credentials.

Render before install. Use a values file containing resource references and
the exact released digest. Neither of these commands contacts the cluster.

```sh
helm lint deploy/helm/kafdeck --strict -f operator-values.yaml
helm template kafdeck deploy/helm/kafdeck -n kafdeck -f operator-values.yaml
```

The probe endpoint checks that the API is serving HTTPS. Broker, identity,
database, upgrade and restore checks require the live acceptance fixture.
