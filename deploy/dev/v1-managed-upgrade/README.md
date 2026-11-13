# Bounded Windows managed-upgrade reference

This is a development acceptance harness, not a product command or generic release publisher. It does not download, sign, publish or touch the operator's browser profile. Node, .NET and an explicitly selected installed Chrome/Playwright are prerequisites. Keep generated state, databases, key rings, checkpoint passwords and process logs private and outside Git.

Use a fresh owned laboratory root, the existing `prepare-keycloak-g2.ps1`, `prepare-managed-provisioning.ps1`, `prepare-kafka-g2.ps1`, and explicit native PostgreSQL binaries. Prepare these isolated fixtures before running:

- Kafka `127.0.0.1:19092`; PostgreSQL `127.0.0.1:15440`, private `g06_upgrade` database and `pgpass`.
- Keycloak `https://localhost:18444`, its CA/P12 in `identity/`, and dedicated `kafdeck-managed-g06-sqlite` / `kafdeck-managed-g06-postgres` realms. Clone the generated managed fixture realms before import; add a public PKCE browser client `g06-browser` with **exact** `https://localhost:18460/signin-oidc` redirect. Do not reuse an already imported realm with a different redirect; Keycloak startup import skips existing realms. Provisioner service account retains only its existing `manage-users` role. `managed/lab.json` retains private fixture provisioner data, `identity/lab.json` the CA/P12 paths.
- Private `postgres-connection.txt` selecting only that fixture database. `services-private.json` is an explicit readiness/ownership marker. No operator database, realm or default `8080` port is used.
- Extract the unchanged signed-release win-x64 archive only after comparing its SHA256 to the original signed manifest. Use its actual `Kafdeck.Api.exe`, not a source rebuild labeled v0.9. Build the identified development candidate separately and build `Probe.csproj` with its locked dependencies. The probe explicitly creates synthetic mutation compatibility snapshots; it never represents them as genuine human approvals or provider effects.

With the fixture services ready, run `node http-fixture.cjs <owned-lab-root>` in its own supervised process. This binds numeric loopback `19084` and supplies **controlled** authenticated registry/Connect/ksql HTTP responses. It is not genuine vendor acceptance. Stop it using only `<owned-lab-root>/stop-http`.

Set `KAFDECK_BROWSER_PLAYWRIGHT` to the installed Playwright module, `KAFDECK_G2_BROWSER_EXECUTABLE` to the explicit Chrome executable, and `KAFDECK_G06_POSTGRES_BIN` to the verified native PostgreSQL bin directory. Run:

```text
node run.cjs <owned-lab-root> <released-exe> <candidate-dll> <probe-dll>
node browser-run.cjs <owned-lab-root> <released-exe> <candidate-dll>
```

Then set `KAFDECK_E01_EDGE_CASES=1` and run the browser command once for the separate quiet-refresh/scroll/deadline/cancellation/cluster-switch cases. Preserve successful output before repairing only failed cases. The two logical profiles in the switch case deliberately share one physical Kafka broker; the second profile includes an approved unavailable fallback bootstrap. This does not establish two-cluster performance or availability. Physical upgrade and browser commands run sequentially because they own the same `18460` port. Native processes run hidden and are stopped by their exact owned PID. Results go to `artifacts/stabilization-results/`; private failure diagnostics stay in the owned lab root.

Keep the same content root/application discriminator across old/new API processes. Explicit candidate key-ring selection copies the actual matching legacy keys; missing keys must deny reads without clearing saved state. Windows DPAPI portability to another user/machine is deliberately not claimed. Fixture checkpoints permit resuming setup/backup preparation; they are not release artifacts and must never be uploaded.

The committed evidence records the actual tested native reference profile. Linux/CNI/provider matrices and final signed-RC media remain separately scoped gates; these scripts do not silently broaden their support claims or rerun W82.
