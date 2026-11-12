# Local Development Baseline

## v0.9 managed onboarding

For the released packages, use [installation](../operator/v0.9-installation-guide.md). To reproduce first-owner setup from source, use the SDK/Node versions below, then the [managed configuration example](../operator/examples/v0.9-managed-appsettings.json), a dedicated HTTPS Keycloak realm and writable private identity state. Run the built API with that content root; issue `--issue-setup-code` using the same configuration, create the first owner, then sign in and [test/save a Kafka connection](../operator/v0.9-dashboard-guide.md). The code expires after 15 minutes. Source/default Local mode and the older Token-mode laboratory are separate evaluation paths; they do not enable the managed setup wizard. Never reuse production identity files or business topics for development fixtures.


## Prerequisites

- Git
- Docker with Docker Compose v2
- .NET 10 SDK (the repository `global.json` selects the supported SDK family)
- Node.js 24 LTS

## Backend

Restore only from the committed NuGet lockfiles, then build and test the complete solution:

```bash
dotnet restore Kafdeck.slnx --locked-mode
dotnet build Kafdeck.slnx --configuration Release --no-restore -warnaserror
dotnet test Kafdeck.slnx --configuration Release --no-build --no-restore
```

The solution is intentionally structured as a modular monolith. Project references must preserve the dependency direction defined by the architecture documents and enforced by `Kafdeck.Architecture.Tests`.

For local API startup:

```bash
dotnet run --project src/backend/Kafdeck.Api/Kafdeck.Api.csproj -- --Kafdeck:Deployment:ListenUrls:0=http://127.0.0.1:8080
```

The default `appsettings.json` has an empty cluster list and a loopback listener. Add a disposable static profile through `Kafdeck:Clusters` for Local-mode development, or use the managed setup above for registered targets. `Kafdeck:Deployment:ListenUrls` is authoritative; changing only `ASPNETCORE_URLS` does not override it. Never expose Local mode on a remote listener.

For a built API/UI with a separate configuration directory, run `dotnet /absolute/path/to/Kafdeck.Api.dll --contentRoot /absolute/path/to/config --webroot /absolute/path/to/wwwroot`. The web root must contain the built frontend. Use absolute Windows paths with PowerShell. Keep setup-code issuance and normal execution on the same content root, environment and service identity. See [native installation](../operator/v0.9-installation-guide.md#native-windows-linux-and-macos).

## Frontend

Install exactly the committed npm dependency graph and run the same checks used by CI:

```bash
cd src/frontend
npm ci --ignore-scripts
npm run lint
npm run typecheck
npm test
npm run build
```

The frontend is the released React/TypeScript dashboard. Run `npm run dev -- --host 127.0.0.1` for Vite development; API requests use same-origin relative paths, so configure a local development proxy to the API when using a separate frontend origin. For an integrated same-origin check, build the frontend and serve its `dist` as the API's explicit web root. No browser-to-Kafka access is permitted.

## Kafka development broker

Start the pinned Apache Kafka 4.3.1 KRaft development broker:

```bash
./scripts/dev/kafka-up.sh
```

Bootstrap server:

```text
localhost:9092
```

Stop and remove development state:

```bash
./scripts/dev/kafka-down.sh
```

This broker is for development/testing only. It is not a production deployment reference architecture.

## Reproducibility

Developers must not depend on locally installed Kafka tooling for required CI behavior. Repository scripts, committed dependency lockfiles, pinned Actions, and containers define the repeatable baseline.
