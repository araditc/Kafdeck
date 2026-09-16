# Local Development Baseline

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
ASPNETCORE_URLS=http://127.0.0.1:8080 dotnet run --project src/backend/Kafdeck.Api/Kafdeck.Api.csproj
```

The W01 development example is loopback-only and contains no credentials. W02 defines the full cluster/security configuration contract.

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

The W01 frontend is a strict TypeScript/React application scaffold. Feature behavior is added by later workstreams; no browser-to-Kafka access is permitted.

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
