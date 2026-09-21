# Kafdeck

<div align="center">

**The Open Source Kafka Control Plane**

Secure, vendor-neutral, operations-first visibility for Apache Kafka.

[![Latest Release](https://img.shields.io/github/v/release/araditc/Kafdeck?display_name=tag&sort=semver)](https://github.com/araditc/Kafdeck/releases)
[![Quality Gate](https://github.com/araditc/Kafdeck/actions/workflows/quality-gate.yml/badge.svg)](https://github.com/araditc/Kafdeck/actions/workflows/quality-gate.yml)
[![CodeQL](https://github.com/araditc/Kafdeck/actions/workflows/codeql.yml/badge.svg)](https://github.com/araditc/Kafdeck/actions/workflows/codeql.yml)
[![License](https://img.shields.io/github/license/araditc/Kafdeck)](LICENSE)
[![Roadmap](https://img.shields.io/badge/roadmap-public-blue)](ROADMAP.md)

[Getting Started](#getting-started) ·
[Features](#feature-matrix) ·
[Configuration](#configuration) ·
[Security](#security-model) ·
[Contributing](#contributing) ·
[Support](#support)

</div>

---

Kafdeck is a self-hosted control plane for Apache Kafka designed for operators who need useful Kafka visibility **without turning the management UI into another reliability or security risk**.

It runs outside the Kafka data path, supports local/on-premise and air-gapped deployments, uses bounded read operations, and keeps metadata access, record access, export, identity, and masking as separate security concerns.

> [!IMPORTANT]
> Kafdeck's current operating posture is read-only. The released and v0.4 candidate capabilities do **not** create topics, produce/replay messages, alter consumer offsets, mutate schemas, restart connectors, or execute ksqlDB statements.

## Why Kafdeck?

- **Safe by default** — bounded work, cancellation, deadlines, rate/byte limits, per-cluster isolation, and fail-closed behavior.
- **Read access is not data access** — metadata permissions never automatically grant Kafka payload visibility.
- **Server-side masking** — sensitive record data is redacted before it reaches the browser, API client, or export path.
- **No mandatory data-plane proxy** — Kafdeck stays out of the Kafka message path.
- **Zero-write observation** — read-only Kafdeck does not create internal Kafka topics or need Kafka write access to store its own state.
- **Vendor-neutral core** — Apache Kafka semantics define the baseline; ecosystem/provider integrations are capability-gated.
- **On-premise and air-gapped friendly** — core runtime functionality has no required SaaS dependency.
- **Governed OSS engineering** — architecture, security, compatibility, release evidence, and project decisions are versioned in the repository.

## Project status

Stable releases are published through [GitHub Releases](https://github.com/araditc/Kafdeck/releases). **v0.4 is the current published release.** The main branch can also contain capabilities that have passed implementation gates but are not yet part of a published release.

| Capability | Status |
| --- | --- |
| Cluster Explorer | Released in v0.1 |
| Operator Identity / OIDC / RBAC | Released in v0.2 |
| Safe Data Explorer + Server-Side Masking | Released in v0.3 |
| Consumers / Schemas / Ecosystem Read Views | Released in v0.4 |
| Controlled Kafka mutations | Planned for v0.5; **not available today** |

See [ROADMAP.md](ROADMAP.md) for the capability roadmap and [docs/releases/](docs/releases/) for exact release evidence.

## Feature matrix

### Kafka Cluster Explorer

- multiple configuration-driven Kafka clusters,
- broker/controller metadata,
- topic and partition visibility,
- leader / replica / ISR inspection,
- topic and broker configuration inspection where authorized,
- evidence-based cluster and partition health,
- bounded snapshots, deadlines, stale semantics, cancellation, and per-cluster load isolation.

### Identity and authorization

- Local, deployment-token, and OIDC access modes,
- OIDC Authorization Code + PKCE,
- server-side sessions,
- immutable default-deny RBAC in OIDC mode,
- subject and external-group bindings,
- action-, cluster-, and resource-scoped authorization,
- backend-authoritative enforcement,
- structured security audit events.

### Safe Data Explorer

- bounded record browsing by topic / partition,
- earliest/latest/offset/timestamp navigation,
- previous-page navigation,
- bounded live tail,
- key/value/header inspection,
- raw, UTF-8, binary/hex, and structured views,
- read-only Schema Registry-assisted Avro / Protobuf / JSON Schema decoding,
- bounded filtering,
- explicit record/byte/time/rate/concurrency budgets,
- server-side masking before UI/API/export,
- separately authorized <code>record.read</code> and <code>record.export</code>,
- bounded JSON / NDJSON / CSV export,
- no record production or replay,
- no consumer-offset mutation,
- no payload persistence by default.

### Consumer read views

Released in v0.4:

- consumer groups and states,
- members and assignments,
- committed and end offsets,
- per-partition and aggregate lag,
- explicit missing / unauthorized / out-of-range states,
- evidence-based inactive/stalled diagnostics,
- metrics/history only when a trustworthy provider exists — unknown data is never fabricated as zero.

### Schema Registry explorer

- subjects and versions,
- schema IDs, formats, content, and references,
- bounded local schema diff,
- compatibility-mode inspection,
- GET/read-only integration,
- no schema lifecycle mutation.

### Kafka Connect and ksqlDB read views

- Kafka Connect worker identity,
- connector and task state,
- bounded task traces,
- fail-closed connector configuration projection,
- GET-only ksqlDB server info / health,
- explicit Unsupported state when metadata discovery would require statement execution,
- no connector mutation,
- no arbitrary SQL / ksqlDB statement execution.

### Topic catalog foundations

- description,
- owner/team,
- domain,
- tags,
- documentation reference,
- classification metadata.

Catalog metadata is descriptive; it does not grant Kafka or record permissions.

## How Kafdeck works

~~~mermaid
flowchart LR
    B[Browser / API client] --> A[Kafdeck HTTP API]
    A --> I[Access boundary<br/>Local / Token / OIDC]
    I --> R[Authorization + Audit]
    R --> S[Application services]

    S --> K[Kafka read adapters]
    S --> SR[Schema Registry read adapter]
    S --> C[Kafka Connect read adapter]
    S --> Q[ksqlDB read adapter]

    K --> KF[(Apache Kafka)]
    SR --> REG[(Schema Registry)]
    C --> CON[(Kafka Connect)]
    Q --> KSQL[(ksqlDB)]

    S --> M[Server-side masking / bounded projection]
    M --> A
~~~

The important part is what is **not** in this diagram: Kafdeck is not a Kafka producer proxy, broker plugin, consumer-group member for normal browsing, or mandatory data-plane gateway.

A typical request flows like this:

1. configuration is loaded and validated at startup;
2. the deployment access boundary authenticates the caller;
3. OIDC deployments apply Kafdeck RBAC before upstream I/O;
4. an application service invokes a Kafdeck-owned read port;
5. the adapter performs bounded/cancellable Kafka or ecosystem reads;
6. record payloads are decoded and masked server-side where required;
7. only a safe projection is returned to the UI/API client;
8. security-sensitive activity is audited without storing secrets or record payloads.

## Getting started

### Recommended path: OCI / Docker

Kafdeck releases are published as a single non-root OCI image:

~~~text
ghcr.io/araditc/kafdeck:<release-tag>
~~~

Use an explicit release tag or immutable digest in production. Do not rely on <code>latest</code>.

### 1. Start a container-reachable Kafka for evaluation

For the Docker quick start, put Kafka and Kafdeck on the same user-defined network and advertise a Kafka listener that is reachable **from the Kafdeck container**:

~~~bash
docker network create kafdeck-demo

docker run -d   --name kafdeck-kafka   --network kafdeck-demo   -p 9092:9092   -e KAFKA_NODE_ID=1   -e KAFKA_PROCESS_ROLES=broker,controller   -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafdeck-kafka:29093   -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER   -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,HOST:PLAINTEXT   -e KAFKA_LISTENERS=CONTROLLER://:29093,INTERNAL://:19092,HOST://:9092   -e KAFKA_ADVERTISED_LISTENERS=INTERNAL://kafdeck-kafka:19092,HOST://localhost:9092   -e KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL   -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1   -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0   -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1   -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1   -e CLUSTER_ID=4L6g3nShT-eMCtK--X86sw   apache/kafka:4.3.1
~~~

The host can use <code>localhost:9092</code>; containers on <code>kafdeck-demo</code> use <code>kafdeck-kafka:19092</code>. This distinction matters because Kafka clients follow broker-advertised listeners after bootstrap.

The repository's simpler <code>deploy/dev/docker-compose.kafka.yml</code> advertises <code>localhost:9092</code> and is intended for **host-native source development**, not for a second Docker container connecting to it.

This evaluation broker is intentionally PLAINTEXT and single-node. Do not copy its security posture into production.

### 2. Create a minimal Kafdeck configuration

Create <code>appsettings.Production.json</code>:

~~~json
{
  "Kafdeck": {
    "Deployment": {
      "ListenUrl": "http://0.0.0.0:8080",
      "AccessMode": "Token",
      "AccessToken": "env:KAFDECK_DEPLOYMENT_TOKEN"
    },
    "Clusters": [
      {
        "Id": "local",
        "BootstrapServers": [
          "kafdeck-kafka:19092"
        ],
        "SecurityProtocol": "Plaintext"
      }
    ]
  },
  "AllowedHosts": "localhost;127.0.0.1"
}
~~~

### 3. Run the released image

Replace <code>&lt;release-tag&gt;</code> with a published version from [Releases](https://github.com/araditc/Kafdeck/releases).

#### Windows / Docker Desktop

~~~powershell
$env:KAFDECK_DEPLOYMENT_TOKEN = "change-this-local-token"
docker run --rm --name kafdeck --network kafdeck-demo -p 127.0.0.1:8080:8080 -e KAFDECK_DEPLOYMENT_TOKEN=$env:KAFDECK_DEPLOYMENT_TOKEN -v "$PWD\appsettings.Production.json:/app/appsettings.Production.json:ro" ghcr.io/araditc/kafdeck:<release-tag>
~~~

#### Linux / Docker Engine

~~~bash
export KAFDECK_DEPLOYMENT_TOKEN='change-this-local-token'

docker run --rm \
  --name kafdeck \
  --network kafdeck-demo \
  -p 127.0.0.1:8080:8080 \
  -e KAFDECK_DEPLOYMENT_TOKEN \
  -v "$PWD/appsettings.Production.json:/app/appsettings.Production.json:ro" \
  ghcr.io/araditc/kafdeck:<release-tag>
~~~

Then open:

~~~text
http://127.0.0.1:8080/#access_token=change-this-local-token
~~~

The token is supplied through the URL **fragment**, not a query string. The UI removes it from the address bar, stores it in browser <code>sessionStorage</code>, and sends it as <code>X-Kafdeck-Access-Token</code> for API calls.

> [!WARNING]
> Token mode is a deployment access boundary, not a multi-user identity system. For multi-user production deployments, use OIDC/RBAC.

### Linux with Podman

~~~bash
podman run --rm \
  --name kafdeck \
  -p 127.0.0.1:8080:8080 \
  -e KAFDECK_DEPLOYMENT_TOKEN \
  -v "$PWD/appsettings.Production.json:/app/appsettings.Production.json:ro,Z" \
  ghcr.io/araditc/kafdeck:<release-tag>
~~~

For Podman, create a Podman network and ensure Kafka advertises a broker hostname reachable on that network. The Docker-specific <code>kafdeck-demo</code> recipe above is not automatically shared with Podman.

## Building from source

Building from source is the best path for contributors and for testing unreleased main capabilities.

### Prerequisites

- Git,
- .NET SDK 10,
- Node.js 24+ and npm,
- Docker + Compose for the integration Kafka environment.

### Linux / macOS

~~~bash
git clone https://github.com/araditc/Kafdeck.git
cd Kafdeck

dotnet restore Kafdeck.slnx --locked-mode
dotnet build Kafdeck.slnx -c Release --no-restore

npm --prefix src/frontend ci
npm --prefix src/frontend run build

rm -rf src/backend/Kafdeck.Api/wwwroot
mkdir -p src/backend/Kafdeck.Api/wwwroot
cp -R src/frontend/dist/. src/backend/Kafdeck.Api/wwwroot/

dotnet run --project src/backend/Kafdeck.Api/Kafdeck.Api.csproj
~~~

The default application configuration binds to <code>http://127.0.0.1:8080</code> and contains no cluster profiles. For a useful source run, either add <code>src/backend/Kafdeck.Api/appsettings.Development.json</code> or set environment variables before starting the API:

~~~bash
export Kafdeck__Clusters__0__Id=local
export Kafdeck__Clusters__0__BootstrapServers__0=localhost:9092
export Kafdeck__Clusters__0__SecurityProtocol=Plaintext
dotnet run --project src/backend/Kafdeck.Api/Kafdeck.Api.csproj
~~~

### Windows / PowerShell

~~~powershell
git clone https://github.com/araditc/Kafdeck.git
Set-Location Kafdeck

dotnet restore Kafdeck.slnx --locked-mode
dotnet build Kafdeck.slnx -c Release --no-restore

npm --prefix src/frontend ci
npm --prefix src/frontend run build

Remove-Item -Recurse -Force src/backend/Kafdeck.Api/wwwroot -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force src/backend/Kafdeck.Api/wwwroot | Out-Null
Copy-Item -Recurse src/frontend/dist/* src/backend/Kafdeck.Api/wwwroot/

dotnet run --project src/backend/Kafdeck.Api/Kafdeck.Api.csproj
~~~

For a local Kafka:

~~~powershell
docker compose -f deploy/dev/docker-compose.kafka.yml up -d
~~~

To point a native Windows source run at that broker:

~~~powershell
$env:Kafdeck__Clusters__0__Id = "local"
$env:Kafdeck__Clusters__0__BootstrapServers__0 = "localhost:9092"
$env:Kafdeck__Clusters__0__SecurityProtocol = "Plaintext"
dotnet run --project src/backend/Kafdeck.Api/Kafdeck.Api.csproj
~~~

### Build the OCI image locally

~~~bash
docker build -t kafdeck:dev .
~~~

The runtime image serves both API and UI, runs as the image-defined non-root user, exposes port 8080, and uses ASP.NET Core 10.

## Configuration

Kafdeck uses the standard ASP.NET Core configuration model. JSON configuration and environment variables can be combined. Nested environment keys use double underscores.

~~~bash
Kafdeck__Clusters__0__Id=prod
Kafdeck__Clusters__0__BootstrapServers__0=kafka-1.example:9093
Kafdeck__Clusters__0__SecurityProtocol=SaslSsl
~~~

### Access modes

| Mode | Intended use | Important behavior |
| --- | --- | --- |
| Local | Single-user local development | Must bind to loopback; no deployment token or OIDC |
| Token | Controlled management boundary / simple deployment | Requires a secret-referenced access token |
| Oidc | Multi-user production operation | Operator identity + Kafdeck RBAC; non-loopback listen URL must use HTTPS |

Local and Token modes are deployment-boundary modes and do not provide identity-scoped Kafdeck RBAC. Kafka-side ACLs and Kafdeck record masking still apply.

### Secret references

Secret fields accept:

~~~text
env:VARIABLE_NAME
file:/absolute/mounted/path
~~~

Examples:

~~~json
{
  "AccessToken": "env:KAFDECK_DEPLOYMENT_TOKEN",
  "ClientSecret": "file:/run/secrets/oidc-client-secret"
}
~~~

Do not commit actual passwords, access tokens, PEM private keys, or client secrets.

### Kafka security

Supported Kafka transport/authentication modes:

- Plaintext,
- Ssl,
- SaslPlaintext,
- SaslSsl.

Supported SASL mechanisms:

- Plain,
- ScramSha256,
- ScramSha512.

Production-oriented example:

~~~json
{
  "Id": "prod",
  "BootstrapServers": [
    "kafka-1.example:9093",
    "kafka-2.example:9093",
    "kafka-3.example:9093"
  ],
  "SecurityProtocol": "SaslSsl",
  "Tls": {
    "VerifyServerCertificate": true,
    "CaCertificate": "file:/run/secrets/kafka-ca.pem"
  },
  "Sasl": {
    "Mechanism": "ScramSha512",
    "Username": "env:KAFDECK_KAFKA_USER",
    "Password": "file:/run/secrets/kafka-password"
  }
}
~~~

TLS server-certificate verification cannot be silently disabled.

### Optional Schema Registry

<code>SchemaRegistry</code> is a property of an individual object inside <code>Kafdeck:Clusters[]</code>. Example cluster object:

~~~json
{
  "Id": "prod",
  "BootstrapServers": [ "kafka-1.example:9093" ],
  "SecurityProtocol": "SaslSsl",
  "SchemaRegistry": {
    "Url": "https://schema-registry.example",
    "Username": "env:KAFDECK_SR_USER",
    "Password": "file:/run/secrets/schema-registry-password"
  }
}
~~~

Remote basic authentication requires HTTPS.

### Optional Kafka Connect

<code>Connect</code> is also configured inside the relevant <code>Kafdeck:Clusters[]</code> object:

~~~json
{
  "Id": "prod",
  "BootstrapServers": [ "kafka-1.example:9093" ],
  "SecurityProtocol": "SaslSsl",
  "Connect": {
    "Url": "https://connect.example",
    "Username": "env:KAFDECK_CONNECT_USER",
    "Password": "file:/run/secrets/connect-password"
  }
}
~~~

Kafdeck uses read-only Connect operations. Connector configuration is projected fail-closed: fields not explicitly considered safe are redacted.

### Optional ksqlDB

<code>KsqlDb</code> is configured inside the relevant <code>Kafdeck:Clusters[]</code> object:

~~~json
{
  "Id": "prod",
  "BootstrapServers": [ "kafka-1.example:9093" ],
  "SecurityProtocol": "SaslSsl",
  "KsqlDb": {
    "Url": "https://ksql.example",
    "Username": "env:KAFDECK_KSQL_USER",
    "Password": "file:/run/secrets/ksql-password"
  }
}
~~~

Kafdeck does not submit SQL or metadata statements. If a provider requires statement execution to discover metadata, the capability is reported unsupported.

### Server-side record masking

~~~json
{
  "Kafdeck": {
    "Records": {
      "Masking": {
        "PolicyId": "prod-default",
        "Version": 1,
        "MaskKey": true,
        "KeyReplacement": "[REDACTED]",
        "StructuredRules": [
          {
            "Path": "/customer/cardNumber",
            "Replacement": "[REDACTED]"
          }
        ],
        "HeaderRules": [
          {
            "Name": "authorization",
            "Replacement": "[REDACTED]"
          }
        ]
      }
    }
  }
}
~~~

An active structured masking rule fails closed when Kafdeck cannot safely apply it.

### OIDC and RBAC

OIDC deployments support roles with action-, cluster-, and resource-scoped permissions.

A non-loopback OIDC deployment must listen on HTTPS. Because Kafdeck calls Kestrel directly, the process/container needs a server certificate; configuring only <code>ListenUrl=https://...</code> is not enough. For the released container, mount a PKCS#12/PFX certificate and configure Kestrel, for example:

~~~bash
docker run --rm \
  --name kafdeck \
  -p 8443:8443 \
  -e ASPNETCORE_Kestrel__Certificates__Default__Path=/run/secrets/kafdeck-https.pfx \
  -e ASPNETCORE_Kestrel__Certificates__Default__Password="$KAFDECK_HTTPS_CERT_PASSWORD" \
  -e KAFDECK_OIDC_CLIENT_SECRET \
  -v "$PWD/kafdeck-https.pfx:/run/secrets/kafdeck-https.pfx:ro" \
  -v "$PWD/appsettings.Production.json:/app/appsettings.Production.json:ro" \
  ghcr.io/araditc/kafdeck:<release-tag>
~~~

Inject <code>KAFDECK_HTTPS_CERT_PASSWORD</code> through your orchestrator/secret manager; it is an ASP.NET/Kestrel setting and does not use Kafdeck's <code>env:</code> secret-reference syntax. In production, use a certificate whose SAN matches the hostname operators use.

The corresponding Kafdeck/OIDC configuration can then use an HTTPS listen URL:

~~~json
{
  "Kafdeck": {
    "Deployment": {
      "ListenUrl": "https://0.0.0.0:8443",
      "AccessMode": "Oidc",
      "Oidc": {
        "Issuer": "https://idp.example",
        "ClientId": "kafdeck",
        "ClientSecret": "env:KAFDECK_OIDC_CLIENT_SECRET",
        "GroupClaim": "groups",
        "Scopes": [ "openid", "profile" ]
      }
    },
    "Authorization": {
      "Roles": [
        {
          "Id": "kafka-readers",
          "Permissions": [
            { "Action": "ClusterRead", "ClusterIds": [ "prod" ] },
            { "Action": "TopicList", "ClusterIds": [ "prod" ] },
            { "Action": "TopicRead", "ClusterIds": [ "prod" ], "ResourcePatterns": [ "*" ] },
            { "Action": "ConsumerRead", "ClusterIds": [ "prod" ], "ResourcePatterns": [ "*" ] },
            { "Action": "SchemaRead", "ClusterIds": [ "prod" ], "ResourcePatterns": [ "*" ] }
          ]
        }
      ],
      "GroupBindings": [
        {
          "ExternalGroup": "kafka-operators",
          "RoleIds": [ "kafka-readers" ]
        }
      ]
    }
  }
}
~~~

Grant RecordRead and RecordExport only where payload access is explicitly required.

## Security model

Core rules include:

- authorization is deny-by-default in OIDC mode;
- metadata access does not imply record access;
- record export is separate from record read;
- masking is server-side and fail-closed;
- secrets are referenced, not serialized into diagnostics;
- record payloads are not persisted by default;
- audit output excludes record payloads and secrets;
- ecosystem integrations do not expose generic HTTP proxying;
- Kafka Connect configuration is redacted fail-closed;
- ksqlDB statement execution is not exposed;
- release images are built with SBOM/vulnerability-scan/signing controls.

Read [SECURITY.md](SECURITY.md) before production deployment.

### Least-privilege Kafka credentials

Give Kafdeck only the Kafka permissions required for the views you intend to use. Do not grant produce, topic mutation, ACL mutation, or offset-alteration rights merely because an operator needs visibility.

### Production deployment checklist

- pin a released OCI digest;
- run the container as the image-defined non-root user;
- mount configuration and secret files read-only;
- use TLS/mTLS or SASL over TLS for Kafka;
- use a distinct least-privilege Kafka principal per environment;
- use OIDC/RBAC for multi-user production access;
- keep Kafdeck on a management network;
- configure an approved HTTPS boundary;
- set AllowedHosts appropriately for the production hostname;
- do not expose development PLAINTEXT listeners to untrusted networks;
- monitor /healthz;
- retain release provenance/SBOM/security evidence according to your environment policy.

## Health check

~~~text
GET /healthz
~~~

~~~bash
curl http://127.0.0.1:8080/healthz
~~~

## API

Kafdeck product capabilities are exposed through versioned HTTP routes under <code>/api/v1</code>. The UI uses the same backend-authoritative contracts rather than bypassing authorization.

The checked-in API documentation for the v0.4 line is available at:

- [docs/api/openapi-v0.4.json](docs/api/openapi-v0.4.json)
- [docs/api/v0.4-read-views.md](docs/api/v0.4-read-views.md)

Treat the API contract from a published release as authoritative for that release; main-branch API docs may describe unreleased work.

## Kafka compatibility

Compatibility is evidence-based rather than assumed from a Kafka-compatible endpoint.

The v0.4 release matrix validates:

| Kafka | Tier |
| --- | --- |
| 4.3.1 | Tier 1 |
| 4.2.1 | Tier 1 |
| 4.1.2 | Tier 1 |
| 3.9.2 | Tier 2 |

Provider-specific services can expose different administrative capabilities. Kafdeck records those as capability profiles instead of claiming blanket equivalence.

## Air-gapped deployment

Kafdeck has no mandatory public SaaS dependency at runtime.

For an air-gapped environment:

1. mirror the exact released OCI digest into the controlled registry;
2. mirror Kafka CA/client certificate material and external ecosystem credentials separately;
3. transfer configuration independently from secrets;
4. verify image digest before import/run;
5. keep secret material outside the image;
6. if rebuilding inside the air gap, mirror .NET/npm/base-image dependencies and preserve lockfiles/provenance.

## Troubleshooting

| Symptom | What to check |
| --- | --- |
| /healthz is unreachable | Listen URL, port mapping, container process, firewall |
| Container port is published but UI does not load | Kafdeck may still be bound to container loopback; use an explicit non-loopback listen URL with Token/OIDC mode |
| UI loads but API returns 401 in Token mode | Deployment token secret reference and fragment bootstrap |
| Kafka cluster is unavailable | DNS/routing, advertised listeners, bootstrap address, operation deadlines |
| TLS errors | CA trust, SAN/hostname, certificate validity, mounted file permissions |
| SASL errors | Security protocol, mechanism, username/password secret references |
| Configuration read returns authorization errors | Kafka principal may lack read/describe permission; do not solve this by granting mutation rights |
| Record decode unavailable | Schema Registry profile, network/TLS/auth, schema references |
| Connect data unavailable | Connect profile, endpoint reachability, TLS/basic-auth configuration |
| ksql metadata says unsupported | Provider requires statement execution; this is intentionally unsupported |
| Data appears partial | Check Kafdeck RBAC, Kafka ACLs, upstream capability/timeout limitations |
| Response is marked stale | A bounded cached observation is being served after a retryable upstream failure |

## Repository layout

~~~text
src/backend/                 .NET application, modules, infrastructure adapters
src/frontend/                React operator UI
tests/Kafdeck.Architecture.Tests/
                             architecture/security/behavior tests
tests/integration/           containerized integration harnesses
deploy/dev/                  local Kafka development infrastructure
docs/architecture/           architecture documentation
docs/security/               security boundaries and threat models
docs/operator/               operator and upgrade guides
docs/releases/               release notes and exact release evidence
docs/rfcs/                   capability RFCs
docs/decisions/              approval and decision records
.github/workflows/           CI, security and release-supply-chain automation
~~~

## Development workflow

### Backend

~~~bash
dotnet restore Kafdeck.slnx --locked-mode
dotnet build Kafdeck.slnx -c Release --no-restore
dotnet test tests/Kafdeck.Architecture.Tests/Kafdeck.Architecture.Tests.csproj -c Release --no-build
~~~

### Frontend

~~~bash
npm --prefix src/frontend ci
npm --prefix src/frontend run lint
npm --prefix src/frontend test
npm --prefix src/frontend run build
~~~

### Local Kafka smoke environment

~~~bash
docker compose -f deploy/dev/docker-compose.kafka.yml up -d
~~~

CI also validates Kafka compatibility, CodeQL, dependency review, repository policy, supply-chain evidence, bounded benchmarks, SBOM generation, and vulnerability scanning where applicable.

## Contributing

Contributions are welcome.

Start with:

- [CONTRIBUTING.md](CONTRIBUTING.md)
- [GOVERNANCE.md](GOVERNANCE.md)
- [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)
- [SECURITY.md](SECURITY.md)
- [ROADMAP.md](ROADMAP.md)

The short version:

1. start from the latest main;
2. keep the change focused;
3. add/update tests and docs;
4. use Conventional Commits;
5. sign off commits for DCO compliance;
6. preserve the safety and vendor-neutrality boundaries;
7. open a PR and resolve review findings;
8. let required exact-head checks and protected-branch governance complete.

DCO example:

~~~bash
git commit -s -m "feat(topics): add safe topic capability"
~~~

Large architecture/security/public-contract changes require an ADR or RFC rather than being hidden inside an implementation PR.

The repository's official engineering/documentation language is English.

## Good first contributions

Useful contribution areas include:

- documentation and examples,
- accessibility improvements,
- test coverage,
- safe provider compatibility fixtures,
- Kafka/version compatibility evidence,
- UX improvements that preserve backend-authoritative security,
- performance measurements and regression detection,
- issue reproduction and diagnostics,
- security hardening.

Before implementing a roadmap feature, check whether its scope is already approved and whether an RFC is required.

## Support

For normal bugs and feature requests, use [GitHub Issues](https://github.com/araditc/Kafdeck/issues).

A good bug report includes:

- Kafdeck release tag or exact commit SHA,
- operating system / container runtime,
- Kafka version and deployment type,
- Kafdeck access mode,
- minimal reproducible configuration with secrets removed,
- reproduction steps,
- expected vs actual behavior,
- relevant sanitized logs.

See [SUPPORT.md](SUPPORT.md) for project support policy.

### Security reports

**Do not report exploitable vulnerabilities in a public issue.**

Follow [SECURITY.md](SECURITY.md) and use GitHub private vulnerability reporting when available, or another private channel explicitly published by the maintainers.

## Governance and maintainers

Kafdeck uses protected-branch PR governance, DCO, exact-head validation, versioned ADR/RFC decisions, and release gates.

See:

- [GOVERNANCE.md](GOVERNANCE.md)
- [MAINTAINERS.md](MAINTAINERS.md)
- [docs/decisions/approval-log.md](docs/decisions/approval-log.md)

## Releases and supply chain

Kafdeck's release pipeline is designed around exact-source evidence:

- locked dependency restore,
- quality gates,
- CodeQL,
- dependency review,
- Kafka compatibility tests,
- OCI build,
- SPDX SBOM generation,
- High/Critical vulnerability scanning,
- keyless signing for approved published candidates,
- immutable image digest promotion,
- GitHub Release bound to the approved source revision.

Release notes and evidence live in [docs/releases/](docs/releases/).

## Roadmap

The roadmap is public and capability-driven:

[**View the Kafdeck Roadmap →**](ROADMAP.md)

Major future themes include governed Kafka mutations, fleet operations, richer ecosystem integrations, observability/automation, governance, and v1.0 stability/hardening.

Nothing in the roadmap should be interpreted as currently available until it is implemented, validated, and released.

## License

Kafdeck is licensed under the [Apache License 2.0](LICENSE).

Copyright and contribution rights are governed by the license and the project's DCO-based contribution model.

---

<div align="center">

**Kafdeck is built for operators who want powerful Kafka visibility without casually handing a web UI the keys to the cluster.**

If the project is useful to you, consider starring the repository, opening high-quality issues, testing it against real Kafka environments, and contributing evidence-backed improvements.

</div>
