# Optional static-cluster deployment-token evaluation

This is a single-deployment laboratory alternative to the [recommended managed Keycloak setup](managed-identity-setup.md). It does not create an initial owner, managed users or registered dashboard targets. For a new multi-user installation, use the [v0.9 installation guide](v0.9-installation-guide.md) and [dashboard walkthrough](v0.9-dashboard-guide.md).

Use disposable resources and a freshly generated deployment token. The PLAINTEXT Kafka and HTTP examples below are for an isolated evaluation network. Use trusted TLS for real deployments and do not reuse example credentials.

## Released image

Kafdeck v0.9 is distributed as a non-root OCI image. Pin the verified immutable digest:

~~~text
ghcr.io/araditc/kafdeck@sha256:8e84e47793f663e02226cb11ca43f60835f87663dca6f76b1075975a91f489f0
~~~

GHCR access is **private under the existing package policy**. Authenticate with a GitHub identity allowed to read the package and a token with `read:packages` when Docker prompts for its password:

~~~sh
docker login ghcr.io --username YOUR_GITHUB_LOGIN
docker pull ghcr.io/araditc/kafdeck@sha256:8e84e47793f663e02226cb11ca43f60835f87663dca6f76b1075975a91f489f0
~~~

The v0.9 application image is published by digest. Use the digest recorded above for Docker/Podman commands. [Release assets](https://github.com/araditc/Kafdeck/releases/tag/v0.9) contain the signed offline mirror, native API/UI/CLI/MCP archives, two OCI architectures, Helm, five Terraform ZIPs, SBOM/scan reports, final acceptance evidence and SHA256SUMS. The runtime image contains no APT package manager. Any customized image must be built and verified before deployment.

## 1. Start a container-reachable Kafka for evaluation

For the Docker quick start, put Kafka and Kafdeck on the same user-defined network and advertise a Kafka listener that is reachable **from the Kafdeck container**:

~~~bash
docker network create kafdeck-demo

docker run -d   --name kafdeck-kafka   --network kafdeck-demo   -p 9092:9092   -e KAFKA_NODE_ID=1   -e KAFKA_PROCESS_ROLES=broker,controller   -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafdeck-kafka:29093   -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER   -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,HOST:PLAINTEXT   -e KAFKA_LISTENERS=CONTROLLER://:29093,INTERNAL://:19092,HOST://:9092   -e KAFKA_ADVERTISED_LISTENERS=INTERNAL://kafdeck-kafka:19092,HOST://localhost:9092   -e KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL   -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1   -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0   -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1   -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1   -e CLUSTER_ID=4L6g3nShT-eMCtK--X86sw   apache/kafka:4.3.1
~~~

The host can use <code>localhost:9092</code>; containers on <code>kafdeck-demo</code> use <code>kafdeck-kafka:19092</code>. This distinction matters because Kafka clients follow broker-advertised listeners after bootstrap.

The repository's simpler <code>deploy/dev/docker-compose.kafka.yml</code> advertises <code>localhost:9092</code> and is intended for **host-native source development**, not for a second Docker container connecting to it.

This evaluation broker is intentionally PLAINTEXT and single-node. Do not copy its security posture into production.

## 2. Create a minimal Kafdeck configuration

Create <code>appsettings.Production.json</code>:

~~~json
{
  "Kafdeck": {
    "Deployment": {
      "ListenUrls": [
        "http://0.0.0.0:8080"
      ],
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
  }
}
~~~

`ListenUrls` is the preferred form and accepts multiple Kestrel endpoints. The legacy single `ListenUrl` key remains supported for existing deployments. When a wildcard bind such as `0.0.0.0` or `[::]` is used, Kafdeck derives a wildcard Host filter only after the deployment has passed the existing non-local Token/OIDC security checks. Concrete IP/hostname bindings derive exact allowed hosts automatically.


## 3. Run the released image

The examples below use the exact verified v0.9 image index digest. Complete the registry login above before running them.

### Windows / Docker Desktop

~~~powershell
$env:KAFDECK_DEPLOYMENT_TOKEN = "change-this-local-token"
docker run --rm --name kafdeck --network kafdeck-demo -p 8080:8080 -e KAFDECK_DEPLOYMENT_TOKEN=$env:KAFDECK_DEPLOYMENT_TOKEN -v "$PWD\appsettings.Production.json:/app/appsettings.Production.json:ro" ghcr.io/araditc/kafdeck@sha256:8e84e47793f663e02226cb11ca43f60835f87663dca6f76b1075975a91f489f0
~~~

### Linux / Docker Engine

~~~bash
export KAFDECK_DEPLOYMENT_TOKEN='change-this-local-token'

docker run --rm \
  --name kafdeck \
  --network kafdeck-demo \
  -p 8080:8080 \
  -e KAFDECK_DEPLOYMENT_TOKEN \
  -v "$PWD/appsettings.Production.json:/app/appsettings.Production.json:ro" \
  ghcr.io/araditc/kafdeck@sha256:8e84e47793f663e02226cb11ca43f60835f87663dca6f76b1075975a91f489f0
~~~

Then open Kafdeck from another machine by using the server's reachable address, for example:

~~~text
http://192.168.10.20:8080/#access_token=change-this-local-token
~~~

For local access on the server itself, `http://127.0.0.1:8080/` remains valid when loopback is also included in `ListenUrls`.

The token is supplied through the URL **fragment**, not a query string. The UI immediately removes it from the address bar/history, keeps it only in page memory, and sends it as <code>X-Kafdeck-Access-Token</code> for API calls. It is not written to <code>localStorage</code> or <code>sessionStorage</code>; after a full page reload, bootstrap Token mode again with the fragment or use OIDC for persistent multi-user sessions.

> [!WARNING]
> Token mode is a deployment access boundary, not a multi-user identity system. For multi-user production deployments, use OIDC/RBAC.

## Linux with Podman

~~~bash
podman run --rm \
  --name kafdeck \
  -p 8080:8080 \
  -e KAFDECK_DEPLOYMENT_TOKEN \
  -v "$PWD/appsettings.Production.json:/app/appsettings.Production.json:ro,Z" \
  ghcr.io/araditc/kafdeck@sha256:8e84e47793f663e02226cb11ca43f60835f87663dca6f76b1075975a91f489f0
~~~

For Podman, create a Podman network and ensure Kafka advertises a broker hostname reachable on that network. The Docker-specific <code>kafdeck-demo</code> recipe above is not automatically shared with Podman.
