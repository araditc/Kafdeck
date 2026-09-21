#!/usr/bin/env bash
set -euo pipefail

compose_file="deploy/dev/docker-compose.kafka-w04-matrix.yml"
secrets_dir="deploy/dev/.secrets/kafka-w04"

cleanup() {
  docker compose -f "$compose_file" down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$secrets_dir"
}
trap cleanup EXIT
cleanup
mkdir -p "$secrets_dir"

export KAFDECK_STORE_PASSWORD="$(openssl rand -hex 24)"
export KAFDECK_BROKER_PASSWORD="$(openssl rand -hex 24)"
export KAFDECK_PLAIN_PASSWORD="$(openssl rand -hex 24)"
export KAFDECK_RESTRICTED_PASSWORD="$(openssl rand -hex 24)"
export KAFDECK_SCRAM256_PASSWORD="$(openssl rand -hex 24)"
export KAFDECK_SCRAM512_PASSWORD="$(openssl rand -hex 24)"

printf '%s' "$KAFDECK_STORE_PASSWORD" > "$secrets_dir/kafka_keystore_creds"
printf '%s' "$KAFDECK_STORE_PASSWORD" > "$secrets_dir/kafka_ssl_key_creds"
printf '%s' "$KAFDECK_STORE_PASSWORD" > "$secrets_dir/kafka_truststore_creds"
printf '%s' 'kafdeck' > "$secrets_dir/plain.username"
printf '%s' "$KAFDECK_PLAIN_PASSWORD" > "$secrets_dir/plain.password"
printf '%s' 'kafdeck_restricted' > "$secrets_dir/restricted.username"
printf '%s' "$KAFDECK_RESTRICTED_PASSWORD" > "$secrets_dir/restricted.password"
printf '%s' 'kafdeck-scram256' > "$secrets_dir/scram256.username"
printf '%s' "$KAFDECK_SCRAM256_PASSWORD" > "$secrets_dir/scram256.password"
printf '%s' 'kafdeck-scram512' > "$secrets_dir/scram512.username"
printf '%s' "$KAFDECK_SCRAM512_PASSWORD" > "$secrets_dir/scram512.password"

cat > "$secrets_dir/kafka_server_jaas.conf" <<EOF
KafkaServer {
  org.apache.kafka.common.security.plain.PlainLoginModule required
  username="admin"
  password="$KAFDECK_BROKER_PASSWORD"
  user_kafdeck="$KAFDECK_PLAIN_PASSWORD"
  user_kafdeck_restricted="$KAFDECK_RESTRICTED_PASSWORD";
};
EOF

openssl req -x509 -newkey rsa:2048 -nodes -keyout "$secrets_dir/ca.key" -out "$secrets_dir/ca.crt" -subj '/CN=Kafdeck W10 Test CA' -days 1 -sha256 >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes -keyout "$secrets_dir/server.key" -out "$secrets_dir/server.csr" -subj '/CN=localhost' >/dev/null 2>&1
cat > "$secrets_dir/server.ext" <<'EOF'
subjectAltName=DNS:localhost,IP:127.0.0.1
extendedKeyUsage=serverAuth
EOF
openssl x509 -req -in "$secrets_dir/server.csr" -CA "$secrets_dir/ca.crt" -CAkey "$secrets_dir/ca.key" -CAcreateserial -out "$secrets_dir/server.crt" -days 1 -sha256 -extfile "$secrets_dir/server.ext" >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes -keyout "$secrets_dir/client.key" -out "$secrets_dir/client.csr" -subj '/CN=kafdeck-client' >/dev/null 2>&1
cat > "$secrets_dir/client.ext" <<'EOF'
extendedKeyUsage=clientAuth
EOF
openssl x509 -req -in "$secrets_dir/client.csr" -CA "$secrets_dir/ca.crt" -CAkey "$secrets_dir/ca.key" -CAcreateserial -out "$secrets_dir/client.crt" -days 1 -sha256 -extfile "$secrets_dir/client.ext" >/dev/null 2>&1
openssl pkcs12 -export -in "$secrets_dir/server.crt" -inkey "$secrets_dir/server.key" -certfile "$secrets_dir/ca.crt" -name kafka -out "$secrets_dir/kafka.keystore.p12" -passout "pass:$KAFDECK_STORE_PASSWORD" >/dev/null 2>&1
keytool -importkeystore -srckeystore "$secrets_dir/kafka.keystore.p12" -srcstoretype PKCS12 -srcstorepass "$KAFDECK_STORE_PASSWORD" -destkeystore "$secrets_dir/kafka.keystore.jks" -deststoretype JKS -deststorepass "$KAFDECK_STORE_PASSWORD" -destkeypass "$KAFDECK_STORE_PASSWORD" -noprompt >/dev/null 2>&1
keytool -importcert -alias kafdeck-test-ca -file "$secrets_dir/ca.crt" -keystore "$secrets_dir/kafka.truststore.jks" -storepass "$KAFDECK_STORE_PASSWORD" -noprompt >/dev/null 2>&1
chmod -R a+rX "$secrets_dir"

docker compose -f "$compose_file" up -d
for _ in {1..60}; do
  status="$(docker inspect --format='{{.State.Health.Status}}' kafdeck-kafka 2>/dev/null || true)"
  [[ "$status" == "healthy" ]] && break
  sleep 2
done
if [[ "$(docker inspect --format='{{.State.Health.Status}}' kafdeck-kafka 2>/dev/null || true)" != "healthy" ]]; then
  docker compose -f "$compose_file" logs kafka
  echo "Kafka W10 connection-matrix broker did not become healthy." >&2
  exit 1
fi

kafka_topics='/opt/kafka/bin/kafka-topics.sh'
kafka_configs='/opt/kafka/bin/kafka-configs.sh'
kafka_acls='/opt/kafka/bin/kafka-acls.sh'

docker exec kafdeck-kafka "$kafka_topics" --bootstrap-server localhost:9092 --create --topic kafdeck-ci-smoke --partitions 1 --replication-factor 1
docker exec kafdeck-kafka "$kafka_topics" --bootstrap-server localhost:9092 --describe --topic kafdeck-ci-smoke
printf 'kafdeck-record-alpha\nkafdeck-record-beta\nkafdeck-record-gamma\n' | \
  docker exec -i kafdeck-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic kafdeck-ci-smoke >/dev/null
docker exec kafdeck-kafka "$kafka_configs" --bootstrap-server localhost:9092 --alter --add-config "SCRAM-SHA-256=[iterations=4096,password=${KAFDECK_SCRAM256_PASSWORD}]" --entity-type users --entity-name kafdeck-scram256 >/dev/null
docker exec kafdeck-kafka "$kafka_configs" --bootstrap-server localhost:9092 --alter --add-config "SCRAM-SHA-512=[iterations=4096,password=${KAFDECK_SCRAM512_PASSWORD}]" --entity-type users --entity-name kafdeck-scram512 >/dev/null

# Run the accepted transport/security matrix before introducing restricted ACLs.
# This keeps an authorization-fixture defect from obscuring TLS/SASL compatibility.
KAFDECK_RUN_KAFKA_INTEGRATION=1 KAFDECK_TEST_SECRETS_DIR="$(pwd)/$secrets_dir" \
  dotnet test tests/Kafdeck.Architecture.Tests/Kafdeck.Architecture.Tests.csproj --configuration Release --no-restore \
  --filter 'FullyQualifiedName~KafkaAdapterIntegrationTests&FullyQualifiedName!~KafkaAdapterIntegrationTestsAuthorization'

# W33 mutation evidence runs before restricted ACL fixtures are installed. The
# test uses only the four admitted typed topic mutation operations and verifies
# each post-condition through the read-only administration adapter.
KAFDECK_RUN_KAFKA_INTEGRATION=1 KAFDECK_TEST_SECRETS_DIR="$(pwd)/$secrets_dir" \
  dotnet test tests/Kafdeck.Architecture.Tests/Kafdeck.Architecture.Tests.csproj --configuration Release --no-restore \
  --filter FullyQualifiedName~KafkaTopicMutationIntegrationTests

# Explicitly retain metadata Describe while denying DescribeConfigs so the second
# pass exercises genuine partial access rather than a total authorization failure.
docker exec kafdeck-kafka "$kafka_acls" --bootstrap-server localhost:9092 --add --allow-principal User:kafdeck_restricted --operation Describe --topic kafdeck-ci-smoke --force >/dev/null
docker exec kafdeck-kafka "$kafka_acls" --bootstrap-server localhost:9092 --add --deny-principal User:kafdeck_restricted --operation DescribeConfigs --topic kafdeck-ci-smoke --force >/dev/null

KAFDECK_RUN_KAFKA_INTEGRATION=1 KAFDECK_TEST_SECRETS_DIR="$(pwd)/$secrets_dir" \
  dotnet test tests/Kafdeck.Architecture.Tests/Kafdeck.Architecture.Tests.csproj --configuration Release --no-restore \
  --filter FullyQualifiedName~KafkaAdapterIntegrationTestsAuthorization

docker exec kafdeck-kafka "$kafka_topics" --bootstrap-server localhost:9092 --delete --topic kafdeck-ci-smoke

echo "Kafka ${KAFDECK_KAFKA_VERSION:-4.3.1} W10 compatibility, security and partial-access matrix passed."
