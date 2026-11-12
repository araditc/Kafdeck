using System.Text.Json;
using Confluent.Kafka;

if (args.Length != 5)
    throw new ArgumentException("Pass mode, bootstrap, topic, expected advertised address and CA file.");

var (mode, bootstrap, topic, advertised, certificate) = (args[0], args[1], args[2], args[3], args[4]);
var connection = new ClientConfig
{
    BootstrapServers = bootstrap,
    SecurityProtocol = SecurityProtocol.Ssl,
    SslCaLocation = certificate,
    EnableSslCertificateVerification = true,
    SslEndpointIdentificationAlgorithm = SslEndpointIdentificationAlgorithm.Https,
};
using var admin = new AdminClientBuilder(new AdminClientConfig(connection)
{
    SocketTimeoutMs = 5_000,
}).SetErrorHandler((_, _) => { }).Build();
var metadata = admin.GetMetadata(topic, TimeSpan.FromSeconds(5));
var broker = metadata.Brokers.Single();
var endpoint = $"{broker.Host}:{broker.Port}";
if (endpoint != advertised || metadata.Topics.Single().Error.IsError)
    throw new InvalidOperationException("Unexpected broker metadata or topic.");

if (mode == "metadata")
{
    Console.WriteLine(JsonSerializer.Serialize(new { bootstrap, advertised = endpoint, tlsVerified = true }));
    return;
}
if (mode is not ("produce-allowed" or "produce-denied"))
    throw new ArgumentException("Unrecognized fixture mode.");

using var producer = new ProducerBuilder<Null, string>(new ProducerConfig(connection)
{
    EnableIdempotence = true,
    Acks = Acks.All,
    AllowAutoCreateTopics = false,
    MessageTimeoutMs = 15_000,
    RequestTimeoutMs = 10_000,
}).SetErrorHandler((_, _) => { }).Build();

try
{
    var delivery = await producer.ProduceAsync(new TopicPartition(topic, new Partition(0)),
        new Message<Null, string> { Value = mode });
    if (mode != "produce-allowed" || delivery.Status != PersistenceStatus.Persisted)
        throw new InvalidOperationException("Unexpected persisted delivery.");
    Console.WriteLine(JsonSerializer.Serialize(new { bootstrap, advertised = endpoint, persisted = true, offset = delivery.Offset.Value }));
}
catch (ProduceException<Null, string> error) when (mode == "produce-denied" &&
    error.Error.Code == ErrorCode.Local_MsgTimedOut && error.DeliveryResult.Status == PersistenceStatus.NotPersisted)
{
    Console.WriteLine(JsonSerializer.Serialize(new { bootstrap, advertised = endpoint, persisted = false, error = error.Error.Code.ToString() }));
}
