using Kafdeck.Core.Kafka;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Kafka;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class KafkaAdapterIntegrationTests
{
    [Fact]
    public async Task Unknown_cluster_fails_as_invalid_configuration_without_network_access()
    {
        using var adapter = new ConfluentKafkaAdministrationAdapter([], new SecretResolver());
        var result = await adapter.GetClusterMetadataAsync("missing", new KafkaOperationContext(DateTimeOffset.UtcNow.AddSeconds(10)), CancellationToken.None);
        AssertFailure(result, KafkaFailureCategory.InvalidConfiguration, "cluster_not_configured");
    }

    [Fact]
    public async Task Expired_deadline_fails_before_creating_a_Kafka_client()
    {
        using var adapter = CreatePlaintextAdapter();
        var result = await adapter.GetClusterMetadataAsync("preflight", new KafkaOperationContext(DateTimeOffset.UtcNow.AddSeconds(-1)), CancellationToken.None);
        AssertFailure(result, KafkaFailureCategory.Timeout, "deadline_exceeded");
    }

    [Fact]
    public async Task Cancelled_operation_fails_before_creating_a_Kafka_client()
    {
        using var adapter = CreatePlaintextAdapter();
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var result = await adapter.GetClusterMetadataAsync("preflight", new KafkaOperationContext(DateTimeOffset.UtcNow.AddSeconds(10)), cancellation.Token);
        AssertFailure(result, KafkaFailureCategory.Cancelled, "operation_cancelled");
    }

    [Fact]
    public async Task Invalid_secret_reference_is_normalized_without_exposing_the_secret_locator()
    {
        var missingPath = Path.Combine(Path.GetTempPath(), $"kafdeck-missing-secret-{Guid.NewGuid():N}");
        var missingSecret = SecretReference.Parse($"file:{missingPath}");
        var profile = new ClusterProfile("invalid-secret", ["localhost:1"], KafkaSecurityProtocol.SaslPlaintext, null, new SaslProfile(SaslMechanism.Plain, missingSecret, missingSecret));
        using var adapter = new ConfluentKafkaAdministrationAdapter([profile], new SecretResolver());
        var result = await adapter.GetClusterMetadataAsync(profile.Id, new KafkaOperationContext(DateTimeOffset.UtcNow.AddSeconds(10)), CancellationToken.None);
        AssertFailure(result, KafkaFailureCategory.InvalidConfiguration, "invalid_configuration");
        Assert.DoesNotContain(missingPath, result.Failure!.SafeMessage, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Invalid_tls_trust_is_rejected_without_exposing_certificate_paths()
    {
        if (!RunKafkaIntegration()) return;
        var secretsDirectory = RequireSecretsDirectory();
        var wrongTrustAnchor = FileReference(secretsDirectory, "client.crt");
        var profile = new ClusterProfile("invalid-tls", ["localhost:9095"], KafkaSecurityProtocol.Ssl, new TlsProfile(true, wrongTrustAnchor, null, null), null);
        using var adapter = new ConfluentKafkaAdministrationAdapter([profile], new SecretResolver());
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var result = await adapter.GetClusterMetadataAsync(profile.Id, new KafkaOperationContext(DateTimeOffset.UtcNow.AddSeconds(10)), cancellation.Token);
        Assert.False(result.IsSuccess);
        Assert.NotNull(result.Failure);
        Assert.Equal(KafkaFailureCategory.TlsFailure, result.Failure.Category);
        Assert.False(result.Failure.IsRetryable);
        Assert.DoesNotContain(secretsDirectory, result.Failure.SafeMessage, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Accepted_connection_modes_support_read_only_administration()
    {
        if (!RunKafkaIntegration()) return;
        var secretsDirectory = RequireSecretsDirectory();
        var ca = FileReference(secretsDirectory, "ca.crt");
        var clientCertificate = FileReference(secretsDirectory, "client.crt");
        var clientKey = FileReference(secretsDirectory, "client.key");
        var plainUsername = FileReference(secretsDirectory, "plain.username");
        var plainPassword = FileReference(secretsDirectory, "plain.password");
        var scram256Username = FileReference(secretsDirectory, "scram256.username");
        var scram256Password = FileReference(secretsDirectory, "scram256.password");
        var scram512Username = FileReference(secretsDirectory, "scram512.username");
        var scram512Password = FileReference(secretsDirectory, "scram512.password");
        var serverTls = new TlsProfile(true, ca, null, null);
        var mutualTls = new TlsProfile(true, ca, clientCertificate, clientKey);
        var profiles = new[]
        {
            new ClusterProfile("plaintext", ["localhost:9092"], KafkaSecurityProtocol.Plaintext, null, null),
            new ClusterProfile("mtls", ["localhost:9093"], KafkaSecurityProtocol.Ssl, mutualTls, null),
            new ClusterProfile("sasl-plaintext-plain", ["localhost:9094"], KafkaSecurityProtocol.SaslPlaintext, null, new SaslProfile(SaslMechanism.Plain, plainUsername, plainPassword)),
            new ClusterProfile("sasl-plaintext-scram256", ["localhost:9094"], KafkaSecurityProtocol.SaslPlaintext, null, new SaslProfile(SaslMechanism.ScramSha256, scram256Username, scram256Password)),
            new ClusterProfile("sasl-plaintext-scram512", ["localhost:9094"], KafkaSecurityProtocol.SaslPlaintext, null, new SaslProfile(SaslMechanism.ScramSha512, scram512Username, scram512Password)),
            new ClusterProfile("sasl-ssl-plain", ["localhost:9095"], KafkaSecurityProtocol.SaslSsl, serverTls, new SaslProfile(SaslMechanism.Plain, plainUsername, plainPassword)),
            new ClusterProfile("sasl-ssl-scram256", ["localhost:9095"], KafkaSecurityProtocol.SaslSsl, serverTls, new SaslProfile(SaslMechanism.ScramSha256, scram256Username, scram256Password)),
            new ClusterProfile("sasl-ssl-scram512", ["localhost:9095"], KafkaSecurityProtocol.SaslSsl, serverTls, new SaslProfile(SaslMechanism.ScramSha512, scram512Username, scram512Password)),
        };
        using var adapter = new ConfluentKafkaAdministrationAdapter(profiles, new SecretResolver());
        foreach (var profile in profiles)
        {
            using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            var operation = new KafkaOperationContext(DateTimeOffset.UtcNow.AddSeconds(20));
            var cluster = await adapter.GetClusterMetadataAsync(profile.Id, operation, cancellation.Token);
            AssertSuccess(profile.Id, cluster);
            Assert.NotEmpty(cluster.Value!.Brokers);
            var topics = await adapter.ListTopicsAsync(profile.Id, operation, cancellation.Token);
            AssertSuccess(profile.Id, topics);
            Assert.Contains(topics.Value!, topic => topic.Name == "kafdeck-ci-smoke");
            var topic = await adapter.GetTopicMetadataAsync(profile.Id, "kafdeck-ci-smoke", operation, cancellation.Token);
            AssertSuccess(profile.Id, topic);
            Assert.Single(topic.Value!.Partitions);
            var configuration = await adapter.GetTopicConfigurationAsync(profile.Id, "kafdeck-ci-smoke", operation, cancellation.Token);
            AssertSuccess(profile.Id, configuration);
            Assert.NotEmpty(configuration.Value!);
            var brokerConfiguration = await adapter.GetBrokerConfigurationAsync(profile.Id, cluster.Value.ControllerBrokerId ?? cluster.Value.Brokers[0].BrokerId, operation, cancellation.Token);
            AssertSuccess(profile.Id, brokerConfiguration);
            Assert.NotEmpty(brokerConfiguration.Value!);
        }
        var capabilities = await adapter.GetCapabilitiesAsync("plaintext", new KafkaOperationContext(DateTimeOffset.UtcNow.AddSeconds(20)), CancellationToken.None);
        AssertSuccess("plaintext", capabilities);
        Assert.Contains(capabilities.Value!.Items, item => item.Capability == KafkaCapabilityKind.ClusterMetadata && item.State == KafkaCapabilityState.Available);
        Assert.Contains(capabilities.Value.Items, item => item.Capability == KafkaCapabilityKind.TopicMetadata && item.State == KafkaCapabilityState.Available);
    }

    private static bool RunKafkaIntegration() => string.Equals(Environment.GetEnvironmentVariable("KAFDECK_RUN_KAFKA_INTEGRATION"), "1", StringComparison.Ordinal);
    private static string RequireSecretsDirectory()
    {
        var directory = Environment.GetEnvironmentVariable("KAFDECK_TEST_SECRETS_DIR");
        Assert.False(string.IsNullOrWhiteSpace(directory));
        Assert.True(Path.IsPathFullyQualified(directory));
        return directory!;
    }
    private static ConfluentKafkaAdministrationAdapter CreatePlaintextAdapter()
    {
        var profile = new ClusterProfile("preflight", ["localhost:1"], KafkaSecurityProtocol.Plaintext, null, null);
        return new ConfluentKafkaAdministrationAdapter([profile], new SecretResolver());
    }
    private static SecretReference FileReference(string directory, string fileName) => SecretReference.Parse($"file:{Path.Combine(directory, fileName)}");
    private static void AssertSuccess<T>(string profileId, KafkaResult<T> result) => Assert.True(result.IsSuccess, $"{profileId}: {result.Failure?.Category} / {result.Failure?.Code} / {result.Failure?.SafeMessage}");
    private static void AssertFailure<T>(KafkaResult<T> result, KafkaFailureCategory category, string code)
    {
        Assert.False(result.IsSuccess);
        Assert.NotNull(result.Failure);
        Assert.Equal(category, result.Failure.Category);
        Assert.Equal(code, result.Failure.Code);
    }
}
