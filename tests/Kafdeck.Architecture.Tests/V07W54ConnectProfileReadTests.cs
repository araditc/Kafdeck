using System.Net;
using System.Text;
using Kafdeck.Core.ReadViews;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Ecosystem;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V07W54ConnectProfileReadTests
{
    [Fact]
    public async Task Profile_scoped_reads_are_isolated_and_legacy_alias_uses_default()
    {
        var defaultHandler = new StubHandler(
            """{"version":"4.1.0","commit":"a","kafka_cluster_id":"default-kafka"}""");
        var analyticsHandler = new StubHandler(
            """{"version":"4.1.0","commit":"b","kafka_cluster_id":"analytics-kafka"}""");

        var cluster = new ClusterProfile(
            "prod",
            new[] { "broker.example:9092" },
            KafkaSecurityProtocol.Plaintext,
            null,
            null,
            null,
            null,
            null,
            new[]
            {
                new KafkaConnectProfile(
                    "https://connect-default.example:8083",
                    null,
                    null,
                    Id: "default"),
                new KafkaConnectProfile(
                    "https://connect-analytics.example:8083",
                    null,
                    null,
                    Id: "analytics"),
            });

        using var adapter = new KafkaConnectReadAdapter(
            new[] { cluster },
            new SecretResolver(),
            (_, profile) =>
                profile.Id == "default"
                    ? defaultHandler
                    : analyticsHandler,
            maxConcurrencyPerProfile: 1);

        var profiles = await adapter.ListProfilesAsync(
            "prod",
            Operation(),
            CancellationToken.None);
        var legacy = await adapter.GetClusterInfoAsync(
            "prod",
            Operation(),
            CancellationToken.None);
        var analytics = await adapter.GetClusterInfoAsync(
            "prod",
            "analytics",
            Operation(),
            CancellationToken.None);

        Assert.True(profiles.IsSuccess, profiles.Failure?.SafeMessage);
        Assert.Equal(
            new[] { "analytics", "default" },
            profiles.Value!.Select(profile => profile.Id).ToArray());
        Assert.True(
            profiles.Value!.Single(profile => profile.Id == "default").IsDefault);

        Assert.True(legacy.IsSuccess, legacy.Failure?.SafeMessage);
        Assert.Equal("default-kafka", legacy.Value!.KafkaClusterId);

        Assert.True(analytics.IsSuccess, analytics.Failure?.SafeMessage);
        Assert.Equal("analytics-kafka", analytics.Value!.KafkaClusterId);

        Assert.Equal(1, defaultHandler.CallCount);
        Assert.Equal(1, analyticsHandler.CallCount);
    }

    [Fact]
    public async Task Missing_non_default_profile_fails_without_provider_io()
    {
        var handler = new StubHandler(
            """{"version":"4.1.0","commit":"a","kafka_cluster_id":"default-kafka"}""");

        var cluster = new ClusterProfile(
            "prod",
            new[] { "broker.example:9092" },
            KafkaSecurityProtocol.Plaintext,
            null,
            null,
            null,
            null,
            null,
            new[]
            {
                new KafkaConnectProfile(
                    "https://connect-default.example:8083",
                    null,
                    null,
                    Id: "default"),
            });

        using var adapter = new KafkaConnectReadAdapter(
            new[] { cluster },
            new SecretResolver(),
            (_, _) => handler);

        var result = await adapter.GetClusterInfoAsync(
            "prod",
            "missing",
            Operation(),
            CancellationToken.None);

        Assert.False(result.IsSuccess);
        Assert.Equal(
            ReadViewFailureCategory.NotConfigured,
            result.Failure!.Category);
        Assert.Equal(
            "connect_profile_not_configured",
            result.Failure.Code);
        Assert.Equal(0, handler.CallCount);
    }

    [Fact]
    public async Task Invalid_profile_id_fails_before_provider_io()
    {
        var handler = new StubHandler(
            """{"version":"4.1.0","commit":"a","kafka_cluster_id":"default-kafka"}""");

        var cluster = new ClusterProfile(
            "prod",
            new[] { "broker.example:9092" },
            KafkaSecurityProtocol.Plaintext,
            null,
            null,
            null,
            null,
            null,
            new[]
            {
                new KafkaConnectProfile(
                    "https://connect-default.example:8083",
                    null,
                    null,
                    Id: "default"),
            });

        using var adapter = new KafkaConnectReadAdapter(
            new[] { cluster },
            new SecretResolver(),
            (_, _) => handler);

        var result = await adapter.ListConnectorsAsync(
            "prod",
            "bad/profile",
            Operation(),
            CancellationToken.None);

        Assert.False(result.IsSuccess);
        Assert.Equal(
            ReadViewFailureCategory.InvalidRequest,
            result.Failure!.Category);
        Assert.Equal(
            "invalid_connect_profile_id",
            result.Failure.Code);
        Assert.Equal(0, handler.CallCount);
    }

    private static ReadViewOperationContext Operation() =>
        new(
            DateTimeOffset.UtcNow.AddSeconds(10),
            maxItems: 100,
            maxResponseBytes: 1024 * 1024);

    private sealed class StubHandler : HttpMessageHandler
    {
        private readonly string _rootBody;

        public StubHandler(string rootBody)
        {
            _rootBody = rootBody;
        }

        public int CallCount { get; private set; }

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            CallCount++;
            var body = request.RequestUri!.AbsolutePath switch
            {
                "/" => _rootBody,
                "/connectors" => "[]",
                _ => "{}",
            };

            return Task.FromResult(
                new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent(
                        body,
                        Encoding.UTF8,
                        "application/json"),
                });
        }
    }
}
