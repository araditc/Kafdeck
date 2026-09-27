using System.Net;
using Kafdeck.Core.ReadViews;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Ecosystem;
using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V07W54ConnectProfileMutationAdapterTests
{
    [Fact]
    public async Task Profile_scoped_mutation_dispatch_isolated_from_default_profile()
    {
        var defaultHandler = new RecordingHandler();
        var analyticsHandler = new RecordingHandler();

        var cluster = new ClusterProfile(
            "prod",
            ["localhost:9092"],
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
                    KafkaConnectMutationProviderProfile.ConfluentCompatibleV1,
                    "default"),
                new KafkaConnectProfile(
                    "https://connect-analytics.example:8083",
                    null,
                    null,
                    KafkaConnectMutationProviderProfile.ConfluentCompatibleV1,
                    "analytics"),
            });

        using var adapter = new KafkaConnectMutationAdapter(
            new[] { cluster },
            new SecretResolver(),
            (_, profile) =>
                profile.Id == "default"
                    ? defaultHandler
                    : analyticsHandler);

        var configuration =
            new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["connector.class"] = "org.example.Sink",
                ["tasks.max"] = "1",
            };

        var defaultResult = await adapter.CreateAsync(
            new ConnectCreateMutation(
                "prod",
                "sink-default",
                configuration));

        var analyticsResult = await adapter.CreateAsync(
            new ConnectCreateMutation(
                "prod",
                "sink-analytics",
                configuration,
                "analytics"));

        Assert.Equal(
            MutationExecutionResultKind.AppliedUnverified,
            defaultResult.ResultKind);
        Assert.Equal(
            MutationExecutionResultKind.AppliedUnverified,
            analyticsResult.ResultKind);

        Assert.Equal(
            new[] { ("POST", "/connectors") },
            defaultHandler.Requests);
        Assert.Equal(
            new[] { ("POST", "/connectors") },
            analyticsHandler.Requests);

        var analyticsCapabilities = await adapter.GetCapabilitiesAsync(
            "prod",
            "analytics",
            Operation(),
            CancellationToken.None);

        Assert.True(
            analyticsCapabilities.IsSuccess,
            analyticsCapabilities.Failure?.SafeMessage);
    }

    [Fact]
    public async Task Missing_profile_fails_before_mutation_or_observation_network_io()
    {
        var handler = new RecordingHandler();
        var cluster = new ClusterProfile(
            "prod",
            ["localhost:9092"],
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
                    KafkaConnectMutationProviderProfile.ConfluentCompatibleV1,
                    "default"),
            });

        using var adapter = new KafkaConnectMutationAdapter(
            new[] { cluster },
            new SecretResolver(),
            (_, _) => handler);

        var mutation = await adapter.DeleteAsync(
            new ConnectDeleteMutation(
                "prod",
                "sink-a",
                "missing"));

        var observation = await adapter.ObserveConnectorAsync(
            "prod",
            "missing",
            "sink-a",
            Operation(),
            CancellationToken.None);

        Assert.Equal(
            MutationExecutionResultKind.FailedDefinitive,
            mutation.ResultKind);
        Assert.Equal(
            "connect_delete_not_configured",
            mutation.ResultCode);

        Assert.False(observation.IsSuccess);
        Assert.NotNull(observation.Failure);
        Assert.Equal(
            Kafdeck.Core.Ecosystem.ConnectMutationObservationFailureCategory.NotConfigured,
            observation.Failure!.Category);
        Assert.Equal(
            "connect_profile_not_configured",
            observation.Failure.Code);
        Assert.Empty(handler.Requests);
    }

    [Fact]
    public async Task Non_admitted_profile_fails_write_but_remains_observable()
    {
        var handler = new RecordingHandler(
            request =>
                request.RequestUri!.AbsolutePath switch
                {
                    "/connectors/sink-a/status" =>
                        Json(HttpStatusCode.NotFound, "{}"),
                    _ => new HttpResponseMessage(HttpStatusCode.NoContent),
                });

        var cluster = new ClusterProfile(
            "prod",
            ["localhost:9092"],
            KafkaSecurityProtocol.Plaintext,
            null,
            null,
            null,
            null,
            null,
            new[]
            {
                new KafkaConnectProfile(
                    "https://connect-readonly.example:8083",
                    null,
                    null,
                    KafkaConnectMutationProviderProfile.None,
                    "readonly"),
            });

        using var adapter = new KafkaConnectMutationAdapter(
            new[] { cluster },
            new SecretResolver(),
            (_, _) => handler);

        var capabilities = await adapter.GetCapabilitiesAsync(
            "prod",
            "readonly",
            Operation(),
            CancellationToken.None);

        var mutation = await adapter.CreateAsync(
            new ConnectCreateMutation(
                "prod",
                "sink-a",
                new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["connector.class"] = "org.example.Sink",
                },
                "readonly"));

        Assert.False(capabilities.IsSuccess);
        Assert.NotNull(capabilities.Failure);
        Assert.Equal(
            Kafdeck.Core.Ecosystem.ConnectMutationObservationFailureCategory.Unsupported,
            capabilities.Failure!.Category);
        Assert.Equal(
            "connect_mutation_profile_not_admitted",
            capabilities.Failure.Code);

        Assert.Equal(
            MutationExecutionResultKind.FailedDefinitive,
            mutation.ResultKind);
        Assert.Equal(
            "connect_create_provider_profile_not_admitted",
            mutation.ResultCode);
        Assert.Empty(handler.Requests);
    }

    private static ReadViewOperationContext Operation() =>
        new(
            DateTimeOffset.UtcNow.AddSeconds(10),
            maxItems: 100,
            maxResponseBytes: 1024 * 1024);

    private static HttpResponseMessage Json(
        HttpStatusCode status,
        string body) =>
        new(status)
        {
            Content = new StringContent(
                body,
                System.Text.Encoding.UTF8,
                "application/json"),
        };

    private sealed class RecordingHandler : HttpMessageHandler
    {
        private readonly Func<HttpRequestMessage, HttpResponseMessage> _response;

        public RecordingHandler()
            : this(_ => new HttpResponseMessage(HttpStatusCode.NoContent))
        {
        }

        public RecordingHandler(
            Func<HttpRequestMessage, HttpResponseMessage> response)
        {
            _response = response;
        }

        public List<(string Method, string Path)> Requests { get; } = [];

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            Requests.Add(
                (
                    request.Method.Method,
                    request.RequestUri?.AbsolutePath
                    ?? throw new InvalidOperationException()));

            return Task.FromResult(_response(request));
        }
    }
}
