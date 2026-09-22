using System.Net;
using System.Text;
using System.Text.Json;
using Kafdeck.Core.ReadViews;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Ecosystem;
using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class KafkaConnectMutationAdapterTests
{
    [Fact]
    public async Task Typed_mutations_use_only_fixed_Kafka_Connect_routes()
    {
        var handler = new RecordingHandler(
            _ => new HttpResponseMessage(HttpStatusCode.NoContent));
        using var adapter = CreateAdapter(handler);

        var configuration =
            new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["connector.class"] = "org.example.Sink",
                ["tasks.max"] = "1",
            };

        var results = new[]
        {
            await adapter.CreateAsync(
                new ConnectCreateMutation("prod", "sink-a", configuration)),
            await adapter.AlterAsync(
                new ConnectAlterMutation("prod", "sink-a", configuration)),
            await adapter.ControlAsync(
                new ConnectControlMutation(
                    "prod",
                    "sink-a",
                    null,
                    ConnectControlAction.Pause)),
            await adapter.ControlAsync(
                new ConnectControlMutation(
                    "prod",
                    "sink-a",
                    null,
                    ConnectControlAction.Resume)),
            await adapter.ControlAsync(
                new ConnectControlMutation(
                    "prod",
                    "sink-a",
                    null,
                    ConnectControlAction.Restart)),
            await adapter.ControlAsync(
                new ConnectControlMutation(
                    "prod",
                    "sink-a",
                    3,
                    ConnectControlAction.Restart)),
            await adapter.DeleteAsync(
                new ConnectDeleteMutation("prod", "sink-a")),
        };

        Assert.All(
            results,
            result => Assert.Equal(
                MutationExecutionResultKind.AppliedUnverified,
                result.ResultKind));

        Assert.Equal(
            new[]
            {
                ("POST", "/connectors"),
                ("PUT", "/connectors/sink-a/config"),
                ("PUT", "/connectors/sink-a/pause"),
                ("PUT", "/connectors/sink-a/resume"),
                ("POST", "/connectors/sink-a/restart"),
                ("POST", "/connectors/sink-a/tasks/3/restart"),
                ("DELETE", "/connectors/sink-a"),
            },
            handler.Requests);
    }

    [Theory]
    [InlineData(HttpStatusCode.Forbidden, MutationExecutionResultKind.FailedDefinitive, "connect_create_authorization_denied")]
    [InlineData(HttpStatusCode.TemporaryRedirect, MutationExecutionResultKind.FailedDefinitive, "connect_create_redirect_rejected")]
    [InlineData(HttpStatusCode.InternalServerError, MutationExecutionResultKind.ExecutionUnknown, "connect_create_ambiguous")]
    [InlineData(HttpStatusCode.RequestTimeout, MutationExecutionResultKind.ExecutionUnknown, "connect_create_ambiguous")]
    [InlineData(HttpStatusCode.TooManyRequests, MutationExecutionResultKind.ExecutionUnknown, "connect_create_ambiguous")]
    public async Task Mutation_http_status_is_mapped_without_raw_provider_text(
        HttpStatusCode status,
        MutationExecutionResultKind expectedKind,
        string expectedCode)
    {
        var handler = new RecordingHandler(
            _ => new HttpResponseMessage(status)
            {
                Content = new StringContent(
                    "provider-private-error-details",
                    Encoding.UTF8,
                    "text/plain"),
            });
        using var adapter = CreateAdapter(handler);

        var result = await adapter.CreateAsync(
            new ConnectCreateMutation(
                "prod",
                "sink-a",
                new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["connector.class"] = "org.example.Sink",
                }));

        Assert.Equal(expectedKind, result.ResultKind);
        Assert.Equal(expectedCode, result.ResultCode);
        Assert.DoesNotContain(
            "provider-private-error-details",
            JsonSerializer.Serialize(result),
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task Mutation_response_body_is_bounded_and_oversize_is_ambiguous()
    {
        var handler = new RecordingHandler(
            _ => new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new ByteArrayContent(
                    new byte[2 * 1024 * 1024 + 1]),
            });
        using var adapter = CreateAdapter(handler);

        var result = await adapter.CreateAsync(
            new ConnectCreateMutation(
                "prod",
                "sink-a",
                new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["connector.class"] = "org.example.Sink",
                }));

        Assert.Equal(
            MutationExecutionResultKind.ExecutionUnknown,
            result.ResultKind);
        Assert.Equal(
            "connect_create_invalid_provider_response",
            result.ResultCode);
    }

    [Fact]
    public async Task Configured_url_without_admitted_mutation_profile_fails_closed_before_network_access()
    {
        var handler = new RecordingHandler(
            _ => throw new InvalidOperationException(
                "Network should not be reached."));

        var profile = new ClusterProfile(
            "prod",
            ["localhost:9092"],
            KafkaSecurityProtocol.Plaintext,
            null,
            null,
            Connect: new KafkaConnectProfile(
                "https://connect.example/",
                null,
                null,
                KafkaConnectMutationProviderProfile.ConfluentCompatibleV1));

        using var adapter = new KafkaConnectMutationAdapter(
            [profile],
            new SecretResolver(),
            _ => handler);

        var capabilities = await adapter.GetCapabilitiesAsync(
            "prod",
            new ReadViewOperationContext(
                DateTimeOffset.UtcNow.AddSeconds(5),
                maxItems: 16,
                maxResponseBytes: 4096),
            CancellationToken.None);

        Assert.False(capabilities.IsSuccess);
        Assert.NotNull(capabilities.Failure);
        Assert.Equal(
            Kafdeck.Core.Ecosystem.ConnectMutationObservationFailureCategory.Unsupported,
            capabilities.Failure!.Category);
        Assert.Equal(
            "connect_mutation_profile_not_admitted",
            capabilities.Failure.Code);

        var result = await adapter.CreateAsync(
            new ConnectCreateMutation(
                "prod",
                "sink-a",
                new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["connector.class"] = "org.example.Sink",
                }));

        Assert.Equal(
            MutationExecutionResultKind.FailedDefinitive,
            result.ResultKind);
        Assert.Equal(
            "connect_create_provider_profile_not_admitted",
            result.ResultCode);
        Assert.Empty(handler.Requests);
    }

    [Fact]
    public async Task Missing_Connect_profile_fails_definitively_before_network_access()
    {
        var handler = new RecordingHandler(
            _ => throw new InvalidOperationException(
                "Network should not be reached."));

        var profile = new ClusterProfile(
            "prod",
            ["localhost:9092"],
            KafkaSecurityProtocol.Plaintext,
            null,
            null);

        using var adapter = new KafkaConnectMutationAdapter(
            [profile],
            new SecretResolver(),
            _ => handler);

        var result = await adapter.CreateAsync(
            new ConnectCreateMutation(
                "prod",
                "sink-a",
                new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["connector.class"] = "org.example.Sink",
                }));

        Assert.Equal(
            MutationExecutionResultKind.FailedDefinitive,
            result.ResultKind);
        Assert.Equal(
            "connect_create_not_configured",
            result.ResultCode);
        Assert.Empty(handler.Requests);
    }

    private static KafkaConnectMutationAdapter CreateAdapter(
        HttpMessageHandler handler)
    {
        var profile = new ClusterProfile(
            "prod",
            ["localhost:9092"],
            KafkaSecurityProtocol.Plaintext,
            null,
            null,
            Connect: new KafkaConnectProfile(
                "https://connect.example/",
                null,
                null));

        return new KafkaConnectMutationAdapter(
            [profile],
            new SecretResolver(),
            _ => handler);
    }

    private sealed class RecordingHandler : HttpMessageHandler
    {
        private readonly Func<HttpRequestMessage, HttpResponseMessage> _response;

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
                    ?? throw new InvalidOperationException(
                        "Kafka Connect request URI was not absolute.")));

            return Task.FromResult(_response(request));
        }
    }
}
