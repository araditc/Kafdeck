using System.Net;
using System.Text;
using Kafdeck.Core.ReadViews;
using Kafdeck.Core.Records;
using Kafdeck.Core.Schemas;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.SchemaRegistry;
using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class SchemaMutationAdapterTests
{
    [Fact]
    public async Task Confluent_mutations_use_only_fixed_typed_routes()
    {
        var handler = new StubHandler(request =>
        {
            var path = request.RequestUri!.PathAndQuery;
            return (request.Method.Method, path) switch
            {
                ("POST", "/compatibility/subjects/orders-value/versions/latest") =>
                    Json(HttpStatusCode.OK, """{"is_compatible":true}"""),
                ("POST", "/subjects/orders-value/versions") =>
                    Json(HttpStatusCode.OK, """{"id":42}"""),
                ("PUT", "/config/orders-value") =>
                    Json(HttpStatusCode.OK, """{"compatibility":"FULL"}"""),
                ("DELETE", "/subjects/orders-value/versions/2") =>
                    Json(HttpStatusCode.OK, "2"),
                ("DELETE", "/subjects/orders-value?permanent=true") =>
                    Json(HttpStatusCode.OK, "[1,2]"),
                _ => Json(HttpStatusCode.NotFound, "{}"),
            };
        });

        using var adapter = CreateAdapter(handler);

        var compatibility = await adapter.TestCompatibilityAsync(
            "cluster-a",
            new SchemaCompatibilityCheckRequest(
                "orders-value",
                RecordSchemaFormat.Avro,
                """{"type":"record","name":"Order","fields":[]}""",
                new[]
                {
                    new RecordSchemaReference(
                        "common.avsc",
                        "common-value",
                        1),
                }),
            Operation(),
            CancellationToken.None);

        Assert.True(compatibility.IsSuccess);
        Assert.True(compatibility.Value!.IsCompatible);

        var created = await adapter.CreateAsync(
            new SchemaCreateMutation(
                "cluster-a",
                "orders-value",
                RecordSchemaFormat.Avro,
                """{"type":"record","name":"Order","fields":[]}""",
                new[]
                {
                    new SchemaMutationReference(
                        "common.avsc",
                        "common-value",
                        1),
                }));

        Assert.Equal(
            MutationExecutionResultKind.AppliedUnverified,
            created.ResultKind);
        Assert.Equal("42", created.SafeEvidence!["schema.id"]);

        var altered = await adapter.AlterCompatibilityAsync(
            new SchemaAlterMutation(
                "cluster-a",
                SchemaCompatibilityScope.Subject,
                "orders-value",
                "FULL"));

        Assert.Equal(
            MutationExecutionResultKind.AppliedUnverified,
            altered.ResultKind);

        var versionDelete = await adapter.DeleteAsync(
            new SchemaDeleteMutation(
                "cluster-a",
                "orders-value",
                2,
                Permanent: false));
        Assert.Equal(
            MutationExecutionResultKind.AppliedUnverified,
            versionDelete.ResultKind);

        var permanentSubjectDelete = await adapter.DeleteAsync(
            new SchemaDeleteMutation(
                "cluster-a",
                "orders-value",
                null,
                Permanent: true));
        Assert.Equal(
            MutationExecutionResultKind.AppliedUnverified,
            permanentSubjectDelete.ResultKind);

        Assert.Equal(
            new[]
            {
                "POST /compatibility/subjects/orders-value/versions/latest",
                "POST /subjects/orders-value/versions",
                "PUT /config/orders-value",
                "DELETE /subjects/orders-value/versions/2",
                "DELETE /subjects/orders-value?permanent=true",
            },
            handler.Requests);

        Assert.All(
            handler.Bodies.Take(2),
            body => Assert.Contains(
                "\"schema\"",
                body,
                StringComparison.Ordinal));
        Assert.Contains(
            "\"compatibility\":\"FULL\"",
            handler.Bodies[2],
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task Karapace_profile_uses_the_admitted_confluent_compatible_lifecycle_contract()
    {
        var handler = new StubHandler(request =>
            request.RequestUri!.PathAndQuery switch
            {
                "/subjects/orders-value/versions" =>
                    Json(HttpStatusCode.OK, """{"id":42}"""),
                _ => Json(HttpStatusCode.NotFound, "{}"),
            });

        using var adapter = CreateAdapter(
            handler,
            SchemaRegistryProviderProfile.KarapaceCompatibleV1);

        var capabilities = await adapter.GetCapabilitiesAsync(
            "cluster-a",
            Operation(),
            CancellationToken.None);

        Assert.True(capabilities.IsSuccess);
        Assert.Equal(
            nameof(SchemaRegistryProviderProfile.KarapaceCompatibleV1),
            capabilities.Value!.ProviderProfile);
        Assert.True(capabilities.Value.SupportsRegistration);

        var result = await adapter.CreateAsync(CreateMutation());

        Assert.Equal(
            MutationExecutionResultKind.AppliedUnverified,
            result.ResultKind);
        Assert.Equal(
            new[] { "POST /subjects/orders-value/versions" },
            handler.Requests);
    }

    [Fact]
    public async Task Apicurio_profile_is_explicitly_unsupported_and_performs_no_provider_io()
    {
        var handler = new StubHandler(_ =>
            throw new InvalidOperationException(
                "Unsupported provider profile must not dispatch Confluent-compatible HTTP."));

        using var adapter = CreateAdapter(
            handler,
            SchemaRegistryProviderProfile.ApicurioV3);

        var capabilities = await adapter.GetCapabilitiesAsync(
            "cluster-a",
            Operation(),
            CancellationToken.None);

        Assert.True(capabilities.IsSuccess);
        Assert.Equal(
            nameof(SchemaRegistryProviderProfile.ApicurioV3),
            capabilities.Value!.ProviderProfile);
        Assert.False(capabilities.Value.SupportsRegistration);
        Assert.False(capabilities.Value.SupportsCompatibilityMutation);
        Assert.Equal(
            "schema_registry_apicurio_adapter_not_admitted",
            capabilities.Value.LimitationCode);

        var compatibility = await adapter.TestCompatibilityAsync(
            "cluster-a",
            new SchemaCompatibilityCheckRequest(
                "orders-value",
                RecordSchemaFormat.JsonSchema,
                """{"type":"object"}""",
                Array.Empty<RecordSchemaReference>()),
            Operation(),
            CancellationToken.None);

        Assert.False(compatibility.IsSuccess);
        Assert.Equal(
            SchemaMutationObservationFailureCategory.Unsupported,
            compatibility.Failure!.Category);

        var mutation = await adapter.CreateAsync(CreateMutation());

        Assert.Equal(
            MutationExecutionResultKind.FailedBeforeDispatch,
            mutation.ResultKind);
        Assert.Equal(0, handler.CallCount);
    }

    [Fact]
    public async Task Delete_readiness_distinguishes_soft_deleted_subject_version_sets()
    {
        var handler = new StubHandler(request =>
            request.RequestUri!.PathAndQuery switch
            {
                "/subjects/orders-value/versions" =>
                    Json(HttpStatusCode.NotFound, "{}"),
                "/subjects/orders-value/versions?deleted=true" =>
                    Json(HttpStatusCode.OK, "[1,2,3]"),
                _ => Json(HttpStatusCode.NotFound, "{}"),
            });

        using var adapter = CreateAdapter(handler);

        var result = await adapter.ObserveDeleteTargetAsync(
            "cluster-a",
            new SchemaDeleteObservationRequest(
                "orders-value",
                null),
            Operation(),
            CancellationToken.None);

        Assert.True(result.IsSuccess, result.Failure?.SafeMessage);
        Assert.False(result.Value!.ExistsActive);
        Assert.True(result.Value.ExistsIncludingDeleted);
        Assert.True(result.Value.IsSoftDeleted);
        Assert.Empty(result.Value.ActiveVersions);
        Assert.Equal(
            new[] { 1, 2, 3 },
            result.Value.VersionsIncludingDeleted);
    }

    [Fact]
    public async Task Mutation_http_failures_preserve_ambiguous_post_dispatch_semantics()
    {
        var unavailable = new StubHandler(_ =>
            Json(HttpStatusCode.InternalServerError, """{"message":"secret-provider-detail"}"""));
        using var unavailableAdapter = CreateAdapter(unavailable);

        var unknown = await unavailableAdapter.CreateAsync(
            CreateMutation());

        Assert.Equal(
            MutationExecutionResultKind.ExecutionUnknown,
            unknown.ResultKind);
        Assert.DoesNotContain(
            "secret-provider-detail",
            unknown.ResultCode,
            StringComparison.Ordinal);

        var rejected = new StubHandler(_ =>
            Json(HttpStatusCode.Conflict, """{"message":"private-validation-detail"}"""));
        using var rejectedAdapter = CreateAdapter(rejected);

        var definitive = await rejectedAdapter.CreateAsync(
            CreateMutation());

        Assert.Equal(
            MutationExecutionResultKind.FailedDefinitive,
            definitive.ResultKind);
        Assert.DoesNotContain(
            "private-validation-detail",
            definitive.ResultCode,
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task Observation_redirects_and_authorization_are_fail_closed()
    {
        var redirect = new StubHandler(_ =>
        {
            var response =
                new HttpResponseMessage(HttpStatusCode.Found);
            response.Headers.Location =
                new Uri("https://attacker.example/steal");
            return response;
        });
        using var redirectAdapter = CreateAdapter(redirect);

        var redirected = await redirectAdapter.TestCompatibilityAsync(
            "cluster-a",
            new SchemaCompatibilityCheckRequest(
                "orders-value",
                RecordSchemaFormat.JsonSchema,
                """{"type":"object"}""",
                Array.Empty<RecordSchemaReference>()),
            Operation(),
            CancellationToken.None);

        Assert.False(redirected.IsSuccess);
        Assert.Equal(
            SchemaMutationObservationFailureCategory.InvalidResponse,
            redirected.Failure!.Category);
        Assert.Equal(1, redirect.CallCount);

        var denied = new StubHandler(_ =>
            Json(HttpStatusCode.Forbidden, """{"message":"secret-detail"}"""));
        using var deniedAdapter = CreateAdapter(denied);

        var deniedResult = await deniedAdapter.TestCompatibilityAsync(
            "cluster-a",
            new SchemaCompatibilityCheckRequest(
                "orders-value",
                RecordSchemaFormat.JsonSchema,
                """{"type":"object"}""",
                Array.Empty<RecordSchemaReference>()),
            Operation(),
            CancellationToken.None);

        Assert.False(deniedResult.IsSuccess);
        Assert.Equal(
            SchemaMutationObservationFailureCategory.Unauthorized,
            deniedResult.Failure!.Category);
        Assert.DoesNotContain(
            "secret-detail",
            deniedResult.Failure.SafeMessage,
            StringComparison.Ordinal);
    }

    private static SchemaCreateMutation CreateMutation() =>
        new(
            "cluster-a",
            "orders-value",
            RecordSchemaFormat.JsonSchema,
            """{"type":"object"}""",
            Array.Empty<SchemaMutationReference>());

    private static ConfluentSchemaMutationAdapter CreateAdapter(
        HttpMessageHandler handler,
        SchemaRegistryProviderProfile providerProfile =
            SchemaRegistryProviderProfile.ConfluentCompatibleV1) =>
        new(
            [
                new ClusterProfile(
                    "cluster-a",
                    ["localhost:9092"],
                    KafkaSecurityProtocol.Plaintext,
                    null,
                    null,
                    new SchemaRegistryProfile(
                        "https://registry.example/",
                        null,
                        null,
                        providerProfile)),
            ],
            new SecretResolver(),
            _ => handler);

    private static ReadViewOperationContext Operation() =>
        new(
            DateTimeOffset.UtcNow.AddSeconds(10),
            100,
            1024 * 1024);

    private static HttpResponseMessage Json(
        HttpStatusCode status,
        string body) =>
        new(status)
        {
            Content = new StringContent(
                body,
                Encoding.UTF8,
                "application/json"),
        };

    private sealed class StubHandler : HttpMessageHandler
    {
        private readonly Func<
            HttpRequestMessage,
            HttpResponseMessage> _response;

        public StubHandler(
            Func<HttpRequestMessage, HttpResponseMessage> response)
        {
            _response = response;
        }

        public int CallCount { get; private set; }
        public List<string> Requests { get; } = [];
        public List<string> Bodies { get; } = [];

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            CallCount++;
            Requests.Add(
                $"{request.Method.Method} {request.RequestUri!.PathAndQuery}");

            if (request.Content is not null)
            {
                Bodies.Add(
                    await request.Content.ReadAsStringAsync(
                        cancellationToken));
            }

            return _response(request);
        }
    }
}
