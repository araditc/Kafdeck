using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;
using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Api;

/// <summary>Closed named mappings sharing typed planners with browser endpoints. No caller-selected handler or kind.</summary>
internal sealed record BearerFamilyRoute(string OriginalName, MutationOperationKind Kind,
    AuthorizationAction Action, Type? RequestType, bool Execution = false, bool Read = false, string Method = "post", Type? ResponseType = null)
{
    public string Name => "v08-bearer-" + OriginalName;
    public string Path => BearerFamilyRoutes.Path(true, OriginalName switch
    {
        "v05-record-production-preview" => "/api/v1/clusters/{clusterId}/topics/{topicName}/mutations/produce/preview",
        "v05-record-production-execute" => "/api/v1/mutations/{operationId}/record-production/execute",
        "v05-records-purge-preview" => "/api/v1/clusters/{clusterId}/records/purge/preview",
        "v05-consumer-offset-alter-preview" => "/api/v1/clusters/{clusterId}/consumer-groups/{groupId}/mutations/offsets/preview",
        "v05-consumer-delete-preview" => "/api/v1/clusters/{clusterId}/consumer-groups/{groupId}/mutations/delete/preview",
        "v05-schema-register-preview" => "/api/v1/clusters/{clusterId}/schemas/subjects/{subject}/mutations/register/preview",
        "v05-schema-subject-compatibility-preview" => "/api/v1/clusters/{clusterId}/schemas/subjects/{subject}/mutations/compatibility/preview",
        "v05-schema-global-compatibility-preview" => "/api/v1/clusters/{clusterId}/schemas/mutations/compatibility/preview",
        "v05-schema-delete-preview" => "/api/v1/clusters/{clusterId}/schemas/subjects/{subject}/mutations/delete/preview",
        "v05-schema-create-execute" => "/api/v1/mutations/{operationId}/schema-create/execute",
        "v05-connect-create-preview" => "/api/v1/clusters/{clusterId}/connect/connectors/{connectorName}/mutations/create/preview",
        "v05-connect-update-preview" => "/api/v1/clusters/{clusterId}/connect/connectors/{connectorName}/mutations/update/preview",
        "v05-connect-control-preview" => "/api/v1/clusters/{clusterId}/connect/connectors/{connectorName}/mutations/control/preview",
        "v05-connect-delete-preview" => "/api/v1/clusters/{clusterId}/connect/connectors/{connectorName}/mutations/delete/preview",
        "v07-connect-profile-create-preview" => "/api/v1/clusters/{clusterId}/connect/profiles/{connectProfileId}/connectors/{connectorName}/mutations/create/preview",
        "v07-connect-profile-update-preview" => "/api/v1/clusters/{clusterId}/connect/profiles/{connectProfileId}/connectors/{connectorName}/mutations/update/preview",
        "v07-connect-profile-control-preview" => "/api/v1/clusters/{clusterId}/connect/profiles/{connectProfileId}/connectors/{connectorName}/mutations/control/preview",
        "v07-connect-profile-delete-preview" => "/api/v1/clusters/{clusterId}/connect/profiles/{connectProfileId}/connectors/{connectorName}/mutations/delete/preview",
        "v05-connect-configuration-execute" => "/api/v1/mutations/{operationId}/connect-configuration/execute",
        "v05-connect-control-delete-execute" => "/api/v1/mutations/{operationId}/connect/execute",
        "v07-data-job-replay-preview" => "/api/v1/data-jobs/replay/preview",
        "v07-data-job-forward-preview" => "/api/v1/data-jobs/forward/preview",
        "v07-data-job-reprocess-preview" => "/api/v1/data-jobs/reprocess/preview",
        "v07-data-job-dlq-forward-preview" => "/api/v1/data-jobs/dlq-forward/preview",
        "v07-data-job-start" => "/api/v1/data-jobs/{operationId}/start",
        "v07-data-job-status" => "/api/v1/data-jobs/{operationId}",
        "v07-data-job-cancel" => "/api/v1/data-jobs/{operationId}/cancel",
        "v07-data-job-reconcile" => "/api/v1/data-jobs/{operationId}/reconcile",
        "v08-data-job-reconcile-preview" => "/api/v1/data-jobs/{operationId}/reconcile/preview",
        "v07-data-generator-preview" => "/api/v1/data-jobs/generator/preview",
        "v07-data-generator-start" => "/api/v1/data-jobs/generator/{operationId}/start",
        "v07-data-generator-status" => "/api/v1/data-jobs/generator/{operationId}",
        "v07-data-generator-cancel" => "/api/v1/data-jobs/generator/{operationId}/cancel",
        "v07-data-generator-reconcile" => "/api/v1/data-jobs/generator/{operationId}/reconcile",
        "v08-data-generator-reconcile-preview" => "/api/v1/data-jobs/generator/{operationId}/reconcile/preview",
        "v07-connect-auto-restart-policy-read" or "v07-connect-auto-restart-policy-apply" => "/api/v1/clusters/{clusterId}/connect/profiles/{connectProfileId}/connectors/{connectorName}/restart-policy",
        "v07-connect-auto-restart-policy-preview" => "/api/v1/clusters/{clusterId}/connect/profiles/{connectProfileId}/connectors/{connectorName}/restart-policy/preview",
        _ => throw new InvalidOperationException("No admitted family path."),
    });
    public bool AcceptsKind(MutationOperationKind kind) => OriginalName switch
    {
        "v05-connect-configuration-execute" => kind is MutationOperationKind.ConnectCreate or MutationOperationKind.ConnectAlter,
        "v05-connect-control-delete-execute" => kind is MutationOperationKind.ConnectAlter or MutationOperationKind.ConnectDelete,
        _ => kind == Kind,
    };
}

internal static class BearerFamilyRoutes
{
    internal static readonly IReadOnlyList<BearerFamilyRoute> Contracts = Array.AsReadOnly(new[]
    {
        new BearerFamilyRoute("v05-record-production-preview", MutationOperationKind.RecordProduce, AuthorizationAction.RecordProduce, typeof(RecordProductionPreviewRequest)),
        new BearerFamilyRoute("v05-record-production-execute", MutationOperationKind.RecordProduce, AuthorizationAction.RecordProduce, typeof(RecordProductionExecuteRequest), true),
        new BearerFamilyRoute("v05-records-purge-preview", MutationOperationKind.RecordsPurge, AuthorizationAction.RecordsPurge, typeof(RecordsPurgePreviewRequest)),
        new BearerFamilyRoute("v05-consumer-offset-alter-preview", MutationOperationKind.ConsumerOffsetAlter, AuthorizationAction.ConsumerOffsetAlter, typeof(ConsumerOffsetAlterPreviewRequest)),
        new BearerFamilyRoute("v05-consumer-delete-preview", MutationOperationKind.ConsumerDelete, AuthorizationAction.ConsumerDelete, typeof(ConsumerDeletePreviewRequest)),
        new BearerFamilyRoute("v05-schema-register-preview", MutationOperationKind.SchemaCreate, AuthorizationAction.SchemaCreate, typeof(SchemaRegistrationPreviewRequest)),
        new BearerFamilyRoute("v05-schema-subject-compatibility-preview", MutationOperationKind.SchemaAlter, AuthorizationAction.SchemaAlter, typeof(SchemaCompatibilityPreviewRequest)),
        new BearerFamilyRoute("v05-schema-global-compatibility-preview", MutationOperationKind.SchemaAlter, AuthorizationAction.SchemaAlter, typeof(SchemaCompatibilityPreviewRequest)),
        new BearerFamilyRoute("v05-schema-delete-preview", MutationOperationKind.SchemaDelete, AuthorizationAction.SchemaDelete, typeof(SchemaDeletePreviewRequest)),
        new BearerFamilyRoute("v05-schema-create-execute", MutationOperationKind.SchemaCreate, AuthorizationAction.SchemaCreate, typeof(SchemaCreateExecuteRequest), true),
        new BearerFamilyRoute("v05-connect-create-preview", MutationOperationKind.ConnectCreate, AuthorizationAction.ConnectCreate, typeof(ConnectConfigurationPreviewRequest)),
        new BearerFamilyRoute("v05-connect-update-preview", MutationOperationKind.ConnectAlter, AuthorizationAction.ConnectAlter, typeof(ConnectConfigurationPreviewRequest)),
        new BearerFamilyRoute("v05-connect-control-preview", MutationOperationKind.ConnectAlter, AuthorizationAction.ConnectAlter, typeof(ConnectControlPreviewRequest)),
        new BearerFamilyRoute("v05-connect-delete-preview", MutationOperationKind.ConnectDelete, AuthorizationAction.ConnectDelete, null),
        new BearerFamilyRoute("v07-connect-profile-create-preview", MutationOperationKind.ConnectCreate, AuthorizationAction.ConnectCreate, typeof(ConnectConfigurationPreviewRequest)),
        new BearerFamilyRoute("v07-connect-profile-update-preview", MutationOperationKind.ConnectAlter, AuthorizationAction.ConnectAlter, typeof(ConnectConfigurationPreviewRequest)),
        new BearerFamilyRoute("v07-connect-profile-control-preview", MutationOperationKind.ConnectAlter, AuthorizationAction.ConnectAlter, typeof(ConnectControlPreviewRequest)),
        new BearerFamilyRoute("v07-connect-profile-delete-preview", MutationOperationKind.ConnectDelete, AuthorizationAction.ConnectDelete, null),
        new BearerFamilyRoute("v05-connect-configuration-execute", MutationOperationKind.ConnectAlter, AuthorizationAction.ConnectAlter, typeof(ConnectConfigurationExecuteRequest), true),
        new BearerFamilyRoute("v05-connect-control-delete-execute", MutationOperationKind.ConnectDelete, AuthorizationAction.ConnectDelete, null, true),
        new BearerFamilyRoute("v07-data-job-replay-preview", MutationOperationKind.DataJob, AuthorizationAction.DataJobPlan, typeof(DataJobPreviewRequest)),
        new BearerFamilyRoute("v07-data-job-forward-preview", MutationOperationKind.DataJob, AuthorizationAction.DataJobPlan, typeof(DataJobPreviewRequest)),
        new BearerFamilyRoute("v07-data-job-reprocess-preview", MutationOperationKind.DataJob, AuthorizationAction.DataJobPlan, typeof(DataJobPreviewRequest)),
        new BearerFamilyRoute("v07-data-job-dlq-forward-preview", MutationOperationKind.DataJob, AuthorizationAction.DataJobPlan, typeof(DataJobPreviewRequest)),
        new BearerFamilyRoute("v07-data-job-start", MutationOperationKind.DataJob, AuthorizationAction.DataJobExecute, null, true),
        new BearerFamilyRoute("v07-data-job-status", MutationOperationKind.DataJob, AuthorizationAction.DataJobExecute, null, true, true, "get", typeof(DataJobStatusData)),
        new BearerFamilyRoute("v07-data-job-cancel", MutationOperationKind.DataJob, AuthorizationAction.DataJobExecute, null, true),
        new BearerFamilyRoute("v07-data-job-reconcile", MutationOperationKind.DataJob, AuthorizationAction.MutationReconcile, typeof(DataJobReconcileRequest), true),
        new BearerFamilyRoute("v08-data-job-reconcile-preview", MutationOperationKind.DataJob, AuthorizationAction.MutationReconcile, typeof(JobReconciliationPreviewRequest)),
        new BearerFamilyRoute("v07-data-generator-preview", MutationOperationKind.DataGenerator, AuthorizationAction.DataGeneratorPlan, typeof(DataGeneratorPreviewRequest)),
        new BearerFamilyRoute("v07-data-generator-start", MutationOperationKind.DataGenerator, AuthorizationAction.DataGeneratorExecute, null, true),
        new BearerFamilyRoute("v07-data-generator-status", MutationOperationKind.DataGenerator, AuthorizationAction.DataGeneratorExecute, null, true, true, "get", typeof(DataGeneratorStatusData)),
        new BearerFamilyRoute("v07-data-generator-cancel", MutationOperationKind.DataGenerator, AuthorizationAction.DataGeneratorExecute, null, true),
        new BearerFamilyRoute("v07-data-generator-reconcile", MutationOperationKind.DataGenerator, AuthorizationAction.MutationReconcile, typeof(DataGeneratorReconcileRequest), true),
        new BearerFamilyRoute("v08-data-generator-reconcile-preview", MutationOperationKind.DataGenerator, AuthorizationAction.MutationReconcile, typeof(JobReconciliationPreviewRequest)),
        new BearerFamilyRoute("v07-connect-auto-restart-policy-preview", MutationOperationKind.ConnectAutoRestartPolicy, AuthorizationAction.ConnectAutoRestartManage, typeof(ConnectAutoRestartPolicyPreviewRequest), Method: "put"),
        new BearerFamilyRoute("v07-connect-auto-restart-policy-apply", MutationOperationKind.ConnectAutoRestartPolicy, AuthorizationAction.ConnectAutoRestartManage, typeof(ConnectAutoRestartPolicyApplyRequest), true, Method: "put"),
        new BearerFamilyRoute("v07-connect-auto-restart-policy-read", MutationOperationKind.ConnectAutoRestartPolicy, AuthorizationAction.ConnectRead, null, Read: true, Method: "get", ResponseType: typeof(ConnectAutoRestartPolicyStatusData)),
    });

    public static bool IsAction(AuthorizationAction action) => BearerTopicMutationScope.IsAction(action) ||
        action is AuthorizationAction.DataJobPlan or AuthorizationAction.DataJobExecute or AuthorizationAction.DataGeneratorPlan or
            AuthorizationAction.DataGeneratorExecute or AuthorizationAction.ConnectRestart or AuthorizationAction.MutationReconcile ||
        Contracts.Any(c => !c.Read && c.Action == action);
    public static bool IsKind(MutationOperationKind kind) => kind == MutationOperationKind.FleetUncertaintyDisposition || BearerTopicMutationScope.IsKind(kind) || Contracts.Any(c => c.Kind == kind);
    public static string Path(bool bearer, string original) => bearer ? original.Replace("/api/v1/", "/api/v1/automation/", StringComparison.Ordinal) : original;
    public static string Name(bool bearer, string original) => bearer ? Contract(original).Name : original;
    private static BearerFamilyRoute Contract(string original) => Contracts.Single(c => c.OriginalName == original);

    public static RouteHandlerBuilder RequireFamilyBoundary(this RouteHandlerBuilder builder, bool bearer, string original)
    {
        var contract = Contract(original);
        if (!bearer)
        {
            if (original is "v07-data-job-reconcile" or "v07-data-generator-reconcile")
                builder.AddEndpointFilter((_, _) => ValueTask.FromResult<object?>(Results.Problem(statusCode: 409,
                    type: "urn:kafdeck:problem:reconciliation-proof-required", title: "Use the governed reconciliation preview and independent approval.")));
            return contract.Read ? builder : builder.RequireKafdeckAntiforgery();
        }
        builder.WithMetadata(contract);
        return builder.AddEndpointFilter(async (invocation, next) =>
        {
            var context = invocation.HttpContext;
            if (context.User.Identity is not NonBrowserReadIdentity identity ||
                !(contract.Read && !IsAction(contract.Action)
                    ? context.RequestServices.GetRequiredService<NonBrowserReadRegistry>().IsRegisteredReadAllowed(identity, contract.Action)
                    : contract.Execution
                    ? context.RequestServices.GetRequiredService<NonBrowserReadRegistry>().IsAnyGovernedTopicMutationAllowed(identity)
                    : context.RequestServices.GetRequiredService<NonBrowserReadRegistry>().IsGovernedMutationAllowed(identity, contract.Action)))
                return Results.Problem(statusCode: 401, title: "A live registered bearer family identity is required.");
            if (context.Request.RouteValues.TryGetValue("operationId", out var value))
            {
                if (!Guid.TryParse(value?.ToString(), out var id)) return Results.Problem(statusCode: 400, title: "Invalid mutation identity.");
                var operation = await context.RequestServices.GetRequiredService<IMutationOperationRepository>().GetAsync(id, context.RequestAborted);
                if (operation is null) return Results.Problem(statusCode: 404, title: "Mutation not found.");
                if (!MutationActor.TryCreate(context.User, out var actor) || actor is null || actor.PrincipalId != operation.RequesterPrincipalId ||
                    !contract.AcceptsKind(operation.OperationKind) || context.RequestServices.GetRequiredService<MutationRequestAuthorizationService>()
                        .AuthorizeForDispatch(context.User, operation) != KafdeckAuthorizationOutcome.Allowed)
                    return Results.Problem(statusCode: 403, title: "Mutation access denied.");
            }
            if (original is "v07-data-job-reconcile" or "v07-data-generator-reconcile")
                return Results.Problem(statusCode: 501, type: "urn:kafdeck:problem:reconciliation-proof-required",
                    title: "Governed durable reconciliation proof and independent review are required.");
            var preflight = BearerPlanningPreflight.Authorize(invocation);
            if (preflight is not null) return preflight;
            context.Response.OnStarting(() =>
            {
                var location = context.Response.Headers.Location.ToString();
                if (location.StartsWith("/api/v1/mutations/", StringComparison.Ordinal))
                    context.Response.Headers.Location = location.Replace("/api/v1/mutations/", "/api/v1/automation/mutations/", StringComparison.Ordinal) + "/status";
                else if (location.StartsWith("/api/v1/data-jobs/", StringComparison.Ordinal))
                    context.Response.Headers.Location = location.Replace("/api/v1/data-jobs/", "/api/v1/automation/data-jobs/", StringComparison.Ordinal);
                return Task.CompletedTask;
            });
            return await next(invocation);
        });
    }

    /// <summary>Before binding: bounded, duplicate-free and closed recursively against the exact named DTO.</summary>
    public static Task<bool> ValidateBodyAsync(HttpRequest request, BearerFamilyRoute contract, CancellationToken token,
        TimeSpan? bodyDeadline = null) => ValidateBodyAsync(request, contract.RequestType, token, bodyDeadline);

    public static async Task<bool> ValidateBodyAsync(HttpRequest request, Type? requestType, CancellationToken token,
        TimeSpan? bodyDeadline = null)
    {
        if (requestType is null)
            return request.HttpContext.Features.Get<Microsoft.AspNetCore.Http.Features.IHttpRequestBodyDetectionFeature>()?.CanHaveBody != true;
        request.EnableBuffering(64 * 1024, 64 * 1024);
        var timeout = bodyDeadline ?? TimeSpan.FromSeconds(10);
        if (timeout <= TimeSpan.Zero || timeout > TimeSpan.FromSeconds(10)) throw new ArgumentOutOfRangeException(nameof(bodyDeadline));
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
        deadline.CancelAfter(timeout);
        try
        {
            using var document = await JsonDocument.ParseAsync(request.Body, new JsonDocumentOptions { MaxDepth = 16 }, deadline.Token);
            return IsClosed(document.RootElement, requestType);
        }
        catch (Exception ex) when (ex is JsonException or IOException or BadHttpRequestException) { return false; }
        catch (OperationCanceledException) when (!token.IsCancellationRequested) { return false; }
        finally { request.Body.Position = 0; }
    }

    internal static bool IsClosed(JsonElement value, Type type)
    {
        type = Nullable.GetUnderlyingType(type) ?? type;
        if (value.ValueKind == JsonValueKind.Null) return true; // Typed handlers retain semantic null validation.
        if (type == typeof(JsonElement)) return IsDuplicateFreeJson(value);
        if (type == typeof(string) || type == typeof(byte[]) || type.IsPrimitive || type.IsEnum || type == typeof(decimal) ||
            type == typeof(DateTimeOffset) || type == typeof(DateTime) || type == typeof(Guid) || type == typeof(Uri))
            return value.ValueKind is not (JsonValueKind.Array or JsonValueKind.Object);
        var dictionary = type.GetInterfaces().Append(type).FirstOrDefault(t => t.IsGenericType &&
            t.GetGenericTypeDefinition() == typeof(IReadOnlyDictionary<,>));
        if (value.ValueKind == JsonValueKind.Object)
        {
            var seen = new HashSet<string>(dictionary is null ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal);
            var properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public);
            foreach (var property in value.EnumerateObject())
            {
                if (!seen.Add(property.Name)) return false;
                var member = properties.SingleOrDefault(p => string.Equals(p.GetCustomAttribute<JsonPropertyNameAttribute>()?.Name ?? p.Name,
                    property.Name, StringComparison.OrdinalIgnoreCase));
                if (dictionary is null && member is null) return false;
                if (!IsClosed(property.Value, dictionary?.GetGenericArguments()[1] ?? member!.PropertyType)) return false;
            }
            return true;
        }
        var sequence = type.GetInterfaces().Append(type).FirstOrDefault(t => t.IsGenericType && t.GetGenericTypeDefinition() == typeof(IEnumerable<>));
        return value.ValueKind == JsonValueKind.Array && sequence is not null && value.EnumerateArray().All(e => IsClosed(e, sequence.GetGenericArguments()[0]));
    }

    private static bool IsDuplicateFreeJson(JsonElement value)
    {
        if (value.ValueKind == JsonValueKind.Array) return value.EnumerateArray().All(IsDuplicateFreeJson);
        if (value.ValueKind != JsonValueKind.Object) return true;
        var seen = new HashSet<string>(StringComparer.Ordinal);
        return value.EnumerateObject().All(property => seen.Add(property.Name) && IsDuplicateFreeJson(property.Value));
    }
}
