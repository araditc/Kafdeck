using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;
using Microsoft.AspNetCore.Mvc;

namespace Kafdeck.Api;

public static class KafdeckBrokerConfigEndpoints
{
    private const string Root = "/api/v1/clusters/{clusterId}/broker-config";
    private const KafkaCompatibilityLine Policy = KafkaCompatibilityLine.RuntimeBrokerConfig;
    private const string Key = DynamicConfigurationRegistry.FleetKey;

    public static WebApplication MapKafdeckBrokerConfigEndpoints(this WebApplication app)
    {
        app.MapGet(Root + "/settings", (DynamicConfigurationRegistry registry) =>
        {
            var setting = registry.Require(Policy, Key);
            return Results.Ok(new[]
            {
                new BrokerConfigSetting(setting.Key, setting.MinValue, setting.MaxValue,
                    [DynamicConfigurationScope.BrokerOverride]),
            });
        }).WithName("v11-broker-config-settings")
          .RequireKafdeckCollectionAuthorization(AuthorizationAction.ClusterRead, "clusterId");

        app.MapPost(Root + "/preview", PreviewAsync)
            .WithName("v11-broker-config-preview")
            .WithMetadata(new RequestSizeLimitAttribute(4096))
            .RequireKafdeckAntiforgery();
        return app;
    }

    private static async Task<IResult> PreviewAsync(string clusterId, BrokerConfigPreviewRequest input, HttpContext context,
        DynamicConfigurationPlanner planner, MutationRequestAuthorizationService authorization, IMutationOperationRepository repository,
        MutationAdmissionService admission, ISecurityAuditSink audit, CancellationToken token)
    {
        DynamicConfigurationTarget target;
        if (input.Key != Key || input.Scope != DynamicConfigurationScope.BrokerOverride ||
            (input.Command, input.Value, input.RestoreOperationId) is not
                (("set", not null, null) or ("inherit", null, null) or ("restore", null, not null)))
            return Problem(400, "invalid-broker-config-preview", "Choose a supported setting and a valid change.");

        try { target = DynamicConfigurationPolicy.NormalizeTarget(new(clusterId, input.Scope, input.BrokerId, Key)); }
        catch (ArgumentException) { return Problem(400, "invalid-broker-config-target", "Choose a valid scope and broker."); }

        var resource = DynamicConfigurationPolicy.ConflictResource(target);
        var outcome = authorization.AuthorizeTargets(context.User,
            [new(AuthorizationAction.ClusterConfigRead, clusterId, resource),
             new(AuthorizationAction.ClusterConfigAlter, clusterId, resource)]);
        if (outcome != KafdeckAuthorizationOutcome.Allowed)
        {
            if (outcome == KafdeckAuthorizationOutcome.Forbidden)
            {
                var principal = OperatorSessionContextFactory.TryCreate(context.User, out var session) && session is not null
                    ? SecurityAuditPrincipal.FromOperator(session.Identity) : SecurityAuditPrincipal.Anonymous;
                await audit.WriteAsync(new(DateTimeOffset.UtcNow, SecurityAuditEventType.AuthorizationDenied, principal,
                    session?.SessionId.Value.ToString("N"), clusterId, resource, SecurityAuditOutcome.Denied,
                    "broker_config_preview_denied"), token).ConfigureAwait(false);
            }

            return Problem(outcome == KafdeckAuthorizationOutcome.Unauthenticated ? 401 : 403,
                "broker-config-preview-denied", "Configuration read and change permission are required.");
        }

        string? expected = null;
        var value = input.Value;
        if (input.RestoreOperationId is { } operationId)
        {
            var previous = await repository.GetAsync(operationId, token).ConfigureAwait(false);
            if (previous is null || previous.OperationKind != MutationOperationKind.ClusterConfigAlter ||
                previous.ClusterId != clusterId || previous.State != MutationOperationState.AppliedVerified ||
                !DynamicBrokerConfigExecutionHandler.IsAdmitted(previous))
                return Problem(404, "broker-config-restore-unavailable", "No verified change is available for this target.");

            var original = DynamicConfigurationPolicy.DeserializePlan(previous.CanonicalIntent);
            if (original.Mutation.Target != target || original.Mutation.Value is null ||
                !previous.SafeProviderEvidence.TryGetValue("configuration.fingerprint", out expected))
                return Problem(404, "broker-config-restore-unavailable", "No verified set operation is available for this target.");

            if (original.PreviousSource == DynamicConfigurationPolicy.ExpectedDynamicSource(target.Scope))
                value = original.PreviousValue;
        }

        var planned = value is null
            ? await planner.PlanResetAsync(Policy, target, token).ConfigureAwait(false)
            : await planner.PlanSetAsync(Policy, target, value, token).ConfigureAwait(false);
        if (!planned.IsSuccess)
        {
            var failure = planned.Failure!;
            var status = failure.Code switch
            {
                DynamicConfigurationPlanningFailureCode.ProviderUnauthorized => 403,
                DynamicConfigurationPlanningFailureCode.ProviderUnavailable or DynamicConfigurationPlanningFailureCode.ObservationFailed => 503,
                DynamicConfigurationPlanningFailureCode.ReadOnlyValue or DynamicConfigurationPlanningFailureCode.ProviderUnsupported or
                    DynamicConfigurationPlanningFailureCode.ResetSourceUnavailable => 409,
                _ => 400,
            };
            return Problem(status, "broker-config-preview-unavailable", failure.SafeMessage);
        }

        if (expected is not null && expected != planned.Plan!.ObservedFingerprint)
            return Problem(409, "broker-config-restore-stale", "The setting has changed since that operation. Preview the intended value against its current state.");

        return await KafdeckTopicMutationEndpoints.AdmitAsync(context, admission, planned.Intent!, planned.Risk!, token,
            responseData: Data).ConfigureAwait(false);
    }

    private static BrokerConfigPreviewData Data(MutationOperationSnapshot operation)
    {
        var plan = DynamicConfigurationPolicy.DeserializePlan(operation.CanonicalIntent);
        return new(MutationStatusData.From(operation), plan.Mutation.Target, plan.PreviousValue!, plan.PreviousSource!,
            plan.Mutation.Value ?? plan.InheritedValue!, plan.Mutation.Value is null
                ? plan.InheritedSource! : DynamicConfigurationPolicy.ExpectedDynamicSource(plan.Mutation.Target.Scope));
    }

    private static IResult Problem(int status, string code, string detail) =>
        Results.Problem(statusCode: status, type: "urn:kafdeck:problem:" + code, detail: detail);
}

public sealed record BrokerConfigSetting(string Key, long MinValue, long MaxValue, IReadOnlyList<DynamicConfigurationScope> Scopes);
public sealed record BrokerConfigPreviewRequest(string Key, DynamicConfigurationScope Scope, int? BrokerId,
    string Command, string? Value, Guid? RestoreOperationId);
public sealed record BrokerConfigPreviewData(MutationStatusData Operation, DynamicConfigurationTarget Target,
    string EffectiveValue, string EffectiveSource, string ExpectedValue, string ExpectedSource);
