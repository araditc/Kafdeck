using System.Text.Json;
using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Api;

/// <summary>Bounded capability metadata. Reading it never admits or executes a mutation.</summary>
public static class KafdeckBearerCapabilities
{
    public static WebApplication MapKafdeckBearerCapabilities(this WebApplication app)
    {
        app.MapGet("/api/v1/automation/capabilities/mutations", () => Results.Ok(CreateInventory()))
            .WithName("v08-bearer-mutation-capabilities")
            .RequireKafdeckAuthorization(AuthorizationAction.SystemRead);
        return app;
    }

    public static BearerMutationCapabilityInventory CreateInventory() => new(1, "supported-ui",
        Enum.GetValues<MutationOperationKind>().Select(CreateFamily).ToArray());

    private static BearerMutationFamilyCapability CreateFamily(MutationOperationKind kind)
    {
        var inheritedId = kind switch
        {
            MutationOperationKind.AclAlter => "acl-administration",
            MutationOperationKind.ScramAlter => "scram-administration",
            MutationOperationKind.QuotaAlter => "client-quotas",
            MutationOperationKind.ClusterConfigAlter => "dynamic-configuration",
            MutationOperationKind.PreferredLeaderElection => "preferred-leader-election",
            MutationOperationKind.PartitionReassign => "partition-reassignment",
            MutationOperationKind.ReplicationFactorAlter => "replication-factor-change",
            MutationOperationKind.ReassignmentThrottle => "reassignment-throttles",
            MutationOperationKind.BrokerMaintenance => "broker-maintenance",
            MutationOperationKind.LogDirectoryMaintenance => "log-directory-maintenance",
            MutationOperationKind.ClusterTransfer => "finite-cluster-transfer",
            MutationOperationKind.ReplicationIntegration => "managed-replication",
            _ => null,
        };
        var inherited = inheritedId is null ? null : FleetCapabilityCatalog.Require(inheritedId);
        var topic = BearerTopicMutationScope.IsKind(kind);
        var reconciliation = kind == MutationOperationKind.FleetUncertaintyDisposition;
        static bool ReconciliationPreview(BearerFamilyRoute c) => c.OriginalName is
            "v08-data-job-reconcile-preview" or "v08-data-generator-reconcile-preview";
        var contracts = BearerFamilyRoutes.Contracts.Where(c => !c.Read &&
            (reconciliation ? ReconciliationPreview(c) : c.AcceptsKind(kind) && !ReconciliationPreview(c))).ToArray();
        var previews = contracts.Where(c => !c.Execution).Select(c => c.Name).ToList();
        if (topic) previews.Add(kind switch
        {
            MutationOperationKind.TopicCreate => "v08-bearer-topic-create-preview",
            MutationOperationKind.TopicAlter => "v08-bearer-topic-alter-preview",
            MutationOperationKind.TopicIncreasePartitions => "v08-bearer-topic-increase-partitions-preview",
            _ => "v08-bearer-topic-delete-preview",
        });
        var hosted = previews.Count > 0;
        var executions = contracts.Where(c => c.Execution && !c.OriginalName.EndsWith("-reconcile", StringComparison.Ordinal))
            .Select(c => c.Name).ToList();
        if (hosted && executions.Count == 0) executions.Add("v08-bearer-mutation-execute");
        var unsupported = !hosted && inherited?.State == FleetCapabilityState.Unsupported;
        var guardedProtocol = kind == MutationOperationKind.FleetUncertaintyDisposition;
        return new(JsonNamingPolicy.CamelCase.ConvertName(kind.ToString()),
            inherited is not null ? JsonNamingPolicy.CamelCase.ConvertName(inherited.State.ToString()) : hosted ? "supported" : "blocked",
            inherited is not null ? inherited.Evidence : new[] { kind == MutationOperationKind.FleetUncertaintyDisposition
                ? "JobReconciliationService and immutable JobReconciliationPlan require current unchanged destination evidence, worker fencing and distinct CRITICAL approval. Physical acceptance is separate."
                : "BearerFamilyRoutes and the corresponding typed browser mutation endpoints." },
            hosted, hosted ? "namedRoutesAvailable" : unsupported ? "inheritedUnsupported" : "notHosted",
            hosted ? "probeRequired" : unsupported ? "inheritedUnsupported" : guardedProtocol ? "guardedProtocolRequired" : "notAdmitted",
            hosted ? "current-provider-probe-required" : inheritedId ?? "public-handler-not-admitted",
            previews.Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToArray(),
            executions.Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToArray(), false);
    }
}

public sealed record BearerMutationCapabilityInventory(int Version, string InventoryBasis,
    IReadOnlyList<BearerMutationFamilyCapability> Families);
public sealed record BearerMutationFamilyCapability(string Kind, string UiStatus, IReadOnlyList<string> SourceEvidence,
    bool HandlerAvailable, string ApiContractStatus, string ProviderCapability, string ReasonCode,
    IReadOnlyList<string> NamedPreviewRoutes, IReadOnlyList<string> MaterialExecuteRoutes, bool AuthorityFromCapability);
