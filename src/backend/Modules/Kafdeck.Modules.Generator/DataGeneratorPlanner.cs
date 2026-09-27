using Kafdeck.Core.Kafka;
using Kafdeck.Core.ReadViews;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Records;
using Kafdeck.Modules.Schemas;

namespace Kafdeck.Modules.Generator;

public sealed record DataGeneratorPlanningSourceRequest(
    DataGeneratorSourceKind Kind,
    string? SchemaSubject = null,
    int? SchemaVersion = null,
    DataGeneratorBuiltInTemplate? Template = null);

public sealed record DataGeneratorPlanningRequest(
    string DestinationClusterId,
    string DestinationProfileVersion,
    string DestinationTopic,
    int DestinationPartition,
    int RecordCount,
    int Seed,
    DataGeneratorBudget Budget,
    DataGeneratorPlanningSourceRequest Source);

public enum DataGeneratorPlanningFailureCode
{
    InvalidInput = 1,
    PolicyDenied = 2,
    DestinationUnauthorized = 3,
    DestinationUnavailable = 4,
    DestinationUnsupported = 5,
    DestinationInvalid = 6,
    SourceUnavailable = 7,
    SourceUnsupported = 8,
    SourceInvalid = 9,
}

public sealed record DataGeneratorPlanningFailure(
    DataGeneratorPlanningFailureCode Code,
    string SafeMessage,
    string? SourceCode = null);

public sealed record DataGeneratorPlanResult(
    DataGeneratorPlan? Plan,
    MutationIntentDescriptor? Intent,
    MutationRiskDecision? Risk,
    DataGeneratorPlanningFailure? Failure)
{
    public bool IsSuccess =>
        Plan is not null &&
        Intent is not null &&
        Risk is not null &&
        Failure is null;

    public static DataGeneratorPlanResult Success(
        DataGeneratorPlan plan,
        MutationIntentDescriptor intent,
        MutationRiskDecision risk) =>
        new(plan, intent, risk, null);

    public static DataGeneratorPlanResult Failed(
        DataGeneratorPlanningFailureCode code,
        string message,
        string? sourceCode = null) =>
        new(
            null,
            null,
            null,
            new DataGeneratorPlanningFailure(
                code,
                message,
                sourceCode));
}

public sealed record DataGeneratorPlannerPolicy(
    TimeSpan ObservationTimeout)
{
    public static DataGeneratorPlannerPolicy Default { get; } =
        new(TimeSpan.FromSeconds(10));
}

public sealed class DataGeneratorPlanner
{
    private readonly IKafkaAdministrationPort _kafka;
    private readonly SchemaExplorerService _schemas;
    private readonly SchemaDeveloperService _schemaTooling;
    private readonly DataGeneratorDeploymentPolicy _deployment;
    private readonly DataGeneratorPlannerPolicy _policy;
    private readonly TimeProvider _timeProvider;

    public DataGeneratorPlanner(
        IKafkaAdministrationPort kafka,
        SchemaExplorerService schemas,
        SchemaDeveloperService schemaTooling,
        DataGeneratorDeploymentPolicy deployment,
        DataGeneratorPlannerPolicy? policy = null,
        TimeProvider? timeProvider = null)
    {
        _kafka =
            kafka ??
            throw new ArgumentNullException(nameof(kafka));
        _schemas =
            schemas ??
            throw new ArgumentNullException(nameof(schemas));
        _schemaTooling =
            schemaTooling ??
            throw new ArgumentNullException(nameof(schemaTooling));
        _deployment =
            deployment ??
            throw new ArgumentNullException(nameof(deployment));
        _policy =
            policy ?? DataGeneratorPlannerPolicy.Default;
        _timeProvider =
            timeProvider ?? TimeProvider.System;

        if (_policy.ObservationTimeout < TimeSpan.FromSeconds(1) ||
            _policy.ObservationTimeout > TimeSpan.FromSeconds(30))
        {
            throw new ArgumentOutOfRangeException(nameof(policy));
        }
    }

    public async Task<DataGeneratorPlanResult> PlanAsync(
        DataGeneratorPlanningRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(request.Source);
        ArgumentNullException.ThrowIfNull(request.Budget);

        string clusterId;
        string profileVersion;
        string topicName;
        try
        {
            clusterId =
                ClusterTransferPolicy.RequireIdentifier(
                    request.DestinationClusterId,
                    "Generator destination cluster",
                    256);
            profileVersion =
                ClusterTransferPolicy.RequireIdentifier(
                    request.DestinationProfileVersion,
                    "Generator destination profile version",
                    256);
            topicName =
                ClusterTransferPolicy.RequireIdentifier(
                    request.DestinationTopic,
                    "Generator destination topic",
                    249);

            if (request.DestinationPartition < 0 ||
                request.RecordCount is < 1 or >
                    DataGeneratorBudget.HardMaxTotalRecords ||
                request.RecordCount >
                    request.Budget.MaxTotalRecords)
            {
                return DataGeneratorPlanResult.Failed(
                    DataGeneratorPlanningFailureCode.InvalidInput,
                    "Generator destination/count is outside the admitted finite bound.");
            }
        }
        catch (ArgumentException)
        {
            return DataGeneratorPlanResult.Failed(
                DataGeneratorPlanningFailureCode.InvalidInput,
                "Generator planning input is invalid.");
        }

        if (!_deployment.IsEnabled(clusterId))
        {
            return DataGeneratorPlanResult.Failed(
                DataGeneratorPlanningFailureCode.PolicyDenied,
                "Data generation is disabled by deployment policy for the requested destination cluster.");
        }

        var observation = new KafkaOperationContext(
            _timeProvider.GetUtcNow().Add(
                _policy.ObservationTimeout));

        var cluster =
            await _kafka.GetClusterMetadataAsync(
                    clusterId,
                    observation,
                    cancellationToken)
                .ConfigureAwait(false);

        if (!cluster.IsSuccess ||
            cluster.Value is null)
        {
            return FromKafkaFailure(
                cluster.Failure,
                "destination cluster");
        }

        if (string.IsNullOrWhiteSpace(
                cluster.Value.KafkaClusterId))
        {
            return DataGeneratorPlanResult.Failed(
                DataGeneratorPlanningFailureCode.DestinationInvalid,
                "Generator destination physical Kafka cluster identity is unavailable.");
        }

        var topic =
            await _kafka.GetTopicMetadataAsync(
                    clusterId,
                    topicName,
                    observation,
                    cancellationToken)
                .ConfigureAwait(false);

        if (!topic.IsSuccess ||
            topic.Value is null)
        {
            return FromKafkaFailure(
                topic.Failure,
                "destination topic");
        }

        if (topic.Value.IsInternal)
        {
            return DataGeneratorPlanResult.Failed(
                DataGeneratorPlanningFailureCode.DestinationInvalid,
                "Internal Kafka topics are not admitted generator destinations.");
        }

        if (!topic.Value.Partitions.Any(
                item =>
                    item.PartitionId ==
                    request.DestinationPartition))
        {
            return DataGeneratorPlanResult.Failed(
                DataGeneratorPlanningFailureCode.DestinationInvalid,
                "Generator destination partition does not exist.");
        }

        var source =
            await ResolveSourceAsync(
                    clusterId,
                    request.Source,
                    request.Seed,
                    cancellationToken)
                .ConfigureAwait(false);

        if (!source.IsSuccess ||
            source.Source is null)
        {
            return DataGeneratorPlanResult.Failed(
                source.Code,
                source.Message,
                source.SourceCode);
        }

        try
        {
            var destination =
                new DataGeneratorDestination(
                    clusterId,
                    profileVersion,
                    cluster.Value.KafkaClusterId!,
                    topicName,
                    request.DestinationPartition,
                    ClusterTransferPolicy.FingerprintTopic(
                        topic.Value));

            var plan =
                DataGeneratorPolicy.CreatePlan(
                    destination,
                    source.Source,
                    request.RecordCount,
                    request.Seed,
                    request.Budget);

            var intent =
                DataGeneratorPolicy.BuildIntent(plan);
            var risk =
                MutationRiskClassifier.EnforceBuiltInFloor(
                    new MutationRiskInput(
                        MutationOperationKind.DataGenerator,
                        TargetCount: 1),
                    DataGeneratorPolicy.ClassifyRisk(plan));

            return DataGeneratorPlanResult.Success(
                plan,
                intent,
                risk);
        }
        catch (Exception exception)
            when (exception is
                ArgumentException or
                MutationStateException or
                OverflowException)
        {
            return DataGeneratorPlanResult.Failed(
                DataGeneratorPlanningFailureCode.InvalidInput,
                "Generator canonical plan could not be created safely.");
        }
    }

    private async Task<SourceResult> ResolveSourceAsync(
        string clusterId,
        DataGeneratorPlanningSourceRequest request,
        int seed,
        CancellationToken cancellationToken)
    {
        if (!Enum.IsDefined(request.Kind))
        {
            return SourceResult.Failed(
                DataGeneratorPlanningFailureCode.SourceInvalid,
                "Generator source kind is invalid.");
        }

        switch (request.Kind)
        {
            case DataGeneratorSourceKind.Schema:
                if (string.IsNullOrWhiteSpace(
                        request.SchemaSubject) ||
                    request.SchemaVersion is null or <= 0 ||
                    request.Template is not null)
                {
                    return SourceResult.Failed(
                        DataGeneratorPlanningFailureCode.SourceInvalid,
                        "Schema generator source identity is invalid.");
                }

                var detail =
                    await _schemas.GetVersionAsync(
                            clusterId,
                            request.SchemaSubject!,
                            request.SchemaVersion.Value,
                            cancellationToken)
                        .ConfigureAwait(false);

                if (!detail.IsSuccess ||
                    detail.Value is null)
                {
                    return FromSchemaFailure(
                        detail.Failure);
                }

                var proof =
                    await _schemaTooling.GenerateMockAsync(
                            clusterId,
                            request.SchemaSubject!,
                            request.SchemaVersion.Value,
                            count: 1,
                            seed,
                            cancellationToken)
                        .ConfigureAwait(false);

                if (!proof.IsSuccess ||
                    proof.Value is null)
                {
                    return FromSchemaFailure(
                        proof.Failure);
                }

                var schema =
                    detail.Value.Schema;
                return SourceResult.Success(
                    new DataGeneratorSource(
                        DataGeneratorSourceKind.Schema,
                        Schema:
                            new DataGeneratorSchemaSource(
                                detail.Value.Subject,
                                detail.Value.Version,
                                schema.Id,
                                schema.Format,
                                DataGeneratorPolicy
                                    .FingerprintSchema(
                                        schema))));

            case DataGeneratorSourceKind.BuiltInTemplate:
                if (request.Template is null ||
                    request.SchemaSubject is not null ||
                    request.SchemaVersion is not null ||
                    request.Template !=
                        DataGeneratorBuiltInTemplate
                            .BasicJsonV1)
                {
                    return SourceResult.Failed(
                        DataGeneratorPlanningFailureCode.SourceInvalid,
                        "Built-in generator template identity is invalid.");
                }

                const int version = 1;
                return SourceResult.Success(
                    new DataGeneratorSource(
                        DataGeneratorSourceKind.BuiltInTemplate,
                        Template:
                            new DataGeneratorTemplateSource(
                                request.Template.Value,
                                version,
                                DataGeneratorPolicy
                                    .BuiltInTemplateFingerprint(
                                        request.Template.Value,
                                        version))));

            default:
                return SourceResult.Failed(
                    DataGeneratorPlanningFailureCode.SourceUnsupported,
                    "Generator source kind is unsupported.");
        }
    }

    private static DataGeneratorPlanResult FromKafkaFailure(
        KafkaFailure? failure,
        string target) =>
        failure?.Category switch
        {
            KafkaFailureCategory.Unauthorized =>
                DataGeneratorPlanResult.Failed(
                    DataGeneratorPlanningFailureCode.DestinationUnauthorized,
                    $"Kafka denied generator {target} observation."),

            KafkaFailureCategory.NotSupported =>
                DataGeneratorPlanResult.Failed(
                    DataGeneratorPlanningFailureCode.DestinationUnsupported,
                    $"Kafka does not support required generator {target} observation."),

            KafkaFailureCategory.Unavailable or
            KafkaFailureCategory.Timeout or
            KafkaFailureCategory.AuthenticationFailed or
            KafkaFailureCategory.TlsFailure =>
                DataGeneratorPlanResult.Failed(
                    DataGeneratorPlanningFailureCode.DestinationUnavailable,
                    $"Kafka generator {target} observation is currently unavailable."),

            _ =>
                DataGeneratorPlanResult.Failed(
                    DataGeneratorPlanningFailureCode.DestinationInvalid,
                    $"Kafka generator {target} observation failed."),
        };

    private static SourceResult FromSchemaFailure(
        ReadViewFailure? failure) =>
        failure?.Category switch
        {
            ReadViewFailureCategory.Unsupported =>
                SourceResult.Failed(
                    DataGeneratorPlanningFailureCode.SourceUnsupported,
                    "Schema source is unsupported by bounded generator tooling.",
                    failure.Code),

            ReadViewFailureCategory.NotConfigured or
            ReadViewFailureCategory.Unavailable or
            ReadViewFailureCategory.Timeout =>
                SourceResult.Failed(
                    DataGeneratorPlanningFailureCode.SourceUnavailable,
                    "Schema source is currently unavailable.",
                    failure.Code),

            ReadViewFailureCategory.Unauthorized =>
                SourceResult.Failed(
                    DataGeneratorPlanningFailureCode.SourceUnavailable,
                    "Schema source cannot be observed with the configured provider authority.",
                    failure.Code),

            _ =>
                SourceResult.Failed(
                    DataGeneratorPlanningFailureCode.SourceInvalid,
                    "Schema source is invalid for bounded generation.",
                    failure?.Code),
        };

    private sealed record SourceResult(
        bool IsSuccess,
        DataGeneratorSource? Source,
        DataGeneratorPlanningFailureCode Code,
        string Message,
        string? SourceCode)
    {
        public static SourceResult Success(
            DataGeneratorSource source) =>
            new(
                true,
                source,
                default,
                string.Empty,
                null);

        public static SourceResult Failed(
            DataGeneratorPlanningFailureCode code,
            string message,
            string? sourceCode = null) =>
            new(
                false,
                null,
                code,
                message,
                sourceCode);
    }
}
