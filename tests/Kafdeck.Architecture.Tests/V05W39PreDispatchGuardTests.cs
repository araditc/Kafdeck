using System.Security.Claims;
using Kafdeck.Api;
using Kafdeck.Core.Consumers;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Records;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Consumers;
using Kafdeck.Modules.Records;
using Kafdeck.Modules.Topics;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V05W39PreDispatchGuardTests
{
    [Fact]
    public async Task Guard_fails_before_provider_observation_without_live_request_identity()
    {
        var kafka = new TopicCreateObservationPort();
        var (guard, _) = CreateGuard(kafka, "alice");
        var operation = CreateTopicCreateOperation(kafka.Cluster);

        var result = await guard.ValidateAsync(operation);

        Assert.Equal(MutationPreDispatchGuardOutcome.AuthorizationDenied, result.Outcome);
        Assert.Equal("current_requester_identity_missing", result.ResultCode);
        Assert.Equal(0, kafka.ObservationCalls);
    }

    [Fact]
    public async Task Guard_revalidates_preconditions_and_current_authorization_before_allowing_dispatch()
    {
        var kafka = new TopicCreateObservationPort();
        var (guard, context) = CreateGuard(kafka, "alice");
        var operation = CreateTopicCreateOperation(kafka.Cluster);

        using var scope = context.Push(CreateOperatorPrincipal("alice"));
        var result = await guard.ValidateAsync(operation);

        Assert.Equal(MutationPreDispatchGuardOutcome.Allowed, result.Outcome);
        Assert.Equal(2, kafka.ObservationCalls);
    }

    [Fact]
    public async Task Guard_rejects_a_different_operator_even_when_that_operator_has_target_permission()
    {
        var kafka = new TopicCreateObservationPort();
        var (guard, context) = CreateGuard(kafka, "alice", "bob");
        var operation = CreateTopicCreateOperation(kafka.Cluster);

        using var scope = context.Push(CreateOperatorPrincipal("bob"));
        var result = await guard.ValidateAsync(operation);

        Assert.Equal(MutationPreDispatchGuardOutcome.AuthorizationDenied, result.Outcome);
        Assert.Equal("current_requester_identity_mismatch", result.ResultCode);
        Assert.Equal(0, kafka.ObservationCalls);
    }

    private static MutationOperationSnapshot CreateTopicCreateOperation(
        ClusterMetadata cluster)
    {
        var mutation = new TopicCreateMutation(
            "prod",
            "payments.events",
            1,
            1,
            new Dictionary<string, string>(StringComparer.Ordinal));
        var intent = TopicMutationPolicy.BuildIntent(
            mutation,
            new[]
            {
                new MutationPrecondition(
                    "topic.absence",
                    TopicMutationPolicy.FingerprintCreateAbsence(
                        cluster,
                        mutation.TopicName)),
            });
        var now = DateTimeOffset.UtcNow;
        var operation = MutationOperation.CreatePreview(
            "oidc:https://idp.example|alice",
            intent,
            MutationRiskClassifier.Classify(
                new MutationRiskInput(MutationOperationKind.TopicCreate)),
            "v0.5-w39",
            now.AddMinutes(5),
            now,
            "w39-guard");
        operation.OpenForConfirmation(now.AddMilliseconds(1));
        operation.Confirm(
            "oidc:https://idp.example|alice",
            operation.Snapshot.PreviewHash,
            now.AddMilliseconds(2),
            operation.Snapshot.ConfirmationChallenge);
        Assert.Equal(MutationOperationState.Ready, operation.Snapshot.State);
        return operation.Snapshot;
    }

    private static (
        W39MutationPreDispatchGuard Guard,
        MutationExecutionRequestContextAccessor Context) CreateGuard(
        IKafkaAdministrationPort kafka,
        params string[] authorizedSubjects)
    {
        var definition = new AuthorizationPolicyDefinition(
            new[]
            {
                new AuthorizationRoleDefinition(
                    "topic-creator",
                    new[]
                    {
                        new AuthorizationPermissionDefinition(
                            AuthorizationAction.TopicCreate,
                            new[] { "prod" },
                            new[] { "payments.*" }),
                    }),
            },
            authorizedSubjects
                .Select(subject => new AuthorizationSubjectBindingDefinition(
                    "https://idp.example",
                    subject,
                    new[] { "topic-creator" }))
                .ToArray(),
            Array.Empty<AuthorizationGroupBindingDefinition>());
        var evaluator = new AuthorizationPolicyEvaluator(
            AuthorizationPolicyCompiler.Compile(definition));
        var options = new KafdeckOptions(
            new DeploymentOptions(
                "http://127.0.0.1:8080",
                null,
                AccessMode.Oidc,
                null),
            Array.Empty<ClusterProfile>());
        var authorization = new MutationRequestAuthorizationService(
            new KafdeckAuthorizationService(options, evaluator));
        var context = new MutationExecutionRequestContextAccessor();
        var guard = new W39MutationPreDispatchGuard(
            context,
            authorization,
            new TopicMutationPreconditionValidator(kafka),
            new RecordProductionPreconditionValidator(kafka),
            new ConsumerMutationPreconditionValidator(
                new ThrowingConsumerMutationObservationPort()),
            new RecordsPurgePreconditionValidator(
                new ThrowingRecordsPurgeObservationPort()));
        return (guard, context);
    }

    private static ClaimsPrincipal CreateOperatorPrincipal(string subject)
    {
        var external = new ClaimsPrincipal(
            new ClaimsIdentity(
                new[]
                {
                    new Claim("sub", subject),
                    new Claim("name", subject),
                },
                authenticationType: "oidc"));
        return OidcIdentityNormalizer.Normalize(
            external,
            "https://idp.example",
            groupClaim: null,
            DateTimeOffset.UtcNow);
    }

    private sealed class ThrowingConsumerMutationObservationPort :
        IConsumerMutationObservationPort
    {
        public Task<KafkaResult<ConsumerMutationObservation>> ObserveAsync(
            string clusterId,
            string groupId,
            IReadOnlyList<ConsumerMutationObservationTarget> targets,
            KafkaOperationContext operation,
            CancellationToken cancellationToken,
            bool includeAllCommittedOffsets = false) =>
            throw new InvalidOperationException(
                "Consumer observation must not be reached by topic guard tests.");
    }

    private sealed class ThrowingRecordsPurgeObservationPort :
        IRecordsPurgeObservationPort
    {
        public Task<KafkaResult<IReadOnlyList<RecordsPurgePartitionObservation>>> ObserveAsync(
            string clusterId,
            IReadOnlyList<RecordsPurgeObservationTarget> targets,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
            throw new InvalidOperationException(
                "Records-purge observation must not be reached by topic guard tests.");
    }

    private sealed class TopicCreateObservationPort : IKafkaAdministrationPort
    {
        private readonly ObservationMetadata _observation = new(
            DateTimeOffset.UtcNow,
            DateTimeOffset.UtcNow.AddMinutes(1),
            DateTimeOffset.UtcNow.AddMinutes(2),
            ObservationSource.Live);

        public ClusterMetadata Cluster { get; } = new(
            "prod",
            "kafka-prod",
            1,
            new[]
            {
                new BrokerMetadata(1, "broker", 9092, null, true),
            });

        public int ObservationCalls { get; private set; }

        public Task<KafkaResult<ClusterMetadata>> GetClusterMetadataAsync(
            string clusterId,
            KafkaOperationContext operation,
            CancellationToken cancellationToken)
        {
            ObservationCalls++;
            return Task.FromResult(
                KafkaResult<ClusterMetadata>.Success(Cluster, _observation));
        }

        public Task<KafkaResult<IReadOnlyList<TopicSummary>>> ListTopicsAsync(
            string clusterId,
            KafkaOperationContext operation,
            CancellationToken cancellationToken)
        {
            ObservationCalls++;
            return Task.FromResult(
                KafkaResult<IReadOnlyList<TopicSummary>>.Success(
                    Array.Empty<TopicSummary>(),
                    _observation));
        }

        public Task<KafkaResult<TopicMetadata>> GetTopicMetadataAsync(
            string clusterId,
            string topicName,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task<KafkaResult<IReadOnlyList<KafkaConfigurationEntry>>> GetTopicConfigurationAsync(
            string clusterId,
            string topicName,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task<KafkaResult<IReadOnlyList<KafkaConfigurationEntry>>> GetBrokerConfigurationAsync(
            string clusterId,
            int brokerId,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task<KafkaResult<KafkaCapabilities>> GetCapabilitiesAsync(
            string clusterId,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();
    }
}
