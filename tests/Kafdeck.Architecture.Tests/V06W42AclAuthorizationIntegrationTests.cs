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

public sealed class V06W42AclAuthorizationIntegrationTests
{
    [Fact]
    public async Task Phase_guard_requires_the_live_requester_identity_even_when_another_operator_is_authorized()
    {
        var operation = CreateAclOperation();
        var (guard, context) = CreateEffectGuard(operation);

        var missing = await guard.ValidateCurrentRequesterAsync(operation);
        Assert.Equal(
            MutationPreDispatchGuardOutcome.AuthorizationDenied,
            missing.Outcome);
        Assert.Equal(
            "current_requester_identity_missing",
            missing.ResultCode);

        using (context.Push(CreateOperatorPrincipal("bob")))
        {
            var mismatch = await guard.ValidateCurrentRequesterAsync(operation);
            Assert.Equal(
                MutationPreDispatchGuardOutcome.AuthorizationDenied,
                mismatch.Outcome);
            Assert.Equal(
                "current_requester_identity_mismatch",
                mismatch.ResultCode);
        }

        using (context.Push(CreateOperatorPrincipal("alice")))
        {
            var allowed = await guard.ValidateCurrentRequesterAsync(operation);
            Assert.Equal(
                MutationPreDispatchGuardOutcome.Allowed,
                allowed.Outcome);
        }
    }

    [Fact]
    public async Task W39_keeps_acl_dispatch_fail_closed_until_acl_validator_is_explicitly_injected()
    {
        var operation = CreateAclOperation();
        var observation = new EmptyAclObservationPort();
        var (_, context, authorization) = CreateAuthorization(operation);
        var guard = CreateW39(
            context,
            authorization,
            acls: null);

        using var scope = context.Push(CreateOperatorPrincipal("alice"));
        var result = await guard.ValidateAsync(operation);

        Assert.Equal(
            MutationPreDispatchGuardOutcome.CapabilityUnsupported,
            result.Outcome);
        Assert.Equal("mutation_handler_not_admitted", result.ResultCode);
        Assert.Equal(0, observation.Calls);
    }

    [Fact]
    public async Task W39_acl_guard_revalidates_live_requester_and_exact_acl_preconditions_when_injected()
    {
        var operation = CreateAclOperation();
        var observation = new EmptyAclObservationPort();
        var (_, context, authorization) = CreateAuthorization(operation);
        var policy = Policy();
        var guard = CreateW39(
            context,
            authorization,
            new AclMutationPreconditionValidator(
                observation,
                policy));

        using var scope = context.Push(CreateOperatorPrincipal("alice"));
        var result = await guard.ValidateAsync(operation);

        Assert.Equal(
            MutationPreDispatchGuardOutcome.Allowed,
            result.Outcome);
        Assert.Equal(1, observation.Calls);
    }

    private static MutationOperationSnapshot CreateAclOperation()
    {
        var binding = Binding();
        var plan = new AclMutationPlan(
            AclMutationMode.Create,
            "prod",
            new[] { binding },
            Array.Empty<KafkaAclBinding>(),
            null,
            AclMutationPolicy.FingerprintBindings(
                Array.Empty<KafkaAclBinding>()));
        var intent = AclMutationPolicy.BuildIntent(plan);
        var risk = AclMutationPolicy.ClassifyRisk(
            plan.CreateBindings,
            plan.RemoveBindings);
        var now = DateTimeOffset.UtcNow;
        return MutationOperation.CreatePreview(
                "oidc:https://idp.example|alice",
                intent,
                risk,
                "v0.6-w42",
                now.AddMinutes(5),
                now,
                $"w42-auth-{Guid.NewGuid():N}")
            .Snapshot;
    }

    private static (
        W42AclEffectAuthorizationGuard Guard,
        MutationExecutionRequestContextAccessor Context) CreateEffectGuard(
        MutationOperationSnapshot operation)
    {
        var (_, context, authorization) = CreateAuthorization(operation);
        return (
            new W42AclEffectAuthorizationGuard(
                context,
                authorization),
            context);
    }

    private static (
        string Resource,
        MutationExecutionRequestContextAccessor Context,
        MutationRequestAuthorizationService Authorization) CreateAuthorization(
        MutationOperationSnapshot operation)
    {
        var resource = Assert.Single(
            operation.AuthorizationTargets,
            target => target.Action == AuthorizationAction.AclAlter)
            .ResourceName;

        var definition = new AuthorizationPolicyDefinition(
            new[]
            {
                new AuthorizationRoleDefinition(
                    "acl-operator",
                    new[]
                    {
                        new AuthorizationPermissionDefinition(
                            AuthorizationAction.AclRead,
                            new[] { "prod" },
                            new[] { resource }),
                        new AuthorizationPermissionDefinition(
                            AuthorizationAction.AclAlter,
                            new[] { "prod" },
                            new[] { resource }),
                    }),
            },
            new[] { "alice", "bob" }
                .Select(subject =>
                    new AuthorizationSubjectBindingDefinition(
                        "https://idp.example",
                        subject,
                        new[] { "acl-operator" }))
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
            new KafdeckAuthorizationService(
                options,
                evaluator));
        return (
            resource,
            new MutationExecutionRequestContextAccessor(),
            authorization);
    }

    private static W39MutationPreDispatchGuard CreateW39(
        MutationExecutionRequestContextAccessor context,
        MutationRequestAuthorizationService authorization,
        AclMutationPreconditionValidator? acls)
    {
        var kafka = new ThrowingKafkaAdministrationPort();
        return new W39MutationPreDispatchGuard(
            context,
            authorization,
            new TopicMutationPreconditionValidator(kafka),
            new RecordProductionPreconditionValidator(kafka),
            new ConsumerMutationPreconditionValidator(
                new ThrowingConsumerMutationObservationPort()),
            new RecordsPurgePreconditionValidator(
                new ThrowingRecordsPurgeObservationPort()),
            acls: acls);
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

    private static KafkaAclBinding Binding() =>
        new(
            KafkaAclResourceType.Topic,
            "payments.orders",
            KafkaAclPatternType.Literal,
            "User:alice",
            "*",
            KafkaAclOperation.Read,
            KafkaAclPermissionType.Allow);

    private static AclServerPolicy Policy() =>
        new(
            Array.Empty<string>(),
            new[] { "User:alice" },
            new[] { KafkaAclResourceType.Topic },
            Enum.GetValues<KafkaAclOperation>(),
            allowPrefixedGrants: true,
            allowWildcardResourceGrants: false,
            allowAllOperationGrants: false,
            maxBindingsPerMutation: 25);

    private sealed class EmptyAclObservationPort : IAclObservationPort
    {
        public int Calls { get; private set; }

        public Task<KafkaResult<IReadOnlyList<KafkaAclBinding>>> DescribeAsync(
            string clusterId,
            KafkaAclBindingFilter filter,
            KafkaOperationContext operation,
            CancellationToken cancellationToken = default)
        {
            Calls++;
            var now = DateTimeOffset.UtcNow;
            return Task.FromResult(
                KafkaResult<IReadOnlyList<KafkaAclBinding>>.Success(
                    Array.Empty<KafkaAclBinding>(),
                    new ObservationMetadata(
                        now,
                        now,
                        now,
                        ObservationSource.Live)));
        }
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
                "Consumer observation must not be reached by ACL guard tests.");
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
                "Records-purge observation must not be reached by ACL guard tests.");
    }

    private sealed class ThrowingKafkaAdministrationPort :
        IKafkaAdministrationPort
    {
        private static InvalidOperationException Unexpected() =>
            new("Kafka administration must not be reached by ACL guard tests.");

        public Task<KafkaResult<ClusterMetadata>> GetClusterMetadataAsync(
            string clusterId,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
            throw Unexpected();

        public Task<KafkaResult<IReadOnlyList<TopicSummary>>> ListTopicsAsync(
            string clusterId,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
            throw Unexpected();

        public Task<KafkaResult<TopicMetadata>> GetTopicMetadataAsync(
            string clusterId,
            string topicName,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
            throw Unexpected();

        public Task<KafkaResult<IReadOnlyList<KafkaConfigurationEntry>>> GetTopicConfigurationAsync(
            string clusterId,
            string topicName,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
            throw Unexpected();

        public Task<KafkaResult<IReadOnlyList<KafkaConfigurationEntry>>> GetBrokerConfigurationAsync(
            string clusterId,
            int brokerId,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
            throw Unexpected();

        public Task<KafkaResult<KafkaCapabilities>> GetCapabilitiesAsync(
            string clusterId,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
            throw Unexpected();
    }
}
