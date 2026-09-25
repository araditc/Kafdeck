using System.Security.Claims;
using System.Text;
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

public sealed class V06W43ScramAuthorizationIntegrationTests
{
    [Fact]
    public async Task Scram_guard_requires_the_live_original_requester()
    {
        var operation = await CreateOperationAsync(
            approvedBy: "oidc:https://idp.example|carol");
        var (_, context, authorization, approvalAuthorizer) =
            CreateAuthorization(
                operation,
                directSubjects: new[] { "alice", "carol" });
        var guard = new W43ScramEffectAuthorizationGuard(
            context,
            authorization,
            approvalAuthorizer);

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
    public async Task Scram_guard_does_not_rehydrate_group_only_approver()
    {
        var operation = await CreateOperationAsync(
            approvedBy: "oidc:https://idp.example|carol");
        var (_, context, authorization, approvalAuthorizer) =
            CreateAuthorization(
                operation,
                directSubjects: new[] { "alice" },
                groupBindings: new[]
                {
                    new AuthorizationGroupBindingDefinition(
                        "scram-admins",
                        new[] { "scram-operator" }),
                });
        var guard = new W43ScramEffectAuthorizationGuard(
            context,
            authorization,
            approvalAuthorizer);

        using var scope = context.Push(CreateOperatorPrincipal("alice"));
        var result = await guard.ValidateCurrentRequesterAsync(operation);

        Assert.Equal(
            MutationPreDispatchGuardOutcome.AuthorizationDenied,
            result.Outcome);
        Assert.Equal(
            "current_required_approver_eligibility_unavailable_or_denied",
            result.ResultCode);
    }

    [Fact]
    public async Task W39_keeps_scram_fail_closed_until_validator_and_live_guard_are_injected()
    {
        var operation = await CreateOperationAsync(
            approvedBy: "oidc:https://idp.example|carol");
        var observation = new EmptyScramObservation();
        var (_, context, authorization, approvalAuthorizer) =
            CreateAuthorization(
                operation,
                directSubjects: new[] { "alice", "carol" });

        using var scope = context.Push(CreateOperatorPrincipal("alice"));

        var closed = CreateW39(
            context,
            authorization,
            scram: null,
            scramAuthorization: null);
        var denied = await closed.ValidateAsync(operation);
        Assert.Equal(
            MutationPreDispatchGuardOutcome.CapabilityUnsupported,
            denied.Outcome);
        Assert.Equal("mutation_handler_not_admitted", denied.ResultCode);
        Assert.Equal(0, observation.Calls);

        var policy = Policy();
        var liveGuard = new W43ScramEffectAuthorizationGuard(
            context,
            authorization,
            approvalAuthorizer);
        var admitted = CreateW39(
            context,
            authorization,
            new ScramMutationPreconditionValidator(
                observation,
                policy,
                "digest-key-v1"),
            liveGuard);
        var allowed = await admitted.ValidateAsync(operation);

        Assert.Equal(
            MutationPreDispatchGuardOutcome.Allowed,
            allowed.Outcome);
        Assert.Equal(1, observation.Calls);
    }

    private static async Task<MutationOperationSnapshot>
        CreateOperationAsync(string approvedBy)
    {
        var observation = new EmptyScramObservation();
        using var digest =
            new HmacMutationMaterialDigestService(new string('k', 32));
        var policy = Policy();
        var operationId = Guid.NewGuid();
        var requester = "oidc:https://idp.example|alice";
        var binding = new ScramPreviewBindingContext(
            operationId,
            requester,
            "policy-v1",
            "digest-key-v1");
        var password = Encoding.UTF8.GetBytes(
            "synthetic-w43-auth-secret");

        try
        {
            var planned = await new ScramMutationPlanner(
                    observation,
                    digest,
                    policy)
                .PlanUpsertAsync(
                    binding,
                    "prod",
                    "User:alice",
                    KafkaScramMechanism.ScramSha256,
                    4096,
                    password);
            Assert.True(planned.IsSuccess);

            var now = DateTimeOffset.UtcNow;
            var created = MutationOperation.CreatePreview(
                operationId,
                requester,
                planned.Intent!,
                planned.Risk!,
                binding.PolicyVersion,
                now.AddMinutes(5),
                now,
                $"w43-auth-{Guid.NewGuid():N}");
            return created.Snapshot with
            {
                State = MutationOperationState.Ready,
                ConfirmedByPrincipalId = requester,
                ConfirmedAtUtc = now,
                ApprovedByPrincipalId = approvedBy,
                ApprovalAuthorizationEvidenceHash = new string('a', 64),
                ApprovedAtUtc = now,
            };
        }
        finally
        {
            System.Security.Cryptography.CryptographicOperations.ZeroMemory(
                password);
        }
    }

    private static (
        string Resource,
        MutationExecutionRequestContextAccessor Context,
        MutationRequestAuthorizationService Authorization,
        MutationApprovalAuthorizer ApprovalAuthorizer) CreateAuthorization(
        MutationOperationSnapshot operation,
        IReadOnlyList<string> directSubjects,
        IReadOnlyList<AuthorizationGroupBindingDefinition>? groupBindings = null)
    {
        var resource = Assert.Single(
            operation.AuthorizationTargets,
            target => target.Action == AuthorizationAction.ScramAlter)
            .ResourceName;

        var definition = new AuthorizationPolicyDefinition(
            new[]
            {
                new AuthorizationRoleDefinition(
                    "scram-operator",
                    new[]
                    {
                        new AuthorizationPermissionDefinition(
                            AuthorizationAction.ScramRead,
                            new[] { "prod" },
                            new[] { resource }),
                        new AuthorizationPermissionDefinition(
                            AuthorizationAction.ScramAlter,
                            new[] { "prod" },
                            new[] { resource }),
                    }),
            },
            directSubjects
                .Select(subject =>
                    new AuthorizationSubjectBindingDefinition(
                        "https://idp.example",
                        subject,
                        new[] { "scram-operator" }))
                .ToArray(),
            groupBindings ??
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
        return (
            resource,
            new MutationExecutionRequestContextAccessor(),
            authorization,
            new MutationApprovalAuthorizer(evaluator));
    }

    private static W39MutationPreDispatchGuard CreateW39(
        MutationExecutionRequestContextAccessor context,
        MutationRequestAuthorizationService authorization,
        ScramMutationPreconditionValidator? scram,
        IScramEffectAuthorizationGuard? scramAuthorization)
    {
        var kafka = new ThrowingKafkaAdministrationPort();
        return new W39MutationPreDispatchGuard(
            context,
            authorization,
            new TopicMutationPreconditionValidator(kafka),
            new RecordProductionPreconditionValidator(kafka),
            new ConsumerMutationPreconditionValidator(
                new ThrowingConsumerObservation()),
            new RecordsPurgePreconditionValidator(
                new ThrowingPurgeObservation()),
            scram: scram,
            scramAuthorization: scramAuthorization);
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

    private static ScramServerPolicy Policy() =>
        new(
            Array.Empty<string>(),
            new[]
            {
                KafkaScramMechanism.ScramSha256,
                KafkaScramMechanism.ScramSha512,
            });

    private sealed class EmptyScramObservation : IScramObservationPort
    {
        public int Calls { get; private set; }

        public Task<KafkaResult<IReadOnlyList<KafkaScramCredentialMetadata>>>
            DescribeUserAsync(
                string clusterId,
                string user,
                KafkaOperationContext operation,
                CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Calls++;
            var now = DateTimeOffset.UtcNow;
            return Task.FromResult(
                KafkaResult<IReadOnlyList<KafkaScramCredentialMetadata>>.Success(
                    Array.Empty<KafkaScramCredentialMetadata>(),
                    new ObservationMetadata(
                        now,
                        now,
                        now,
                        ObservationSource.Live)));
        }
    }

    private sealed class ThrowingConsumerObservation :
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
                "Consumer observation must not be reached by SCRAM tests.");
    }

    private sealed class ThrowingPurgeObservation :
        IRecordsPurgeObservationPort
    {
        public Task<KafkaResult<IReadOnlyList<RecordsPurgePartitionObservation>>>
            ObserveAsync(
                string clusterId,
                IReadOnlyList<RecordsPurgeObservationTarget> targets,
                KafkaOperationContext operation,
                CancellationToken cancellationToken) =>
            throw new InvalidOperationException(
                "Purge observation must not be reached by SCRAM tests.");
    }

    private sealed class ThrowingKafkaAdministrationPort :
        IKafkaAdministrationPort
    {
        private static InvalidOperationException Unexpected() =>
            new("Kafka administration must not be reached by SCRAM tests.");

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

        public Task<KafkaResult<IReadOnlyList<KafkaConfigurationEntry>>>
            GetTopicConfigurationAsync(
                string clusterId,
                string topicName,
                KafkaOperationContext operation,
                CancellationToken cancellationToken) =>
            throw Unexpected();

        public Task<KafkaResult<IReadOnlyList<KafkaConfigurationEntry>>>
            GetBrokerConfigurationAsync(
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
