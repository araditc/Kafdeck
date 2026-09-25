using System.Security.Cryptography;
using System.Text;
using Kafdeck.Core.Kafka;
using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V06W43ScramExecutionTests
{
    [Fact]
    public async Task Successful_upsert_is_applied_unverified_even_after_metadata_readback()
    {
        var provider = new StatefulScramProvider();
        var store = new InMemoryFleetStore();
        var guard = new SequenceScramAuthorizationGuard();
        var fixture = await CreateUpsertAsync(provider);

        using (fixture.Context.Material)
        {
            var service = Service(
                provider,
                store,
                guard,
                fixture.Policy);
            var result = await service.ExecuteAsync(fixture.Context);

            Assert.Equal(
                MutationExecutionResultKind.AppliedUnverified,
                result.ResultKind);
            Assert.Equal(
                "scram_upsert_applied_metadata_observed",
                result.ResultCode);
            Assert.Equal(1, provider.UpsertCalls);
            Assert.Equal(2, guard.Calls);
            Assert.Single(store.Obligations);
            Assert.Equal(
                FleetConflictObligationState.ObservedTerminalEffect,
                store.Obligations.Single().State);
        }
    }

    [Fact]
    public async Task Ambiguous_upsert_remains_unknown_even_when_metadata_matches()
    {
        var provider = new StatefulScramProvider
        {
            UpsertResultKind = MutationExecutionResultKind.ExecutionUnknown,
            ApplyUpsertBeforeReturning = true,
        };
        var store = new InMemoryFleetStore();
        var guard = new SequenceScramAuthorizationGuard();
        var fixture = await CreateUpsertAsync(provider);

        using (fixture.Context.Material)
        {
            var service = Service(
                provider,
                store,
                guard,
                fixture.Policy);
            var first = await service.ExecuteAsync(fixture.Context);

            Assert.Equal(
                MutationExecutionResultKind.ExecutionUnknown,
                first.ResultKind);
            Assert.Equal(1, provider.UpsertCalls);
            Assert.Single(store.Obligations);
            Assert.Equal(
                FleetConflictObligationState.Outstanding,
                store.Obligations.Single().State);

            var second = await service.ExecuteAsync(fixture.Context);
            Assert.Equal(
                MutationExecutionResultKind.ExecutionUnknown,
                second.ResultKind);
            Assert.Equal(
                "scram_upsert_existing_effect_unresolved",
                second.ResultCode);
            Assert.Equal(1, provider.UpsertCalls);
            Assert.Equal(
                FleetConflictObligationState.Outstanding,
                store.Obligations.Single().State);
        }
    }

    [Fact]
    public async Task Revocation_after_obligation_admission_prevents_provider_write_and_resolves_nonapplication()
    {
        var provider = new StatefulScramProvider();
        var store = new InMemoryFleetStore();
        var guard = new SequenceScramAuthorizationGuard(denyCall: 2);
        var fixture = await CreateUpsertAsync(provider);

        using (fixture.Context.Material)
        {
            var service = Service(
                provider,
                store,
                guard,
                fixture.Policy);
            var result = await service.ExecuteAsync(fixture.Context);

            Assert.Equal(
                MutationExecutionResultKind.FailedDefinitive,
                result.ResultKind);
            Assert.Equal(0, provider.UpsertCalls);
            Assert.Single(store.Obligations);
            Assert.Equal(
                FleetConflictObligationState.ObservedNonApplication,
                store.Obligations.Single().State);
        }
    }

    [Fact]
    public async Task Ambiguous_delete_can_be_verified_only_by_observed_absence()
    {
        var provider = new StatefulScramProvider(
            new KafkaScramCredentialMetadata(
                "User:alice",
                KafkaScramMechanism.ScramSha256,
                4096))
        {
            DeleteResultKind = MutationExecutionResultKind.ExecutionUnknown,
            ApplyDeleteBeforeReturning = true,
        };
        var store = new InMemoryFleetStore();
        var guard = new SequenceScramAuthorizationGuard();
        var fixture = await CreateDeleteAsync(provider);

        using (fixture.Context.Material)
        {
            var service = Service(
                provider,
                store,
                guard,
                fixture.Policy);
            var result = await service.ExecuteAsync(fixture.Context);

            Assert.Equal(
                MutationExecutionResultKind.AppliedVerified,
                result.ResultKind);
            Assert.Equal(1, provider.DeleteCalls);
            Assert.Single(store.Obligations);
            Assert.Equal(
                FleetConflictObligationState.ObservedTerminalEffect,
                store.Obligations.Single().State);
        }
    }

    private static ScramMutationExecutionService Service(
        StatefulScramProvider provider,
        InMemoryFleetStore store,
        SequenceScramAuthorizationGuard guard,
        ScramServerPolicy policy) =>
        new(
            provider,
            provider,
            store,
            guard,
            new ScramMutationPreconditionValidator(
                provider,
                policy,
                "digest-key-v1"),
            policy,
            new ScramMutationExecutionPolicy(
                TimeSpan.FromSeconds(2),
                TimeSpan.FromSeconds(2),
                TimeSpan.FromMilliseconds(50)));

    private static async Task<Fixture> CreateUpsertAsync(
        StatefulScramProvider provider)
    {
        var policy = Policy();
        var operationId = Guid.NewGuid();
        var requester = "oidc:https://idp.example|alice";
        var preview = new ScramPreviewBindingContext(
            operationId,
            requester,
            "policy-v1",
            "digest-key-v1");
        using var digest =
            new HmacMutationMaterialDigestService(new string('k', 32));
        var planner = new ScramMutationPlanner(
            provider,
            digest,
            policy);
        var password = Encoding.UTF8.GetBytes(
            "synthetic-w43-execution-secret");

        try
        {
            var planned = await planner.PlanUpsertAsync(
                preview,
                "prod",
                "User:alice",
                KafkaScramMechanism.ScramSha256,
                4096,
                password);
            Assert.True(planned.IsSuccess);

            var operation = ReadyOperation(
                operationId,
                requester,
                preview.PolicyVersion,
                planned.Intent!,
                planned.Risk!);

            var material =
                ScramMutationExecutionMaterialBuilder.BuildUpsert(
                    operation,
                    password);
            return new Fixture(
                new MutationExecutionContext(
                    operation,
                    material,
                    DateTimeOffset.UtcNow.AddSeconds(20)),
                policy);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(password);
        }
    }

    private static async Task<Fixture> CreateDeleteAsync(
        StatefulScramProvider provider)
    {
        var policy = Policy();
        var operationId = Guid.NewGuid();
        var requester = "oidc:https://idp.example|alice";
        var preview = new ScramPreviewBindingContext(
            operationId,
            requester,
            "policy-v1",
            DigestKeyId: null);
        using var digest =
            new HmacMutationMaterialDigestService(new string('k', 32));
        var planner = new ScramMutationPlanner(
            provider,
            digest,
            policy);
        var planned = await planner.PlanDeleteAsync(
            preview,
            "prod",
            "User:alice",
            KafkaScramMechanism.ScramSha256);
        Assert.True(planned.IsSuccess);

        var operation = ReadyOperation(
            operationId,
            requester,
            preview.PolicyVersion,
            planned.Intent!,
            planned.Risk!);
        return new Fixture(
            new MutationExecutionContext(
                operation,
                new MutationExecutionMaterial(),
                DateTimeOffset.UtcNow.AddSeconds(20)),
            policy);
    }

    private static MutationOperationSnapshot ReadyOperation(
        Guid operationId,
        string requester,
        string policyVersion,
        MutationIntentDescriptor intent,
        MutationRiskDecision risk)
    {
        var now = DateTimeOffset.UtcNow;
        var created = MutationOperation.CreatePreview(
            operationId,
            requester,
            intent,
            risk,
            policyVersion,
            now.AddMinutes(5),
            now,
            $"w43-execution-{Guid.NewGuid():N}");

        return created.Snapshot with
        {
            State = MutationOperationState.Ready,
            ConfirmedByPrincipalId = requester,
            ConfirmedAtUtc = now,
            ApprovedByPrincipalId =
                "oidc:https://idp.example|independent",
            ApprovalAuthorizationEvidenceHash = new string('a', 64),
            ApprovedAtUtc = now,
        };
    }

    private static ScramServerPolicy Policy() =>
        new(
            Array.Empty<string>(),
            new[]
            {
                KafkaScramMechanism.ScramSha256,
                KafkaScramMechanism.ScramSha512,
            });

    private sealed record Fixture(
        MutationExecutionContext Context,
        ScramServerPolicy Policy);

    private sealed class SequenceScramAuthorizationGuard :
        IScramEffectAuthorizationGuard
    {
        private readonly int? _denyCall;

        public SequenceScramAuthorizationGuard(int? denyCall = null)
        {
            _denyCall = denyCall;
        }

        public int Calls { get; private set; }

        public Task<MutationPreDispatchGuardResult>
            ValidateCurrentRequesterAsync(
                MutationOperationSnapshot operation,
                CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Calls++;
            return Task.FromResult(
                _denyCall == Calls
                    ? new MutationPreDispatchGuardResult(
                        MutationPreDispatchGuardOutcome.AuthorizationDenied,
                        "test_scram_authorization_revoked")
                    : MutationPreDispatchGuardResult.Allowed);
        }
    }

    private sealed class StatefulScramProvider :
        IScramObservationPort,
        IScramMutationPort
    {
        private readonly List<KafkaScramCredentialMetadata> _values = new();

        public StatefulScramProvider(
            params KafkaScramCredentialMetadata[] values)
        {
            _values.AddRange(values);
        }

        public int UpsertCalls { get; private set; }
        public int DeleteCalls { get; private set; }
        public MutationExecutionResultKind UpsertResultKind { get; set; } =
            MutationExecutionResultKind.AppliedUnverified;
        public MutationExecutionResultKind DeleteResultKind { get; set; } =
            MutationExecutionResultKind.AppliedUnverified;
        public bool ApplyUpsertBeforeReturning { get; set; } = true;
        public bool ApplyDeleteBeforeReturning { get; set; } = true;

        public Task<KafkaResult<IReadOnlyList<KafkaScramCredentialMetadata>>>
            DescribeUserAsync(
                string clusterId,
                string user,
                KafkaOperationContext operation,
                CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var now = DateTimeOffset.UtcNow;
            return Task.FromResult(
                KafkaResult<IReadOnlyList<KafkaScramCredentialMetadata>>.Success(
                    _values
                        .Where(item =>
                            string.Equals(
                                item.User,
                                user,
                                StringComparison.Ordinal))
                        .OrderBy(item => item.Mechanism)
                        .ToArray(),
                    new ObservationMetadata(
                        now,
                        now,
                        now,
                        ObservationSource.Live)));
        }

        public Task<MutationProviderResult> UpsertAsync(
            ScramUpsertMutation request,
            ReadOnlyMemory<byte> password,
            KafkaOperationContext operation,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Assert.True(password.Length > 0);
            UpsertCalls++;
            if (ApplyUpsertBeforeReturning)
            {
                _values.RemoveAll(item =>
                    string.Equals(
                        item.User,
                        request.User,
                        StringComparison.Ordinal) &&
                    item.Mechanism == request.Mechanism);
                _values.Add(
                    new KafkaScramCredentialMetadata(
                        request.User,
                        request.Mechanism,
                        request.Iterations));
            }

            return Task.FromResult(
                new MutationProviderResult(
                    UpsertResultKind,
                    "test_scram_upsert"));
        }

        public Task<MutationProviderResult> DeleteAsync(
            ScramDeleteMutation request,
            KafkaOperationContext operation,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            DeleteCalls++;
            if (ApplyDeleteBeforeReturning)
            {
                _values.RemoveAll(item =>
                    string.Equals(
                        item.User,
                        request.User,
                        StringComparison.Ordinal) &&
                    item.Mechanism == request.Mechanism);
            }

            return Task.FromResult(
                new MutationProviderResult(
                    DeleteResultKind,
                    "test_scram_delete"));
        }
    }

    private sealed class InMemoryFleetStore :
        IFleetMutationStateStore
    {
        private readonly Dictionary<Guid, FleetConflictObligationSnapshot>
            _obligations = new();
        private readonly Dictionary<string, Guid> _identity =
            new(StringComparer.Ordinal);

        public IReadOnlyList<FleetConflictObligationSnapshot> Obligations =>
            _obligations.Values.ToArray();

        public Task InitializeAsync(
            CancellationToken cancellationToken = default) =>
            Task.CompletedTask;

        public Task<FleetProgressCreateResult> CreateProgressAsync(
            FleetOperationProgressSnapshot progress,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<FleetOperationProgressSnapshot?> GetProgressAsync(
            Guid operationId,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<FleetProgressSaveResult> TrySaveProgressAsync(
            FleetOperationProgressSnapshot progress,
            long expectedVersion,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public async Task<FleetConflictObligationCreateResult>
            CreateConflictObligationAsync(
                FleetConflictObligationSnapshot obligation,
                CancellationToken cancellationToken = default)
        {
            var batch = await CreateConflictObligationsAsync(
                new[] { obligation },
                cancellationToken);
            return new FleetConflictObligationCreateResult(
                batch.Outcome switch
                {
                    FleetConflictObligationBatchCreateOutcome.Created =>
                        FleetConflictObligationCreateOutcome.Created,
                    FleetConflictObligationBatchCreateOutcome.ExistingSameEffects =>
                        FleetConflictObligationCreateOutcome.ExistingSameEffect,
                    FleetConflictObligationBatchCreateOutcome.ExistingDifferentEffect =>
                        FleetConflictObligationCreateOutcome.ExistingDifferentEffect,
                    FleetConflictObligationBatchCreateOutcome.ParentOperationNotFound =>
                        FleetConflictObligationCreateOutcome.ParentOperationNotFound,
                    FleetConflictObligationBatchCreateOutcome.LegacyResourceClaimConflict =>
                        FleetConflictObligationCreateOutcome.LegacyResourceClaimConflict,
                    FleetConflictObligationBatchCreateOutcome.FleetConflictScopeConflict =>
                        FleetConflictObligationCreateOutcome.FleetConflictScopeConflict,
                    _ => throw new ArgumentOutOfRangeException(),
                },
                batch.Obligations.SingleOrDefault() ??
                batch.ConflictingObligation);
        }

        public Task<FleetConflictObligationBatchCreateResult>
            CreateConflictObligationsAsync(
                IReadOnlyList<FleetConflictObligationSnapshot> obligations,
                CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Assert.Single(obligations);
            var requested = obligations[0];
            var key = Identity(requested);

            if (_identity.TryGetValue(key, out var existingId))
            {
                var existing = _obligations[existingId];
                return Task.FromResult(
                    string.Equals(
                        existing.EffectFingerprint,
                        requested.EffectFingerprint,
                        StringComparison.Ordinal)
                        ? new FleetConflictObligationBatchCreateResult(
                            FleetConflictObligationBatchCreateOutcome
                                .ExistingSameEffects,
                            new[] { existing })
                        : new FleetConflictObligationBatchCreateResult(
                            FleetConflictObligationBatchCreateOutcome
                                .ExistingDifferentEffect,
                            Array.Empty<FleetConflictObligationSnapshot>(),
                            existing));
            }

            _identity[key] = requested.ObligationId;
            _obligations[requested.ObligationId] = requested;
            return Task.FromResult(
                new FleetConflictObligationBatchCreateResult(
                    FleetConflictObligationBatchCreateOutcome.Created,
                    new[] { requested }));
        }

        public Task<FleetConflictObligationSnapshot?>
            GetConflictObligationAsync(
                Guid obligationId,
                CancellationToken cancellationToken = default) =>
            Task.FromResult(
                _obligations.TryGetValue(obligationId, out var value)
                    ? value
                    : null);

        public Task<FleetConflictObligationSnapshot?>
            FindBlockingConflictObligationAsync(
                string conflictKey,
                CancellationToken cancellationToken = default) =>
            Task.FromResult(
                _obligations.Values.FirstOrDefault(item =>
                    item.BlocksConflictingDispatch &&
                    string.Equals(
                        item.ConflictKey,
                        conflictKey,
                        StringComparison.Ordinal)));

        public Task<FleetConflictObligationSaveResult>
            TrySaveConflictObligationAsync(
                FleetConflictObligationSnapshot obligation,
                long expectedVersion,
                CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!_obligations.TryGetValue(
                    obligation.ObligationId,
                    out var current))
            {
                return Task.FromResult(
                    new FleetConflictObligationSaveResult(
                        FleetConflictObligationSaveOutcome.NotFound,
                        null));
            }

            if (current.Version != expectedVersion)
            {
                return Task.FromResult(
                    new FleetConflictObligationSaveResult(
                        FleetConflictObligationSaveOutcome.VersionConflict,
                        current));
            }

            _obligations[obligation.ObligationId] = obligation;
            return Task.FromResult(
                new FleetConflictObligationSaveResult(
                    FleetConflictObligationSaveOutcome.Saved,
                    obligation));
        }

        private static string Identity(
            FleetConflictObligationSnapshot obligation) =>
            string.Join(
                "|",
                obligation.OperationId.ToString("D"),
                obligation.StepId,
                obligation.ConflictKey);
    }
}
