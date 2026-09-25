using Kafdeck.Core.Kafka;
using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V06W42AclExecutionTests
{
    [Fact]
    public async Task Create_persists_conflict_obligation_before_provider_write_and_resolves_after_readback()
    {
        var state = new StatefulAclProvider(Array.Empty<KafkaAclBinding>());
        var store = new InMemoryFleetStore();
        state.BeforeCreate = bindings =>
        {
            Assert.Equal(bindings.Count, store.BlockingCount);
        };
        var guard = new SequenceAuthorizationGuard();
        var (context, policy, preconditions) = await CreateContextAsync(
            state,
            requested: new[]
            {
                Binding(KafkaAclOperation.Read),
            });

        using (context.Material)
        {
            var service = Service(state, store, guard, preconditions, policy);
            var result = await service.ExecuteAsync(context);

            Assert.Equal(MutationExecutionResultKind.AppliedVerified, result.ResultKind);
            Assert.Equal(1, state.CreateCalls);
            Assert.Equal(0, state.RemoveCalls);
            Assert.Equal(1, guard.Calls);
            Assert.Single(store.Obligations);
            Assert.All(
                store.Obligations,
                obligation => Assert.Equal(
                    FleetConflictObligationState.ObservedTerminalEffect,
                    obligation.State));
        }
    }

    [Fact]
    public async Task Replace_reauthorizes_before_create_and_does_not_dispatch_after_revocation()
    {
        var current = Binding(KafkaAclOperation.Read);
        var desired = Binding(KafkaAclOperation.Write);
        var state = new StatefulAclProvider(new[] { current });
        var store = new InMemoryFleetStore();
        var guard = new SequenceAuthorizationGuard(denyCall: 2);
        var policy = Policy();
        var planner = new AclMutationPlanner(state, policy);
        var planned = await planner.PlanReplaceAsync(
            "prod",
            SourceFilter(),
            new[] { desired });
        Assert.True(planned.IsSuccess);

        using var material = new MutationExecutionMaterial();
        var operation = MutationOperation.CreatePreview(
            "oidc:https://idp.example|alice",
            planned.Intent!,
            planned.Risk!,
            "v0.6-w42",
            DateTimeOffset.UtcNow.AddMinutes(5),
            DateTimeOffset.UtcNow,
            $"w42-replace-{Guid.NewGuid():N}");
        var context = new MutationExecutionContext(
            operation.Snapshot,
            material,
            DateTimeOffset.UtcNow.AddSeconds(20));
        var preconditions = new AclMutationPreconditionValidator(
            state,
            policy);

        var service = Service(state, store, guard, preconditions, policy);
        var result = await service.ExecuteAsync(context);

        Assert.True(
            result.ResultKind == MutationExecutionResultKind.PartiallyApplied,
            $"Expected PartiallyApplied, got {result.ResultKind} ({result.ResultCode}); " +
            $"guard={guard.Calls}, remove={state.RemoveCalls}, create={state.CreateCalls}.");
        Assert.Equal(1, state.RemoveCalls);
        Assert.Equal(0, state.CreateCalls);
        Assert.Equal(2, guard.Calls);
        Assert.DoesNotContain(current, state.Bindings);
        Assert.DoesNotContain(desired, state.Bindings);
        Assert.Single(store.Obligations);
        Assert.Equal(
            FleetConflictObligationState.ObservedTerminalEffect,
            store.Obligations.Single().State);
    }

    [Fact]
    public async Task Create_only_replace_denied_before_create_is_definitive_not_partial_zero()
    {
        var desired = Binding(KafkaAclOperation.Read);
        var state = new StatefulAclProvider(Array.Empty<KafkaAclBinding>());
        var store = new InMemoryFleetStore();
        var guard = new SequenceAuthorizationGuard(denyCall: 1);
        var policy = Policy();
        var planner = new AclMutationPlanner(state, policy);
        var planned = await planner.PlanReplaceAsync(
            "prod",
            SourceFilter(),
            new[] { desired });
        Assert.True(planned.IsSuccess);
        Assert.Empty(planned.Plan!.RemoveBindings);
        Assert.Single(planned.Plan.CreateBindings);

        using var material = new MutationExecutionMaterial();
        var now = DateTimeOffset.UtcNow;
        var operation = MutationOperation.CreatePreview(
            "oidc:https://idp.example|alice",
            planned.Intent!,
            planned.Risk!,
            "v0.6-w42",
            now.AddMinutes(5),
            now,
            $"w42-replace-create-only-{Guid.NewGuid():N}");
        var context = new MutationExecutionContext(
            operation.Snapshot,
            material,
            now.AddSeconds(20));
        var service = Service(
            state,
            store,
            guard,
            new AclMutationPreconditionValidator(state, policy),
            policy);

        var result = await service.ExecuteAsync(context);

        Assert.Equal(
            MutationExecutionResultKind.FailedDefinitive,
            result.ResultKind);
        Assert.Equal(
            "acl_create_current_authorization_denied",
            result.ResultCode);
        Assert.Equal(1, guard.Calls);
        Assert.Equal(0, state.CreateCalls);
        Assert.Equal(0, state.RemoveCalls);
        Assert.Empty(store.Obligations);
        Assert.Empty(state.Bindings);
    }

    [Fact]
    public async Task Ambiguous_provider_outcome_can_only_be_promoted_by_observed_terminal_state()
    {
        var state = new StatefulAclProvider(Array.Empty<KafkaAclBinding>())
        {
            CreateResultKind = MutationExecutionResultKind.ExecutionUnknown,
            ApplyCreateBeforeReturning = true,
        };
        var store = new InMemoryFleetStore();
        var guard = new SequenceAuthorizationGuard();
        var (context, policy, preconditions) = await CreateContextAsync(
            state,
            requested: new[]
            {
                Binding(KafkaAclOperation.Read),
            });

        using (context.Material)
        {
            var service = Service(state, store, guard, preconditions, policy);
            var result = await service.ExecuteAsync(context);

            Assert.Equal(MutationExecutionResultKind.AppliedVerified, result.ResultKind);
            Assert.Equal("observed", result.SafeEvidence!["verification.state"]);
            Assert.Single(store.Obligations);
            Assert.Equal(
                FleetConflictObligationState.ObservedTerminalEffect,
                store.Obligations.Single().State);
        }
    }

    [Fact]
    public async Task Definitive_provider_failure_resolves_obligation_as_nonapplication()
    {
        var state = new StatefulAclProvider(Array.Empty<KafkaAclBinding>())
        {
            CreateResultKind = MutationExecutionResultKind.FailedDefinitive,
            ApplyCreateBeforeReturning = false,
        };
        var store = new InMemoryFleetStore();
        var guard = new SequenceAuthorizationGuard();
        var (context, policy, preconditions) = await CreateContextAsync(
            state,
            requested: new[]
            {
                Binding(KafkaAclOperation.Read),
            });

        using (context.Material)
        {
            var service = Service(state, store, guard, preconditions, policy);
            var result = await service.ExecuteAsync(context);

            Assert.Equal(MutationExecutionResultKind.FailedDefinitive, result.ResultKind);
            Assert.Single(store.Obligations);
            Assert.Equal(
                FleetConflictObligationState.ObservedNonApplication,
                store.Obligations.Single().State);
        }
    }

    private static AclMutationExecutionService Service(
        StatefulAclProvider provider,
        InMemoryFleetStore store,
        SequenceAuthorizationGuard guard,
        AclMutationPreconditionValidator preconditions,
        AclServerPolicy policy) =>
        new(
            provider,
            provider,
            store,
            guard,
            preconditions,
            policy,
            new AclMutationExecutionPolicy(
                TimeSpan.FromSeconds(2),
                TimeSpan.FromSeconds(2),
                TimeSpan.FromMilliseconds(50)));

    private static async Task<(
        MutationExecutionContext Context,
        AclServerPolicy Policy,
        AclMutationPreconditionValidator Preconditions)> CreateContextAsync(
        StatefulAclProvider state,
        IReadOnlyList<KafkaAclBinding> requested)
    {
        var policy = Policy();
        var planner = new AclMutationPlanner(state, policy);
        var planned = await planner.PlanCreateAsync("prod", requested);
        Assert.True(planned.IsSuccess);

        var now = DateTimeOffset.UtcNow;
        var operation = MutationOperation.CreatePreview(
            "oidc:https://idp.example|alice",
            planned.Intent!,
            planned.Risk!,
            "v0.6-w42",
            now.AddMinutes(5),
            now,
            $"w42-create-{Guid.NewGuid():N}");

        var context = new MutationExecutionContext(
            operation.Snapshot,
            new MutationExecutionMaterial(),
            now.AddSeconds(20));
        return (
            context,
            policy,
            new AclMutationPreconditionValidator(state, policy));
    }

    private static KafkaAclBinding Binding(KafkaAclOperation operation) =>
        new(
            KafkaAclResourceType.Topic,
            "payments.orders",
            KafkaAclPatternType.Literal,
            "User:alice",
            "*",
            operation,
            KafkaAclPermissionType.Allow);

    private static KafkaAclBindingFilter SourceFilter() =>
        new(
            KafkaAclResourceType.Topic,
            "payments.orders",
            KafkaAclFilterPatternMode.Literal,
            "User:alice",
            "*");

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

    private sealed class SequenceAuthorizationGuard :
        IAclEffectAuthorizationGuard
    {
        private readonly int? _denyCall;

        public SequenceAuthorizationGuard(int? denyCall = null)
        {
            _denyCall = denyCall;
        }

        public int Calls { get; private set; }

        public Task<MutationPreDispatchGuardResult> ValidateCurrentRequesterAsync(
            MutationOperationSnapshot operation,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Calls++;
            return Task.FromResult(
                _denyCall == Calls
                    ? new MutationPreDispatchGuardResult(
                        MutationPreDispatchGuardOutcome.AuthorizationDenied,
                        "test_authorization_revoked")
                    : MutationPreDispatchGuardResult.Allowed);
        }
    }

    private sealed class StatefulAclProvider :
        IAclObservationPort,
        IAclMutationPort
    {
        private readonly List<KafkaAclBinding> _bindings;

        public StatefulAclProvider(
            IEnumerable<KafkaAclBinding> bindings)
        {
            _bindings = bindings.ToList();
        }

        public IReadOnlyList<KafkaAclBinding> Bindings => _bindings;
        public int CreateCalls { get; private set; }
        public int RemoveCalls { get; private set; }
        public MutationExecutionResultKind CreateResultKind { get; set; } =
            MutationExecutionResultKind.AppliedUnverified;
        public bool ApplyCreateBeforeReturning { get; set; } = true;
        public Action<IReadOnlyList<KafkaAclBinding>>? BeforeCreate { get; set; }

        public Task<KafkaResult<IReadOnlyList<KafkaAclBinding>>> DescribeAsync(
            string clusterId,
            KafkaAclBindingFilter filter,
            KafkaOperationContext operation,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var matches = _bindings
                .Where(binding => AclMutationPolicy.MatchesFilter(binding, filter))
                .OrderBy(AclBindingIdentity.Canonical, StringComparer.Ordinal)
                .ToArray();
            var now = DateTimeOffset.UtcNow;
            return Task.FromResult(
                KafkaResult<IReadOnlyList<KafkaAclBinding>>.Success(
                    Array.AsReadOnly(matches),
                    new ObservationMetadata(
                        now,
                        now,
                        now,
                        ObservationSource.Live)));
        }

        public Task<MutationProviderResult> CreateAsync(
            AclCreateMutation request,
            KafkaOperationContext operation,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            CreateCalls++;
            BeforeCreate?.Invoke(request.Bindings);
            if (ApplyCreateBeforeReturning)
            {
                foreach (var binding in request.Bindings)
                {
                    if (!_bindings.Contains(binding))
                    {
                        _bindings.Add(binding);
                    }
                }
            }

            return Task.FromResult(new MutationProviderResult(
                CreateResultKind,
                CreateResultKind == MutationExecutionResultKind.FailedDefinitive
                    ? "test_create_failed_definitive"
                    : "test_create_provider_result"));
        }

        public Task<MutationProviderResult> RemoveAsync(
            AclRemoveMutation request,
            KafkaOperationContext operation,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            RemoveCalls++;
            foreach (var binding in request.Bindings)
            {
                _bindings.Remove(binding);
            }

            return Task.FromResult(new MutationProviderResult(
                MutationExecutionResultKind.AppliedUnverified,
                "test_remove_provider_result"));
        }
    }

    private sealed class InMemoryFleetStore : IFleetMutationStateStore
    {
        private readonly Dictionary<Guid, FleetConflictObligationSnapshot> _byId = new();
        private readonly Dictionary<string, Guid> _byIdentity = new(StringComparer.Ordinal);

        public IReadOnlyList<FleetConflictObligationSnapshot> Obligations =>
            _byId.Values
                .OrderBy(item => item.ConflictKey, StringComparer.Ordinal)
                .ToArray();

        public int BlockingCount =>
            _byId.Values.Count(item => item.BlocksConflictingDispatch);

        public Task InitializeAsync(CancellationToken cancellationToken = default) =>
            Task.CompletedTask;

        public Task<FleetConflictObligationBatchCreateResult> CreateConflictObligationsAsync(
            IReadOnlyList<FleetConflictObligationSnapshot> obligations,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var existing = new List<FleetConflictObligationSnapshot>();
            foreach (var obligation in obligations)
            {
                var key = Identity(obligation);
                if (!_byIdentity.TryGetValue(key, out var existingId))
                {
                    continue;
                }

                var snapshot = _byId[existingId];
                if (!string.Equals(
                        snapshot.EffectFingerprint,
                        obligation.EffectFingerprint,
                        StringComparison.Ordinal))
                {
                    return Task.FromResult(
                        new FleetConflictObligationBatchCreateResult(
                            FleetConflictObligationBatchCreateOutcome.ExistingDifferentEffect,
                            Array.Empty<FleetConflictObligationSnapshot>(),
                            snapshot));
                }

                existing.Add(snapshot);
            }

            if (existing.Count == obligations.Count)
            {
                return Task.FromResult(
                    new FleetConflictObligationBatchCreateResult(
                        FleetConflictObligationBatchCreateOutcome.ExistingSameEffects,
                        existing));
            }

            if (existing.Count != 0)
            {
                throw new InvalidOperationException(
                    "The test store expects all-or-none atomic replay.");
            }

            foreach (var obligation in obligations)
            {
                _byId.Add(obligation.ObligationId, obligation);
                _byIdentity.Add(Identity(obligation), obligation.ObligationId);
            }

            return Task.FromResult(
                new FleetConflictObligationBatchCreateResult(
                    FleetConflictObligationBatchCreateOutcome.Created,
                    obligations));
        }

        public Task<FleetConflictObligationSaveResult> TrySaveConflictObligationAsync(
            FleetConflictObligationSnapshot obligation,
            long expectedVersion,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!_byId.TryGetValue(obligation.ObligationId, out var current))
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

            _byId[obligation.ObligationId] = obligation;
            return Task.FromResult(
                new FleetConflictObligationSaveResult(
                    FleetConflictObligationSaveOutcome.Saved,
                    obligation));
        }

        public Task<FleetConflictObligationCreateResult> CreateConflictObligationAsync(
            FleetConflictObligationSnapshot obligation,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<FleetConflictObligationSnapshot?> GetConflictObligationAsync(
            Guid obligationId,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(
                _byId.TryGetValue(obligationId, out var value)
                    ? value
                    : null);

        public Task<FleetConflictObligationSnapshot?> FindBlockingConflictObligationAsync(
            string conflictKey,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(
                _byId.Values.FirstOrDefault(item =>
                    item.BlocksConflictingDispatch &&
                    string.Equals(
                        item.ConflictKey,
                        conflictKey,
                        StringComparison.Ordinal)));

        public Task<FleetProgressCreateResult> CreateProgressAsync(
            FleetOperationProgressSnapshot progress,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<FleetOperationProgressSnapshot?> GetProgressAsync(
            Guid operationId,
            CancellationToken cancellationToken = default) =>
            Task.FromResult<FleetOperationProgressSnapshot?>(null);

        public Task<FleetProgressSaveResult> TrySaveProgressAsync(
            FleetOperationProgressSnapshot progress,
            long expectedVersion,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        private static string Identity(FleetConflictObligationSnapshot obligation) =>
            string.Join(
                "\n",
                obligation.OperationId.ToString("D"),
                obligation.StepId,
                obligation.ConflictKey);
    }
}
