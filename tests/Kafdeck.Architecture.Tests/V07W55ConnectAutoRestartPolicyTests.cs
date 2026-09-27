using System.Security.Cryptography;
using System.Text;
using Kafdeck.Core.Ecosystem;
using Kafdeck.Core.ReadViews;
using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Connect;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V07W55ConnectAutoRestartPolicyTests
{
    private static readonly DateTimeOffset Now =
        new(2026, 9, 27, 10, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task Enable_preview_is_high_risk_and_requires_manage_plus_read()
    {
        var runtime = Runtime(enabled: true);
        var observations = new FakeObservationPort(
            Existing(
                "sink-a",
                "FAILED",
                Configuration(
                    ("connector.class", "org.example.SinkConnector"),
                    ("tasks.max", "1"))));
        using var digest = Digest();
        var connect = new ConnectMutationPlanner(
            observations,
            digest,
            timeProvider: new FixedTimeProvider(Now));
        var store = new FakeStore();
        var governance = new FakeGovernance(runtime);

        var planner = new ConnectAutoRestartPolicyPlanner(
            connect,
            store,
            governance,
            new FakeRuntimePolicy(runtime),
            new FixedTimeProvider(Now));

        var result = await planner.PlanAsync(
            new ConnectAutoRestartPolicyRequest(
                "prod",
                "analytics",
                "sink-a",
                Enabled: true),
            "oidc:https://idp.example|alice");

        Assert.True(result.IsSuccess, result.Failure?.SafeMessage);
        var plan = result.Plan!;
        Assert.Equal(
            MutationRiskClass.High,
            plan.Risk.RiskClass);
        Assert.Equal(
            MutationConfirmationMode.TypedTarget,
            plan.Risk.ConfirmationMode);

        Assert.Equal(
            new[]
            {
                AuthorizationAction.ConnectRead,
                AuthorizationAction.ConnectAutoRestartManage,
            },
            plan.Intent.AuthorizationTargets!
                .Select(item => item.Action)
                .OrderBy(item => item)
                .ToArray());

        Assert.All(
            plan.Intent.AuthorizationTargets!,
            item => Assert.Equal(
                "connect-profile/analytics/connector/sink-a",
                item.ResourceName));

        Assert.Equal(
            "oidc:https://idp.example|alice",
            plan.Canonical.AutomationPrincipalId);
        Assert.Equal(
            "analytics",
            plan.Canonical.Target.ConnectProfileId);
        Assert.Equal(
            ConnectAutoRestartPolicyMutationMode.EnableOrReplace,
            plan.Canonical.Mode);
        Assert.NotNull(plan.Canonical.DesiredPolicy);
        Assert.Equal(3, plan.Canonical.DesiredPolicy!.MaxAttempts);
    }

    [Fact]
    public async Task Disable_preview_is_moderate_and_requires_manage_only_without_provider_read()
    {
        var runtime = Runtime(enabled: true);
        var active = Activation(
            "default",
            "sink-a",
            runtime);
        var store = new FakeStore
        {
            Active = active,
        };
        var observations = new FakeObservationPort(
            Existing(
                "sink-a",
                "FAILED",
                Configuration(
                    ("connector.class", "org.example.SinkConnector"))));

        using var digest = Digest();
        var planner = new ConnectAutoRestartPolicyPlanner(
            new ConnectMutationPlanner(observations, digest),
            store,
            new FakeGovernance(runtime),
            new FakeRuntimePolicy(runtime));

        var result = await planner.PlanAsync(
            new ConnectAutoRestartPolicyRequest(
                "prod",
                "default",
                "sink-a",
                Enabled: false),
            "oidc:https://idp.example|alice");

        Assert.True(result.IsSuccess, result.Failure?.SafeMessage);
        Assert.Equal(
            MutationRiskClass.Moderate,
            result.Plan!.Risk.RiskClass);
        var target = Assert.Single(
            result.Plan.Intent.AuthorizationTargets!);
        Assert.Equal(
            AuthorizationAction.ConnectAutoRestartManage,
            target.Action);
        Assert.Equal("connector/sink-a", target.ResourceName);
        Assert.Equal(0, observations.ObservationCalls);
        Assert.Equal(
            active.ActivationId,
            result.Plan.Canonical.ExistingActivationId);
        Assert.Equal(
            active.Version,
            result.Plan.Canonical.ExistingActivationVersion);
    }

    [Fact]
    public async Task Preview_blocks_recognized_mm2_before_policy_admission()
    {
        var runtime = Runtime(enabled: true);
        var observations = new FakeObservationPort(
            Existing(
                "replicator",
                "FAILED",
                Configuration(
                    ("connector.class",
                        "org.apache.kafka.connect.mirror.MirrorSourceConnector"),
                    ("tasks.max", "1"))));

        using var digest = Digest();
        var planner = new ConnectAutoRestartPolicyPlanner(
            new ConnectMutationPlanner(observations, digest),
            new FakeStore(),
            new FakeGovernance(runtime),
            new FakeRuntimePolicy(runtime));

        var result = await planner.PlanAsync(
            new ConnectAutoRestartPolicyRequest(
                "prod",
                "default",
                "replicator",
                Enabled: true),
            "oidc:https://idp.example|alice");

        Assert.False(result.IsSuccess);
        Assert.Equal(
            ConnectAutoRestartPolicyPlanningFailureCode.ReplicationBlocked,
            result.Failure!.Code);
    }

    [Fact]
    public async Task Preview_fails_when_automation_principal_lacks_restart_authority()
    {
        var runtime = Runtime(enabled: true);
        var observations = new FakeObservationPort(
            Existing(
                "sink-a",
                "FAILED",
                Configuration(
                    ("connector.class", "org.example.SinkConnector"))));
        using var digest = Digest();

        var governance = new FakeGovernance(runtime)
        {
            AuthorizationAllowed = false,
        };

        var planner = new ConnectAutoRestartPolicyPlanner(
            new ConnectMutationPlanner(observations, digest),
            new FakeStore(),
            governance,
            new FakeRuntimePolicy(runtime));

        var result = await planner.PlanAsync(
            new ConnectAutoRestartPolicyRequest(
                "prod",
                "default",
                "sink-a",
                Enabled: true),
            "oidc:https://idp.example|alice");

        Assert.False(result.IsSuccess);
        Assert.Equal(
            ConnectAutoRestartPolicyPlanningFailureCode
                .AutomationAuthorizationDenied,
            result.Failure!.Code);
    }

    [Fact]
    public async Task Requested_policy_values_remain_bounded_by_hard_caps()
    {
        var runtime = Runtime(enabled: true);
        var observations = new FakeObservationPort(
            Existing(
                "sink-a",
                "FAILED",
                Configuration(
                    ("connector.class", "org.example.SinkConnector"))));
        using var digest = Digest();

        var planner = new ConnectAutoRestartPolicyPlanner(
            new ConnectMutationPlanner(observations, digest),
            new FakeStore(),
            new FakeGovernance(runtime),
            new FakeRuntimePolicy(runtime));

        var result = await planner.PlanAsync(
            new ConnectAutoRestartPolicyRequest(
                "prod",
                "default",
                "sink-a",
                Enabled: true,
                MaxAttempts:
                    ConnectAutoRestartPolicy.HardMaxAttempts + 1),
            "oidc:https://idp.example|alice");

        Assert.False(result.IsSuccess);
        Assert.Equal(
            ConnectAutoRestartPolicyPlanningFailureCode.InvalidInput,
            result.Failure!.Code);
    }

    private static ConnectAutoRestartRuntimePolicySnapshot Runtime(
        bool enabled) =>
        new(
            enabled,
            "w55-test",
            new ConnectAutoRestartPolicy(
                enabled,
                jitterBasisPoints: 0));

    private static ConnectAutoRestartActivation Activation(
        string profile,
        string connector,
        ConnectAutoRestartRuntimePolicySnapshot runtime)
    {
        var policy = runtime.Policy;
        return ConnectAutoRestartActivation.Create(
            new ConnectAutoRestartTarget(
                "prod",
                profile,
                connector,
                null),
            policy,
            Now,
            Fingerprint($"provider:{profile}"),
            Fingerprint($"config:{connector}"),
            ConnectAutoRestartPolicyFingerprint.ComputeEffective(
                runtime,
                policy),
            "oidc:https://idp.example|alice",
            Guid.NewGuid());
    }

    private static HmacMutationMaterialDigestService Digest() =>
        new("0123456789abcdef0123456789abcdef");

    private static IReadOnlyList<ConnectConfigurationObservationItem>
        Configuration(
            params (string Key, string Value)[] items) =>
        items
            .OrderBy(item => item.Key, StringComparer.Ordinal)
            .Select(item =>
            {
                var hash = ConnectMutationCanonicalization.Sha256(
                    Encoding.UTF8.GetBytes(item.Value));
                var safe =
                    ConnectSafeConfigurationPolicy
                        .IsExplicitlySafeConfigKey(item.Key) &&
                    !ConnectSafeConfigurationPolicy
                        .IsSecretKey(item.Key)
                        ? item.Value
                        : "[REDACTED]";

                return new ConnectConfigurationObservationItem(
                    item.Key,
                    hash,
                    safe);
            })
            .ToArray();

    private static ConnectMutationObservation Existing(
        string connector,
        string state,
        IReadOnlyList<ConnectConfigurationObservationItem> config)
    {
        var builder = new StringBuilder();
        foreach (var item in config.OrderBy(
                     item => item.Key,
                     StringComparer.Ordinal))
        {
            builder.Append(item.Key)
                .Append('=')
                .Append(Convert.ToBase64String(
                    Encoding.UTF8.GetBytes(item.ValueSha256)))
                .Append('\n');
        }

        return new ConnectMutationObservation(
            connector,
            true,
            state,
            Array.Empty<ConnectMutationTaskObservation>(),
            config,
            ConnectMutationCanonicalization.Sha256(
                Encoding.UTF8.GetBytes(builder.ToString())));
    }

    private static string Fingerprint(string value) =>
        Convert.ToHexString(
                SHA256.HashData(
                    Encoding.UTF8.GetBytes(value)))
            .ToLowerInvariant();

    private sealed class FakeRuntimePolicy :
        IConnectAutoRestartRuntimePolicyProvider
    {
        private readonly ConnectAutoRestartRuntimePolicySnapshot _runtime;

        public FakeRuntimePolicy(
            ConnectAutoRestartRuntimePolicySnapshot runtime)
        {
            _runtime = runtime;
        }

        public Task<ConnectAutoRestartRuntimePolicySnapshot> GetCurrentAsync(
            ConnectAutoRestartTarget target,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(_runtime);
    }

    private sealed class FakeGovernance :
        IConnectAutoRestartGovernancePort
    {
        private readonly ConnectAutoRestartRuntimePolicySnapshot _runtime;

        public FakeGovernance(
            ConnectAutoRestartRuntimePolicySnapshot runtime)
        {
            _runtime = runtime;
        }

        public bool AuthorizationAllowed { get; set; } = true;

        public Task<ConnectAutoRestartGovernanceSnapshot> GetCurrentAsync(
            ConnectAutoRestartActivation activation,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(
                new ConnectAutoRestartGovernanceSnapshot(
                    AuthorizationAllowed,
                    Fingerprint(
                        $"provider:{activation.Target.ConnectProfileId}"),
                    ConnectAutoRestartPolicyFingerprint.ComputeEffective(
                        _runtime,
                        activation.Policy),
                    AuthorizationAllowed
                        ? null
                        : "auto_restart_authorization_revoked"));
    }

    private sealed class FakeStore :
        IConnectAutoRestartStateStore
    {
        public ConnectAutoRestartActivation? Active { get; set; }

        public Task InitializeAsync(
            CancellationToken cancellationToken = default) =>
            Task.CompletedTask;

        public Task<ConnectAutoRestartActivation?> GetAsync(
            Guid activationId,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(
                Active?.ActivationId == activationId
                    ? Active
                    : null);

        public Task<ConnectAutoRestartActivation?> GetActiveByTargetAsync(
            ConnectAutoRestartTarget target,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(
                Active?.Target == target
                    ? Active
                    : null);

        public Task<bool> TryCreateAsync(
            ConnectAutoRestartActivation activation,
            int aggregateProfileLimit,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<bool> TryUpdateAsync(
            ConnectAutoRestartActivation activation,
            long expectedVersion,
            ConnectAutoRestartLease lease,
            DateTimeOffset now,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<ConnectAutoRestartLease?> TryAcquireLeaseAsync(
            Guid activationId,
            string ownerId,
            DateTimeOffset now,
            TimeSpan leaseTtl,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<bool> ReleaseLeaseAsync(
            ConnectAutoRestartLease lease,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<IReadOnlyList<ConnectAutoRestartActivation>>
            ListActiveAsync(
                string clusterId,
                string connectProfileId,
                int limit,
                CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();
    }

    private sealed class FakeObservationPort :
        IConnectMutationObservationPort
    {
        public FakeObservationPort(
            ConnectMutationObservation current)
        {
            Current = current;
        }

        public ConnectMutationObservation Current { get; set; }
        public int ObservationCalls { get; private set; }

        public Task<ConnectMutationObservationResult<ConnectMutationCapabilities>>
            GetCapabilitiesAsync(
                string clusterId,
                ReadViewOperationContext operation,
                CancellationToken cancellationToken) =>
            GetCapabilitiesAsync(
                clusterId,
                "default",
                operation,
                cancellationToken);

        public Task<ConnectMutationObservationResult<ConnectMutationCapabilities>>
            GetCapabilitiesAsync(
                string clusterId,
                string connectProfileId,
                ReadViewOperationContext operation,
                CancellationToken cancellationToken) =>
            Task.FromResult(
                ConnectMutationObservationResult<ConnectMutationCapabilities>
                    .Success(
                        new ConnectMutationCapabilities(
                            SupportsCreate: true,
                            SupportsUpdate: true,
                            SupportsPause: true,
                            SupportsResume: true,
                            SupportsRestart: true,
                            SupportsTaskRestart: true,
                            SupportsDelete: true)));

        public Task<ConnectMutationObservationResult<ConnectMutationObservation>>
            ObserveConnectorAsync(
                string clusterId,
                string connectorName,
                ReadViewOperationContext operation,
                CancellationToken cancellationToken) =>
            ObserveConnectorAsync(
                clusterId,
                "default",
                connectorName,
                operation,
                cancellationToken);

        public Task<ConnectMutationObservationResult<ConnectMutationObservation>>
            ObserveConnectorAsync(
                string clusterId,
                string connectProfileId,
                string connectorName,
                ReadViewOperationContext operation,
                CancellationToken cancellationToken)
        {
            ObservationCalls++;
            return Task.FromResult(
                ConnectMutationObservationResult<ConnectMutationObservation>
                    .Success(Current));
        }
    }

    private sealed class FixedTimeProvider : TimeProvider
    {
        private readonly DateTimeOffset _now;

        public FixedTimeProvider(DateTimeOffset now)
        {
            _now = now;
        }

        public override DateTimeOffset GetUtcNow() => _now;
    }
}
