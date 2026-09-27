using System.Security.Cryptography;
using System.Text;
using Kafdeck.Core.Ecosystem;
using Kafdeck.Core.ReadViews;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Connect;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V07W55ConnectAutoRestartW54BridgeTests
{
    private static readonly DateTimeOffset Now =
        new(2026, 9, 27, 9, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task Revalidator_blocks_mm2_restart_on_alternate_profile()
    {
        var observations = new FakeObservationPort(
            Existing(
                "replicator",
                "FAILED",
                Configuration(
                    ("connector.class",
                        "org.apache.kafka.connect.mirror.MirrorSourceConnector"),
                    ("tasks.max", "1"))));

        using var digest = Digest();
        var planner = new ConnectMutationPlanner(
            observations,
            digest,
            timeProvider: new FixedTimeProvider(Now));
        var validator =
            new ConnectMutationPreconditionValidator(planner);

        var protectedObservation = await planner.ObserveAsync(
            "prod",
            "analytics",
            "replicator",
            CancellationToken.None);
        Assert.True(protectedObservation.IsSuccess);

        var activation = Activation(
            "analytics",
            "replicator",
            protectedObservation.Value!.ConfigurationFingerprint);

        var revalidator =
            new ConnectAutoRestartAttemptRevalidator(
                planner,
                validator,
                new FakeGovernance(
                    authorizationAllowed: true,
                    activation.ProviderIdentityFingerprint,
                    activation.PolicyFingerprint),
                new FixedTimeProvider(Now));

        var result = await revalidator.RevalidateAsync(
            activation);

        Assert.Equal(
            ConnectAutoRestartRevalidationOutcome
                .ReplicationGuardBlocked,
            result.Outcome);
        Assert.Contains(
            "replication",
            result.Code,
            StringComparison.OrdinalIgnoreCase);
        Assert.All(
            observations.ObservedProfiles,
            profile => Assert.Equal("analytics", profile));
    }

    [Fact]
    public async Task Revalidator_stops_on_provider_profile_identity_drift_before_provider_observation()
    {
        var observations = new FakeObservationPort(
            Existing(
                "sink-a",
                "FAILED",
                Configuration(
                    ("connector.class", "org.example.SinkConnector"))));
        using var digest = Digest();
        var planner = new ConnectMutationPlanner(observations, digest);
        var activation = Activation(
            "analytics",
            "sink-a",
            Fingerprint("config"));

        var revalidator = new ConnectAutoRestartAttemptRevalidator(
            planner,
            new ConnectMutationPreconditionValidator(planner),
            new FakeGovernance(
                authorizationAllowed: true,
                Fingerprint("different-provider-profile"),
                activation.PolicyFingerprint));

        var result = await revalidator.RevalidateAsync(activation);

        Assert.Equal(
            ConnectAutoRestartRevalidationOutcome.ProviderIdentityDrift,
            result.Outcome);
        Assert.Equal("auto_restart_provider_identity_drift", result.Code);
        Assert.Empty(observations.ObservedProfiles);
    }

    [Fact]
    public async Task Revalidator_stops_on_effective_policy_drift_before_provider_observation()
    {
        var observations = new FakeObservationPort(
            Existing(
                "sink-a",
                "FAILED",
                Configuration(
                    ("connector.class", "org.example.SinkConnector"))));
        using var digest = Digest();
        var planner = new ConnectMutationPlanner(observations, digest);
        var activation = Activation(
            "default",
            "sink-a",
            Fingerprint("config"));

        var revalidator = new ConnectAutoRestartAttemptRevalidator(
            planner,
            new ConnectMutationPreconditionValidator(planner),
            new FakeGovernance(
                authorizationAllowed: true,
                activation.ProviderIdentityFingerprint,
                Fingerprint("different-effective-policy")));

        var result = await revalidator.RevalidateAsync(activation);

        Assert.Equal(
            ConnectAutoRestartRevalidationOutcome.PolicyDrift,
            result.Outcome);
        Assert.Equal("auto_restart_policy_drift", result.Code);
        Assert.Empty(observations.ObservedProfiles);
    }

    [Fact]
    public async Task Revalidator_stops_on_configuration_drift_before_restart_planning()
    {
        var observations = new FakeObservationPort(
            Existing(
                "sink-a",
                "FAILED",
                Configuration(
                    ("connector.class", "org.example.SinkConnector"),
                    ("tasks.max", "1"))));

        using var digest = Digest();
        var planner = new ConnectMutationPlanner(
            observations,
            digest);
        var validator =
            new ConnectMutationPreconditionValidator(planner);

        var activation = Activation(
            "default",
            "sink-a",
            Fingerprint("stale-config"));

        var revalidator =
            new ConnectAutoRestartAttemptRevalidator(
                planner,
                validator,
                new FakeGovernance(
                    authorizationAllowed: true,
                    activation.ProviderIdentityFingerprint,
                    activation.PolicyFingerprint));

        var result = await revalidator.RevalidateAsync(
            activation);

        Assert.Equal(
            ConnectAutoRestartRevalidationOutcome.ConfigurationDrift,
            result.Outcome);
        Assert.Equal(
            "auto_restart_connector_configuration_drift",
            result.Code);
    }

    [Fact]
    public async Task Revalidator_marks_non_failed_target_recovered_without_dispatch()
    {
        var observations = new FakeObservationPort(
            Existing(
                "sink-a",
                "RUNNING",
                Configuration(
                    ("connector.class", "org.example.SinkConnector"),
                    ("tasks.max", "1"))));

        using var digest = Digest();
        var planner = new ConnectMutationPlanner(
            observations,
            digest);

        var observed = await planner.ObserveAsync(
            "prod",
            "default",
            "sink-a",
            CancellationToken.None);
        Assert.True(observed.IsSuccess);

        var activation = Activation(
            "default",
            "sink-a",
            observed.Value!.ConfigurationFingerprint);

        var revalidator =
            new ConnectAutoRestartAttemptRevalidator(
                planner,
                new ConnectMutationPreconditionValidator(planner),
                new FakeGovernance(
                    authorizationAllowed: true,
                    activation.ProviderIdentityFingerprint,
                    activation.PolicyFingerprint));

        var result = await revalidator.RevalidateAsync(
            activation);

        Assert.Equal(
            ConnectAutoRestartRevalidationOutcome.Recovered,
            result.Outcome);
        Assert.Equal(
            "auto_restart_connector_recovered",
            result.Code);
    }

    [Fact]
    public async Task Typed_dispatcher_blocks_mm2_without_calling_mutation_port()
    {
        var observations = new FakeObservationPort(
            Existing(
                "replicator",
                "FAILED",
                Configuration(
                    ("connector.class",
                        "org.apache.kafka.connect.mirror.MirrorCheckpointConnector"),
                    ("tasks.max", "1"))));
        using var digest = Digest();
        var planner = new ConnectMutationPlanner(
            observations,
            digest);
        var validator =
            new ConnectMutationPreconditionValidator(planner);
        var mutations = new FakeMutationPort();
        var execution =
            new ConnectMutationExecutionService(
                mutations,
                observations,
                digest);

        var dispatcher =
            new ConnectAutoRestartTypedDispatchAdapter(
                planner,
                validator,
                execution,
                new FixedTimeProvider(Now));

        var result = await dispatcher.RestartAsync(
            new ConnectAutoRestartDispatchRequest(
                Guid.NewGuid(),
                Guid.NewGuid(),
                new ConnectAutoRestartTarget(
                    "prod",
                    "analytics",
                    "replicator",
                    null),
                "oidc:https://idp.example|kafdeck-auto-restart"));

        Assert.Equal(
            ConnectAutoRestartDispatchOutcome.Blocked,
            result.Outcome);
        Assert.Equal(0, mutations.ControlCalls);
    }

    [Fact]
    public async Task Typed_dispatcher_uses_W54_profile_scoped_control_path()
    {
        var observations = new FakeObservationPort(
            Existing(
                "sink-a",
                "FAILED",
                Configuration(
                    ("connector.class", "org.example.SinkConnector"),
                    ("tasks.max", "1"))));
        using var digest = Digest();
        var planner = new ConnectMutationPlanner(
            observations,
            digest);
        var validator =
            new ConnectMutationPreconditionValidator(planner);
        var mutations = new FakeMutationPort
        {
            ControlResult = new MutationProviderResult(
                MutationExecutionResultKind.AppliedUnverified,
                "connect_restart_accepted"),
        };
        var execution =
            new ConnectMutationExecutionService(
                mutations,
                observations,
                digest);

        var dispatcher =
            new ConnectAutoRestartTypedDispatchAdapter(
                planner,
                validator,
                execution,
                new FixedTimeProvider(Now));

        var result = await dispatcher.RestartAsync(
            new ConnectAutoRestartDispatchRequest(
                Guid.NewGuid(),
                Guid.NewGuid(),
                new ConnectAutoRestartTarget(
                    "prod",
                    "analytics",
                    "sink-a",
                    null),
                "oidc:https://idp.example|kafdeck-auto-restart"));

        Assert.Equal(
            ConnectAutoRestartDispatchOutcome.Accepted,
            result.Outcome);
        Assert.Equal(1, mutations.ControlCalls);
        Assert.NotNull(mutations.LastControl);
        Assert.Equal(
            "analytics",
            mutations.LastControl!.ConnectProfileId);
        Assert.Equal(
            ConnectControlAction.Restart,
            mutations.LastControl.Action);
    }

    private static ConnectAutoRestartActivation Activation(
        string profile,
        string connector,
        string configFingerprint) =>
        ConnectAutoRestartActivation.Create(
            new ConnectAutoRestartTarget(
                "prod",
                profile,
                connector,
                null),
            new ConnectAutoRestartPolicy(
                enabled: true),
            Now,
            Fingerprint($"provider:{profile}"),
            configFingerprint,
            Fingerprint("policy"),
            "oidc:https://idp.example|kafdeck-auto-restart",
            Guid.NewGuid());

    private static HmacMutationMaterialDigestService Digest() =>
        new("0123456789abcdef0123456789abcdef");

    private static IReadOnlyList<ConnectConfigurationObservationItem>
        Configuration(
            params (string Key, string Value)[] items) =>
        items
            .OrderBy(item => item.Key, StringComparer.Ordinal)
            .Select(item =>
            {
                var hash =
                    ConnectMutationCanonicalization.Sha256(
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

    private sealed class FakeGovernance :
        IConnectAutoRestartGovernancePort
    {
        private readonly ConnectAutoRestartGovernanceSnapshot _snapshot;

        public FakeGovernance(
            bool authorizationAllowed,
            string providerIdentityFingerprint,
            string policyFingerprint)
        {
            _snapshot = new(
                authorizationAllowed,
                providerIdentityFingerprint,
                policyFingerprint,
                authorizationAllowed
                    ? null
                    : "auto_restart_authorization_revoked");
        }

        public Task<ConnectAutoRestartGovernanceSnapshot> GetCurrentAsync(
            ConnectAutoRestartActivation activation,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(_snapshot);
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
        public List<string> ObservedProfiles { get; } = [];

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
            ObservedProfiles.Add(connectProfileId);
            return Task.FromResult(
                ConnectMutationObservationResult<ConnectMutationObservation>
                    .Success(Current));
        }
    }

    private sealed class FakeMutationPort : IConnectMutationPort
    {
        public int ControlCalls { get; private set; }
        public ConnectControlMutation? LastControl { get; private set; }

        public MutationProviderResult ControlResult { get; set; } =
            new(
                MutationExecutionResultKind.AppliedUnverified,
                "connect_restart_accepted");

        public Task<MutationProviderResult> CreateAsync(
            ConnectCreateMutation request,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(
                new MutationProviderResult(
                    MutationExecutionResultKind.FailedDefinitive,
                    "not_used"));

        public Task<MutationProviderResult> AlterAsync(
            ConnectAlterMutation request,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(
                new MutationProviderResult(
                    MutationExecutionResultKind.FailedDefinitive,
                    "not_used"));

        public Task<MutationProviderResult> ControlAsync(
            ConnectControlMutation request,
            CancellationToken cancellationToken = default)
        {
            ControlCalls++;
            LastControl = request;
            return Task.FromResult(ControlResult);
        }

        public Task<MutationProviderResult> DeleteAsync(
            ConnectDeleteMutation request,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(
                new MutationProviderResult(
                    MutationExecutionResultKind.FailedDefinitive,
                    "not_used"));
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
