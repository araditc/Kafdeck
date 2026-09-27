using System.Text;
using Kafdeck.Core.Ecosystem;
using Kafdeck.Core.ReadViews;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Connect;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V07W54ConnectProfilePlanningTests
{
    private static readonly DateTimeOffset Now =
        new(2026, 9, 27, 4, 30, 0, TimeSpan.Zero);

    [Fact]
    public async Task Non_default_profile_is_bound_into_canonical_resource_and_authorization_identity()
    {
        var observations = new ProfileAwareObservationPort(
            Missing("sink-a"));
        using var digest = Digest();
        var planner = new ConnectMutationPlanner(
            observations,
            digest,
            timeProvider: new FixedTimeProvider(Now));

        var planning = await planner.PlanCreateAsync(
            new ConnectCreateRequest(
                "prod",
                "sink-a",
                RequestConfiguration("org.example.SinkConnector"),
                "analytics"));

        Assert.True(planning.IsSuccess, planning.Failure?.SafeMessage);
        using var material = planning.ExecutionMaterial!;
        var plan = planning.Plan!;
        Assert.Equal("analytics", plan.Canonical.ConnectProfileId);
        Assert.Equal(
            "cluster/prod/connect-profile/analytics/connector/sink-a",
            Assert.Single(plan.Intent.ResourceKeys));
        Assert.Equal(
            "connect-profile/analytics/connector/sink-a",
            Assert.Single(plan.Intent.AuthorizationTargets).ResourceName);
        Assert.All(
            observations.CapabilityProfiles,
            profile => Assert.Equal("analytics", profile));
        Assert.All(
            observations.ObservationProfiles,
            profile => Assert.Equal("analytics", profile));
    }

    [Fact]
    public async Task Pre_dispatch_revalidation_observes_the_exact_planned_profile()
    {
        var observations = new ProfileAwareObservationPort(
            Missing("sink-a"));
        using var digest = Digest();
        var planner = new ConnectMutationPlanner(
            observations,
            digest,
            timeProvider: new FixedTimeProvider(Now));

        var planning = await planner.PlanCreateAsync(
            new ConnectCreateRequest(
                "prod",
                "sink-a",
                RequestConfiguration("org.example.SinkConnector"),
                "analytics"));
        Assert.True(planning.IsSuccess, planning.Failure?.SafeMessage);
        using var material = planning.ExecutionMaterial!;

        var operation = MutationOperation.CreatePreview(
            "oidc:https://idp.example|alice",
            planning.Plan!.Intent,
            planning.Plan.Risk,
            "v0.7-w54-profile-binding",
            Now.AddMinutes(5),
            Now,
            "profile-create");

        observations.ObservationProfiles.Clear();

        var result = await new ConnectMutationPreconditionValidator(planner)
            .ValidateAsync(operation.Snapshot);

        Assert.Equal(
            MutationPreDispatchGuardOutcome.Allowed,
            result.Outcome);
        Assert.Equal(
            new[] { "analytics" },
            observations.ObservationProfiles);
    }

    [Fact]
    public async Task Mm2_activation_guard_cannot_be_bypassed_through_alternate_profile()
    {
        var observations = new ProfileAwareObservationPort(
            Existing(
                "replicator",
                Configuration(
                    ("connector.class", "org.apache.kafka.connect.mirror.MirrorSourceConnector"),
                    ("tasks.max", "1")),
                state: "PAUSED"));
        using var digest = Digest();
        var planner = new ConnectMutationPlanner(
            observations,
            digest,
            timeProvider: new FixedTimeProvider(Now));

        var planning = await planner.PlanControlAsync(
            new ConnectControlRequest(
                "prod",
                "replicator",
                ConnectControlAction.Resume,
                null,
                "analytics"));

        Assert.True(planning.IsSuccess, planning.Failure?.SafeMessage);
        var operation = MutationOperation.CreatePreview(
            "oidc:https://idp.example|alice",
            planning.Plan!.Intent,
            planning.Plan.Risk,
            "v0.7-w54-mm2-profile",
            Now.AddMinutes(5),
            Now,
            "mm2-profile-resume");

        observations.ObservationProfiles.Clear();

        var result = await new ConnectMutationPreconditionValidator(planner)
            .ValidateAsync(operation.Snapshot);

        Assert.Equal(
            MutationPreDispatchGuardOutcome.CapabilityUnsupported,
            result.Outcome);
        Assert.Equal(
            "connect_resume_replication_managed_activation_not_admitted",
            result.ResultCode);
        Assert.Equal(
            new[] { "analytics" },
            observations.ObservationProfiles);
    }

    [Fact]
    public void Default_profile_preserves_v05_resource_identity()
    {
        Assert.Equal(
            "cluster/prod/connect/sink-a",
            ConnectMutationCanonicalization.ResourceKey(
                "prod",
                "default",
                "sink-a"));
        Assert.Equal(
            "connector/sink-a",
            ConnectMutationCanonicalization.AuthorizationResource(
                "default",
                "sink-a"));

        Assert.Equal(
            ConnectMutationCanonicalization.ResourceKey("prod", "sink-a"),
            ConnectMutationCanonicalization.ResourceKey(
                "prod",
                "default",
                "sink-a"));
        Assert.Equal(
            ConnectMutationCanonicalization.AuthorizationResource("sink-a"),
            ConnectMutationCanonicalization.AuthorizationResource(
                "default",
                "sink-a"));
    }

    private static HmacMutationMaterialDigestService Digest() =>
        new("0123456789abcdef0123456789abcdef");

    private static IReadOnlyDictionary<string, string> RequestConfiguration(
        string connectorClass) =>
        new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["connector.class"] = connectorClass,
            ["tasks.max"] = "1",
        };

    private static IReadOnlyList<ConnectConfigurationObservationItem> Configuration(
        params (string Key, string Value)[] items) =>
        items
            .OrderBy(item => item.Key, StringComparer.Ordinal)
            .Select(item =>
            {
                var hash = ConnectMutationCanonicalization.Sha256(
                    Encoding.UTF8.GetBytes(item.Value));
                var safe =
                    ConnectSafeConfigurationPolicy.IsExplicitlySafeConfigKey(
                        item.Key) &&
                    !ConnectSafeConfigurationPolicy.IsSecretKey(item.Key)
                        ? item.Value
                        : "[REDACTED]";
                return new ConnectConfigurationObservationItem(
                    item.Key,
                    hash,
                    safe);
            })
            .ToArray();

    private static ConnectMutationObservation Existing(
        string connectorName,
        IReadOnlyList<ConnectConfigurationObservationItem> configuration,
        string state)
    {
        return new ConnectMutationObservation(
            connectorName,
            true,
            state,
            Array.Empty<ConnectMutationTaskObservation>(),
            configuration,
            ConfigurationFingerprint(configuration));
    }

    private static ConnectMutationObservation Missing(
        string connectorName)
    {
        var configuration =
            Array.Empty<ConnectConfigurationObservationItem>();
        return new ConnectMutationObservation(
            connectorName,
            false,
            "MISSING",
            Array.Empty<ConnectMutationTaskObservation>(),
            configuration,
            ConfigurationFingerprint(configuration));
    }

    private static string ConfigurationFingerprint(
        IEnumerable<ConnectConfigurationObservationItem> values)
    {
        var builder = new StringBuilder();
        foreach (var item in values.OrderBy(
                     value => value.Key,
                     StringComparer.Ordinal))
        {
            builder.Append(item.Key)
                .Append('=')
                .Append(Convert.ToBase64String(
                    Encoding.UTF8.GetBytes(item.ValueSha256)))
                .Append('\n');
        }

        return ConnectMutationCanonicalization.Sha256(
            Encoding.UTF8.GetBytes(builder.ToString()));
    }

    private sealed class ProfileAwareObservationPort :
        IConnectMutationObservationPort
    {
        public ProfileAwareObservationPort(
            ConnectMutationObservation current)
        {
            Current = current;
        }

        public ConnectMutationObservation Current { get; set; }
        public List<string> CapabilityProfiles { get; } = [];
        public List<string> ObservationProfiles { get; } = [];

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
                CancellationToken cancellationToken)
        {
            CapabilityProfiles.Add(connectProfileId);
            return Task.FromResult(
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
        }

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
            ObservationProfiles.Add(connectProfileId);
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
