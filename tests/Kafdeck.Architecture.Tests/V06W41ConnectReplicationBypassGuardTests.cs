using System.Text;
using Kafdeck.Core.Ecosystem;
using Kafdeck.Core.ReadViews;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Connect;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V06W41ConnectReplicationBypassGuardTests
{
    private static readonly DateTimeOffset Now =
        new(2026, 9, 24, 12, 30, 0, TimeSpan.Zero);

    [Theory]
    [InlineData("org.apache.kafka.connect.mirror.MirrorSourceConnector")]
    [InlineData("MirrorSourceConnector")]
    [InlineData("MirrorSource")]
    [InlineData("org.apache.kafka.connect.mirror.MirrorCheckpointConnector")]
    [InlineData("MirrorCheckpointConnector")]
    [InlineData("MirrorCheckpoint")]
    [InlineData("org.apache.kafka.connect.mirror.MirrorHeartbeatConnector")]
    [InlineData("MirrorHeartbeatConnector")]
    [InlineData("MirrorHeartbeat")]
    public async Task Existing_connect_create_cannot_activate_recognized_mm2_connector(
        string connectorClass)
    {
        var observations = new FakeObservationPort(Missing("replicator"));
        using var digest = Digest();
        var planner = new ConnectMutationPlanner(
            observations,
            digest,
            timeProvider: new FixedTimeProvider(Now));

        var plan = await planner.PlanCreateAsync(
            new ConnectCreateRequest(
                "prod",
                "replicator",
                RequestConfiguration(connectorClass, "1")));

        Assert.True(plan.IsSuccess, plan.Failure?.SafeMessage);
        using var material = plan.ExecutionMaterial!;
        var operation = CreateOperation(
            plan.Plan!.Intent,
            plan.Plan.Risk,
            "mm2-create");

        var result = await new ConnectMutationPreconditionValidator(planner)
            .ValidateAsync(operation.Snapshot);

        Assert.Equal(
            MutationPreDispatchGuardOutcome.CapabilityUnsupported,
            result.Outcome);
        Assert.Equal(
            "connect_create_replication_managed_activation_not_admitted",
            result.ResultCode);
    }

    [Fact]
    public async Task Existing_connect_update_cannot_restart_mm2_data_movement()
    {
        var current = Existing(
            "replicator",
            Configuration(
                ("connector.class", "org.apache.kafka.connect.mirror.MirrorSourceConnector"),
                ("tasks.max", "1")));
        var observations = new FakeObservationPort(current);
        using var digest = Digest();
        var planner = new ConnectMutationPlanner(observations, digest);

        var plan = await planner.PlanUpdateAsync(
            new ConnectUpdateRequest(
                "prod",
                "replicator",
                RequestConfiguration(
                    "org.apache.kafka.connect.mirror.MirrorSourceConnector",
                    "2")));

        Assert.True(plan.IsSuccess, plan.Failure?.SafeMessage);
        using var material = plan.ExecutionMaterial!;
        var operation = CreateOperation(
            plan.Plan!.Intent,
            plan.Plan.Risk,
            "mm2-update");

        var result = await new ConnectMutationPreconditionValidator(planner)
            .ValidateAsync(operation.Snapshot);

        Assert.Equal(
            MutationPreDispatchGuardOutcome.CapabilityUnsupported,
            result.Outcome);
        Assert.Equal(
            "connect_update_replication_managed_activation_not_admitted",
            result.ResultCode);
    }

    [Theory]
    [InlineData(ConnectControlAction.Resume, null)]
    [InlineData(ConnectControlAction.Restart, null)]
    [InlineData(ConnectControlAction.Restart, 3)]
    public async Task Existing_connect_control_cannot_resume_or_restart_mm2(
        ConnectControlAction action,
        int? taskId)
    {
        var current = Existing(
            "replicator",
            Configuration(
                ("connector.class", "org.apache.kafka.connect.mirror.MirrorCheckpointConnector"),
                ("tasks.max", "1")),
            state: action == ConnectControlAction.Resume ? "PAUSED" : "RUNNING",
            tasks: taskId.HasValue
                ? new[] { new ConnectMutationTaskObservation(taskId.Value, "RUNNING") }
                : Array.Empty<ConnectMutationTaskObservation>());
        var observations = new FakeObservationPort(current);
        using var digest = Digest();
        var planner = new ConnectMutationPlanner(observations, digest);

        var plan = await planner.PlanControlAsync(
            new ConnectControlRequest(
                "prod",
                "replicator",
                action,
                taskId));

        Assert.True(plan.IsSuccess, plan.Failure?.SafeMessage);
        var operation = CreateOperation(
            plan.Plan!.Intent,
            plan.Plan.Risk,
            $"mm2-control-{action}-{taskId}");

        var result = await new ConnectMutationPreconditionValidator(planner)
            .ValidateAsync(operation.Snapshot);

        Assert.Equal(
            MutationPreDispatchGuardOutcome.CapabilityUnsupported,
            result.Outcome);
        Assert.Equal(
            action == ConnectControlAction.Resume
                ? "connect_resume_replication_managed_activation_not_admitted"
                : "connect_restart_replication_managed_activation_not_admitted",
            result.ResultCode);
    }

    [Fact]
    public async Task Pause_remains_available_for_recognized_mm2_because_it_cannot_start_movement()
    {
        var observations = new FakeObservationPort(
            Existing(
                "replicator",
                Configuration(
                    ("connector.class", "org.apache.kafka.connect.mirror.MirrorHeartbeatConnector"),
                    ("tasks.max", "1")),
                state: "RUNNING"));
        using var digest = Digest();
        var planner = new ConnectMutationPlanner(observations, digest);

        var plan = await planner.PlanControlAsync(
            new ConnectControlRequest(
                "prod",
                "replicator",
                ConnectControlAction.Pause));
        Assert.True(plan.IsSuccess, plan.Failure?.SafeMessage);

        var operation = CreateOperation(
            plan.Plan!.Intent,
            plan.Plan.Risk,
            "mm2-pause");
        var result = await new ConnectMutationPreconditionValidator(planner)
            .ValidateAsync(operation.Snapshot);

        Assert.Equal(MutationPreDispatchGuardOutcome.Allowed, result.Outcome);
    }

    [Fact]
    public async Task Ordinary_non_replication_connect_create_remains_on_v05_path()
    {
        var observations = new FakeObservationPort(Missing("sink-a"));
        using var digest = Digest();
        var planner = new ConnectMutationPlanner(observations, digest);

        var plan = await planner.PlanCreateAsync(
            new ConnectCreateRequest(
                "prod",
                "sink-a",
                RequestConfiguration("org.example.SinkConnector", "1")));
        Assert.True(plan.IsSuccess, plan.Failure?.SafeMessage);
        using var material = plan.ExecutionMaterial!;

        var operation = CreateOperation(
            plan.Plan!.Intent,
            plan.Plan.Risk,
            "generic-create");
        var result = await new ConnectMutationPreconditionValidator(planner)
            .ValidateAsync(operation.Snapshot);

        Assert.Equal(MutationPreDispatchGuardOutcome.Allowed, result.Outcome);
    }

    [Fact]
    public async Task Activation_without_safe_connector_classification_fails_closed()
    {
        var observations = new FakeObservationPort(
            Existing(
                "connector-a",
                Configuration(("tasks.max", "1")),
                state: "PAUSED"));
        using var digest = Digest();
        var planner = new ConnectMutationPlanner(observations, digest);

        var plan = await planner.PlanControlAsync(
            new ConnectControlRequest(
                "prod",
                "connector-a",
                ConnectControlAction.Resume));
        Assert.True(plan.IsSuccess, plan.Failure?.SafeMessage);

        var operation = CreateOperation(
            plan.Plan!.Intent,
            plan.Plan.Risk,
            "unclassified-resume");
        var result = await new ConnectMutationPreconditionValidator(planner)
            .ValidateAsync(operation.Snapshot);

        Assert.Equal(
            MutationPreDispatchGuardOutcome.CapabilityUnsupported,
            result.Outcome);
        Assert.Equal(
            "connect_resume_replication_classification_unavailable",
            result.ResultCode);
    }

    private static MutationOperation CreateOperation(
        MutationIntentDescriptor intent,
        MutationRiskDecision risk,
        string idempotencyKey) =>
        MutationOperation.CreatePreview(
            "oidc:https://idp.example|alice",
            intent,
            risk,
            "v0.6-w41-replication-bypass-guard",
            Now.AddMinutes(5),
            Now,
            idempotencyKey);

    private static HmacMutationMaterialDigestService Digest() =>
        new("0123456789abcdef0123456789abcdef");

    private static IReadOnlyDictionary<string, string> RequestConfiguration(
        string connectorClass,
        string tasksMax) =>
        new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["connector.class"] = connectorClass,
            ["tasks.max"] = tasksMax,
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
                    ConnectSafeConfigurationPolicy.IsExplicitlySafeConfigKey(item.Key) &&
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
        string state = "RUNNING",
        IReadOnlyList<ConnectMutationTaskObservation>? tasks = null) =>
        new(
            connectorName,
            true,
            state,
            tasks ?? Array.Empty<ConnectMutationTaskObservation>(),
            configuration,
            ObservationFingerprint(configuration));

    private static ConnectMutationObservation Missing(string connectorName)
    {
        var configuration = Array.Empty<ConnectConfigurationObservationItem>();
        return new ConnectMutationObservation(
            connectorName,
            false,
            "MISSING",
            Array.Empty<ConnectMutationTaskObservation>(),
            configuration,
            ObservationFingerprint(configuration));
    }

    private static string ObservationFingerprint(
        IEnumerable<ConnectConfigurationObservationItem> values)
    {
        var builder = new StringBuilder();
        foreach (var item in values.OrderBy(value => value.Key, StringComparer.Ordinal))
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

    private sealed class FakeObservationPort : IConnectMutationObservationPort
    {
        public FakeObservationPort(ConnectMutationObservation current)
        {
            Current = current;
        }

        public ConnectMutationObservation Current { get; set; }

        public Task<ConnectMutationObservationResult<ConnectMutationCapabilities>>
            GetCapabilitiesAsync(
                string clusterId,
                ReadViewOperationContext operation,
                CancellationToken cancellationToken) =>
            Task.FromResult(
                ConnectMutationObservationResult<ConnectMutationCapabilities>.Success(
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
            Task.FromResult(
                ConnectMutationObservationResult<ConnectMutationObservation>.Success(
                    Current));
    }

    private sealed class FixedTimeProvider : TimeProvider
    {
        private readonly DateTimeOffset _utcNow;

        public FixedTimeProvider(DateTimeOffset utcNow)
        {
            _utcNow = utcNow;
        }

        public override DateTimeOffset GetUtcNow() => _utcNow;
    }
}
