using System.Text;
using System.Text.Json;
using Kafdeck.Core.Ecosystem;
using Kafdeck.Core.ReadViews;
using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Connect;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V05ConnectMutationTests
{
    private static readonly DateTimeOffset Now =
        new(2026, 9, 22, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task Create_preview_is_secret_safe_exact_and_digest_bound()
    {
        var observations = new FakeObservationPort(Missing("sink-a"));
        using var digest = new HmacMutationMaterialDigestService(
            "0123456789abcdef0123456789abcdef");
        var planner = new ConnectMutationPlanner(
            observations,
            digest,
            timeProvider: new FixedTimeProvider(Now));

        const string secret = "w37-super-secret";
        var result = await planner.PlanCreateAsync(
            new ConnectCreateRequest(
                "prod",
                "sink-a",
                new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["connector.class"] = "org.example.Sink",
                    ["tasks.max"] = "1",
                    ["db.password"] = secret,
                    ["plugin.opaque"] = "opaque-value",
                }));

        Assert.True(result.IsSuccess, result.Failure?.SafeMessage);
        Assert.NotNull(result.Plan);
        Assert.NotNull(result.ExecutionMaterial);
        using var material = result.ExecutionMaterial!;

        Assert.Equal(
            MutationRiskClass.Moderate,
            result.Plan!.Risk.RiskClass);
        Assert.Single(result.Plan.Intent.AuthorizationTargets!);
        Assert.Equal(
            AuthorizationAction.ConnectCreate,
            result.Plan.Intent.AuthorizationTargets![0].Action);
        Assert.Equal(
            "connector/sink-a",
            result.Plan.Intent.AuthorizationTargets[0].ResourceName);
        Assert.Single(result.Plan.Intent.MaterialDigests!);
        Assert.Equal(
            "connect/configuration",
            result.Plan.Intent.MaterialDigests![0].Name);

        var password = Assert.Single(
            result.Plan.Canonical.RequestedConfiguration,
            item => item.Key == "db.password");
        Assert.Equal("[REDACTED]", password.SafeValue);

        var opaque = Assert.Single(
            result.Plan.Canonical.RequestedConfiguration,
            item => item.Key == "plugin.opaque");
        Assert.Equal("[REDACTED]", opaque.SafeValue);

        var tasks = Assert.Single(
            result.Plan.Canonical.RequestedConfiguration,
            item => item.Key == "tasks.max");
        Assert.Equal("1", tasks.SafeValue);

        var durableCanonical = JsonSerializer.Serialize(result.Plan.Canonical);
        Assert.DoesNotContain(secret, durableCanonical, StringComparison.Ordinal);
        Assert.DoesNotContain(
            "opaque-value",
            durableCanonical,
            StringComparison.Ordinal);

        var operation = MutationOperation.CreatePreview(
            "oidc:https://idp.example|alice",
            result.Plan.Intent,
            result.Plan.Risk,
            "w37-test",
            Now.AddMinutes(5),
            Now,
            "connect-create");

        var durableSnapshot = JsonSerializer.Serialize(operation.Snapshot);
        Assert.DoesNotContain(secret, durableSnapshot, StringComparison.Ordinal);
        Assert.DoesNotContain(
            "opaque-value",
            durableSnapshot,
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task Planner_rejects_create_existing_and_update_missing()
    {
        using var digest = Digest();

        var existingPlanner = new ConnectMutationPlanner(
            new FakeObservationPort(Existing("sink-a")),
            digest);
        var create = await existingPlanner.PlanCreateAsync(
            new ConnectCreateRequest(
                "prod",
                "sink-a",
                BaseConfiguration()));

        Assert.False(create.IsSuccess);
        Assert.Equal(
            ConnectMutationPlanningFailureCode.ConnectorAlreadyExists,
            create.Failure!.Code);

        var missingPlanner = new ConnectMutationPlanner(
            new FakeObservationPort(Missing("sink-a")),
            digest);
        var update = await missingPlanner.PlanUpdateAsync(
            new ConnectUpdateRequest(
                "prod",
                "sink-a",
                BaseConfiguration()));

        Assert.False(update.IsSuccess);
        Assert.Equal(
            ConnectMutationPlanningFailureCode.ConnectorNotFound,
            update.Failure!.Code);
    }

    [Fact]
    public async Task Update_diff_is_sorted_deterministic_and_secret_safe()
    {
        var current = Existing(
            "sink-a",
            Configuration(
                ("connector.class", "org.example.Sink"),
                ("tasks.max", "1"),
                ("old.plugin.secret", "old-value")));
        var observations = new FakeObservationPort(current);
        using var digest = Digest();
        var planner = new ConnectMutationPlanner(observations, digest);

        const string newSecret = "new-private-value";
        var result = await planner.PlanUpdateAsync(
            new ConnectUpdateRequest(
                "prod",
                "sink-a",
                new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["connector.class"] = "org.example.Sink",
                    ["tasks.max"] = "2",
                    ["new.plugin.secret"] = newSecret,
                }));

        Assert.True(result.IsSuccess, result.Failure?.SafeMessage);
        using var material = result.ExecutionMaterial!;

        Assert.Equal(
            new[] { "new.plugin.secret", "old.plugin.secret", "tasks.max" },
            result.Plan!.Canonical.Diff.Select(item => item.Key));
        Assert.Equal(
            new[]
            {
                ConnectConfigurationChangeKind.Added,
                ConnectConfigurationChangeKind.Removed,
                ConnectConfigurationChangeKind.Changed,
            },
            result.Plan.Canonical.Diff.Select(item => item.ChangeKind));

        var added = result.Plan.Canonical.Diff[0];
        Assert.Equal("[REDACTED]", added.RequestedSafeValue);
        var removed = result.Plan.Canonical.Diff[1];
        Assert.Equal("[REDACTED]", removed.CurrentSafeValue);

        var canonicalJson = JsonSerializer.Serialize(result.Plan.Canonical);
        Assert.DoesNotContain(newSecret, canonicalJson, StringComparison.Ordinal);
        Assert.DoesNotContain("old-value", canonicalJson, StringComparison.Ordinal);

        Assert.Equal(
            MutationRiskClass.Moderate,
            result.Plan.Risk.RiskClass);
        Assert.Single(result.Plan.Intent.AuthorizationTargets!);
        Assert.Equal(
            AuthorizationAction.ConnectAlter,
            result.Plan.Intent.AuthorizationTargets![0].Action);
    }

    [Fact]
    public async Task Control_planning_binds_exact_task_and_rejects_unknown_task()
    {
        var observation = Existing(
            "sink-a",
            tasks:
            [
                new ConnectMutationTaskObservation(3, "RUNNING"),
            ]);
        var observations = new FakeObservationPort(observation);
        using var digest = Digest();
        var planner = new ConnectMutationPlanner(observations, digest);

        var exact = await planner.PlanControlAsync(
            new ConnectControlRequest(
                "prod",
                "sink-a",
                ConnectControlAction.Restart,
                TaskId: 3));

        Assert.True(exact.IsSuccess, exact.Failure?.SafeMessage);
        Assert.Equal(3, exact.Plan!.Canonical.TaskId);
        Assert.Equal("RUNNING", exact.Plan.Canonical.CurrentTaskState);
        Assert.Equal(
            MutationRiskClass.Moderate,
            exact.Plan.Risk.RiskClass);
        Assert.Equal(
            AuthorizationAction.ConnectAlter,
            Assert.Single(exact.Plan.Intent.AuthorizationTargets!).Action);

        var missing = await planner.PlanControlAsync(
            new ConnectControlRequest(
                "prod",
                "sink-a",
                ConnectControlAction.Restart,
                TaskId: 4));

        Assert.False(missing.IsSuccess);
        Assert.Equal(
            ConnectMutationPlanningFailureCode.TaskNotFound,
            missing.Failure!.Code);
    }

    [Fact]
    public async Task Delete_has_high_floor_and_exact_authorization()
    {
        var observations = new FakeObservationPort(Existing("sink-a"));
        using var digest = Digest();
        var planner = new ConnectMutationPlanner(observations, digest);

        var result = await planner.PlanDeleteAsync(
            new ConnectDeleteRequest("prod", "sink-a"));

        Assert.True(result.IsSuccess, result.Failure?.SafeMessage);
        Assert.Equal(MutationRiskClass.High, result.Plan!.Risk.RiskClass);
        Assert.Equal(
            MutationConfirmationMode.TypedTarget,
            result.Plan.Risk.ConfirmationMode);
        var auth = Assert.Single(result.Plan.Intent.AuthorizationTargets!);
        Assert.Equal(AuthorizationAction.ConnectDelete, auth.Action);
        Assert.Equal("connector/sink-a", auth.ResourceName);
    }

    [Fact]
    public async Task Pre_dispatch_rejects_connector_state_drift()
    {
        var initial = Existing(
            "sink-a",
            state: "RUNNING",
            configuration: Configuration(
                ("connector.class", "org.example.Sink"),
                ("tasks.max", "1")));
        var observations = new FakeObservationPort(initial);
        using var digest = Digest();
        var planner = new ConnectMutationPlanner(observations, digest);

        var plan = await planner.PlanUpdateAsync(
            new ConnectUpdateRequest(
                "prod",
                "sink-a",
                new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["connector.class"] = "org.example.Sink",
                    ["tasks.max"] = "2",
                }));
        Assert.True(plan.IsSuccess, plan.Failure?.SafeMessage);
        using var material = plan.ExecutionMaterial!;

        var operation = MutationOperation.CreatePreview(
            "oidc:https://idp.example|alice",
            plan.Plan!.Intent,
            plan.Plan.Risk,
            "w37-test",
            Now.AddMinutes(5),
            Now,
            "connect-update");

        observations.Current = initial with { State = "PAUSED" };

        var validator = new ConnectMutationPreconditionValidator(planner);
        var result = await validator.ValidateAsync(operation.Snapshot);

        Assert.Equal(
            MutationPreDispatchGuardOutcome.StalePreview,
            result.Outcome);
    }

    [Fact]
    public async Task Restart_acceptance_remains_applied_unverified()
    {
        var observations = new FakeObservationPort(
            Existing(
                "sink-a",
                state: "RUNNING",
                tasks:
                [
                    new ConnectMutationTaskObservation(3, "RUNNING"),
                ]));
        var mutations = new FakeMutationPort();
        var service = new ConnectMutationExecutionService(
            mutations,
            observations);

        var canonical = new ConnectControlCanonicalIntent(
            ConnectAlterIntentKind.Control,
            "prod",
            "sink-a",
            ConnectControlAction.Restart,
            3,
            "RUNNING",
            "RUNNING",
            new string('a', 64));

        var result = await service.ControlAsync(canonical);

        Assert.Equal(
            MutationExecutionResultKind.AppliedUnverified,
            result.ResultKind);
        Assert.Equal(
            "connect_task_restart_verification_inconclusive",
            result.ResultCode);
        Assert.Equal(1, mutations.ControlCalls);
    }

    [Fact]
    public void Connect_mutation_contract_has_no_generic_write_proxy_surface()
    {
        var methods = typeof(IConnectMutationPort)
            .GetMethods()
            .OrderBy(method => method.Name, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(
            new[] { "AlterAsync", "ControlAsync", "CreateAsync", "DeleteAsync" },
            methods.Select(method => method.Name));

        var forbidden = new[]
        {
            "url",
            "path",
            "method",
            "header",
            "proxy",
            "command",
            "script",
        };

        foreach (var type in new[]
                 {
                     typeof(ConnectCreateMutation),
                     typeof(ConnectAlterMutation),
                     typeof(ConnectControlMutation),
                     typeof(ConnectDeleteMutation),
                 })
        {
            foreach (var property in type.GetProperties())
            {
                Assert.DoesNotContain(
                    forbidden,
                    term => property.Name.Contains(
                        term,
                        StringComparison.OrdinalIgnoreCase));
            }
        }
    }

    private static HmacMutationMaterialDigestService Digest() =>
        new("0123456789abcdef0123456789abcdef");

    private static IReadOnlyDictionary<string, string> BaseConfiguration() =>
        new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["connector.class"] = "org.example.Sink",
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
        IReadOnlyList<ConnectConfigurationObservationItem>? configuration = null,
        string state = "RUNNING",
        IReadOnlyList<ConnectMutationTaskObservation>? tasks = null)
    {
        configuration ??= Configuration(
            ("connector.class", "org.example.Sink"),
            ("tasks.max", "1"));
        tasks ??= Array.Empty<ConnectMutationTaskObservation>();

        return new ConnectMutationObservation(
            connectorName,
            true,
            state,
            tasks,
            configuration,
            ConnectMutationCanonicalization.ConfigurationFingerprint(
                configuration));
    }

    private static ConnectMutationObservation Missing(string connectorName)
    {
        var configuration =
            Array.Empty<ConnectConfigurationObservationItem>();
        return new ConnectMutationObservation(
            connectorName,
            false,
            "MISSING",
            Array.Empty<ConnectMutationTaskObservation>(),
            configuration,
            ConnectMutationCanonicalization.ConfigurationFingerprint(
                configuration));
    }

    private sealed class FakeObservationPort :
        IConnectMutationObservationPort
    {
        public FakeObservationPort(ConnectMutationObservation current)
        {
            Current = current;
        }

        public ConnectMutationObservation Current { get; set; }

        public ConnectMutationCapabilities Capabilities { get; set; } =
            new(
                SupportsCreate: true,
                SupportsUpdate: true,
                SupportsPause: true,
                SupportsResume: true,
                SupportsRestart: true,
                SupportsTaskRestart: true,
                SupportsDelete: true);

        public Task<ConnectMutationObservationResult<ConnectMutationCapabilities>>
            GetCapabilitiesAsync(
                string clusterId,
                ReadViewOperationContext operation,
                CancellationToken cancellationToken) =>
            Task.FromResult(
                ConnectMutationObservationResult<ConnectMutationCapabilities>
                    .Success(Capabilities));

        public Task<ConnectMutationObservationResult<ConnectMutationObservation>>
            ObserveConnectorAsync(
                string clusterId,
                string connectorName,
                ReadViewOperationContext operation,
                CancellationToken cancellationToken) =>
            Task.FromResult(
                ConnectMutationObservationResult<ConnectMutationObservation>
                    .Success(Current));
    }

    private sealed class FakeMutationPort : IConnectMutationPort
    {
        public int ControlCalls { get; private set; }

        public MutationProviderResult CreateResult { get; set; } =
            Accepted("connect_create_accepted");
        public MutationProviderResult AlterResult { get; set; } =
            Accepted("connect_update_accepted");
        public MutationProviderResult ControlResult { get; set; } =
            Accepted("connect_control_accepted");
        public MutationProviderResult DeleteResult { get; set; } =
            Accepted("connect_delete_accepted");

        public Task<MutationProviderResult> CreateAsync(
            ConnectCreateMutation request,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(CreateResult);

        public Task<MutationProviderResult> AlterAsync(
            ConnectAlterMutation request,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(AlterResult);

        public Task<MutationProviderResult> ControlAsync(
            ConnectControlMutation request,
            CancellationToken cancellationToken = default)
        {
            ControlCalls++;
            return Task.FromResult(ControlResult);
        }

        public Task<MutationProviderResult> DeleteAsync(
            ConnectDeleteMutation request,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(DeleteResult);

        private static MutationProviderResult Accepted(string code) =>
            new(
                MutationExecutionResultKind.AppliedUnverified,
                code,
                new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["provider.accepted"] = "true",
                });
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
