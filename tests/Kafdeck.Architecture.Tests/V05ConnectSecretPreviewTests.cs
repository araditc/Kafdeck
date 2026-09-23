using System.Security.Cryptography;
using System.Text;
using Kafdeck.Core.Ecosystem;
using Kafdeck.Core.ReadViews;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Connect;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V05ConnectSecretPreviewTests
{
    private static readonly DateTimeOffset Now =
        new(2026, 9, 23, 4, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task Secret_and_opaque_values_use_keyed_non_guessable_fingerprints()
    {
        var firstPasswordValue = TestValue();
        var firstOpaqueValue = TestValue();
        var secondPasswordValue = TestValue();
        var secondOpaqueValue = TestValue();
        using var digest = Digest();
        var planner = new ConnectMutationPlanner(
            new FakeObservationPort(Missing("sink-a")),
            digest);

        var first = await planner.PlanCreateAsync(
            Request(firstPasswordValue, firstOpaqueValue));
        var second = await planner.PlanCreateAsync(
            Request(secondPasswordValue, secondOpaqueValue));

        Assert.True(first.IsSuccess, first.Failure?.SafeMessage);
        Assert.True(second.IsSuccess, second.Failure?.SafeMessage);
        using var firstMaterial = first.ExecutionMaterial!;
        using var secondMaterial = second.ExecutionMaterial!;

        var firstPassword = Assert.Single(
            first.Plan!.Canonical.RequestedConfiguration,
            item => item.Key == "db.password");
        var secondPassword = Assert.Single(
            second.Plan!.Canonical.RequestedConfiguration,
            item => item.Key == "db.password");
        var firstOpaque = Assert.Single(
            first.Plan.Canonical.RequestedConfiguration,
            item => item.Key == "plugin.opaque");
        var secondOpaque = Assert.Single(
            second.Plan.Canonical.RequestedConfiguration,
            item => item.Key == "plugin.opaque");

        Assert.Equal("[REDACTED]", firstPassword.SafeValue);
        Assert.Equal("[REDACTED]", firstOpaque.SafeValue);
        Assert.NotEqual(firstPassword.ValueSha256, secondPassword.ValueSha256);
        Assert.NotEqual(firstOpaque.ValueSha256, secondOpaque.ValueSha256);
        Assert.NotEqual(Sha256(firstPasswordValue), firstPassword.ValueSha256);
        Assert.NotEqual(Sha256(firstOpaqueValue), firstOpaque.ValueSha256);
        Assert.NotEqual(
            first.Plan.Canonical.RequestedConfigurationFingerprint,
            second.Plan.Canonical.RequestedConfigurationFingerprint);

        // W32 retains a separate whole-material HMAC binding for execution.
        Assert.NotEqual(
            Assert.Single(first.Plan.Intent.MaterialDigests!).Digest,
            Assert.Single(second.Plan.Intent.MaterialDigests!).Digest);
    }

    [Fact]
    public async Task Unchanged_secret_configuration_is_detected_without_persisting_raw_hashes()
    {
        var password = TestValue();
        var opaque = TestValue();
        var configuration = Request(password, opaque).Configuration;
        var rawObservation = Existing("sink-a", configuration);
        using var digest = Digest();
        var planner = new ConnectMutationPlanner(
            new FakeObservationPort(rawObservation),
            digest);

        var result = await planner.PlanUpdateAsync(
            new ConnectUpdateRequest(
                "prod",
                "sink-a",
                configuration));

        Assert.False(result.IsSuccess);
        Assert.Equal(
            ConnectMutationPlanningFailureCode.NoChange,
            result.Failure!.Code);
    }

    [Fact]
    public async Task Secret_only_change_is_detected_and_remains_redacted()
    {
        var oldPassword = TestValue();
        var newPassword = TestValue();
        var opaque = TestValue();
        var current = Request(oldPassword, opaque).Configuration;
        var requested = Request(newPassword, opaque).Configuration;
        using var digest = Digest();
        var planner = new ConnectMutationPlanner(
            new FakeObservationPort(Existing("sink-a", current)),
            digest);

        var result = await planner.PlanUpdateAsync(
            new ConnectUpdateRequest(
                "prod",
                "sink-a",
                requested));

        Assert.True(result.IsSuccess, result.Failure?.SafeMessage);
        using var material = result.ExecutionMaterial!;
        var change = Assert.Single(result.Plan!.Canonical.Diff);
        Assert.Equal("db.password", change.Key);
        Assert.Equal(ConnectConfigurationChangeKind.Changed, change.ChangeKind);
        Assert.Equal("[REDACTED]", change.CurrentSafeValue);
        Assert.Equal("[REDACTED]", change.RequestedSafeValue);
    }

    [Fact]
    public async Task Execution_material_builder_reuses_keyed_secret_projection()
    {
        var password = TestValue();
        var opaque = TestValue();
        var configuration = Request(password, opaque).Configuration;
        using var digest = Digest();
        var planner = new ConnectMutationPlanner(
            new FakeObservationPort(Missing("sink-a")),
            digest);

        var planned = await planner.PlanCreateAsync(
            new ConnectCreateRequest(
                "prod",
                "sink-a",
                configuration));

        Assert.True(planned.IsSuccess, planned.Failure?.SafeMessage);
        using var plannedMaterial = planned.ExecutionMaterial!;
        var operation = MutationOperation.CreatePreview(
            "oidc:https://idp.example|alice",
            planned.Plan!.Intent,
            planned.Plan.Risk,
            "w39-connect-material-builder-test",
            Now.AddMinutes(5),
            Now,
            "connect-create-secret-material-builder");

        using var rebuilt = ConnectMutationExecutionMaterialBuilder.BuildConfiguration(
            operation.Snapshot,
            configuration,
            digest);

        Assert.Equal(
            planned.Plan.Canonical.MaterialName,
            Assert.Single(rebuilt.Names));
        Assert.True(
            rebuilt.GetRequired(planned.Plan.Canonical.MaterialName).Length > 0);
    }

    [Fact]
    public async Task Create_with_secret_configuration_can_be_verified_after_provider_acceptance()
    {
        var password = TestValue();
        var opaque = TestValue();
        var configuration = Request(password, opaque).Configuration;
        var observations = new FakeObservationPort(Missing("sink-a"));
        using var digest = Digest();
        var planner = new ConnectMutationPlanner(observations, digest);

        var plan = await planner.PlanCreateAsync(
            new ConnectCreateRequest(
                "prod",
                "sink-a",
                configuration));

        Assert.True(plan.IsSuccess, plan.Failure?.SafeMessage);
        using var material = plan.ExecutionMaterial!;
        var operation = MutationOperation.CreatePreview(
            "oidc:https://idp.example|alice",
            plan.Plan!.Intent,
            plan.Plan.Risk,
            "w39-connect-secret-test",
            Now.AddMinutes(5),
            Now,
            "connect-create-secret-verification");

        observations.Current = Existing("sink-a", configuration);
        var service = new ConnectMutationExecutionService(
            new FakeMutationPort(),
            observations,
            digest);

        var result = await service.CreateAsync(
            new MutationExecutionContext(
                operation.Snapshot,
                material));

        Assert.Equal(
            MutationExecutionResultKind.AppliedVerified,
            result.ResultKind);
        Assert.Equal("connect_create_verified", result.ResultCode);
        Assert.Equal(
            plan.Plan.Canonical.RequestedConfigurationFingerprint,
            result.SafeEvidence!["configuration.fingerprint"]);
    }

    [Fact]
    public async Task Delete_preview_does_not_persist_raw_observation_secret_fingerprint()
    {
        var password = TestValue();
        var opaque = TestValue();
        var rawObservation = Existing(
            "sink-a",
            Request(password, opaque).Configuration);
        using var digest = Digest();
        var planner = new ConnectMutationPlanner(
            new FakeObservationPort(rawObservation),
            digest);

        var result = await planner.PlanDeleteAsync(
            new ConnectDeleteRequest("prod", "sink-a"));

        Assert.True(result.IsSuccess, result.Failure?.SafeMessage);
        Assert.NotEqual(
            rawObservation.ConfigurationFingerprint,
            result.Plan!.Canonical.CurrentConfigurationFingerprint);
        Assert.NotEqual(
            ConnectMutationCanonicalization.ObservationFingerprint(rawObservation),
            result.Plan.Canonical.StateFingerprint);
    }

    private static HmacMutationMaterialDigestService Digest() =>
        new(Convert.ToHexString(RandomNumberGenerator.GetBytes(32)));

    private static string TestValue() =>
        Convert.ToBase64String(RandomNumberGenerator.GetBytes(24));

    private static ConnectCreateRequest Request(
        string password,
        string opaque) =>
        new(
            "prod",
            "sink-a",
            new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["connector.class"] = "org.example.Sink",
                ["tasks.max"] = "1",
                ["db.password"] = password,
                ["plugin.opaque"] = opaque,
            });

    private static ConnectMutationObservation Existing(
        string connectorName,
        IReadOnlyDictionary<string, string> configuration)
    {
        var items = configuration
            .OrderBy(pair => pair.Key, StringComparer.Ordinal)
            .Select(pair =>
            {
                var rawHash = Sha256(pair.Value);
                var safe =
                    ConnectSafeConfigurationPolicy.IsExplicitlySafeConfigKey(pair.Key) &&
                    !ConnectSafeConfigurationPolicy.IsSecretKey(pair.Key)
                        ? pair.Value
                        : "[REDACTED]";
                return new ConnectConfigurationObservationItem(
                    pair.Key,
                    rawHash,
                    safe);
            })
            .ToArray();

        return new ConnectMutationObservation(
            connectorName,
            Exists: true,
            State: "RUNNING",
            Tasks: Array.Empty<ConnectMutationTaskObservation>(),
            Configuration: items,
            ConfigurationFingerprint: RawConfigurationFingerprint(items));
    }

    private static ConnectMutationObservation Missing(string connectorName) =>
        new(
            connectorName,
            Exists: false,
            State: "MISSING",
            Tasks: Array.Empty<ConnectMutationTaskObservation>(),
            Configuration: Array.Empty<ConnectConfigurationObservationItem>(),
            ConfigurationFingerprint: RawConfigurationFingerprint(
                Array.Empty<ConnectConfigurationObservationItem>()));

    private static string RawConfigurationFingerprint(
        IEnumerable<ConnectConfigurationObservationItem> items)
    {
        var builder = new StringBuilder();
        foreach (var item in items.OrderBy(item => item.Key, StringComparer.Ordinal))
        {
            builder.Append(item.Key)
                .Append('=')
                .Append(Convert.ToBase64String(Encoding.UTF8.GetBytes(item.ValueSha256)))
                .Append('\n');
        }

        return Sha256(builder.ToString());
    }

    private static string Sha256(string value) =>
        Convert.ToHexString(
                SHA256.HashData(Encoding.UTF8.GetBytes(value)))
            .ToLowerInvariant();

    private sealed class FakeObservationPort : IConnectMutationObservationPort
    {
        public FakeObservationPort(ConnectMutationObservation observation)
        {
            Current = observation;
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

    private sealed class FakeMutationPort : IConnectMutationPort
    {
        public Task<MutationProviderResult> CreateAsync(
            ConnectCreateMutation request,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(Accepted("connect_create_accepted"));

        public Task<MutationProviderResult> AlterAsync(
            ConnectAlterMutation request,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(Accepted("connect_update_accepted"));

        public Task<MutationProviderResult> ControlAsync(
            ConnectControlMutation request,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(Accepted("connect_control_accepted"));

        public Task<MutationProviderResult> DeleteAsync(
            ConnectDeleteMutation request,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(Accepted("connect_delete_accepted"));

        private static MutationProviderResult Accepted(string code) =>
            new(
                MutationExecutionResultKind.AppliedUnverified,
                code,
                new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["provider.accepted"] = "true",
                });
    }
}
