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
    private const string ExistingSecretValue = "w40-test-password-value";
    private const string ChangedSecretValue = "w40-test-password-changed";
    private const string ExistingOpaqueValue = "w40-test-opaque-value";
    private const string ConnectorClassRawSha256 =
        "1e387c76059ce310f37b38e9aaa5621890430240336a507b66404615f6bd4b86";
    private const string TasksMaxRawSha256 =
        "6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b";
    private const string ExistingSecretRawSha256 =
        "fe7edc6f1a772be049176518a9ce214331bb13bdae13171f1915522d04aca798";
    private const string ChangedSecretRawSha256 =
        "26ff2ce78c4d1dd232f72106f7ca51a340785e1d9c78197269fca96b003cd8fc";
    private const string ExistingOpaqueRawSha256 =
        "0ca2680f969cb57e80dc3e376332e43d5d71165ac42dcebeb8b51dcc8ec16aa0";

    private static readonly DateTimeOffset Now =
        new(2026, 9, 23, 4, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task Secret_and_opaque_values_use_keyed_non_guessable_fingerprints()
    {
        var firstSecretValue = TestValue();
        var firstOpaqueValue = TestValue();
        var secondSecretValue = TestValue();
        var secondOpaqueValue = TestValue();
        using var firstDigest = Digest();
        using var secondDigest = Digest();
        var firstPlanner = new ConnectMutationPlanner(
            new FakeObservationPort(Missing("sink-a")),
            firstDigest);
        var secondPlanner = new ConnectMutationPlanner(
            new FakeObservationPort(Missing("sink-a")),
            secondDigest);

        var first = await firstPlanner.PlanCreateAsync(
            Request(firstSecretValue, firstOpaqueValue));
        var second = await firstPlanner.PlanCreateAsync(
            Request(secondSecretValue, secondOpaqueValue));
        var sameValuesDifferentKey = await secondPlanner.PlanCreateAsync(
            Request(firstSecretValue, firstOpaqueValue));

        Assert.True(first.IsSuccess, first.Failure?.SafeMessage);
        Assert.True(second.IsSuccess, second.Failure?.SafeMessage);
        Assert.True(
            sameValuesDifferentKey.IsSuccess,
            sameValuesDifferentKey.Failure?.SafeMessage);
        using var firstMaterial = first.ExecutionMaterial!;
        using var secondMaterial = second.ExecutionMaterial!;
        using var sameValuesDifferentKeyMaterial = sameValuesDifferentKey.ExecutionMaterial!;

        var firstSecret = Assert.Single(
            first.Plan!.Canonical.RequestedConfiguration,
            item => item.Key == "db.password");
        var secondSecret = Assert.Single(
            second.Plan!.Canonical.RequestedConfiguration,
            item => item.Key == "db.password");
        var firstOpaque = Assert.Single(
            first.Plan.Canonical.RequestedConfiguration,
            item => item.Key == "plugin.opaque");
        var secondOpaque = Assert.Single(
            second.Plan.Canonical.RequestedConfiguration,
            item => item.Key == "plugin.opaque");
        var differentlyKeyedSecret = Assert.Single(
            sameValuesDifferentKey.Plan!.Canonical.RequestedConfiguration,
            item => item.Key == "db.password");
        var differentlyKeyedOpaque = Assert.Single(
            sameValuesDifferentKey.Plan.Canonical.RequestedConfiguration,
            item => item.Key == "plugin.opaque");

        Assert.Equal("[REDACTED]", firstSecret.SafeValue);
        Assert.Equal("[REDACTED]", firstOpaque.SafeValue);
        Assert.NotEqual(firstSecret.ValueSha256, secondSecret.ValueSha256);
        Assert.NotEqual(firstOpaque.ValueSha256, secondOpaque.ValueSha256);
        Assert.NotEqual(firstSecret.ValueSha256, differentlyKeyedSecret.ValueSha256);
        Assert.NotEqual(firstOpaque.ValueSha256, differentlyKeyedOpaque.ValueSha256);
        Assert.NotEqual(
            first.Plan.Canonical.RequestedConfigurationFingerprint,
            second.Plan.Canonical.RequestedConfigurationFingerprint);
        Assert.NotEqual(
            first.Plan.Canonical.RequestedConfigurationFingerprint,
            sameValuesDifferentKey.Plan.Canonical.RequestedConfigurationFingerprint);

        // W32 retains a separate whole-material HMAC binding for execution.
        Assert.NotEqual(
            Assert.Single(first.Plan.Intent.MaterialDigests!).Digest,
            Assert.Single(second.Plan.Intent.MaterialDigests!).Digest);
    }

    [Fact]
    public async Task Unchanged_secret_configuration_is_detected_without_persisting_raw_hashes()
    {
        var configuration = Request(
            ExistingSecretValue,
            ExistingOpaqueValue).Configuration;
        var rawObservation = Existing(
            "sink-a",
            configuration,
            RawFingerprints(ExistingSecretRawSha256));
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
        var current = Request(
            ExistingSecretValue,
            ExistingOpaqueValue).Configuration;
        var requested = Request(
            ChangedSecretValue,
            ExistingOpaqueValue).Configuration;
        using var digest = Digest();
        var planner = new ConnectMutationPlanner(
            new FakeObservationPort(
                Existing(
                    "sink-a",
                    current,
                    RawFingerprints(ExistingSecretRawSha256))),
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
        var secretValue = TestValue();
        var opaque = TestValue();
        var configuration = Request(secretValue, opaque).Configuration;
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
        var configuration = Request(
            ExistingSecretValue,
            ExistingOpaqueValue).Configuration;
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

        observations.Current = Existing(
            "sink-a",
            configuration,
            RawFingerprints(ExistingSecretRawSha256));
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
        var rawObservation = Existing(
            "sink-a",
            Request(
                ExistingSecretValue,
                ExistingOpaqueValue).Configuration,
            RawFingerprints(ExistingSecretRawSha256));
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
        string secretValue,
        string opaque) =>
        new(
            "prod",
            "sink-a",
            new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["connector.class"] = "org.example.Sink",
                ["tasks.max"] = "1",
                ["db.password"] = secretValue,
                ["plugin.opaque"] = opaque,
            });

    private static IReadOnlyDictionary<string, string> RawFingerprints(
        string secretRawSha256) =>
        new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["connector.class"] = ConnectorClassRawSha256,
            ["tasks.max"] = TasksMaxRawSha256,
            ["db.password"] = secretRawSha256,
            ["plugin.opaque"] = ExistingOpaqueRawSha256,
        };

    private static ConnectMutationObservation Existing(
        string connectorName,
        IReadOnlyDictionary<string, string> configuration,
        IReadOnlyDictionary<string, string> rawFingerprints)
    {
        var items = configuration
            .OrderBy(pair => pair.Key, StringComparer.Ordinal)
            .Select(pair =>
            {
                if (!rawFingerprints.TryGetValue(pair.Key, out var rawFingerprint))
                {
                    throw new InvalidOperationException(
                        $"Missing fixed raw fingerprint fixture for '{pair.Key}'.");
                }

                var safe =
                    ConnectSafeConfigurationPolicy.IsExplicitlySafeConfigKey(pair.Key) &&
                    !ConnectSafeConfigurationPolicy.IsSecretKey(pair.Key)
                        ? pair.Value
                        : "[REDACTED]";
                return new ConnectConfigurationObservationItem(
                    pair.Key,
                    rawFingerprint,
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

        return Convert.ToHexString(
                SHA256.HashData(Encoding.UTF8.GetBytes(builder.ToString())))
            .ToLowerInvariant();
    }

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
