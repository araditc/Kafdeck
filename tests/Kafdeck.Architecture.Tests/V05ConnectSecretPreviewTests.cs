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
    [Fact]
    public async Task Secret_and_opaque_values_do_not_persist_guessable_value_hashes()
    {
        using var digest = new HmacMutationMaterialDigestService(
            "0123456789abcdef0123456789abcdef");
        var planner = new ConnectMutationPlanner(
            new MissingConnectorObservationPort(),
            digest);

        var first = await planner.PlanCreateAsync(
            Request("low-entropy-password", "opaque-one"));
        var second = await planner.PlanCreateAsync(
            Request("different-password", "opaque-two"));

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
        Assert.Equal(firstPassword.ValueSha256, secondPassword.ValueSha256);
        Assert.Equal(firstOpaque.ValueSha256, secondOpaque.ValueSha256);
        Assert.NotEqual(Sha256("low-entropy-password"), firstPassword.ValueSha256);
        Assert.NotEqual(Sha256("opaque-one"), firstOpaque.ValueSha256);

        // The durable preview intentionally does not distinguish secret values.
        // The W32 HMAC-bound execution-material digest remains the authoritative
        // binding from preview to the re-submitted secret-bearing configuration.
        Assert.Equal(
            first.Plan.Canonical.RequestedConfigurationFingerprint,
            second.Plan.Canonical.RequestedConfigurationFingerprint);
        Assert.NotEqual(
            Assert.Single(first.Plan.Intent.MaterialDigests!).Digest,
            Assert.Single(second.Plan.Intent.MaterialDigests!).Digest);
    }

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

    private static string Sha256(string value) =>
        Convert.ToHexString(
                SHA256.HashData(Encoding.UTF8.GetBytes(value)))
            .ToLowerInvariant();

    private sealed class MissingConnectorObservationPort :
        IConnectMutationObservationPort
    {
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
                    new ConnectMutationObservation(
                        connectorName,
                        Exists: false,
                        State: "MISSING",
                        Tasks: Array.Empty<ConnectMutationTaskObservation>(),
                        Configuration: Array.Empty<ConnectConfigurationObservationItem>(),
                        ConfigurationFingerprint: new string('0', 64))));
    }
}
