using System.Text;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Records;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Kafka;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class KafkaRecordReadAdapterIntegrationTests
{
    [Fact]
    public async Task Unknown_cluster_fails_without_network_access()
    {
        using var adapter = new ConfluentKafkaRecordReadAdapter([], new SecretResolver());
        var result = await adapter.ReadPageAsync(
            Request("missing", RecordAnchor.Earliest()),
            Operation(),
            CancellationToken.None);

        AssertFailure(result, KafkaFailureCategory.InvalidConfiguration, "cluster_not_configured");
    }

    [Fact]
    public async Task Expired_deadline_fails_before_consumer_creation()
    {
        using var adapter = CreatePlaintextAdapter();
        var result = await adapter.ReadPageAsync(
            Request("preflight", RecordAnchor.Earliest()),
            new KafkaOperationContext(DateTimeOffset.UtcNow.AddSeconds(-1)),
            CancellationToken.None);

        AssertFailure(result, KafkaFailureCategory.Timeout, "deadline_exceeded");
    }

    [Fact]
    public async Task Cancelled_operation_fails_before_consumer_creation()
    {
        using var adapter = CreatePlaintextAdapter();
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        var result = await adapter.ReadPageAsync(
            Request("preflight", RecordAnchor.Earliest()),
            Operation(),
            cancellation.Token);

        AssertFailure(result, KafkaFailureCategory.Cancelled, "operation_cancelled");
    }

    [Fact]
    public void Bulkhead_configuration_is_bounded()
    {
        var profile = new ClusterProfile("preflight", ["localhost:1"], KafkaSecurityProtocol.Plaintext, null, null);

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ConfluentKafkaRecordReadAdapter([profile], new SecretResolver(), 0, 1));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ConfluentKafkaRecordReadAdapter([profile], new SecretResolver(), 2, 3));
    }

    [Fact]
    public async Task Plaintext_reader_supports_bounded_navigation_without_offset_commit()
    {
        if (!RunKafkaIntegration()) return;

        var profile = new ClusterProfile("plaintext", ["localhost:9092"], KafkaSecurityProtocol.Plaintext, null, null);
        using var adapter = new ConfluentKafkaRecordReadAdapter([profile], new SecretResolver());

        var forward = await adapter.ReadPageAsync(
            Request(profile.Id, RecordAnchor.Earliest()),
            Operation(),
            CancellationToken.None);

        AssertSuccess(forward);
        Assert.NotEmpty(forward.Value!.Records);
        Assert.True(forward.Value.HighWatermark >= forward.Value.Records.Count);
        Assert.All(forward.Value.Records, record => Assert.True(record.Offset >= forward.Value.LowWatermark));
        Assert.Contains(forward.Value.Records, record =>
            record.Value.HasValue && Encoding.UTF8.GetString(record.Value.Value.Span) == "kafdeck-record-1");

        var previous = await adapter.ReadPageAsync(
            Request(profile.Id, RecordAnchor.Latest(), RecordReadDirection.Previous),
            Operation(),
            CancellationToken.None);

        AssertSuccess(previous);
        Assert.NotEmpty(previous.Value!.Records);
        Assert.Equal(forward.Value.HighWatermark, previous.Value.HighWatermark);
        Assert.True(previous.Value.LastReturnedOffset < previous.Value.HighWatermark);

        var timestamp = await adapter.ReadPageAsync(
            Request(profile.Id, RecordAnchor.AtTimestamp(DateTimeOffset.UtcNow.AddMinutes(-5))),
            Operation(),
            CancellationToken.None);

        AssertSuccess(timestamp);
        Assert.NotEmpty(timestamp.Value!.Records);
    }

    [Fact]
    public async Task Explicit_offset_outside_partition_range_is_rejected_safely()
    {
        if (!RunKafkaIntegration()) return;

        var profile = new ClusterProfile("plaintext", ["localhost:9092"], KafkaSecurityProtocol.Plaintext, null, null);
        using var adapter = new ConfluentKafkaRecordReadAdapter([profile], new SecretResolver());

        var result = await adapter.ReadPageAsync(
            Request(profile.Id, RecordAnchor.AtOffset(long.MaxValue)),
            Operation(),
            CancellationToken.None);

        AssertFailure(result, KafkaFailureCategory.ProtocolError, "offset_out_of_range");
    }

    [Fact]
    public void Record_reader_surface_contains_no_mutating_operation_names()
    {
        var methods = typeof(ConfluentKafkaRecordReadAdapter)
            .GetMethods(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public)
            .Where(method => method.DeclaringType == typeof(ConfluentKafkaRecordReadAdapter))
            .ToArray();

        Assert.Contains(methods, method => method.Name == nameof(ConfluentKafkaRecordReadAdapter.ReadPageAsync));
        var forbidden = new[] { "Produce", "Commit", "Subscribe", "StoreOffset", "Alter", "Delete", "Create", "Reset" };
        Assert.DoesNotContain(methods, method => forbidden.Any(token =>
            method.Name.Contains(token, StringComparison.OrdinalIgnoreCase)));
    }

    private static RecordReadRequest Request(
        string clusterId,
        RecordAnchor anchor,
        RecordReadDirection direction = RecordReadDirection.Forward) =>
        new(
            clusterId,
            "kafdeck-ci-smoke",
            0,
            anchor,
            direction,
            RecordOperationBudget.Default);

    private static KafkaOperationContext Operation() =>
        new(DateTimeOffset.UtcNow.AddSeconds(20));

    private static bool RunKafkaIntegration() =>
        string.Equals(Environment.GetEnvironmentVariable("KAFDECK_RUN_KAFKA_INTEGRATION"), "1", StringComparison.Ordinal);

    private static ConfluentKafkaRecordReadAdapter CreatePlaintextAdapter()
    {
        var profile = new ClusterProfile("preflight", ["localhost:1"], KafkaSecurityProtocol.Plaintext, null, null);
        return new ConfluentKafkaRecordReadAdapter([profile], new SecretResolver());
    }

    private static void AssertSuccess(KafkaResult<RecordReadBatch> result) =>
        Assert.True(result.IsSuccess, $"{result.Failure?.Category} / {result.Failure?.Code} / {result.Failure?.SafeMessage}");

    private static void AssertFailure(
        KafkaResult<RecordReadBatch> result,
        KafkaFailureCategory category,
        string code)
    {
        Assert.False(result.IsSuccess);
        Assert.NotNull(result.Failure);
        Assert.Equal(category, result.Failure.Category);
        Assert.Equal(code, result.Failure.Code);
    }
}
