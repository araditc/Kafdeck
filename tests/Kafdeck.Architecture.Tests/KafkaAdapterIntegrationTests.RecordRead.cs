using System.Text;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Records;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Kafka;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class KafkaAdapterIntegrationTestsRecordRead
{
    [Fact]
    public async Task Bounded_record_reader_supports_earliest_timestamp_previous_and_out_of_range()
    {
        if (!RunKafkaIntegration())
        {
            return;
        }

        var profile = new ClusterProfile(
            "plaintext",
            ["localhost:9092"],
            KafkaSecurityProtocol.Plaintext,
            null,
            null);

        using var adapter = new ConfluentKafkaRecordReadAdapter([profile], new SecretResolver());
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var budget = new RecordOperationBudget(
            maxRecords: 10,
            maxRawBytes: 1024 * 1024,
            maxProjectedBytes: 1024 * 1024,
            maxDuration: TimeSpan.FromSeconds(10),
            maxRecordsPerSecond: 100);

        var earliest = await adapter.ReadPageAsync(
            new RecordReadRequest(
                profile.Id,
                "kafdeck-ci-smoke",
                0,
                RecordAnchor.Earliest(),
                RecordReadDirection.Forward,
                budget),
            Operation(),
            cancellation.Token);

        Assert.True(earliest.IsSuccess, earliest.Failure?.SafeMessage);
        Assert.NotNull(earliest.Value);
        Assert.Equal(3, earliest.Value.Records.Count);
        Assert.Equal(
            ["kafdeck-record-alpha", "kafdeck-record-beta", "kafdeck-record-gamma"],
            earliest.Value.Records.Select(record => Utf8(record.Value)).ToArray());
        Assert.Equal(0, earliest.Value.LowWatermark);
        Assert.True(earliest.Value.HighWatermark >= 3);
        Assert.Equal(RecordBudgetOutcome.Complete, earliest.Value.BudgetOutcome);

        var fromTimestamp = await adapter.ReadPageAsync(
            new RecordReadRequest(
                profile.Id,
                "kafdeck-ci-smoke",
                0,
                RecordAnchor.AtTimestamp(DateTimeOffset.UtcNow.AddMinutes(-5)),
                RecordReadDirection.Forward,
                budget),
            Operation(),
            cancellation.Token);

        Assert.True(fromTimestamp.IsSuccess, fromTimestamp.Failure?.SafeMessage);
        Assert.Equal(3, fromTimestamp.Value!.Records.Count);

        var previousBudget = new RecordOperationBudget(
            maxRecords: 2,
            maxRawBytes: 1024 * 1024,
            maxProjectedBytes: 1024 * 1024,
            maxDuration: TimeSpan.FromSeconds(10),
            maxRecordsPerSecond: 100);

        var previous = await adapter.ReadPageAsync(
            new RecordReadRequest(
                profile.Id,
                "kafdeck-ci-smoke",
                0,
                RecordAnchor.Latest(),
                RecordReadDirection.Previous,
                previousBudget),
            Operation(),
            cancellation.Token);

        Assert.True(previous.IsSuccess, previous.Failure?.SafeMessage);
        Assert.Equal(2, previous.Value!.Records.Count);
        Assert.Equal(
            ["kafdeck-record-beta", "kafdeck-record-gamma"],
            previous.Value.Records.Select(record => Utf8(record.Value)).ToArray());

        var atHighWatermark = await adapter.ReadPageAsync(
            new RecordReadRequest(
                profile.Id,
                "kafdeck-ci-smoke",
                0,
                RecordAnchor.AtOffset(earliest.Value.HighWatermark),
                RecordReadDirection.Forward,
                budget),
            Operation(),
            cancellation.Token);

        Assert.True(atHighWatermark.IsSuccess, atHighWatermark.Failure?.SafeMessage);
        Assert.Empty(atHighWatermark.Value!.Records);

        var outOfRange = await adapter.ReadPageAsync(
            new RecordReadRequest(
                profile.Id,
                "kafdeck-ci-smoke",
                0,
                RecordAnchor.AtOffset(earliest.Value.HighWatermark + 100),
                RecordReadDirection.Forward,
                budget),
            Operation(),
            cancellation.Token);

        Assert.False(outOfRange.IsSuccess);
        Assert.Equal("offset_out_of_range", outOfRange.Failure!.Code);
    }

    [Fact]
    public async Task Raw_byte_budget_stops_before_exposing_oversize_record()
    {
        if (!RunKafkaIntegration())
        {
            return;
        }

        var profile = new ClusterProfile(
            "plaintext",
            ["localhost:9092"],
            KafkaSecurityProtocol.Plaintext,
            null,
            null);

        using var adapter = new ConfluentKafkaRecordReadAdapter([profile], new SecretResolver());

        var budget = new RecordOperationBudget(
            maxRecords: 10,
            maxRawBytes: 4,
            maxProjectedBytes: 1024,
            maxDuration: TimeSpan.FromSeconds(10),
            maxRecordsPerSecond: 100);

        var result = await adapter.ReadPageAsync(
            new RecordReadRequest(
                profile.Id,
                "kafdeck-ci-smoke",
                0,
                RecordAnchor.Earliest(),
                RecordReadDirection.Forward,
                budget),
            Operation(),
            CancellationToken.None);

        Assert.True(result.IsSuccess, result.Failure?.SafeMessage);
        Assert.Empty(result.Value!.Records);
        Assert.Equal(RecordBudgetOutcome.RawByteLimit, result.Value.BudgetOutcome);
    }

    private static KafkaOperationContext Operation() =>
        new(DateTimeOffset.UtcNow.AddSeconds(20));

    private static bool RunKafkaIntegration() =>
        string.Equals(
            Environment.GetEnvironmentVariable("KAFDECK_RUN_KAFKA_INTEGRATION"),
            "1",
            StringComparison.Ordinal);

    private static string Utf8(ReadOnlyMemory<byte>? value) =>
        value.HasValue
            ? Encoding.UTF8.GetString(value.Value.Span)
            : throw new InvalidOperationException("Expected a non-null record value fixture.");
}
