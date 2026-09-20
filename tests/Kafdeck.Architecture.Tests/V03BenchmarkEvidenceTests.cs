using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Kafdeck.Core.Records;
using Kafdeck.Modules.Records;
using Xunit;
using Xunit.Abstractions;

namespace Kafdeck.Architecture.Tests;

/// <summary>
/// Repeatable release-evidence microbenchmarks for bounded in-process record processing.
/// These tests intentionally have no performance pass/fail threshold: CI hardware is variable
/// and the measured values are evidence, not a public SLA or Kafka end-to-end throughput claim.
/// </summary>
public sealed class V03BenchmarkEvidenceTests
{
    private const int WarmupIterations = 1_000;
    private const int MeasurementIterations = 20_000;
    private readonly ITestOutputHelper _output;

    public V03BenchmarkEvidenceTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public void Structured_filter_bounded_microbenchmark()
    {
        var plan = RecordFilterCompiler.Compile(
            new RecordFilterRequest(
                structuredFilter: new RecordStructuredFilter(
                    RecordFilterLanguage.Cel,
                    """has(value.customer.name) && startsWith(value.customer.name, "al") && value.customer.age >= 18""")));
        var evaluator = new RecordFilterEvaluator();
        using var document = JsonDocument.Parse(
            """{"customer":{"name":"alpha","age":31,"ssn":"111-22-3333"},"amount":125.50}""");

        for (var index = 0; index < WarmupIterations; index++)
        {
            Assert.True(evaluator.MatchesStructuredValue(document.RootElement, plan));
        }

        var stopwatch = Stopwatch.StartNew();
        var matched = 0;
        for (var index = 0; index < MeasurementIterations; index++)
        {
            if (evaluator.MatchesStructuredValue(document.RootElement, plan))
            {
                matched++;
            }
        }
        stopwatch.Stop();

        Assert.Equal(MeasurementIterations, matched);
        Report("structured_filter", MeasurementIterations, stopwatch.Elapsed);
    }

    [Fact]
    public void Structured_masking_bounded_microbenchmark()
    {
        using var document = JsonDocument.Parse(
            """{"customer":{"name":"alpha","age":31,"ssn":"111-22-3333"},"amount":125.50}""");
        var raw = Encoding.UTF8.GetBytes(document.RootElement.GetRawText());
        var item = new RecordFilteredItem(
            new KafkaRawRecord(
                42,
                DateTimeOffset.UnixEpoch,
                Encoding.UTF8.GetBytes("acct:42"),
                raw,
                [new KafkaRecordHeader("tenant", Encoding.UTF8.GetBytes("bank-a"))]),
            new RecordDecodedValue(1, RecordSchemaFormat.JsonSchema, document.RootElement.Clone()));
        var policy = RecordMaskingPolicyCompiler.Compile(
            new RecordMaskingPolicyDefinition(
                "benchmark-mask",
                1,
                [new RecordStructuredMaskRule("/customer/ssn")],
                MaskKey: true));
        var service = new RecordMaskingService();

        for (var index = 0; index < WarmupIterations; index++)
        {
            var projected = service.Apply(0, item, policy);
            Assert.Equal(RecordPayloadProjectionKind.Structured, projected.ValueKind);
        }

        var stopwatch = Stopwatch.StartNew();
        var projectedCount = 0;
        for (var index = 0; index < MeasurementIterations; index++)
        {
            var projected = service.Apply(0, item, policy);
            if (projected.ValueKind == RecordPayloadProjectionKind.Structured)
            {
                projectedCount++;
            }
        }
        stopwatch.Stop();

        Assert.Equal(MeasurementIterations, projectedCount);
        Report("structured_masking", MeasurementIterations, stopwatch.Elapsed);
    }

    private void Report(string scenario, int operations, TimeSpan elapsed)
    {
        Assert.True(elapsed > TimeSpan.Zero);
        var operationsPerSecond = operations / elapsed.TotalSeconds;
        _output.WriteLine(
            "V03_BENCHMARK scenario={0} operations={1} elapsed_ms={2:F3} ops_per_sec={3:F2} framework={4} processor_count={5}",
            scenario,
            operations,
            elapsed.TotalMilliseconds,
            operationsPerSecond,
            Environment.Version,
            Environment.ProcessorCount);
    }
}
