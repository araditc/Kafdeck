using System.Diagnostics;
using System.Text.Json;
using Kafdeck.Core.Consumers;
using Kafdeck.Core.Records;
using Kafdeck.Infrastructure.Ecosystem;
using Kafdeck.Modules.Consumers;
using Kafdeck.Modules.Schemas;
using Xunit;
using Xunit.Abstractions;

namespace Kafdeck.Architecture.Tests;

/// <summary>
/// Repeatable bounded in-process release-evidence microbenchmarks for v0.4 read-view projections.
/// These measurements intentionally have no pass/fail performance threshold because hosted CI
/// hardware varies. They are implementation evidence, not Kafka/provider throughput or an SLA.
/// </summary>
public sealed class V04BenchmarkEvidenceTests
{
    private const int WarmupIterations = 1_000;
    private const int MeasurementIterations = 20_000;
    private readonly ITestOutputHelper _output;

    public V04BenchmarkEvidenceTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public void Consumer_lag_projection_bounded_microbenchmark()
    {
        var partitions = Enumerable.Range(0, 64)
            .Select(index => new ConsumerOffsetProjection(
                "payments",
                index,
                10_000 + index,
                20_000 + index,
                10_000,
                ConsumerOffsetState.Observed))
            .ToArray();

        for (var index = 0; index < WarmupIterations; index++)
        {
            Assert.Equal(640_000, ConsumerExplorerService.BuildLagProjection("payments-worker", partitions).TotalLag);
        }

        var stopwatch = Stopwatch.StartNew();
        long total = 0;
        for (var index = 0; index < MeasurementIterations; index++)
        {
            total += ConsumerExplorerService.BuildLagProjection("payments-worker", partitions).TotalLag ?? 0;
        }

        stopwatch.Stop();
        Assert.Equal(640_000L * MeasurementIterations, total);
        Report("consumer_lag_projection_64_partitions", MeasurementIterations, stopwatch.Elapsed);
    }

    [Fact]
    public void Schema_diff_bounded_microbenchmark()
    {
        var left = new RecordSchemaDocument(
            1,
            RecordSchemaFormat.JsonSchema,
            BuildSchema("A"),
            []);
        var right = left with { Id = 2, SchemaText = BuildSchema("B") };
        var service = new SchemaDiffService();

        for (var index = 0; index < WarmupIterations; index++)
        {
            Assert.False(service.Compare(left, right).IsEqual);
        }

        var stopwatch = Stopwatch.StartNew();
        var changed = 0;
        for (var index = 0; index < MeasurementIterations; index++)
        {
            if (!service.Compare(left, right).IsEqual)
            {
                changed++;
            }
        }

        stopwatch.Stop();
        Assert.Equal(MeasurementIterations, changed);
        Report("schema_diff_128_lines", MeasurementIterations, stopwatch.Elapsed);
    }

    [Fact]
    public void Connect_fail_closed_projection_bounded_microbenchmark()
    {
        using var statusDocument = JsonDocument.Parse(
            """{"name":"payments","connector":{"state":"RUNNING","worker_id":"worker:8083"},"tasks":[{"id":0,"state":"RUNNING","worker_id":"worker:8083"}]}""");

        var configEntries = Enumerable.Range(0, 64)
            .ToDictionary(
                index => index % 4 == 0 ? $"custom.secret.{index}" : $"custom.field.{index}",
                index => (object?)$"value-{index}",
                StringComparer.Ordinal);
        configEntries["connector.class"] = "example.PaymentsConnector";
        configEntries["topics"] = "payments";
        configEntries["connection.url"] = "jdbc:postgresql://user:secret@db.example/payments";

        using var configDocument = JsonDocument.Parse(JsonSerializer.Serialize(configEntries));

        for (var index = 0; index < WarmupIterations; index++)
        {
            AssertProjection(KafkaConnectReadAdapter.ProjectConnector(
                "payments",
                statusDocument.RootElement,
                configDocument.RootElement,
                100));
        }

        Kafdeck.Core.Ecosystem.ConnectConnectorDetail? lastProjection = null;
        var projectedFieldCount = 0;

        var stopwatch = Stopwatch.StartNew();
        for (var index = 0; index < MeasurementIterations; index++)
        {
            lastProjection = KafkaConnectReadAdapter.ProjectConnector(
                "payments",
                statusDocument.RootElement,
                configDocument.RootElement,
                100);
            projectedFieldCount += lastProjection.SafeConfiguration.Count;
        }

        stopwatch.Stop();

        Assert.NotNull(lastProjection);
        Assert.Equal(configEntries.Count * MeasurementIterations, projectedFieldCount);
        AssertProjection(lastProjection);
        Report("connect_fail_closed_projection_67_fields", MeasurementIterations, stopwatch.Elapsed);
    }

    private static string BuildSchema(string marker)
    {
        var lines = Enumerable.Range(0, 124)
            .Select(index => $"    \"field_{index}\": {{ \"type\": \"string\" }},")
            .ToList();

        lines.Add($"    \"marker\": {{ \"const\": \"{marker}\" }}");

        return "{\n  \"type\": \"object\",\n  \"properties\": {\n" +
               string.Join("\n", lines) +
               "\n  }\n}";
    }

    private static void AssertProjection(Kafdeck.Core.Ecosystem.ConnectConnectorDetail projection)
    {
        Assert.Equal("payments", projection.Name);
        Assert.Equal("example.PaymentsConnector", projection.SafeConfiguration["connector.class"]);
        Assert.Equal("payments", projection.SafeConfiguration["topics"]);
        Assert.Equal("[REDACTED]", projection.SafeConfiguration["connection.url"]);
        Assert.Equal("[REDACTED]", projection.SafeConfiguration["custom.field.1"]);
        Assert.Equal("[REDACTED]", projection.SafeConfiguration["custom.secret.0"]);
    }

    private void Report(string scenario, int operations, TimeSpan elapsed)
    {
        Assert.True(elapsed > TimeSpan.Zero);
        var operationsPerSecond = operations / elapsed.TotalSeconds;
        _output.WriteLine(
            "V04_BENCHMARK scenario={0} operations={1} elapsed_ms={2:F3} ops_per_sec={3:F2} framework={4} processor_count={5}",
            scenario,
            operations,
            elapsed.TotalMilliseconds,
            operationsPerSecond,
            Environment.Version,
            Environment.ProcessorCount);
    }
}
