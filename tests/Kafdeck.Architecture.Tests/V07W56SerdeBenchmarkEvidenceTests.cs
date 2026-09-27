using System.Diagnostics;
using System.Text.Json;
using Kafdeck.Core.Records;
using Kafdeck.Infrastructure.SerDe;
using Xunit;
using Xunit.Abstractions;

namespace Kafdeck.Architecture.Tests;

/// <summary>
/// Repeatable bounded in-process evidence for W56 controlled SerDe codecs.
/// Measurements have no pass/fail performance threshold because CI hardware varies.
/// They are implementation evidence, not a public throughput or allocation SLA.
/// </summary>
public sealed class V07W56SerdeBenchmarkEvidenceTests
{
    private const int WarmupIterations = 100;
    private const int MeasurementIterations = 1_000;
    private readonly ITestOutputHelper _output;

    public V07W56SerdeBenchmarkEvidenceTests(
        ITestOutputHelper output)
    {
        _output = output;
    }

    [Theory]
    [InlineData(ControlledSerdeFormat.Cbor)]
    [InlineData(ControlledSerdeFormat.MessagePack)]
    public void Binary_codec_bounded_round_trip_evidence(
        ControlledSerdeFormat format)
    {
        using var source = JsonDocument.Parse(BuildPayload());
        var service = new ControlledSerdeService();

        for (var index = 0; index < WarmupIterations; index++)
        {
            RoundTrip(service, format, source.RootElement);
        }

        var beforeAllocated =
            GC.GetAllocatedBytesForCurrentThread();
        var stopwatch = Stopwatch.StartNew();

        for (var index = 0; index < MeasurementIterations; index++)
        {
            RoundTrip(service, format, source.RootElement);
        }

        stopwatch.Stop();
        var allocated =
            GC.GetAllocatedBytesForCurrentThread() - beforeAllocated;

        Report(
            $"serde_{format.ToString().ToLowerInvariant()}_roundtrip",
            MeasurementIterations,
            stopwatch.Elapsed,
            allocated);
    }

    [Fact]
    public void Xml_bounded_round_trip_evidence()
    {
        using var source = JsonDocument.Parse(
            """
            {
              "name":"orders",
              "attributes":{"version":"1"},
              "content":[
                {"name":"order","attributes":{"id":"42"},"content":["hello"]},
                {"name":"order","attributes":{"id":"43"},"content":["world"]}
              ]
            }
            """);
        var service = new ControlledSerdeService();

        for (var index = 0; index < WarmupIterations; index++)
        {
            RoundTrip(
                service,
                ControlledSerdeFormat.Xml,
                source.RootElement);
        }

        var beforeAllocated =
            GC.GetAllocatedBytesForCurrentThread();
        var stopwatch = Stopwatch.StartNew();

        for (var index = 0; index < MeasurementIterations; index++)
        {
            RoundTrip(
                service,
                ControlledSerdeFormat.Xml,
                source.RootElement);
        }

        stopwatch.Stop();
        var allocated =
            GC.GetAllocatedBytesForCurrentThread() - beforeAllocated;

        Report(
            "serde_xml_roundtrip",
            MeasurementIterations,
            stopwatch.Elapsed,
            allocated);
    }

    private static void RoundTrip(
        ControlledSerdeService service,
        ControlledSerdeFormat format,
        JsonElement source)
    {
        var deadline = DateTimeOffset.UtcNow.AddSeconds(10);
        var encoded = service.EncodeAsync(
                new ControlledSerdeEncodeRequest(
                    format,
                    source.Clone()),
                ControlledSerdeLimits.Default,
                deadline)
            .GetAwaiter()
            .GetResult();

        Assert.True(
            encoded.IsSuccess,
            encoded.Failure?.SafeMessage);
        Assert.NotNull(encoded.Value);
        Assert.InRange(
            encoded.Value!.Payload.Length,
            1,
            ControlledSerdeLimits.Default.MaxOutputBytes);

        var decoded = service.DecodeAsync(
                new ControlledSerdeDecodeRequest(
                    format,
                    encoded.Value.Payload),
                ControlledSerdeLimits.Default,
                deadline)
            .GetAwaiter()
            .GetResult();

        Assert.True(
            decoded.IsSuccess,
            decoded.Failure?.SafeMessage);
        Assert.NotNull(decoded.Value);
        Assert.True(
            JsonElement.DeepEquals(
                source,
                decoded.Value!.StructuredValue));
    }

    private static string BuildPayload()
    {
        var values = Enumerable.Range(0, 64)
            .ToDictionary(
                index => $"field_{index:D2}",
                index => (object?)new
                {
                    index,
                    active = index % 2 == 0,
                    label = $"value-{index:D2}",
                },
                StringComparer.Ordinal);

        return JsonSerializer.Serialize(values);
    }

    private void Report(
        string scenario,
        int operations,
        TimeSpan elapsed,
        long allocatedBytes)
    {
        Assert.True(elapsed > TimeSpan.Zero);
        Assert.True(allocatedBytes >= 0);

        _output.WriteLine(
            "V07_W56_SERDE_BENCHMARK scenario={0} operations={1} elapsed_ms={2:F3} ops_per_sec={3:F2} allocated_bytes={4} allocated_bytes_per_op={5:F2} framework={6} processor_count={7}",
            scenario,
            operations,
            elapsed.TotalMilliseconds,
            operations / elapsed.TotalSeconds,
            allocatedBytes,
            allocatedBytes / (double)operations,
            Environment.Version,
            Environment.ProcessorCount);
    }
}
