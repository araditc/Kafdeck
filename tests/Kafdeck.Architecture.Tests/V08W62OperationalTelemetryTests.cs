using Kafdeck.Api;
using Kafdeck.Core.Ecosystem;
using Kafdeck.Core.ReadViews;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W62OperationalTelemetryTests
{
    [Fact]
    public async Task Ksql_provider_decorator_emits_only_fixed_operational_dimensions()
    {
        using var telemetry =
            new ApiTelemetry(maxActiveSeries: 8);
        var inner =
            new StubKsqlQueryPort(
                ReadViewResult<KsqlQueryResult>.Failed(
                    new ReadViewFailure(
                        ReadViewFailureCategory.Unauthorized,
                        "denied",
                        "Denied.",
                        false)));
        var decorated =
            new TelemetryKsqlQueryPort(
                inner,
                telemetry);

        var result =
            await decorated.ExecuteQueryAsync(
                "secret-cluster-name",
                "SELECT sensitive_column FROM secret_topic EMIT CHANGES;",
                KsqlQueryLimits.Default,
                CancellationToken.None);

        Assert.False(result.IsSuccess);

        var snapshot = telemetry.Snapshot();
        var series =
            Assert.Single(snapshot.OperationalSeries);
        Assert.Equal(
            "provider",
            series.Kind);
        Assert.Equal(
            "ksql_query",
            series.Family);
        Assert.Equal(
            "denied",
            series.Outcome);

        var prometheus =
            telemetry.RenderPrometheus();
        Assert.DoesNotContain(
            "secret-cluster-name",
            prometheus,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "sensitive_column",
            prometheus,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "secret_topic",
            prometheus,
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task Provider_exception_records_failed_without_swallowing_exception()
    {
        using var telemetry =
            new ApiTelemetry(maxActiveSeries: 8);
        var decorated =
            new TelemetryKsqlQueryPort(
                new ThrowingKsqlQueryPort(),
                telemetry);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => decorated.ExecuteQueryAsync(
                "prod",
                "SELECT 1 EMIT CHANGES;",
                KsqlQueryLimits.Default,
                CancellationToken.None));

        var series =
            Assert.Single(
                telemetry.Snapshot().OperationalSeries);
        Assert.Equal(
            "failed",
            series.Outcome);
    }

    private sealed class StubKsqlQueryPort
        : IKsqlQueryPort
    {
        private readonly ReadViewResult<KsqlQueryResult> _result;

        public StubKsqlQueryPort(
            ReadViewResult<KsqlQueryResult> result)
        {
            _result = result;
        }

        public Task<ReadViewResult<KsqlQueryResult>>
            ExecuteQueryAsync(
                string clusterId,
                string statement,
                KsqlQueryLimits limits,
                CancellationToken cancellationToken)
        {
            _ = clusterId;
            _ = statement;
            _ = limits;
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(_result);
        }
    }

    private sealed class ThrowingKsqlQueryPort
        : IKsqlQueryPort
    {
        public Task<ReadViewResult<KsqlQueryResult>>
            ExecuteQueryAsync(
                string clusterId,
                string statement,
                KsqlQueryLimits limits,
                CancellationToken cancellationToken)
        {
            _ = clusterId;
            _ = statement;
            _ = limits;
            cancellationToken.ThrowIfCancellationRequested();
            throw new InvalidOperationException(
                "provider failure");
        }
    }
}
