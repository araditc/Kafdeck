using System.Diagnostics;
using Kafdeck.Core;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W62RuntimeTracingTests
{
    [Fact]
    public void Runtime_trace_names_are_closed_and_emit_no_resource_identifiers()
    {
        var observed = new List<Activity>();

        using var listener = new ActivityListener
        {
            ShouldListenTo = source =>
                string.Equals(
                    source.Name,
                    KafdeckRuntimeTelemetry.InstrumentationName,
                    StringComparison.Ordinal),
            Sample = static (
                ref ActivityCreationOptions<ActivityContext> _) =>
                ActivitySamplingResult.AllData,
            ActivityStopped = activity =>
                observed.Add(activity),
        };
        ActivitySource.AddActivityListener(listener);

        using (var activity =
               KafdeckRuntimeTelemetry.StartMutationDispatch())
        {
            Assert.NotNull(activity);
            KafdeckRuntimeTelemetry.MarkSucceeded(activity);
        }

        using (var activity =
               KafdeckRuntimeTelemetry.StartDataJobCycle())
        {
            Assert.NotNull(activity);
        }

        using (var activity =
               KafdeckRuntimeTelemetry.StartDataGeneratorCycle())
        {
            Assert.NotNull(activity);
        }

        using (var activity =
               KafdeckRuntimeTelemetry.StartKsqlQuery())
        {
            Assert.NotNull(activity);
            KafdeckRuntimeTelemetry.MarkFailed(
                activity,
                RuntimeTelemetryFailure.Timeout);
        }

        Assert.Equal(
            new[]
            {
                "kafdeck.mutation.dispatch",
                "kafdeck.data_job.cycle",
                "kafdeck.data_generator.cycle",
                "kafdeck.ksql.query",
            },
            observed.Select(item => item.OperationName).ToArray());

        Assert.All(
            observed,
            activity => Assert.Empty(activity.Tags));

        Assert.Equal(
            ActivityStatusCode.Ok,
            observed[0].Status);
        Assert.Equal(
            ActivityStatusCode.Error,
            observed[3].Status);
        Assert.Equal(
            "timeout",
            observed[3].StatusDescription);
    }

    [Theory]
    [InlineData(RuntimeTelemetryFailure.Cancelled, "cancelled")]
    [InlineData(RuntimeTelemetryFailure.Timeout, "timeout")]
    [InlineData(
        RuntimeTelemetryFailure.ProviderUnavailable,
        "provider_unavailable")]
    [InlineData(
        RuntimeTelemetryFailure.InvalidResponse,
        "invalid_response")]
    [InlineData(
        RuntimeTelemetryFailure.ExecutionFailed,
        "execution_failed")]
    public void Runtime_failure_descriptions_are_closed_safe_categories(
        RuntimeTelemetryFailure failure,
        string expected)
    {
        Activity? stopped = null;

        using var listener = new ActivityListener
        {
            ShouldListenTo = source =>
                string.Equals(
                    source.Name,
                    KafdeckRuntimeTelemetry.InstrumentationName,
                    StringComparison.Ordinal),
            Sample = static (
                ref ActivityCreationOptions<ActivityContext> _) =>
                ActivitySamplingResult.AllData,
            ActivityStopped = activity =>
                stopped = activity,
        };
        ActivitySource.AddActivityListener(listener);

        using (var activity =
               KafdeckRuntimeTelemetry.StartKsqlQuery())
        {
            KafdeckRuntimeTelemetry.MarkFailed(
                activity,
                failure);
        }

        Assert.NotNull(stopped);
        Assert.Equal(
            ActivityStatusCode.Error,
            stopped!.Status);
        Assert.Equal(
            expected,
            stopped.StatusDescription);
        Assert.Empty(stopped.Tags);
    }
}
