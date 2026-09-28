using System.Diagnostics;

namespace Kafdeck.Core;

public static class KafdeckRuntimeTelemetry
{
    public const string InstrumentationName = "Kafdeck.Runtime";

    private static readonly ActivitySource Source =
        new(InstrumentationName);

    public static Activity? StartMutationDispatch() =>
        Source.StartActivity(
            "kafdeck.mutation.dispatch",
            ActivityKind.Internal);

    public static Activity? StartDataJobCycle() =>
        Source.StartActivity(
            "kafdeck.data_job.cycle",
            ActivityKind.Internal);

    public static Activity? StartDataGeneratorCycle() =>
        Source.StartActivity(
            "kafdeck.data_generator.cycle",
            ActivityKind.Internal);

    public static Activity? StartKsqlQuery() =>
        Source.StartActivity(
            "kafdeck.ksql.query",
            ActivityKind.Client);

    public static void MarkSucceeded(Activity? activity) =>
        activity?.SetStatus(ActivityStatusCode.Ok);

    public static void MarkFailed(
        Activity? activity,
        RuntimeTelemetryFailure failure)
    {
        if (activity is null)
        {
            return;
        }

        activity.SetStatus(
            ActivityStatusCode.Error,
            failure switch
            {
                RuntimeTelemetryFailure.Cancelled =>
                    "cancelled",
                RuntimeTelemetryFailure.Timeout =>
                    "timeout",
                RuntimeTelemetryFailure.ProviderUnavailable =>
                    "provider_unavailable",
                RuntimeTelemetryFailure.InvalidResponse =>
                    "invalid_response",
                RuntimeTelemetryFailure.ExecutionFailed =>
                    "execution_failed",
                _ => "failed",
            });
    }
}

public enum RuntimeTelemetryFailure
{
    Failed = 1,
    Cancelled = 2,
    Timeout = 3,
    ProviderUnavailable = 4,
    InvalidResponse = 5,
    ExecutionFailed = 6,
}
