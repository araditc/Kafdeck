using System.Diagnostics;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class NotificationHttpTelemetryCaptureTests
{
    [Fact]
    public void Exact_secret_watch_scans_every_http_activity_without_global_event_overflow()
    {
        const string secret = "g2-telemetry-secret-sentinel";
        using var capture = new NotificationHttpTelemetryCapture(secret);
        using var source = new ActivitySource("System.Net.Http.Kafdeck.TestProbe");

        // More than the legacy capture's 4,096-entry budget, with a single
        // deliberately exposed credential at the end of the sequence.
        // The bounded-memory streaming proof must still detect that leak.
        for (var index = 0; index < 5_000; index++)
        {
            using var activity = source.StartActivity("bounded-telemetry-probe");
            Assert.NotNull(activity);
            activity.SetTag("proof-value", index == 4_999 ? secret : "nonsecret");
        }

        Assert.True(capture.Contains(secret));
        Assert.Throws<ArgumentException>(() => capture.Contains("unregistered-secret"));
    }

    [Fact]
    public void Exact_secret_watch_keeps_clean_high_volume_traffic_nonleaking()
    {
        using var capture = new NotificationHttpTelemetryCapture("never-present-secret");
        using var source = new ActivitySource("System.Net.Http.Kafdeck.CleanProbe");
        for (var index = 0; index < 4_200; index++)
        {
            using var activity = source.StartActivity("clean-telemetry");
            Assert.NotNull(activity);
            activity.SetTag("status", "ok");
        }

        Assert.False(capture.Contains("never-present-secret"));
    }
}
