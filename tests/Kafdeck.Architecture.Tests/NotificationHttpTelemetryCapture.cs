using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Tracing;

namespace Kafdeck.Architecture.Tests;

internal sealed class NotificationHttpTelemetryCapture : EventListener
{
    private const int MaxEntries = 4096;
    private const int MaxEntryBytes = 8 * 1024;
    private readonly ConcurrentQueue<string> _captured = new();
    private readonly ActivityListener _activities;
    private int _entries;
    private int _overflowed;
    internal NotificationHttpTelemetryCapture()
    {
        _activities = new ActivityListener
        {
            ShouldListenTo = source => source.Name.StartsWith("System.Net.Http", StringComparison.Ordinal),
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            SampleUsingParentId = (ref ActivityCreationOptions<string> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity =>
            {
                Capture(activity.DisplayName);
                foreach (var tag in activity.TagObjects) Capture(tag.Value?.ToString() ?? string.Empty);
            },
        };
        ActivitySource.AddActivityListener(_activities);
    }
    protected override void OnEventSourceCreated(EventSource source)
    {
        if (source.Name == "System.Net.Http") EnableEvents(source, EventLevel.Verbose, EventKeywords.All);
    }
    protected override void OnEventWritten(EventWrittenEventArgs data)
    {
        if (_captured is null || data.Payload is null) return;
        foreach (var item in data.Payload) Capture(item?.ToString() ?? string.Empty);
    }
    private void Capture(string value)
    {
        if (System.Text.Encoding.UTF8.GetByteCount(value) > MaxEntryBytes ||
            Interlocked.Increment(ref _entries) > MaxEntries)
        {
            Volatile.Write(ref _overflowed, 1);
            return;
        }
        _captured.Enqueue(value);
    }
    internal bool Contains(string secret)
    {
        if (Volatile.Read(ref _overflowed) != 0)
            throw new InvalidOperationException("HTTP telemetry capture exceeded its finite proof budget; absence of a leak cannot be established.");
        return _captured.Any(value => value.Contains(secret, StringComparison.Ordinal));
    }
    public override void Dispose()
    {
        _activities.Dispose();
        base.Dispose();
    }
}
