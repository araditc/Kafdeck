using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Tracing;

namespace Kafdeck.Architecture.Tests;

internal sealed class NotificationHttpTelemetryCapture : EventListener
{
    private const int MaxEntries = 4096;
    private const int MaxEntryBytes = 8 * 1024;
    private readonly ConcurrentQueue<string> _captured = new();
    // In guarded provider tests, search exact credentials as events arrive.
    // This keeps memory bounded by the finite watched set rather than the
    // process-wide HTTP event rate from parallel integration laboratories.
    // EventListener can deliver events from its base constructor before
    // our watched set is installed. Empty defaults keep that interval safe.
    private readonly string[] _watchedSecrets = [];
    private readonly int[] _watchedMatches = [];
    private readonly ActivityListener _activities;
    private int _entries;
    private int _overflowed;
    internal NotificationHttpTelemetryCapture(params string[] watchedSecrets)
    {
        ArgumentNullException.ThrowIfNull(watchedSecrets);
        if (watchedSecrets.Length > 8 || watchedSecrets.Any(secret =>
                string.IsNullOrEmpty(secret) || secret.Length > 1024 || secret.Any(char.IsControl)))
            throw new ArgumentException("Telemetry leak checks require up to eight bounded, predeclared credentials.", nameof(watchedSecrets));
        var distinctSecrets = watchedSecrets.Distinct(StringComparer.Ordinal).ToArray();
        _watchedMatches = new int[distinctSecrets.Length];
        _watchedSecrets = distinctSecrets;
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
        if (_watchedSecrets.Length != 0)
        {
            // Never retain sensitive telemetry or drop events due to a global
            // item-count cap. Each HTTP payload is checked in full as observed;
            // only one atomic bit per registered secret is retained.
            for (var i = 0; i < _watchedSecrets.Length; i++)
                if (value.Contains(_watchedSecrets[i], StringComparison.Ordinal))
                    Interlocked.Exchange(ref _watchedMatches[i], 1);
            return;
        }
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
        if (_watchedSecrets.Length != 0)
        {
            var index = Array.IndexOf(_watchedSecrets, secret);
            if (index < 0) throw new ArgumentException("Unregistered telemetry leak probe.", nameof(secret));
            return Volatile.Read(ref _watchedMatches[index]) != 0;
        }
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
