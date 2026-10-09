namespace Kafdeck.Architecture.Tests;

/// <summary>
/// Test-only, one-shot virtual timer source. Provider invocation and durable
/// claims run normally, but CI thread scheduling cannot consume fake time.
/// </summary>
internal sealed class DeterministicTimeoutTimeProvider : TimeProvider
{
    private readonly object _gate = new();
    private readonly List<ControlledTimer> _timers = [];
    private readonly TaskCompletionSource<bool> _twoArmed =
        new(TaskCreationOptions.RunContinuationsAsynchronously);
    private DateTimeOffset _now;
    private int _scheduledTimers;

    public DeterministicTimeoutTimeProvider(DateTimeOffset now) => _now = now;
    public Task TwoTimeoutTimersArmed => _twoArmed.Task;

    public override DateTimeOffset GetUtcNow()
    {
        lock (_gate) return _now;
    }

    public override ITimer CreateTimer(
        TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        ArgumentNullException.ThrowIfNull(callback);
        var timer = new ControlledTimer(this, callback, state);
        lock (_gate) _timers.Add(timer);
        timer.Change(dueTime, period);
        return timer;
    }

    public void Advance(TimeSpan elapsed)
    {
        if (elapsed < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(elapsed));

        List<(TimerCallback Callback, object? State)> due = [];
        lock (_gate)
        {
            _now = _now.Add(elapsed);
            foreach (var timer in _timers)
            {
                if (timer.Disposed || timer.DueAtUtc is not { } deadline ||
                    deadline > _now) continue;

                timer.DueAtUtc = null;
                due.Add((timer.Callback, timer.State));
            }
        }

        // Callback may cancel tasks and dispose timers: never invoke under lock.
        foreach (var (callback, state) in due) callback(state);
    }

    private sealed class ControlledTimer : ITimer
    {
        private readonly DeterministicTimeoutTimeProvider _owner;
        public ControlledTimer(
            DeterministicTimeoutTimeProvider owner, TimerCallback callback, object? state)
        {
            _owner = owner;
            Callback = callback;
            State = state;
        }

        internal TimerCallback Callback { get; }
        internal object? State { get; }
        internal DateTimeOffset? DueAtUtc { get; set; }
        internal bool Disposed { get; private set; }

        public bool Change(TimeSpan dueTime, TimeSpan period)
        {
            if (period != Timeout.InfiniteTimeSpan && period != TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(period), "One-shot timers only.");
            if (dueTime < TimeSpan.Zero && dueTime != Timeout.InfiniteTimeSpan)
                throw new ArgumentOutOfRangeException(nameof(dueTime));

            lock (_owner._gate)
            {
                if (Disposed) return false;
                DueAtUtc = dueTime == Timeout.InfiniteTimeSpan
                    ? null
                    : _owner._now.Add(dueTime);
                if (DueAtUtc is not null && ++_owner._scheduledTimers >= 2)
                    _owner._twoArmed.TrySetResult(true);
                return true;
            }
        }

        public void Dispose()
        {
            lock (_owner._gate)
            {
                Disposed = true;
                DueAtUtc = null;
            }
        }

        public ValueTask DisposeAsync()
        {
            Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
