using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Api;

public enum RuntimeTelemetryFamily
{
    MutationDispatch = 1,
    DataJobProvider = 2,
    DataGeneratorProvider = 3,
    DataJobWorker = 4,
    DataGeneratorWorker = 5,
}

public enum RuntimeTelemetryOutcome
{
    Completed = 1,
    Idle = 2,
    AppliedVerified = 3,
    AppliedUnverified = 4,
    PartiallyApplied = 5,
    ExecutionUnknown = 6,
    FailedBeforeDispatch = 7,
    FailedDefinitive = 8,
    RateLimited = 9,
    AuthorizationDenied = 10,
    Cancelled = 11,
    Exception = 12,
}

public sealed class RuntimeTelemetry : IDisposable
{
    public const string InstrumentationName = "Kafdeck.Runtime";
    public const string LogCategory = "Kafdeck.Telemetry";

    private static readonly EventId CompletionEvent =
        new(8201, "RuntimeOperationCompleted");

    private readonly ActivitySource _activitySource =
        new(InstrumentationName);
    private readonly ILogger _logger;
    private readonly int _maxTraceAttributes;
    private readonly int _maxLogAttributes;
    private readonly int _maxDiagnosticStringBytes;

    public RuntimeTelemetry(
        KafdeckOptions options,
        SafeRuntimeTelemetryLoggerFactory loggerFactory)
        : this(
            ObservabilityOptions
                .Effective(options)
                .MaxTraceAttributes,
            ObservabilityOptions
                .Effective(options)
                .MaxLogAttributes,
            ObservabilityOptions
                .Effective(options)
                .MaxDiagnosticStringBytes,
            loggerFactory.CreateLogger())
    {
    }

    public RuntimeTelemetry(
        int maxTraceAttributes = ObservabilityOptions.DefaultMaxTraceAttributes,
        int maxLogAttributes = ObservabilityOptions.DefaultMaxLogAttributes,
        int maxDiagnosticStringBytes = ObservabilityOptions.DefaultMaxDiagnosticStringBytes,
        ILogger? logger = null)
    {
        if (maxTraceAttributes <
                ObservabilityOptions.MinimumMaxTraceAttributes ||
            maxTraceAttributes >
                ObservabilityOptions.HardMaxTraceAttributes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxTraceAttributes));
        }

        if (maxLogAttributes <
                ObservabilityOptions.MinimumMaxLogAttributes ||
            maxLogAttributes >
                ObservabilityOptions.HardMaxLogAttributes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxLogAttributes));
        }

        if (maxDiagnosticStringBytes <
                ObservabilityOptions.MinimumMaxDiagnosticStringBytes ||
            maxDiagnosticStringBytes >
                ObservabilityOptions.HardMaxDiagnosticStringBytes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxDiagnosticStringBytes));
        }

        _maxTraceAttributes = maxTraceAttributes;
        _maxLogAttributes = maxLogAttributes;
        _maxDiagnosticStringBytes = maxDiagnosticStringBytes;
        _logger = logger ?? NullLogger.Instance;
    }

    public RuntimeTelemetryScope Start(
        RuntimeTelemetryFamily family)
    {
        var activity = _activitySource.StartActivity(
            "kafdeck.runtime.operation",
            ActivityKind.Internal);

        if (_maxTraceAttributes >= 1)
        {
            activity?.SetTag(
                "kafdeck.runtime.family",
                FamilyName(family));
        }

        return new RuntimeTelemetryScope(
            this,
            family,
            activity,
            Stopwatch.GetTimestamp());
    }

    public void RecordWorkerCycle(
        RuntimeTelemetryFamily family,
        int attempted,
        double elapsedMilliseconds)
    {
        if (family is not (
                RuntimeTelemetryFamily.DataJobWorker or
                RuntimeTelemetryFamily.DataGeneratorWorker))
        {
            throw new ArgumentOutOfRangeException(
                nameof(family));
        }

        var outcome = attempted > 0
            ? RuntimeTelemetryOutcome.Completed
            : RuntimeTelemetryOutcome.Idle;

        WriteSafeLog(
            family,
            outcome,
            elapsedMilliseconds,
            attempted);
    }

    internal void Complete(
        RuntimeTelemetryFamily family,
        RuntimeTelemetryOutcome outcome,
        Activity? activity,
        long startedTimestamp)
    {
        var elapsed =
            Stopwatch.GetElapsedTime(
                startedTimestamp)
            .TotalMilliseconds;

        if (_maxTraceAttributes >= 2)
        {
            activity?.SetTag(
                "kafdeck.runtime.outcome",
                OutcomeName(outcome));
        }

        if (IsFailure(outcome))
        {
            activity?.SetStatus(
                ActivityStatusCode.Error,
                OutcomeName(outcome));
        }

        WriteSafeLog(
            family,
            outcome,
            elapsed,
            workItems: null);
    }

    private void WriteSafeLog(
        RuntimeTelemetryFamily family,
        RuntimeTelemetryOutcome outcome,
        double elapsedMilliseconds,
        int? workItems)
    {
        // This is deliberately a fixed message template. Runtime telemetry
        // accepts only bounded enums and numeric measurements: no resource
        // names, operation IDs, principals, provider bodies, payloads,
        // query text, exception text or credentials enter the log event.
        // Configuration validation guarantees at least four attributes and
        // a diagnostic-string budget large enough for this fixed template.
        if (_maxLogAttributes < 4 ||
            _maxDiagnosticStringBytes < 64)
        {
            return;
        }

        if (workItems is null)
        {
            _logger.LogInformation(
                CompletionEvent,
                "Kafdeck runtime telemetry event. Family={Family} Outcome={Outcome} DurationMs={DurationMs}",
                FamilyName(family),
                OutcomeName(outcome),
                Math.Max(0, elapsedMilliseconds));
            return;
        }

        _logger.LogInformation(
            CompletionEvent,
            "Kafdeck runtime telemetry event. Family={Family} Outcome={Outcome} DurationMs={DurationMs} WorkItems={WorkItems}",
            FamilyName(family),
            OutcomeName(outcome),
            Math.Max(0, elapsedMilliseconds),
            Math.Max(0, workItems.Value));
    }

    public static RuntimeTelemetryOutcome FromMutationResult(
        MutationExecutionResultKind result) =>
        result switch
        {
            MutationExecutionResultKind.AppliedVerified =>
                RuntimeTelemetryOutcome.AppliedVerified,
            MutationExecutionResultKind.AppliedUnverified =>
                RuntimeTelemetryOutcome.AppliedUnverified,
            MutationExecutionResultKind.PartiallyApplied =>
                RuntimeTelemetryOutcome.PartiallyApplied,
            MutationExecutionResultKind.ExecutionUnknown =>
                RuntimeTelemetryOutcome.ExecutionUnknown,
            MutationExecutionResultKind.FailedBeforeDispatch =>
                RuntimeTelemetryOutcome.FailedBeforeDispatch,
            MutationExecutionResultKind.FailedDefinitive =>
                RuntimeTelemetryOutcome.FailedDefinitive,
            _ => RuntimeTelemetryOutcome.Exception,
        };

    public static RuntimeTelemetryOutcome FromMutationState(
        MutationOperationState state) =>
        state switch
        {
            MutationOperationState.AppliedVerified =>
                RuntimeTelemetryOutcome.AppliedVerified,
            MutationOperationState.AppliedUnverified =>
                RuntimeTelemetryOutcome.AppliedUnverified,
            MutationOperationState.PartiallyApplied =>
                RuntimeTelemetryOutcome.PartiallyApplied,
            MutationOperationState.ExecutionUnknown =>
                RuntimeTelemetryOutcome.ExecutionUnknown,
            MutationOperationState.FailedBeforeDispatch =>
                RuntimeTelemetryOutcome.FailedBeforeDispatch,
            MutationOperationState.FailedDefinitive =>
                RuntimeTelemetryOutcome.FailedDefinitive,
            MutationOperationState.Cancelled =>
                RuntimeTelemetryOutcome.Cancelled,
            _ => RuntimeTelemetryOutcome.Completed,
        };

    public static string FamilyName(
        RuntimeTelemetryFamily family) =>
        family switch
        {
            RuntimeTelemetryFamily.MutationDispatch =>
                "mutation-dispatch",
            RuntimeTelemetryFamily.DataJobProvider =>
                "data-job-provider",
            RuntimeTelemetryFamily.DataGeneratorProvider =>
                "data-generator-provider",
            RuntimeTelemetryFamily.DataJobWorker =>
                "data-job-worker",
            RuntimeTelemetryFamily.DataGeneratorWorker =>
                "data-generator-worker",
            _ => "other",
        };

    public static string OutcomeName(
        RuntimeTelemetryOutcome outcome) =>
        outcome switch
        {
            RuntimeTelemetryOutcome.Completed => "completed",
            RuntimeTelemetryOutcome.Idle => "idle",
            RuntimeTelemetryOutcome.AppliedVerified =>
                "applied-verified",
            RuntimeTelemetryOutcome.AppliedUnverified =>
                "applied-unverified",
            RuntimeTelemetryOutcome.PartiallyApplied =>
                "partially-applied",
            RuntimeTelemetryOutcome.ExecutionUnknown =>
                "execution-unknown",
            RuntimeTelemetryOutcome.FailedBeforeDispatch =>
                "failed-before-dispatch",
            RuntimeTelemetryOutcome.FailedDefinitive =>
                "failed-definitive",
            RuntimeTelemetryOutcome.RateLimited =>
                "rate-limited",
            RuntimeTelemetryOutcome.AuthorizationDenied =>
                "authorization-denied",
            RuntimeTelemetryOutcome.Cancelled => "cancelled",
            RuntimeTelemetryOutcome.Exception => "exception",
            _ => "other",
        };

    private static bool IsFailure(
        RuntimeTelemetryOutcome outcome) =>
        outcome is
            RuntimeTelemetryOutcome.PartiallyApplied or
            RuntimeTelemetryOutcome.ExecutionUnknown or
            RuntimeTelemetryOutcome.FailedBeforeDispatch or
            RuntimeTelemetryOutcome.FailedDefinitive or
            RuntimeTelemetryOutcome.AuthorizationDenied or
            RuntimeTelemetryOutcome.Exception;

    public void Dispose() =>
        _activitySource.Dispose();
}

public sealed class RuntimeTelemetryScope : IDisposable
{
    private readonly RuntimeTelemetry _owner;
    private readonly RuntimeTelemetryFamily _family;
    private readonly Activity? _activity;
    private readonly long _startedTimestamp;
    private int _completed;

    internal RuntimeTelemetryScope(
        RuntimeTelemetry owner,
        RuntimeTelemetryFamily family,
        Activity? activity,
        long startedTimestamp)
    {
        _owner = owner;
        _family = family;
        _activity = activity;
        _startedTimestamp = startedTimestamp;
    }

    public void Complete(
        RuntimeTelemetryOutcome outcome)
    {
        if (Interlocked.Exchange(
                ref _completed,
                1) != 0)
        {
            return;
        }

        _owner.Complete(
            _family,
            outcome,
            _activity,
            _startedTimestamp);
        _activity?.Dispose();
    }

    public void Complete(
        MutationExecutionResultKind result) =>
        Complete(
            RuntimeTelemetry.FromMutationResult(
                result));

    public void Dispose()
    {
        if (Volatile.Read(
                ref _completed) == 0)
        {
            Complete(
                RuntimeTelemetryOutcome.Exception);
        }
    }
}
