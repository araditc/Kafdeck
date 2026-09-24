using Kafdeck.Core.Security;

namespace Kafdeck.Modules.Administration;

public enum FleetUncertaintyDispositionOutcome
{
    ObservedNonApplication = 1,
    ObservedTerminalEffect = 2,
    QuarantineUnknown = 3,
    SupersedeUnknownForNewIntent = 4,
}

public enum FleetConflictObligationState
{
    Outstanding = 1,
    ObservedNonApplication = 2,
    ObservedTerminalEffect = 3,
    QuarantinedUnknown = 4,
    SupersededUnknown = 5,
}

public sealed record FleetConflictObligationSnapshot
{
    public const int CurrentSchemaVersion = 1;

    public required Guid ObligationId { get; init; }
    public required Guid OperationId { get; init; }
    public required string StepId { get; init; }
    public required string ConflictKey { get; init; }
    public required string EffectFingerprint { get; init; }
    public int SchemaVersion { get; init; } = CurrentSchemaVersion;
    public FleetConflictObligationState State { get; init; } =
        FleetConflictObligationState.Outstanding;
    public Guid? DispositionOperationId { get; init; }
    public string? SafeResolutionEvidenceHash { get; init; }
    public bool NoRedispatchTombstone { get; init; }
    public long Version { get; init; }
    public required DateTimeOffset CreatedAtUtc { get; init; }
    public required DateTimeOffset UpdatedAtUtc { get; init; }

    public bool BlocksConflictingDispatch =>
        State is
            FleetConflictObligationState.Outstanding or
            FleetConflictObligationState.QuarantinedUnknown;

    public bool RequiresUnresolvedPredecessorBinding =>
        State == FleetConflictObligationState.SupersededUnknown;
}

public sealed record FleetUncertaintyDispositionBinding(
    Guid OriginalOperationId,
    IReadOnlyList<string> StepIds,
    IReadOnlyList<string> ConflictKeys,
    string LastReadbackEvidenceHash,
    string ProviderCapabilityVersion,
    string Reason)
{
    public FleetUncertaintyDispositionBinding Normalize()
    {
        if (OriginalOperationId == Guid.Empty)
        {
            throw new MutationStateException(
                "Uncertainty disposition requires the unresolved original operation ID.");
        }

        var steps = NormalizeStrings(StepIds, "step ID", 128);
        var conflicts = NormalizeStrings(ConflictKeys, "conflict key", 2048);
        if (steps.Count == 0 || conflicts.Count == 0)
        {
            throw new MutationStateException(
                "Uncertainty disposition must bind at least one step and conflict key.");
        }

        return this with
        {
            StepIds = steps,
            ConflictKeys = conflicts,
            LastReadbackEvidenceHash = RequireBounded(
                LastReadbackEvidenceHash,
                "last readback evidence hash",
                256),
            ProviderCapabilityVersion = RequireBounded(
                ProviderCapabilityVersion,
                "provider capability version",
                256),
            Reason = RequireBounded(Reason, "disposition reason", 1024),
        };
    }

    private static IReadOnlyList<string> NormalizeStrings(
        IReadOnlyList<string> values,
        string fieldName,
        int maxLength)
    {
        ArgumentNullException.ThrowIfNull(values);
        return Array.AsReadOnly(values
            .Select(value => RequireBounded(value, fieldName, maxLength))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(value => value, StringComparer.Ordinal)
            .ToArray());
    }

    private static string RequireBounded(
        string value,
        string fieldName,
        int maxLength)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        var normalized = value.Trim();
        if (normalized.Length > maxLength || normalized.Any(char.IsControl))
        {
            throw new MutationStateException(
                $"{fieldName} is invalid or exceeds the admitted bound.");
        }

        return normalized;
    }
}

public sealed class FleetConflictObligation
{
    private FleetConflictObligation(FleetConflictObligationSnapshot snapshot)
    {
        Snapshot = Validate(snapshot);
    }

    public FleetConflictObligationSnapshot Snapshot { get; private set; }

    public static FleetConflictObligation Create(
        Guid operationId,
        string stepId,
        string conflictKey,
        string effectFingerprint,
        DateTimeOffset nowUtc)
    {
        if (operationId == Guid.Empty)
        {
            throw new ArgumentException("Operation ID is required.", nameof(operationId));
        }

        return new FleetConflictObligation(new FleetConflictObligationSnapshot
        {
            ObligationId = Guid.NewGuid(),
            OperationId = operationId,
            StepId = RequireBounded(stepId, nameof(stepId), 128),
            ConflictKey = RequireBounded(conflictKey, nameof(conflictKey), 2048),
            EffectFingerprint = RequireBounded(
                effectFingerprint,
                nameof(effectFingerprint),
                256),
            CreatedAtUtc = nowUtc,
            UpdatedAtUtc = nowUtc,
        });
    }

    public static FleetConflictObligation Restore(
        FleetConflictObligationSnapshot snapshot) =>
        new(snapshot ?? throw new ArgumentNullException(nameof(snapshot)));

    public void ApplyDisposition(
        FleetUncertaintyDispositionOutcome outcome,
        Guid dispositionOperationId,
        string safeResolutionEvidenceHash,
        DateTimeOffset nowUtc)
    {
        if (dispositionOperationId == Guid.Empty)
        {
            throw new ArgumentException(
                "Disposition operation ID is required.",
                nameof(dispositionOperationId));
        }

        if (Snapshot.State is not (
            FleetConflictObligationState.Outstanding or
            FleetConflictObligationState.QuarantinedUnknown))
        {
            throw new MutationStateException(
                $"Conflict obligation in state '{Snapshot.State}' cannot receive another uncertainty disposition.");
        }

        var evidence = RequireBounded(
            safeResolutionEvidenceHash,
            nameof(safeResolutionEvidenceHash),
            256);

        var targetState = outcome switch
        {
            FleetUncertaintyDispositionOutcome.ObservedNonApplication =>
                FleetConflictObligationState.ObservedNonApplication,
            FleetUncertaintyDispositionOutcome.ObservedTerminalEffect =>
                FleetConflictObligationState.ObservedTerminalEffect,
            FleetUncertaintyDispositionOutcome.QuarantineUnknown =>
                FleetConflictObligationState.QuarantinedUnknown,
            FleetUncertaintyDispositionOutcome.SupersedeUnknownForNewIntent =>
                FleetConflictObligationState.SupersededUnknown,
            _ => throw new ArgumentOutOfRangeException(
                nameof(outcome),
                outcome,
                "Unsupported uncertainty disposition outcome."),
        };

        Snapshot = Snapshot with
        {
            State = targetState,
            DispositionOperationId = dispositionOperationId,
            SafeResolutionEvidenceHash = evidence,
            NoRedispatchTombstone =
                Snapshot.NoRedispatchTombstone ||
                outcome == FleetUncertaintyDispositionOutcome.SupersedeUnknownForNewIntent,
            Version = checked(Snapshot.Version + 1),
            UpdatedAtUtc = nowUtc,
        };
    }

    private static FleetConflictObligationSnapshot Validate(
        FleetConflictObligationSnapshot snapshot)
    {
        if (snapshot.ObligationId == Guid.Empty || snapshot.OperationId == Guid.Empty)
        {
            throw new MutationStateException(
                "Fleet conflict obligation requires non-empty identities.");
        }

        if (snapshot.SchemaVersion != FleetConflictObligationSnapshot.CurrentSchemaVersion)
        {
            throw new MutationStateException(
                $"Fleet conflict obligation schema version '{snapshot.SchemaVersion}' is unsupported.");
        }

        var normalizedStep = RequireBounded(snapshot.StepId, nameof(snapshot.StepId), 128);
        var normalizedConflict = RequireBounded(
            snapshot.ConflictKey,
            nameof(snapshot.ConflictKey),
            2048);
        var normalizedFingerprint = RequireBounded(
            snapshot.EffectFingerprint,
            nameof(snapshot.EffectFingerprint),
            256);

        if (snapshot.State == FleetConflictObligationState.SupersededUnknown &&
            (!snapshot.NoRedispatchTombstone ||
             snapshot.DispositionOperationId is null))
        {
            throw new MutationStateException(
                "Superseded uncertainty must retain a no-redispatch tombstone and disposition identity.");
        }

        if (snapshot.State == FleetConflictObligationState.QuarantinedUnknown &&
            snapshot.DispositionOperationId is null)
        {
            throw new MutationStateException(
                "Quarantined uncertainty must retain its disposition identity.");
        }

        return snapshot with
        {
            StepId = normalizedStep,
            ConflictKey = normalizedConflict,
            EffectFingerprint = normalizedFingerprint,
            SafeResolutionEvidenceHash = snapshot.SafeResolutionEvidenceHash is null
                ? null
                : RequireBounded(
                    snapshot.SafeResolutionEvidenceHash,
                    nameof(snapshot.SafeResolutionEvidenceHash),
                    256),
        };
    }

    private static string RequireBounded(
        string value,
        string fieldName,
        int maxLength)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        var normalized = value.Trim();
        if (normalized.Length > maxLength || normalized.Any(char.IsControl))
        {
            throw new MutationStateException(
                $"{fieldName} is invalid or exceeds the admitted bound.");
        }

        return normalized;
    }
}

public enum FleetCurrentIdentityResolutionState
{
    Available = 1,
    Unavailable = 2,
    Ineligible = 3,
}

public sealed record FleetCurrentIdentityResolution(
    FleetCurrentIdentityResolutionState State,
    OperatorIdentity? Identity = null);

public interface IFleetCurrentIdentityResolver
{
    ValueTask<FleetCurrentIdentityResolution> ResolveCurrentAsync(
        string canonicalPrincipalId,
        CancellationToken cancellationToken = default);
}
