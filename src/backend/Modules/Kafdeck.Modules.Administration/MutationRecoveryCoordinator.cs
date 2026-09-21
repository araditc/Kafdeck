namespace Kafdeck.Modules.Administration;

public sealed class MutationRecoveryCoordinator
{
    public const int DefaultMaxRecoveryOperations = 1_000;

    private readonly IMutationOperationRepository _repository;
    private readonly IMutationAuditSink _audit;
    private readonly TimeProvider _timeProvider;

    public MutationRecoveryCoordinator(
        IMutationOperationRepository repository,
        IMutationAuditSink audit,
        TimeProvider? timeProvider = null)
    {
        _repository = repository ?? throw new ArgumentNullException(nameof(repository));
        _audit = audit ?? throw new ArgumentNullException(nameof(audit));
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public async Task<int> RecoverInterruptedExecutionsAsync(
        int maxOperations = DefaultMaxRecoveryOperations,
        CancellationToken cancellationToken = default)
    {
        if (maxOperations is < 1 or > 10_000)
        {
            throw new ArgumentOutOfRangeException(nameof(maxOperations));
        }

        var interrupted = await _repository.ListByStateAsync(
            MutationOperationState.Executing,
            checked(maxOperations + 1),
            cancellationToken).ConfigureAwait(false);

        if (interrupted.Count > maxOperations)
        {
            throw new MutationStateException(
                $"More than {maxOperations} interrupted mutation executions require reconciliation. Mutation startup fails closed.");
        }

        var recovered = 0;
        foreach (var snapshot in interrupted)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var operation = MutationOperation.Restore(snapshot);
            var code = snapshot.DispatchStartedAtUtc is null
                ? "process_interrupted_before_dispatch"
                : "process_interrupted_after_dispatch";

            operation.Complete(
                snapshot.DispatchStartedAtUtc is null
                    ? MutationExecutionResultKind.FailedBeforeDispatch
                    : MutationExecutionResultKind.ExecutionUnknown,
                code,
                _timeProvider.GetUtcNow());

            var saved = await _repository.TrySaveAsync(
                operation.Snapshot,
                snapshot.Version,
                cancellationToken).ConfigureAwait(false);

            if (saved.Outcome == MutationSaveOutcome.VersionConflict)
            {
                continue;
            }

            if (saved.Outcome != MutationSaveOutcome.Saved)
            {
                throw new MutationStateException(
                    $"Interrupted mutation '{snapshot.OperationId:D}' could not be reconciled: {saved.Outcome}.");
            }

            if (snapshot.ExecutionClaimGeneration > 0)
            {
                await _repository.ReleaseResourceClaimsAsync(
                    snapshot.OperationId,
                    snapshot.ExecutionClaimGeneration,
                    CancellationToken.None).ConfigureAwait(false);
            }

            await _audit.WriteAsync(
                new MutationAuditEvent(
                    _timeProvider.GetUtcNow(),
                    MutationAuditEventType.Completed,
                    operation.Snapshot.OperationId,
                    operation.Snapshot.RequesterPrincipalId,
                    operation.Snapshot.ClusterId,
                    operation.Snapshot.OperationKind,
                    operation.Snapshot.Risk.RiskClass,
                    operation.Snapshot.State,
                    operation.Snapshot.ResourceKeys,
                    operation.Snapshot.PreviewHash,
                    code),
                CancellationToken.None).ConfigureAwait(false);

            recovered++;
        }

        return recovered;
    }
}
