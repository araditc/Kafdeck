using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Generator;

public sealed class DataGeneratorActivationHandler :
    IMutationExecutionHandler
{
    private readonly IFleetMutationStateStore _store;
    private readonly DataGeneratorStateCoordinator _state;
    private readonly TimeProvider _timeProvider;

    public DataGeneratorActivationHandler(
        IFleetMutationStateStore store,
        DataGeneratorStateCoordinator state,
        TimeProvider? timeProvider = null)
    {
        _store =
            store ??
            throw new ArgumentNullException(
                nameof(store));
        _state =
            state ??
            throw new ArgumentNullException(
                nameof(state));
        _timeProvider =
            timeProvider ?? TimeProvider.System;
    }

    public MutationOperationKind OperationKind =>
        MutationOperationKind.DataGenerator;

    public async Task<MutationProviderResult> ExecuteAsync(
        MutationExecutionContext context,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);

        if (context.Operation.OperationKind !=
                MutationOperationKind.DataGenerator ||
            context.Operation.ExecutionClaimGeneration <= 0)
        {
            return FailedBeforeDispatch(
                "data_generator_activation_operation_invalid");
        }

        DataGeneratorPlan plan;
        try
        {
            plan =
                DataGeneratorPolicy.DeserializePlan(
                    context.Operation.CanonicalIntent);
        }
        catch
        {
            return FailedBeforeDispatch(
                "data_generator_activation_plan_invalid");
        }

        var now = _timeProvider.GetUtcNow();

        DataGeneratorStateResult initialized;
        try
        {
            initialized =
                await _state.InitializeAsync(
                        context.Operation.OperationId,
                        context.Operation
                            .ExecutionClaimGeneration,
                        plan,
                        now,
                        cancellationToken)
                    .ConfigureAwait(false);
        }
        catch
        {
            return FailedBeforeDispatch(
                "data_generator_progress_persistence_unavailable");
        }

        if (initialized.Outcome is not (
                DataGeneratorStateOutcome.Applied or
                DataGeneratorStateOutcome.Existing))
        {
            return FailedBeforeDispatch(
                initialized.Code);
        }

        FleetConflictObligationSnapshot obligation;
        try
        {
            var conflictKey =
                FleetConflictKeyCodec.TopicPartition(
                    plan.Destination.KafkaClusterId,
                    plan.Destination.TopicName,
                    plan.Destination.Partition);

            obligation =
                FleetConflictObligation.Create(
                        context.Operation.OperationId,
                        "data-generator-destination",
                        conflictKey,
                        plan.PlanFingerprint,
                        now)
                    .Snapshot;
        }
        catch
        {
            await StopBestEffortAsync(
                    context.Operation,
                    plan,
                    cancellationToken)
                .ConfigureAwait(false);

            return FailedBeforeDispatch(
                "data_generator_conflict_contract_invalid");
        }

        FleetConflictObligationCreateResult admitted;
        try
        {
            admitted =
                await _store.CreateConflictObligationAsync(
                        obligation,
                        cancellationToken)
                    .ConfigureAwait(false);
        }
        catch
        {
            await StopBestEffortAsync(
                    context.Operation,
                    plan,
                    CancellationToken.None)
                .ConfigureAwait(false);

            return FailedBeforeDispatch(
                "data_generator_conflict_persistence_unavailable");
        }

        if (admitted.Outcome is
            FleetConflictObligationCreateOutcome.Created or
            FleetConflictObligationCreateOutcome.ExistingSameEffect)
        {
            return new MutationProviderResult(
                MutationExecutionResultKind.AppliedVerified,
                "data_generator_activated",
                new Dictionary<string, string>(
                    StringComparer.Ordinal)
                {
                    ["data-generator.plan"] =
                        plan.PlanFingerprint,
                    ["data-generator.records"] =
                        plan.RecordCount.ToString(
                            System.Globalization
                                .CultureInfo
                                .InvariantCulture),
                });
        }

        await StopBestEffortAsync(
                context.Operation,
                plan,
                CancellationToken.None)
            .ConfigureAwait(false);

        return FailedBeforeDispatch(
            admitted.Outcome switch
            {
                FleetConflictObligationCreateOutcome
                    .LegacyResourceClaimConflict =>
                    "data_generator_legacy_resource_conflict",

                FleetConflictObligationCreateOutcome
                    .FleetConflictScopeConflict =>
                    "data_generator_fleet_conflict",

                FleetConflictObligationCreateOutcome
                    .ExistingDifferentEffect =>
                    "data_generator_existing_effect_conflict",

                FleetConflictObligationCreateOutcome
                    .ParentOperationNotFound =>
                    "data_generator_parent_operation_missing",

                _ =>
                    "data_generator_conflict_admission_failed",
            });
    }

    private async Task StopBestEffortAsync(
        MutationOperationSnapshot operation,
        DataGeneratorPlan plan,
        CancellationToken cancellationToken)
    {
        try
        {
            _ = await _state.StopAsync(
                    operation.OperationId,
                    plan,
                    operation.ExecutionClaimGeneration,
                    _timeProvider.GetUtcNow(),
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch
        {
            // Parent mutation reports failed-before-dispatch.
            // Inert progress cannot cross the runtime effect guard.
        }
    }

    private static MutationProviderResult FailedBeforeDispatch(
        string code) =>
        new(
            MutationExecutionResultKind.FailedBeforeDispatch,
            code);
}
