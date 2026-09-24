using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V06W41FleetProgressTests
{
    private static readonly DateTimeOffset Now =
        new(2026, 9, 24, 9, 30, 0, TimeSpan.Zero);

    [Fact]
    public void Observation_lifetime_survives_restore_and_generation_change()
    {
        var progress = FleetOperationProgress.Create(Guid.NewGuid(), 1, Now);
        Assert.Equal(
            TimeSpan.FromHours(2),
            progress.ChargeActiveObservation(TimeSpan.FromHours(2), Now.AddHours(2)));

        var restored = FleetOperationProgress.Restore(progress.Snapshot);
        restored.FenceToGeneration(2, Now.AddHours(3));

        Assert.Equal(
            TimeSpan.FromHours(22),
            restored.ChargeActiveObservation(TimeSpan.FromHours(30), Now.AddHours(25)));
        Assert.Equal(
            FleetRuntimeLimits.MaxActiveObservationWindow,
            restored.Snapshot.ActiveObservationElapsed);
        Assert.Equal(FleetProgressPhase.WaitingForExternalAction, restored.Snapshot.Phase);
        Assert.Equal(2, restored.Snapshot.WorkerGeneration);

        Assert.Equal(
            TimeSpan.Zero,
            restored.ChargeActiveObservation(TimeSpan.FromMinutes(1), Now.AddHours(26)));
    }

    [Fact]
    public void Negative_observation_delta_is_rejected_without_reducing_total()
    {
        var progress = FleetOperationProgress.Create(Guid.NewGuid(), 1, Now);
        progress.ChargeActiveObservation(TimeSpan.FromMinutes(10), Now.AddMinutes(10));

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            progress.ChargeActiveObservation(TimeSpan.FromTicks(-1), Now.AddMinutes(9)));

        Assert.Equal(TimeSpan.FromMinutes(10), progress.Snapshot.ActiveObservationElapsed);
    }

    [Fact]
    public void Post_cap_reads_are_reserved_before_dispatch_and_cannot_be_released_after_dispatch()
    {
        var progress = AtObservationCap();
        var reservation = progress.ReservePostCapReconciliationReads(8, Now.AddHours(25));

        Assert.Equal(8, progress.Snapshot.PostCapReconciliationReadCount);
        Assert.Equal(
            FleetPostCapReadReservationState.Reserved,
            Assert.Single(progress.Snapshot.PostCapReadReservations).State);

        var restored = FleetOperationProgress.Restore(progress.Snapshot);
        restored.FenceToGeneration(2, Now.AddHours(26));
        restored.MarkPostCapReservationDispatched(
            reservation.ReservationId,
            Now.AddHours(26).AddMinutes(1));

        Assert.Equal(8, restored.Snapshot.PostCapReconciliationReadCount);
        Assert.Throws<MutationStateException>(() =>
            restored.ReleasePostCapReservationBeforeDispatch(
                reservation.ReservationId,
                Now.AddHours(26).AddMinutes(2)));
    }

    [Fact]
    public void Proven_no_call_release_replenishes_only_the_reserved_units()
    {
        var progress = AtObservationCap();
        var reservation = progress.ReservePostCapReconciliationReads(8, Now.AddHours(25));

        progress.ReleasePostCapReservationBeforeDispatch(
            reservation.ReservationId,
            Now.AddHours(25).AddMinutes(1));

        Assert.Equal(0, progress.Snapshot.PostCapReconciliationReadCount);
        Assert.Equal(
            FleetPostCapReadReservationState.ReleasedBeforeDispatch,
            Assert.Single(progress.Snapshot.PostCapReadReservations).State);

        progress.ReservePostCapReconciliationReads(8, Now.AddHours(25).AddMinutes(2));
        Assert.Equal(8, progress.Snapshot.PostCapReconciliationReadCount);
    }

    [Fact]
    public void Post_cap_limits_fail_closed()
    {
        var progress = AtObservationCap();

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            progress.ReservePostCapReconciliationReads(
                FleetRuntimeLimits.MaxPostCapReconciliationReadsPerRequest + 1,
                Now.AddHours(25)));

        for (var index = 0; index < 8; index++)
        {
            progress.ReservePostCapReconciliationReads(
                FleetRuntimeLimits.MaxPostCapReconciliationReadsPerRequest,
                Now.AddHours(25).AddMinutes(index));
        }

        Assert.Equal(
            FleetRuntimeLimits.MaxPostCapReconciliationReadCount,
            progress.Snapshot.PostCapReconciliationReadCount);

        Assert.Throws<MutationStateException>(() =>
            progress.ReservePostCapReconciliationReads(1, Now.AddHours(26)));
    }

    [Fact]
    public void Post_cap_reads_are_unavailable_before_active_observation_cap()
    {
        var progress = FleetOperationProgress.Create(Guid.NewGuid(), 1, Now);

        Assert.Throws<MutationStateException>(() =>
            progress.ReservePostCapReconciliationReads(1, Now.AddMinutes(1)));
    }

    [Fact]
    public void Restore_rejects_counter_reservation_mismatch()
    {
        var snapshot = new FleetOperationProgressSnapshot
        {
            OperationId = Guid.NewGuid(),
            WorkerGeneration = 1,
            ActiveObservationElapsedTicks = FleetRuntimeLimits.MaxActiveObservationWindow.Ticks,
            PostCapReconciliationReadCount = 1,
            PostCapReadReservations = Array.Empty<FleetPostCapReadReservation>(),
            Phase = FleetProgressPhase.WaitingForExternalAction,
            UpdatedAtUtc = Now,
        };

        Assert.Throws<MutationStateException>(() => FleetOperationProgress.Restore(snapshot));
    }

    private static FleetOperationProgress AtObservationCap()
    {
        var progress = FleetOperationProgress.Create(Guid.NewGuid(), 1, Now);
        progress.ChargeActiveObservation(
            FleetRuntimeLimits.MaxActiveObservationWindow,
            Now.AddHours(24));
        return progress;
    }
}
