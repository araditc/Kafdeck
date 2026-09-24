using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V06W41FleetConflictObligationTests
{
    private static readonly DateTimeOffset Now =
        new(2026, 9, 24, 10, 0, 0, TimeSpan.Zero);

    [Fact]
    public void Quarantine_unknown_remains_blocking()
    {
        var obligation = NewObligation();

        obligation.ApplyDisposition(
            FleetUncertaintyDispositionOutcome.QuarantineUnknown,
            Guid.NewGuid(),
            "sha256:readback",
            Now.AddMinutes(1));

        Assert.True(obligation.Snapshot.BlocksConflictingDispatch);
        Assert.False(obligation.Snapshot.RequiresUnresolvedPredecessorBinding);
        Assert.False(obligation.Snapshot.NoRedispatchTombstone);
        Assert.Equal(
            FleetConflictObligationState.QuarantinedUnknown,
            obligation.Snapshot.State);
    }

    [Fact]
    public void Supersede_unknown_preserves_predecessor_binding_and_no_redispatch_tombstone()
    {
        var obligation = NewObligation();
        var dispositionId = Guid.NewGuid();

        obligation.ApplyDisposition(
            FleetUncertaintyDispositionOutcome.SupersedeUnknownForNewIntent,
            dispositionId,
            "sha256:still-unknown",
            Now.AddMinutes(1));

        Assert.False(obligation.Snapshot.BlocksConflictingDispatch);
        Assert.True(obligation.Snapshot.RequiresUnresolvedPredecessorBinding);
        Assert.True(obligation.Snapshot.NoRedispatchTombstone);
        Assert.Equal(dispositionId, obligation.Snapshot.DispositionOperationId);
        Assert.Equal(
            FleetConflictObligationState.SupersededUnknown,
            obligation.Snapshot.State);
    }

    [Theory]
    [InlineData(FleetUncertaintyDispositionOutcome.ObservedNonApplication)]
    [InlineData(FleetUncertaintyDispositionOutcome.ObservedTerminalEffect)]
    public void Observed_evidence_releases_blocking_without_manual_clear(
        FleetUncertaintyDispositionOutcome outcome)
    {
        var obligation = NewObligation();

        obligation.ApplyDisposition(
            outcome,
            Guid.NewGuid(),
            "sha256:typed-readback",
            Now.AddMinutes(1));

        Assert.False(obligation.Snapshot.BlocksConflictingDispatch);
        Assert.False(obligation.Snapshot.RequiresUnresolvedPredecessorBinding);
    }

    [Fact]
    public void Closed_outcome_set_cannot_be_applied_twice_after_terminal_resolution()
    {
        var obligation = NewObligation();
        obligation.ApplyDisposition(
            FleetUncertaintyDispositionOutcome.ObservedTerminalEffect,
            Guid.NewGuid(),
            "sha256:terminal",
            Now.AddMinutes(1));

        Assert.Throws<MutationStateException>(() =>
            obligation.ApplyDisposition(
                FleetUncertaintyDispositionOutcome.QuarantineUnknown,
                Guid.NewGuid(),
                "sha256:later",
                Now.AddMinutes(2)));
    }

    [Fact]
    public void Disposition_binding_normalizes_and_binds_original_evidence()
    {
        var originalId = Guid.NewGuid();
        var conflictA = FleetConflictKeyCodec.Topic("prod", "payments-a");
        var conflictB = FleetConflictKeyCodec.TopicPartition("prod", "payments-b", 2);
        var normalized = new FleetUncertaintyDispositionBinding(
            originalId,
            new[] { " step-b ", "step-a", "step-a" },
            new[] { conflictB, conflictA },
            " sha256:evidence ",
            " kafka-4.3.1/confluent-dotnet-2.15.1 ",
            " bounded readback remains inconclusive ")
            .Normalize();

        Assert.Equal(originalId, normalized.OriginalOperationId);
        Assert.Equal(new[] { "step-a", "step-b" }, normalized.StepIds);
        Assert.Equal(
            new[] { conflictA, conflictB }.OrderBy(value => value, StringComparer.Ordinal),
            normalized.ConflictKeys);
        Assert.Equal("sha256:evidence", normalized.LastReadbackEvidenceHash);
        Assert.Equal(
            "kafka-4.3.1/confluent-dotnet-2.15.1",
            normalized.ProviderCapabilityVersion);
    }

    [Fact]
    public void Arbitrary_or_legacy_conflict_key_is_not_accepted_as_durable_fleet_identity()
    {
        Assert.Throws<MutationStateException>(() =>
            FleetConflictObligation.Create(
                Guid.NewGuid(),
                "step-1",
                "cluster/prod/topic/payments",
                "sha256:effect",
                Now));
    }

    [Fact]
    public void Restored_superseded_obligation_without_tombstone_is_rejected()
    {
        var snapshot = NewObligation().Snapshot with
        {
            State = FleetConflictObligationState.SupersededUnknown,
            DispositionOperationId = Guid.NewGuid(),
            NoRedispatchTombstone = false,
        };

        Assert.Throws<MutationStateException>(() =>
            FleetConflictObligation.Restore(snapshot));
    }

    private static FleetConflictObligation NewObligation() =>
        FleetConflictObligation.Create(
            Guid.NewGuid(),
            "step-1",
            FleetConflictKeyCodec.Topic("prod", "payments"),
            "sha256:effect",
            Now);
}
