namespace Kafdeck.Core.Compensation;

/// <summary>
/// Advisory, closed-world safe-inverse evaluation. An eligible preview is
/// not authorization to execute; mutation/RBAC/risk/approval still govern
/// every future effect. No provider invocation occurs here.
/// </summary>
public enum CompensationEligibility
{
    NotCompensatable = 1,
    RequiresFreshPlan = 2,
    BlockedByDrift = 3,
    BlockedByUnknownEffect = 4,
    Compensatable = 5,
}

public enum CompensationInverseFamily
{
    None = 0,
    TypedConfigurationRestore = 1,
    PauseResume = 2,
    AutomationPolicyToggle = 3,
}

public sealed record CompensationEligibilityEvidence(
    CompensationInverseFamily InverseFamily,
    bool OriginalEffectVerified,
    bool OriginalEffectAmbiguous,
    bool CurrentStateReadable,
    bool ExactCurrentStateMatches,
    bool InverseCapabilityAvailable,
    bool CurrentPolicyAllowsInverse,
    bool ExactPriorStateAvailable,
    bool RequiresNewPlan);

public static class SafeInverseEligibility
{
    public static CompensationEligibility Evaluate(CompensationEligibilityEvidence evidence)
    {
        ArgumentNullException.ThrowIfNull(evidence);
        if (!Enum.IsDefined(evidence.InverseFamily))
            throw new ArgumentOutOfRangeException(nameof(evidence));

        // Closed-world: irreversible and unclassified families cannot acquire
        // compensation eligibility from permissive booleans.
        if (evidence.InverseFamily == CompensationInverseFamily.None)
            return CompensationEligibility.NotCompensatable;

        // Never assume an external effect was either applied or not applied.
        if (evidence.OriginalEffectAmbiguous || !evidence.OriginalEffectVerified)
            return CompensationEligibility.BlockedByUnknownEffect;

        if (!evidence.CurrentStateReadable)
            return CompensationEligibility.RequiresFreshPlan;

        if (!evidence.ExactCurrentStateMatches)
            return CompensationEligibility.BlockedByDrift;

        // Prior state, capability, and current policy are independently
        // necessary. This decision alone never schedules or approves a write.
        if (!evidence.InverseCapabilityAvailable ||
            !evidence.CurrentPolicyAllowsInverse ||
            !evidence.ExactPriorStateAvailable ||
            evidence.RequiresNewPlan)
            return CompensationEligibility.RequiresFreshPlan;

        return CompensationEligibility.Compensatable;
    }
}
