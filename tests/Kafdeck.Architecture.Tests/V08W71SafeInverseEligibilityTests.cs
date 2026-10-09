using Kafdeck.Core.Compensation;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W71SafeInverseEligibilityTests
{
    private static CompensationEligibilityEvidence Eligible() => new(
        CompensationInverseFamily.TypedConfigurationRestore,
        OriginalEffectVerified: true,
        OriginalEffectAmbiguous: false,
        CurrentStateReadable: true,
        ExactCurrentStateMatches: true,
        InverseCapabilityAvailable: true,
        CurrentPolicyAllowsInverse: true,
        ExactPriorStateAvailable: true,
        RequiresNewPlan: false);

    [Fact]
    public void No_inverse_family_is_never_compensatable()
    {
        var input = Eligible() with { InverseFamily = CompensationInverseFamily.None };
        Assert.Equal(CompensationEligibility.NotCompensatable,
            SafeInverseEligibility.Evaluate(input));
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, true)]
    [InlineData(false, true)]
    public void Unverified_or_ambiguous_external_effect_fails_closed(
        bool verified, bool ambiguous)
    {
        var input = Eligible() with
        {
            OriginalEffectVerified = verified,
            OriginalEffectAmbiguous = ambiguous,
        };
        Assert.Equal(CompensationEligibility.BlockedByUnknownEffect,
            SafeInverseEligibility.Evaluate(input));
    }

    [Fact]
    public void Drift_blocks_even_fully_authorized_inverse()
    {
        var input = Eligible() with { ExactCurrentStateMatches = false };
        Assert.Equal(CompensationEligibility.BlockedByDrift,
            SafeInverseEligibility.Evaluate(input));
    }

    [Fact]
    public void Missing_readback_cannot_claim_compensatable()
    {
        var input = Eligible() with { CurrentStateReadable = false };
        Assert.Equal(CompensationEligibility.RequiresFreshPlan,
            SafeInverseEligibility.Evaluate(input));
    }

    [Fact]
    public void Every_mutation_policy_and_prior_state_gate_is_independent()
    {
        foreach (var input in new[]
        {
            Eligible() with { InverseCapabilityAvailable = false },
            Eligible() with { CurrentPolicyAllowsInverse = false },
            Eligible() with { ExactPriorStateAvailable = false },
            Eligible() with { RequiresNewPlan = true },
        })
            Assert.Equal(CompensationEligibility.RequiresFreshPlan,
                SafeInverseEligibility.Evaluate(input));
    }

    [Theory]
    [InlineData(CompensationInverseFamily.TypedConfigurationRestore)]
    [InlineData(CompensationInverseFamily.PauseResume)]
    [InlineData(CompensationInverseFamily.AutomationPolicyToggle)]
    public void Only_explicit_typed_inverse_families_can_be_eligible(
        CompensationInverseFamily family)
    {
        Assert.Equal(CompensationEligibility.Compensatable,
            SafeInverseEligibility.Evaluate(Eligible() with { InverseFamily = family }));
    }

    [Fact]
    public void Unknown_inverse_enum_fails_closed()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            SafeInverseEligibility.Evaluate(
                Eligible() with { InverseFamily = (CompensationInverseFamily)999 }));
    }
}
