using Kafdeck.Core.Security;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W67NonBrowserPrincipalProposalTests
{
    private static NonBrowserServicePrincipalProposal Candidate(
        string issuer = "https://sso.example.test/realms/kafdeck",
        string audience = "kafdeck-api",
        string subject = "service-account-client-a",
        string clientId = "client-a",
        string registrationId = "automation-a",
        IReadOnlyList<AuthorizationAction>? actions = null) =>
        new(
            registrationId,
            "ops-team",
            issuer,
            audience,
            subject,
            clientId,
            actions ?? [AuthorizationAction.SystemRead]);

    [Fact]
    public void Proposed_machine_identity_is_immutable_and_never_a_runtime_grant()
    {
        var input = new List<AuthorizationAction>
        {
            AuthorizationAction.TopicRead,
            AuthorizationAction.SystemRead,
        };
        var draft = Candidate(actions: input);
        input.Clear();

        Assert.Equal(
            [AuthorizationAction.SystemRead, AuthorizationAction.TopicRead],
            draft.RequestedReadActions);
        Assert.Throws<NotSupportedException>(
            () => ((IList<AuthorizationAction>)draft.RequestedReadActions)[0] =
                AuthorizationAction.TopicDelete);

        var plan = new NonBrowserPrincipalProposalSet([draft]);
        Assert.Single(plan.Proposals);
        Assert.Null(typeof(NonBrowserPrincipalProposalSet)
            .GetMethod("Authenticate"));
        Assert.Null(typeof(NonBrowserPrincipalProposalSet)
            .GetMethod("Authorize"));
        Assert.Null(typeof(NonBrowserPrincipalProposalSet)
            .GetMethod("Enable"));
    }

    [Theory]
    [InlineData("http://sso.example.test/realms/kafdeck")]
    [InlineData("https://user:secret@sso.example.test/realms/kafdeck")]
    [InlineData("https://sso.example.test/realms/kafdeck?aud=other")]
    [InlineData("https://sso.example.test/realms/kafdeck#fragment")]
    [InlineData(" https://sso.example.test/realms/kafdeck")]
    [InlineData("https://SSO.example.test/realms/kafdeck")]
    public void Only_exact_canonical_https_issuer_is_a_valid_proposal(string issuer)
    {
        Assert.Throws<ArgumentException>(() => Candidate(issuer: issuer));
    }

    [Theory]
    [InlineData("kafdeck-*")]
    [InlineData("kafdeck-api?admin")]
    [InlineData(" kafdeck-api")]
    [InlineData("kafdeck-api\n")]
    public void Non_exact_or_wildcard_audiences_are_rejected(string audience)
    {
        Assert.Throws<ArgumentException>(() => Candidate(audience: audience));
    }

    [Theory]
    [InlineData(AuthorizationAction.TopicDelete)]
    [InlineData(AuthorizationAction.RecordRead)]
    [InlineData(AuthorizationAction.MutationReconcile)]
    [InlineData(AuthorizationAction.NotificationManage)]
    [InlineData(AuthorizationAction.RecordExport)]
    [InlineData(AuthorizationAction.DataQualityManage)]
    public void Sensitive_read_or_mutation_permission_cannot_be_proposed_by_initial_contract(
        AuthorizationAction action)
    {
        Assert.Throws<ArgumentException>(
            () => Candidate(actions: [action]));
    }

    [Fact]
    public void Duplicate_action_and_missing_actions_fail_closed()
    {
        Assert.Throws<ArgumentException>(() =>
            Candidate(actions: [AuthorizationAction.SystemRead, AuthorizationAction.SystemRead]));
        Assert.Throws<ArgumentException>(() => Candidate(actions: []));
    }

    [Fact]
    public void Duplicate_registration_or_identity_is_rejected()
    {
        Assert.Throws<ArgumentException>(() =>
            new NonBrowserPrincipalProposalSet(
            [
                Candidate(),
                Candidate(clientId: "client-b", subject: "other-subject"),
            ]));

        Assert.Throws<ArgumentException>(() =>
            new NonBrowserPrincipalProposalSet(
            [
                Candidate(),
                Candidate(registrationId: "automation-b"),
            ]));
    }

    [Fact]
    public void Distinct_exact_machine_identities_have_independent_proposals()
    {
        var plans = new NonBrowserPrincipalProposalSet(
        [
            Candidate(),
            Candidate(
                clientId: "client-b",
                subject: "service-account-client-b",
                registrationId: "automation-b"),
        ]);
        Assert.Equal(2, plans.Proposals.Count);
    }

    [Fact]
    public void Number_of_machine_proposals_is_finite()
    {
        var proposals = Enumerable.Range(0, 129)
            .Select(index => Candidate(
                subject: "service-account-" + index,
                clientId: "client-" + index,
                registrationId: "automation-" + index))
            .ToArray();

        Assert.Throws<ArgumentOutOfRangeException>(
            () => new NonBrowserPrincipalProposalSet(proposals));
    }
}
