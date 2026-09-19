using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Security;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class IdentityFoundationTests
{
    [Fact]
    public void Canonical_operator_identity_is_issuer_and_subject()
    {
        var first = new OperatorIdentityKey("https://idp.example", "operator-123");
        var same = new OperatorIdentityKey("https://idp.example", "operator-123");
        var differentIssuer = new OperatorIdentityKey("https://other.example", "operator-123");
        var differentSubject = new OperatorIdentityKey("https://idp.example", "operator-456");

        Assert.Equal(first, same);
        Assert.NotEqual(first, differentIssuer);
        Assert.NotEqual(first, differentSubject);
    }

    [Fact]
    public void Presentation_claims_do_not_change_canonical_identity()
    {
        var key = new OperatorIdentityKey("https://idp.example", "operator-123");

        var identity = new OperatorIdentity(
            key,
            displayName: "Alice Operator",
            email: "alice@example.test",
            externalGroups: new[] { "kafka-readers", "kafka-readers", "platform" });

        Assert.Equal(key, identity.Key);
        Assert.Equal(new[] { "kafka-readers", "platform" }, identity.ExternalGroups);
    }

    [Theory]
    [InlineData("", "subject")]
    [InlineData("issuer", "")]
    [InlineData(" ", "subject")]
    [InlineData("issuer", " ")]
    public void Canonical_identity_rejects_missing_issuer_or_subject(string issuer, string subject)
    {
        Assert.Throws<ArgumentException>(() => new OperatorIdentityKey(issuer, subject));
    }

    [Fact]
    public void Session_identifier_does_not_render_raw_value()
    {
        var value = Guid.NewGuid();
        var sessionId = new SessionId(value);

        Assert.Equal(value, sessionId.Value);
        Assert.DoesNotContain(value.ToString(), sessionId.ToString(), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Deployment_token_is_never_an_oidc_operator_identity_path()
    {
        Assert.True(DeploymentAccessModePolicy.UsesDeploymentToken(AccessMode.Token));
        Assert.False(DeploymentAccessModePolicy.UsesDeploymentToken(AccessMode.Local));
        Assert.False(DeploymentAccessModePolicy.UsesDeploymentToken(AccessMode.Oidc));

        Assert.True(DeploymentAccessModePolicy.RequiresOperatorIdentity(AccessMode.Oidc));
        Assert.False(DeploymentAccessModePolicy.RequiresOperatorIdentity(AccessMode.Token));
        Assert.False(DeploymentAccessModePolicy.RequiresOperatorIdentity(AccessMode.Local));
    }
}
