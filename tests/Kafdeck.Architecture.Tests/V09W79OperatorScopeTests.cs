using Kafdeck.Api;
using Kafdeck.Core.Security;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V09W79OperatorScopeTests
{
    private static readonly DateTimeOffset At = new(2026, 11, 6, 0, 0, 0, TimeSpan.Zero);
    private static readonly Guid Session = Guid.Parse("11111111-1111-4111-8111-111111111111");
    private static OperatorSessionContext Context(string subject = "operator-one", Guid? session = null,
        string? name = null, string? email = null, string[]? groups = null) => new(
            new OperatorIdentity(new("https://identity.example/realm", subject), name, email, groups),
            new SessionId(session ?? Session), At);

    [Fact]
    public void Same_verified_session_preserves_opaque_scope_across_cosmetic_identity_updates()
    {
        var first = OperatorSessionUiScope.Create(Context(), new string('a', 64));
        var renamed = OperatorSessionUiScope.Create(Context(name: "New display", email: "new@example.test"), new string('a', 64));
        Assert.NotNull(first); Assert.Matches("^[a-f0-9]{64}$", first); Assert.Equal(first, renamed);
    }

    [Fact]
    public void Distinct_subjects_with_same_email_and_authentication_time_never_share_ui_scope()
    {
        Assert.NotEqual(OperatorSessionUiScope.Create(Context(email: "shared@example.test"), new string('a', 64)),
            OperatorSessionUiScope.Create(Context("operator-two", email: "shared@example.test"), new string('a', 64)));
    }

    [Fact]
    public void Reauthentication_authority_change_and_group_change_each_invalidate_ui_scope()
    {
        var first = OperatorSessionUiScope.Create(Context(), new string('a', 64));
        Assert.NotEqual(first, OperatorSessionUiScope.Create(Context(session: Guid.Parse("22222222-2222-4222-8222-222222222222")), new string('a', 64)));
        Assert.NotEqual(first, OperatorSessionUiScope.Create(Context(), new string('b', 64)));
        Assert.NotEqual(first, OperatorSessionUiScope.Create(Context(groups: ["new-group"]), new string('a', 64)));
    }

    [Theory]
    [InlineData(null)] [InlineData("")] [InlineData("legacy-unbound-authority")]
    public void Missing_or_invalid_authority_never_creates_a_reusable_scope(string? fingerprint) =>
        Assert.Null(OperatorSessionUiScope.Create(Context(), fingerprint));
}
