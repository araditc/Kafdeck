using Kafdeck.Core.Security;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class SecurityAuditTests
{
    [Fact]
    public void Operator_audit_principal_uses_canonical_issuer_and_subject_only()
    {
        var identity = new OperatorIdentity(
            new OperatorIdentityKey("https://idp.example", "alice"),
            "Alice Example",
            "alice@example.test",
            new[] { "admins" });

        var principal = SecurityAuditPrincipal.FromOperator(identity);

        Assert.Equal("oidc:https://idp.example|alice", principal);
        Assert.DoesNotContain("alice@example.test", principal, StringComparison.Ordinal);
        Assert.DoesNotContain("admins", principal, StringComparison.Ordinal);
    }

    [Fact]
    public void Audit_contract_contains_no_secret_or_token_fields()
    {
        var names = typeof(SecurityAuditEvent)
            .GetProperties()
            .Select(property => property.Name)
            .ToArray();

        Assert.DoesNotContain(names, name => name.Contains("Token", StringComparison.OrdinalIgnoreCase));
        Assert.DoesNotContain(names, name => name.Contains("Cookie", StringComparison.OrdinalIgnoreCase));
        Assert.DoesNotContain(names, name => name.Contains("Secret", StringComparison.OrdinalIgnoreCase));
        Assert.DoesNotContain(names, name => name.Contains("Password", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Audit_contract_requires_non_empty_principal_and_reason()
    {
        Assert.Throws<ArgumentException>(() => new SecurityAuditEvent(
            DateTimeOffset.UtcNow,
            SecurityAuditEventType.AuthorizationDenied,
            "",
            null,
            null,
            null,
            SecurityAuditOutcome.Denied,
            "rbac_denied"));

        Assert.Throws<ArgumentException>(() => new SecurityAuditEvent(
            DateTimeOffset.UtcNow,
            SecurityAuditEventType.AuthorizationDenied,
            SecurityAuditPrincipal.Anonymous,
            null,
            null,
            null,
            SecurityAuditOutcome.Denied,
            ""));
    }
}
