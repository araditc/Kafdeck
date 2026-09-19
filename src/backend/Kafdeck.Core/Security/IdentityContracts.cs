namespace Kafdeck.Core.Security;

public enum AccessMode
{
    Local = 1,
    Token = 2,
    Oidc = 3,
}

public sealed record OperatorIdentityKey
{
    public OperatorIdentityKey(string issuer, string subject)
    {
        Issuer = RequireValue(issuer, nameof(issuer), 2048);
        Subject = RequireValue(subject, nameof(subject), 512);
    }

    public string Issuer { get; }

    public string Subject { get; }

    private static string RequireValue(string value, string parameterName, int maxLength)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value, parameterName);
        var normalized = value.Trim();

        if (normalized.Length > maxLength)
        {
            throw new ArgumentOutOfRangeException(parameterName, $"Value must not exceed {maxLength} characters.");
        }

        return normalized;
    }
}

public sealed record OperatorIdentity
{
    public OperatorIdentity(
        OperatorIdentityKey key,
        string? displayName = null,
        string? email = null,
        IReadOnlyCollection<string>? externalGroups = null)
    {
        Key = key ?? throw new ArgumentNullException(nameof(key));
        DisplayName = NormalizeOptional(displayName, 512, nameof(displayName));
        Email = NormalizeOptional(email, 512, nameof(email));

        var groups = (externalGroups ?? Array.Empty<string>())
            .Select(group => RequireGroup(group))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(group => group, StringComparer.Ordinal)
            .ToArray();

        if (groups.Length > 256)
        {
            throw new ArgumentOutOfRangeException(nameof(externalGroups), "External group count must not exceed 256.");
        }

        ExternalGroups = Array.AsReadOnly(groups);
    }

    public OperatorIdentityKey Key { get; }

    public string? DisplayName { get; }

    public string? Email { get; }

    public IReadOnlyList<string> ExternalGroups { get; }

    private static string RequireGroup(string group)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(group);
        var normalized = group.Trim();

        if (normalized.Length > 512)
        {
            throw new ArgumentOutOfRangeException(nameof(group), "External group value must not exceed 512 characters.");
        }

        return normalized;
    }

    private static string? NormalizeOptional(string? value, int maxLength, string parameterName)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        var normalized = value.Trim();
        if (normalized.Length > maxLength)
        {
            throw new ArgumentOutOfRangeException(parameterName, $"Value must not exceed {maxLength} characters.");
        }

        return normalized;
    }
}

public readonly record struct SessionId
{
    public SessionId(Guid value)
    {
        if (value == Guid.Empty)
        {
            throw new ArgumentException("Session ID must not be empty.", nameof(value));
        }

        Value = value;
    }

    public Guid Value { get; }

    public override string ToString() => "[session-id]";
}

public sealed record OperatorSessionContext
{
    public OperatorSessionContext(
        OperatorIdentity identity,
        SessionId sessionId,
        DateTimeOffset authenticatedAtUtc)
    {
        Identity = identity ?? throw new ArgumentNullException(nameof(identity));
        SessionId = sessionId;
        AuthenticatedAtUtc = authenticatedAtUtc;
    }

    public OperatorIdentity Identity { get; }

    public SessionId SessionId { get; }

    public DateTimeOffset AuthenticatedAtUtc { get; }
}

public enum AuthenticationFailureCategory
{
    MissingIdentity = 1,
    InvalidIdentity = 2,
    InvalidSession = 3,
    SessionExpired = 4,
    ProviderUnavailable = 5,
    ProtocolRejected = 6,
    InvalidConfiguration = 7,
}

public enum AuthorizationFailureCategory
{
    Unauthenticated = 1,
    NoMatchingBinding = 2,
    ActionDenied = 3,
    ResourceDenied = 4,
    InvalidPolicy = 5,
}
