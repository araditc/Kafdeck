using System.Globalization;
using System.Security.Cryptography;
using System.Text.Json;
using Kafdeck.Core.Security;

namespace Kafdeck.Api;

/// <summary>Opaque UI cache boundary, never an authorization grant or authentication credential.</summary>
internal static class OperatorSessionUiScope
{
    internal static string? ManagedAuthority(string? fingerprint, ManagedUser? user)
    {
        if (user is null) return fingerprint;
        if (fingerprint is not { Length: 64 } || fingerprint.Any(c => !char.IsAsciiHexDigit(c))) return null;
        return Convert.ToHexString(SHA256.HashData(JsonSerializer.SerializeToUtf8Bytes(new
        {
            policy = fingerprint, user.Id, user.Role, user.Enabled, user.SessionsValidFrom,
            clusters = user.ClusterIds.Order(StringComparer.Ordinal),
        }))).ToLowerInvariant();
    }

    internal static string? Create(OperatorSessionContext session, string? authorizationFingerprint)
    {
        if (authorizationFingerprint is not { Length: 64 } ||
            authorizationFingerprint.Any(c => !char.IsAsciiHexDigit(c))) return null;
        var binding = JsonSerializer.SerializeToUtf8Bytes(new
        {
            actor = SecurityAuditPrincipal.FromOperator(session.Identity),
            session = session.SessionId.Value.ToString("N"),
            authenticatedAt = session.AuthenticatedAtUtc.ToString("O", CultureInfo.InvariantCulture),
            authority = authorizationFingerprint,
            groups = session.Identity.ExternalGroups,
        });
        return Convert.ToHexString(SHA256.HashData(binding)).ToLowerInvariant();
    }
}
