using System.Globalization;
using System.Security.Cryptography;
using System.Text.Json;
using Kafdeck.Core.Security;

namespace Kafdeck.Api;

/// <summary>Opaque UI cache boundary, never an authorization grant or authentication credential.</summary>
internal static class OperatorSessionUiScope
{
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
