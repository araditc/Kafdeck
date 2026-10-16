namespace Kafdeck.Core.Security;

/// <summary>Canonical attribution only. Parsing never authenticates or grants authority.</summary>
public sealed record NonBrowserHumanPrincipalId(OperatorIdentityKey HumanKey, string ClientId, string RegistrationId)
{
    public static bool TryParse(string value, out NonBrowserHumanPrincipalId? principal)
    {
        principal = null;
        const string prefix = "oidc-cli-human:";
        if (string.IsNullOrEmpty(value) || value.Length > 4096 || !value.StartsWith(prefix, StringComparison.Ordinal)) return false;
        var fields = value[prefix.Length..].Split('|');
        if (fields.Length != 4) return false;
        var decoded = fields.Select(Uri.UnescapeDataString).ToArray();
        if (decoded.Any(string.IsNullOrWhiteSpace) || decoded.Any(v => v.Trim() != v || v.Any(char.IsControl)) ||
            decoded[2].Length > 256 || decoded[3].Length > 128 ||
            !fields.SequenceEqual(decoded.Select(Uri.EscapeDataString), StringComparer.Ordinal)) return false;
        try { principal = new(new(decoded[0], decoded[1]), decoded[2], decoded[3]); return true; }
        catch (ArgumentException) { return false; }
    }
}
