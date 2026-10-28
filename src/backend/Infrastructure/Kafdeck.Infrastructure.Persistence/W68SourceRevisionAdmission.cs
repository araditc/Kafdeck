namespace Kafdeck.Infrastructure.Persistence;

// Pure comparison guard. A real runtime must fetch trusted, locked persisted
// source registration and ownership from a governed transaction.
internal sealed record W68SourceRevisionEvidence(
    string RegisteredOwnerId, string RegisteredSourceId, string SourceGenerationId,
    string PinnedRevisionSha256, string OwnershipGenerationId);
internal static class W68SourceRevisionAdmission
{
    internal static bool Matches(W68SourceRevisionEvidence? stored, string expectedOwner,
        string expectedSource, string expectedSourceGeneration, string expectedRevisionSha256,
        string expectedOwnershipGeneration)
    {
        if (stored is null || !Valid(expectedOwner) || !Valid(expectedSource) ||
            !Uuid(expectedSourceGeneration) || !Uuid(expectedOwnershipGeneration) ||
            !Uuid(stored.SourceGenerationId) || !Uuid(stored.OwnershipGenerationId) ||
            !Sha(expectedRevisionSha256) || !Sha(stored.PinnedRevisionSha256))
            return false;
        return string.Equals(stored.RegisteredOwnerId, expectedOwner, StringComparison.Ordinal) &&
               string.Equals(stored.RegisteredSourceId, expectedSource, StringComparison.Ordinal) &&
               string.Equals(stored.SourceGenerationId, expectedSourceGeneration, StringComparison.Ordinal) &&
               string.Equals(stored.OwnershipGenerationId, expectedOwnershipGeneration, StringComparison.Ordinal) &&
               string.Equals(stored.PinnedRevisionSha256, expectedRevisionSha256, StringComparison.Ordinal);
    }
    private static bool Valid(string? s) => !string.IsNullOrWhiteSpace(s) &&
        s.Length <= 128 && s == s.Trim();
    private static bool Uuid(string? s) =>
        s is { Length: 36 } && Guid.TryParseExact(s, "D", out var id) &&
        id != Guid.Empty && string.Equals(id.ToString("D"), s, StringComparison.Ordinal);
    private static bool Sha(string? s) => s is { Length: 64 } &&
        s.All(c => c is >= '0' and <= '9' or >= 'a' and <= 'f');
}
