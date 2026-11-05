namespace Kafdeck.Modules.Administration;

public static class MutationGovernancePreconditions
{
    /// <summary>Governance markers remain in preview/idempotency hashes and are checked by the shared gate.
    /// Existing provider material comparisons continue to validate their own closed precondition family.</summary>
    public static IReadOnlyList<MutationPrecondition> ProviderOnly(MutationOperationSnapshot operation) =>
        operation.Preconditions.Where(p=>!p.Key.StartsWith("v09.governance:",StringComparison.Ordinal)).ToArray();
}
