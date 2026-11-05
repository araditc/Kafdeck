namespace Kafdeck.Modules.Administration;

/// <summary>Thrown only before durable dispatch when a transactional live governance gate rejects.</summary>
public sealed class GovernanceEffectRejectedException(string code, bool stale = false) : Exception(code)
{
    public string Code { get; } = code;
    public bool Stale { get; } = stale;
}
