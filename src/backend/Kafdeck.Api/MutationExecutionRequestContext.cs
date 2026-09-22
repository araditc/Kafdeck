using System.Security.Claims;

namespace Kafdeck.Api;

/// <summary>
/// Carries only the live authenticated request principal through the synchronous
/// mutation execution call chain. The value is never persisted and is cleared
/// when the request-scoped execution scope is disposed.
/// </summary>
public sealed class MutationExecutionRequestContextAccessor
{
    private readonly AsyncLocal<ClaimsPrincipal?> _current = new();

    public ClaimsPrincipal? CurrentPrincipal => _current.Value;

    public IDisposable Push(ClaimsPrincipal principal)
    {
        ArgumentNullException.ThrowIfNull(principal);
        var previous = _current.Value;
        _current.Value = principal;
        return new Scope(this, previous);
    }

    private sealed class Scope : IDisposable
    {
        private MutationExecutionRequestContextAccessor? _owner;
        private readonly ClaimsPrincipal? _previous;

        public Scope(
            MutationExecutionRequestContextAccessor owner,
            ClaimsPrincipal? previous)
        {
            _owner = owner;
            _previous = previous;
        }

        public void Dispose()
        {
            var owner = Interlocked.Exchange(ref _owner, null);
            if (owner is not null)
            {
                owner._current.Value = _previous;
            }
        }
    }
}
