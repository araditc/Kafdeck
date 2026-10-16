using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Microsoft.Extensions.Primitives;

namespace Kafdeck.Api;

/// <summary>One immutable snapshot per decision; invalid reload removes every grant.</summary>
public sealed class ReloadingAuthorizationPolicy : IDisposable
{
    private readonly IConfiguration _configuration;
    private readonly IDisposable _subscription;
    private AuthorizationPolicySnapshot _current;
    private static readonly AuthorizationPolicySnapshot Denied = AuthorizationPolicyCompiler.Compile(new([], [], []));
    public AuthorizationPolicySnapshot Current => Volatile.Read(ref _current);

    public ReloadingAuthorizationPolicy(IConfiguration configuration)
    {
        _configuration = configuration;
        _current = AuthorizationPolicyCompiler.Compile(AuthorizationPolicyConfigurationLoader.Load(configuration));
        _subscription = ChangeToken.OnChange(configuration.GetReloadToken, Reload);
    }
    private void Reload()
    {
        AuthorizationPolicySnapshot next;
        try { next = AuthorizationPolicyCompiler.Compile(AuthorizationPolicyConfigurationLoader.Load(_configuration)); }
        catch (Exception error) when (error is AuthorizationPolicyException or KafdeckConfigurationException or ArgumentException or InvalidOperationException)
        { next = Denied; }
        Volatile.Write(ref _current, next);
    }
    public void Dispose() => _subscription.Dispose();
}
