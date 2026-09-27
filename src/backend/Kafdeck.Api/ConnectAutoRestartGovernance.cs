using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Modules.Connect;

namespace Kafdeck.Api;

public sealed class ConfiguredConnectAutoRestartRuntimePolicyProvider :
    IConnectAutoRestartRuntimePolicyProvider
{
    private readonly KafdeckOptions _options;

    public ConfiguredConnectAutoRestartRuntimePolicyProvider(
        KafdeckOptions options)
    {
        _options =
            options ?? throw new ArgumentNullException(nameof(options));
    }

    public Task<ConnectAutoRestartRuntimePolicySnapshot> GetCurrentAsync(
        ConnectAutoRestartTarget target,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(target);
        cancellationToken.ThrowIfCancellationRequested();

        var configured =
            _options.Administration?.ConnectAutoRestart ??
            ConnectAutoRestartOptions.DisabledDefault;

        var policy = new ConnectAutoRestartPolicy(
            configured.Enabled,
            configured.MaxAttempts,
            TimeSpan.FromSeconds(
                configured.InitialBackoffSeconds),
            TimeSpan.FromSeconds(
                configured.MaxBackoffSeconds),
            TimeSpan.FromSeconds(
                configured.ActivationLifetimeSeconds),
            configured.MaxActivePoliciesPerProfile,
            configured.JitterBasisPoints);

        return Task.FromResult(
            new ConnectAutoRestartRuntimePolicySnapshot(
                configured.Enabled,
                configured.PolicyVersion,
                policy));
    }
}

public sealed class ConfiguredConnectAutoRestartGovernancePort :
    IConnectAutoRestartGovernancePort
{
    private static readonly string MissingProviderFingerprint =
        new('0', 64);

    private readonly KafdeckOptions _options;
    private readonly AuthorizationPolicyEvaluator _authorization;
    private readonly IConnectAutoRestartRuntimePolicyProvider _runtimePolicy;

    public ConfiguredConnectAutoRestartGovernancePort(
        KafdeckOptions options,
        AuthorizationPolicyEvaluator authorization,
        IConnectAutoRestartRuntimePolicyProvider runtimePolicy)
    {
        _options =
            options ?? throw new ArgumentNullException(nameof(options));
        _authorization =
            authorization ??
            throw new ArgumentNullException(nameof(authorization));
        _runtimePolicy =
            runtimePolicy ??
            throw new ArgumentNullException(nameof(runtimePolicy));
    }

    public async Task<ConnectAutoRestartGovernanceSnapshot> GetCurrentAsync(
        ConnectAutoRestartActivation activation,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(activation);

        var policy =
            await _runtimePolicy
                .GetCurrentAsync(
                    activation.Target,
                    cancellationToken)
                .ConfigureAwait(false);

        var decision =
            _authorization.EvaluateCanonicalDirectSubject(
                activation.AutomationPrincipalId,
                new AuthorizationRequest(
                    AuthorizationAction.ConnectRestart,
                    activation.Target.ClusterId,
                    activation.Target.AuthorizationResource));

        var providerFingerprint =
            CurrentProviderFingerprint(
                activation.Target);

        return new ConnectAutoRestartGovernanceSnapshot(
            decision.IsAllowed,
            providerFingerprint,
            policy.Fingerprint,
            decision.IsAllowed
                ? null
                : "auto_restart_authorization_revoked");
    }

    private string CurrentProviderFingerprint(
        ConnectAutoRestartTarget target)
    {
        var cluster = _options.Clusters
            .SingleOrDefault(item =>
                string.Equals(
                    item.Id,
                    target.ClusterId,
                    StringComparison.Ordinal));

        if (cluster is null ||
            !KafkaConnectProfileSet
                .Effective(cluster)
                .Any(profile =>
                    string.Equals(
                        profile.Id,
                        target.ConnectProfileId,
                        StringComparison.Ordinal)))
        {
            return MissingProviderFingerprint;
        }

        return KafkaConnectProfileIdentityFingerprint.Compute(
            cluster,
            target.ConnectProfileId);
    }
}
