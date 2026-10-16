using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Modules.Connect;

namespace Kafdeck.Api;

/// <summary>Finite round-robin scheduler. Existing per-activation DB leases, current guards and ambiguity circuits own all effects.</summary>
public sealed class ConnectAutoRestartHostedService(KafdeckOptions options, IConnectAutoRestartStateStore store,
    ConnectAutoRestartController controller) : BackgroundService
{
    private readonly string _worker = $"connect-auto-restart:{Environment.ProcessId}:{Guid.NewGuid():N}";
    private readonly Dictionary<string, int> _positions = new(StringComparer.Ordinal);
    private int _profilePosition;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (options.Administration?.Mutations.Enabled != true || options.Administration.ConnectAutoRestart?.Enabled != true) return;
        while (!stoppingToken.IsCancellationRequested)
        {
            using var deadline = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
            deadline.CancelAfter(TimeSpan.FromSeconds(30));
            try { await RunOnceAsync(deadline.Token).ConfigureAwait(false); }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { return; }
            catch { /* Discovery retries; effects remain fenced by the existing durable controller. No raw provider reply is logged. */ }
            try { await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken).ConfigureAwait(false); }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { return; }
        }
    }

    public async Task RunOnceAsync(CancellationToken cancellationToken)
    {
        if (options.Administration?.Mutations.Enabled != true || options.Administration.ConnectAutoRestart?.Enabled != true) return;
        var profiles = options.Clusters.SelectMany(cluster => KafkaConnectProfileSet.Effective(cluster)
            .Select(profile => (Cluster: cluster.Id, Profile: profile.Id))).OrderBy(p => p.Cluster, StringComparer.Ordinal)
            .ThenBy(p => p.Profile, StringComparer.Ordinal).ToArray();
        if (profiles.Length == 0) return;
        var profile = profiles[(int)((uint)_profilePosition++ % (uint)profiles.Length)];
        var active = (await store.ListActiveAsync(profile.Cluster, profile.Profile, ConnectAutoRestartPolicy.HardMaxActivePoliciesPerProfile,
            cancellationToken).ConfigureAwait(false)).OrderBy(a => a.ActivationId).ToArray();
        if (active.Length == 0) return;
        var key = profile.Cluster + "\n" + profile.Profile;
        _positions.TryGetValue(key, out var position);
        var count = Math.Min(8, active.Length);
        _positions[key] = (int)(((uint)position + (uint)count) % (uint)active.Length);
        for (var index = 0; index < count; index++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var activation = active[(int)(((uint)position + (uint)index) % (uint)active.Length)];
            await controller.RunOnceAsync(activation.ActivationId, _worker, cancellationToken).ConfigureAwait(false);
        }
    }
}
