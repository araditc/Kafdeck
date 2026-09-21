using Kafdeck.Modules.Administration;

namespace Kafdeck.Api;

public sealed class MutationRecoveryHostedService : BackgroundService
{
    private static readonly TimeSpan RecoveryInterval = TimeSpan.FromSeconds(15);

    private readonly MutationRecoveryCoordinator _recovery;
    private readonly ILogger<MutationRecoveryHostedService> _logger;

    public MutationRecoveryHostedService(
        MutationRecoveryCoordinator recovery,
        ILogger<MutationRecoveryHostedService> logger)
    {
        _recovery = recovery ?? throw new ArgumentNullException(nameof(recovery));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(RecoveryInterval, stoppingToken).ConfigureAwait(false);
                var recovered = await _recovery
                    .RecoverInterruptedExecutionsAsync(cancellationToken: stoppingToken)
                    .ConfigureAwait(false);

                if (recovered > 0)
                {
                    _logger.LogWarning(
                        "Kafdeck reconciled {RecoveredMutationCount} expired/interrupted mutation execution(s).",
                        recovered);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                _logger.LogError(
                    exception,
                    "Mutation recovery reconciliation failed; mutation execution remains persistence-gated and will retry.");
            }
        }
    }
}
