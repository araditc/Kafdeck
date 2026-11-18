using Kafdeck.Core.Kafka;

namespace Kafdeck.Modules.Administration;

public sealed class DynamicBrokerConfigExecutionHandler(IDynamicConfigurationPort provider, TimeProvider time)
    : IMutationExecutionHandler
{
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(250);

    public MutationOperationKind OperationKind => MutationOperationKind.ClusterConfigAlter;

    public static bool IsAdmitted(MutationOperationSnapshot operation)
    {
        try
        {
            var plan = DynamicConfigurationPolicy.DeserializePlan(operation.CanonicalIntent);
            return plan.Compatibility == KafkaCompatibilityLine.RuntimeBrokerConfig &&
                plan.Mutation.Target is { Scope: DynamicConfigurationScope.BrokerOverride, Key: DynamicConfigurationRegistry.FleetKey } &&
                !string.IsNullOrWhiteSpace(plan.KafkaClusterId);
        }
        catch (ArgumentException)
        {
            return false;
        }
    }

    public async Task<MutationProviderResult> ExecuteAsync(MutationExecutionContext context, CancellationToken cancellationToken)
    {
        if (!IsAdmitted(context.Operation))
            return new(MutationExecutionResultKind.FailedBeforeDispatch, "dynamic_config_policy_not_admitted");

        var plan = DynamicConfigurationPolicy.DeserializePlan(context.Operation.CanonicalIntent);
        var deadline = time.GetUtcNow().AddSeconds(18);
        if (context.ExecutionDeadlineUtc is { } limit && limit < deadline)
            deadline = limit;

        var outcome = await provider.AlterAsync(plan.Mutation, new KafkaOperationContext(deadline), cancellationToken)
            .ConfigureAwait(false);
        if (outcome.ResultKind != MutationExecutionResultKind.AppliedUnverified)
            return outcome;

        var expectedValue = plan.Mutation.Value ?? plan.InheritedValue;
        var expectedSource = plan.Mutation.Value is null ? plan.InheritedSource
            : DynamicConfigurationPolicy.ExpectedDynamicSource(plan.Mutation.Target.Scope);

        while (time.GetUtcNow() < deadline && !cancellationToken.IsCancellationRequested)
        {
            var result = await provider.DescribeAsync(plan.Mutation.Target, new KafkaOperationContext(deadline), cancellationToken)
                .ConfigureAwait(false);
            if (result.IsSuccess && result.Value is { } observed && !observed.IsSensitive && !observed.IsReadOnly &&
                observed.Target == plan.Mutation.Target && observed.KafkaClusterId == plan.KafkaClusterId &&
                observed.EffectiveValue == expectedValue && observed.EffectiveSource == expectedSource)
            {
                return new(MutationExecutionResultKind.AppliedVerified, "dynamic_config_readback_verified",
                    new Dictionary<string, string>
                    {
                        ["configuration.value"] = observed.EffectiveValue!,
                        ["configuration.source"] = observed.EffectiveSource,
                        ["configuration.fingerprint"] = DynamicConfigurationPolicy.Fingerprint(observed),
                    });
            }

            if (cancellationToken.IsCancellationRequested || time.GetUtcNow().Add(PollInterval) >= deadline)
                break;

            try { await Task.Delay(PollInterval, time, cancellationToken).ConfigureAwait(false); }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { break; }
        }

        return outcome with
        {
            ResultCode = "dynamic_config_readback_unverified",
            SafeEvidence = new Dictionary<string, string>
            {
                ["provider.accepted"] = "true",
                ["verification.state"] = "inconclusive",
            },
        };
    }
}
