using Kafdeck.Core.Kafka;

namespace Kafdeck.Modules.Administration;

/// <summary>
/// Typed read-only ACL observation boundary used by W42 planning, access
/// analysis and pre-dispatch readback. It exposes no generic AdminClient
/// escape hatch and returns only normalized ACL metadata.
/// </summary>
public interface IAclObservationPort
{
    Task<KafkaResult<IReadOnlyList<KafkaAclBinding>>> DescribeAsync(
        string clusterId,
        KafkaAclBindingFilter filter,
        KafkaOperationContext operation,
        CancellationToken cancellationToken = default);
}


/// <summary>
/// Typed ACL mutation boundary for W42. Requests contain only the exact,
/// server-materialized ACL bindings that were frozen by the governed preview.
/// There is deliberately no arbitrary filter delete or generic AdminClient
/// surface here.
/// </summary>
public sealed record AclCreateMutation(
    string ClusterId,
    IReadOnlyList<KafkaAclBinding> Bindings);

public sealed record AclRemoveMutation(
    string ClusterId,
    IReadOnlyList<KafkaAclBinding> Bindings);

public interface IAclMutationPort
{
    Task<MutationProviderResult> CreateAsync(
        AclCreateMutation request,
        KafkaOperationContext operation,
        CancellationToken cancellationToken = default);

    Task<MutationProviderResult> RemoveAsync(
        AclRemoveMutation request,
        KafkaOperationContext operation,
        CancellationToken cancellationToken = default);
}


public interface IAclEffectAuthorizationGuard
{
    Task<MutationPreDispatchGuardResult> ValidateCurrentRequesterAsync(
        MutationOperationSnapshot operation,
        CancellationToken cancellationToken = default);
}

public sealed class FailClosedAclEffectAuthorizationGuard :
    IAclEffectAuthorizationGuard
{
    public Task<MutationPreDispatchGuardResult> ValidateCurrentRequesterAsync(
        MutationOperationSnapshot operation,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(new MutationPreDispatchGuardResult(
            MutationPreDispatchGuardOutcome.AuthorizationDenied,
            "current_requester_authorization_unavailable"));
    }
}
