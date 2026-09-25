using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Connect;
using Kafdeck.Modules.Consumers;
using Kafdeck.Modules.Records;
using Kafdeck.Modules.Schemas;
using Kafdeck.Modules.Topics;

namespace Kafdeck.Api;

/// <summary>
/// W39 integration guard for mutation kinds whose HTTP/runtime activation has
/// been explicitly admitted. It performs a live requester authorization check,
/// operation-specific stale-preview validation, and a second authorization
/// check immediately before returning Allowed to the executor.
/// </summary>
public sealed class W39MutationPreDispatchGuard : IMutationPreDispatchGuard
{
    private readonly MutationExecutionRequestContextAccessor _requestContext;
    private readonly MutationRequestAuthorizationService _authorization;
    private readonly TopicMutationPreconditionValidator _topics;
    private readonly RecordProductionPreconditionValidator _recordProduction;
    private readonly ConsumerMutationPreconditionValidator _consumers;
    private readonly RecordsPurgePreconditionValidator _recordsPurge;
    private readonly SchemaMutationPreconditionValidator? _schemas;
    private readonly ConnectMutationPreconditionValidator? _connect;
    private readonly AclMutationPreconditionValidator? _acls;
    private readonly IAclEffectAuthorizationGuard? _aclAuthorization;

    public W39MutationPreDispatchGuard(
        MutationExecutionRequestContextAccessor requestContext,
        MutationRequestAuthorizationService authorization,
        TopicMutationPreconditionValidator topics,
        RecordProductionPreconditionValidator recordProduction,
        ConsumerMutationPreconditionValidator consumers,
        RecordsPurgePreconditionValidator recordsPurge,
        SchemaMutationPreconditionValidator? schemas = null,
        ConnectMutationPreconditionValidator? connect = null,
        AclMutationPreconditionValidator? acls = null,
        IAclEffectAuthorizationGuard? aclAuthorization = null)
    {
        _requestContext = requestContext ?? throw new ArgumentNullException(nameof(requestContext));
        _authorization = authorization ?? throw new ArgumentNullException(nameof(authorization));
        _topics = topics ?? throw new ArgumentNullException(nameof(topics));
        _recordProduction = recordProduction ?? throw new ArgumentNullException(nameof(recordProduction));
        _consumers = consumers ?? throw new ArgumentNullException(nameof(consumers));
        _recordsPurge = recordsPurge ?? throw new ArgumentNullException(nameof(recordsPurge));
        _schemas = schemas;
        _connect = connect;
        _acls = acls;
        _aclAuthorization = aclAuthorization;
    }

    public async Task<MutationPreDispatchGuardResult> ValidateAsync(
        MutationOperationSnapshot operation,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);
        cancellationToken.ThrowIfCancellationRequested();

        var initialAuthorization = AuthorizeCurrentRequester(operation);
        if (initialAuthorization.Outcome != MutationPreDispatchGuardOutcome.Allowed)
        {
            return initialAuthorization;
        }

        MutationPreDispatchGuardResult preconditions;
        switch (operation.OperationKind)
        {
            case MutationOperationKind.TopicCreate:
            case MutationOperationKind.TopicAlter:
            case MutationOperationKind.TopicIncreasePartitions:
            case MutationOperationKind.TopicDelete:
                preconditions = await _topics
                    .ValidateAsync(operation, cancellationToken)
                    .ConfigureAwait(false);
                break;

            case MutationOperationKind.RecordProduce:
                preconditions = await _recordProduction
                    .ValidateAsync(operation, cancellationToken)
                    .ConfigureAwait(false);
                break;

            case MutationOperationKind.ConsumerOffsetAlter:
            case MutationOperationKind.ConsumerDelete:
                preconditions = await _consumers
                    .ValidateAsync(operation, cancellationToken)
                    .ConfigureAwait(false);
                break;

            case MutationOperationKind.SchemaCreate:
            case MutationOperationKind.SchemaAlter:
            case MutationOperationKind.SchemaDelete:
                if (_schemas is null)
                {
                    return Unsupported();
                }

                preconditions = await _schemas
                    .ValidateAsync(operation, cancellationToken)
                    .ConfigureAwait(false);
                break;

            case MutationOperationKind.ConnectCreate:
            case MutationOperationKind.ConnectAlter:
            case MutationOperationKind.ConnectDelete:
                if (_connect is null)
                {
                    return Unsupported();
                }

                preconditions = await _connect
                    .ValidateAsync(operation, cancellationToken)
                    .ConfigureAwait(false);
                break;

            case MutationOperationKind.RecordsPurge:
                preconditions = await _recordsPurge
                    .ValidateAsync(operation, cancellationToken)
                    .ConfigureAwait(false);
                break;

            case MutationOperationKind.AclAlter:
                if (_acls is null || _aclAuthorization is null)
                {
                    return Unsupported();
                }

                var aclAuthorization = await _aclAuthorization
                    .ValidateCurrentRequesterAsync(operation, cancellationToken)
                    .ConfigureAwait(false);
                if (aclAuthorization.Outcome !=
                    MutationPreDispatchGuardOutcome.Allowed)
                {
                    return aclAuthorization;
                }

                preconditions = await _acls
                    .ValidateAsync(operation, cancellationToken)
                    .ConfigureAwait(false);
                break;

            default:
                return Unsupported();
        }

        if (preconditions.Outcome != MutationPreDispatchGuardOutcome.Allowed)
        {
            return preconditions;
        }

        // Re-evaluate after all provider observations and immediately before the
        // executor is permitted to cross its dispatch boundary. ACL effects
        // additionally require the still-current independent approver when the
        // risk decision is CRITICAL.
        if (operation.OperationKind == MutationOperationKind.AclAlter)
        {
            return await _aclAuthorization!
                .ValidateCurrentRequesterAsync(operation, cancellationToken)
                .ConfigureAwait(false);
        }

        return AuthorizeCurrentRequester(operation);
    }

    private MutationPreDispatchGuardResult AuthorizeCurrentRequester(
        MutationOperationSnapshot operation) =>
        MutationCurrentRequesterAuthorization.Evaluate(
            _requestContext,
            _authorization,
            operation);

    private static MutationPreDispatchGuardResult Unsupported() =>
        new(
            MutationPreDispatchGuardOutcome.CapabilityUnsupported,
            "mutation_handler_not_admitted");

    private static MutationPreDispatchGuardResult Denied(string code) =>
        new(MutationPreDispatchGuardOutcome.AuthorizationDenied, code);
}
