using System.Security.Claims;
using System.Text.Json;
using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Connect;
using Kafdeck.Modules.Records;
using Kafdeck.Modules.Schemas;

namespace Kafdeck.Api;

public enum MutationDispatchOutcome
{
    Executed = 1,
    NotFound = 2,
    Unauthenticated = 3,
    Forbidden = 4,
    NotReady = 5,
    CapabilityUnsupported = 6,
    InvalidExecutionMaterial = 7,
}

public sealed record MutationDispatchResult(
    MutationDispatchOutcome Outcome,
    MutationOperationSnapshot? Operation = null,
    string? Code = null);

public sealed class MutationDispatchService
{
    private readonly IMutationOperationRepository _repository;
    private readonly MutationRequestAuthorizationService _authorization;
    private readonly MutationExecutionRequestContextAccessor _requestContext;
    private readonly MutationExecutor _executor;

    public MutationDispatchService(
        IMutationOperationRepository repository,
        MutationRequestAuthorizationService authorization,
        MutationExecutionRequestContextAccessor requestContext,
        MutationExecutor executor)
    {
        _repository = repository ?? throw new ArgumentNullException(nameof(repository));
        _authorization = authorization ?? throw new ArgumentNullException(nameof(authorization));
        _requestContext = requestContext ?? throw new ArgumentNullException(nameof(requestContext));
        _executor = executor ?? throw new ArgumentNullException(nameof(executor));
    }

    public Task<MutationDispatchResult> ExecuteTopicAsync(
        ClaimsPrincipal? principal,
        Guid operationId,
        CancellationToken cancellationToken = default) =>
        ExecuteWithoutMaterialAsync(principal, operationId, cancellationToken);

    public async Task<MutationDispatchResult> ExecuteWithoutMaterialAsync(
        ClaimsPrincipal? principal,
        Guid operationId,
        CancellationToken cancellationToken = default)
    {
        var admission = await AuthorizeReadyRequesterAsync(
                principal,
                operationId,
                cancellationToken)
            .ConfigureAwait(false);
        if (admission.Result is not null)
        {
            return admission.Result;
        }

        var operation = admission.Operation!;
        if (!IsAdmittedWithoutExecutionMaterial(operation.OperationKind))
        {
            return new MutationDispatchResult(
                MutationDispatchOutcome.CapabilityUnsupported,
                operation,
                "mutation_handler_not_admitted");
        }

        return await ExecuteAdmittedAsync(
                principal!,
                operationId,
                executionMaterial: null,
                cancellationToken)
            .ConfigureAwait(false);
    }

    public async Task<MutationDispatchResult> ExecuteRecordProductionAsync(
        ClaimsPrincipal? principal,
        Guid operationId,
        IReadOnlyList<RecordProductionRecordInput>? records,
        CancellationToken cancellationToken = default)
    {
        var admission = await AuthorizeReadyRequesterAsync(
                principal,
                operationId,
                cancellationToken)
            .ConfigureAwait(false);
        if (admission.Result is not null)
        {
            return admission.Result;
        }

        var operation = admission.Operation!;
        if (operation.OperationKind != MutationOperationKind.RecordProduce)
        {
            return Unsupported(operation);
        }

        if (records is null)
        {
            return InvalidMaterial(operation);
        }

        RecordProductionExecutionMaterial material;
        try
        {
            material = RecordProductionExecutionMaterialBuilder.Build(
                operation,
                records);
        }
        catch (Exception exception)
            when (exception is
                ArgumentException or
                MutationStateException or
                OverflowException)
        {
            return InvalidMaterial(operation);
        }

        using (material)
        {
            return await ExecuteAdmittedAsync(
                    principal!,
                    operationId,
                    material.Items,
                    cancellationToken)
                .ConfigureAwait(false);
        }
    }

    public async Task<MutationDispatchResult> ExecuteSchemaCreateAsync(
        ClaimsPrincipal? principal,
        Guid operationId,
        string? schema,
        CancellationToken cancellationToken = default)
    {
        var admission = await AuthorizeReadyRequesterAsync(
                principal,
                operationId,
                cancellationToken)
            .ConfigureAwait(false);
        if (admission.Result is not null)
        {
            return admission.Result;
        }

        var operation = admission.Operation!;
        if (operation.OperationKind != MutationOperationKind.SchemaCreate)
        {
            return Unsupported(operation);
        }

        if (string.IsNullOrWhiteSpace(schema))
        {
            return InvalidMaterial(operation);
        }

        MutationExecutionMaterial material;
        try
        {
            material = SchemaMutationExecutionMaterialBuilder.BuildCreate(
                operation,
                schema);
        }
        catch (Exception exception)
            when (exception is
                ArgumentException or
                MutationStateException or
                OverflowException)
        {
            return InvalidMaterial(operation);
        }

        using (material)
        {
            return await ExecuteAdmittedAsync(
                    principal!,
                    operationId,
                    ToMaterialDictionary(material),
                    cancellationToken)
                .ConfigureAwait(false);
        }
    }

    public async Task<MutationDispatchResult> ExecuteConnectConfigurationAsync(
        ClaimsPrincipal? principal,
        Guid operationId,
        IReadOnlyDictionary<string, string>? configuration,
        CancellationToken cancellationToken = default)
    {
        var admission = await AuthorizeReadyRequesterAsync(
                principal,
                operationId,
                cancellationToken)
            .ConfigureAwait(false);
        if (admission.Result is not null)
        {
            return admission.Result;
        }

        var operation = admission.Operation!;
        if (configuration is null ||
            operation.OperationKind is not (
                MutationOperationKind.ConnectCreate or
                MutationOperationKind.ConnectAlter))
        {
            return configuration is null
                ? InvalidMaterial(operation)
                : Unsupported(operation);
        }

        MutationExecutionMaterial material;
        try
        {
            material = ConnectMutationExecutionMaterialBuilder.BuildConfiguration(
                operation,
                configuration);
        }
        catch (Exception exception)
            when (exception is
                ArgumentException or
                MutationStateException or
                OverflowException or
                JsonException)
        {
            return InvalidMaterial(operation);
        }

        using (material)
        {
            return await ExecuteAdmittedAsync(
                    principal!,
                    operationId,
                    ToMaterialDictionary(material),
                    cancellationToken)
                .ConfigureAwait(false);
        }
    }

    public async Task<MutationDispatchResult> ExecuteConnectWithoutMaterialAsync(
        ClaimsPrincipal? principal,
        Guid operationId,
        CancellationToken cancellationToken = default)
    {
        var admission = await AuthorizeReadyRequesterAsync(
                principal,
                operationId,
                cancellationToken)
            .ConfigureAwait(false);
        if (admission.Result is not null)
        {
            return admission.Result;
        }

        var operation = admission.Operation!;
        if (!ConnectMutationExecutionMaterialBuilder.IsNoMaterialExecution(operation))
        {
            return Unsupported(operation);
        }

        return await ExecuteAdmittedAsync(
                principal!,
                operationId,
                executionMaterial: null,
                cancellationToken)
            .ConfigureAwait(false);
    }

    private async Task<MutationDispatchResult> ExecuteAdmittedAsync(
        ClaimsPrincipal principal,
        Guid operationId,
        IReadOnlyDictionary<string, ReadOnlyMemory<byte>>? executionMaterial,
        CancellationToken cancellationToken)
    {
        using var requestScope = _requestContext.Push(principal);
        var executed = executionMaterial is null
            ? await _executor
                .ExecuteAsync(operationId, cancellationToken)
                .ConfigureAwait(false)
            : await _executor
                .ExecuteAsync(operationId, executionMaterial, cancellationToken)
                .ConfigureAwait(false);

        return Executed(executed);
    }

    private async Task<(
        MutationOperationSnapshot? Operation,
        MutationDispatchResult? Result)> AuthorizeReadyRequesterAsync(
        ClaimsPrincipal? principal,
        Guid operationId,
        CancellationToken cancellationToken)
    {
        if (!OperatorSessionContextFactory.TryCreate(principal, out var session) ||
            session is null)
        {
            return (null, new MutationDispatchResult(
                MutationDispatchOutcome.Unauthenticated));
        }

        var operation = await _repository
            .GetAsync(operationId, cancellationToken)
            .ConfigureAwait(false);
        if (operation is null)
        {
            return (null, new MutationDispatchResult(
                MutationDispatchOutcome.NotFound));
        }

        var currentPrincipalId = SecurityAuditPrincipal.FromOperator(session.Identity);
        if (!string.Equals(
                currentPrincipalId,
                operation.RequesterPrincipalId,
                StringComparison.Ordinal))
        {
            return (operation, new MutationDispatchResult(
                MutationDispatchOutcome.Forbidden,
                operation));
        }

        var authorization = _authorization.AuthorizeForDispatch(
            principal,
            operation);
        if (authorization == KafdeckAuthorizationOutcome.Unauthenticated)
        {
            return (operation, new MutationDispatchResult(
                MutationDispatchOutcome.Unauthenticated,
                operation));
        }

        if (authorization != KafdeckAuthorizationOutcome.Allowed)
        {
            return (operation, new MutationDispatchResult(
                MutationDispatchOutcome.Forbidden,
                operation));
        }

        if (operation.State != MutationOperationState.Ready)
        {
            return (operation, new MutationDispatchResult(
                MutationDispatchOutcome.NotReady,
                operation,
                "mutation_not_ready"));
        }

        return (operation, null);
    }

    private static IReadOnlyDictionary<string, ReadOnlyMemory<byte>> ToMaterialDictionary(
        MutationExecutionMaterial material) =>
        material.Names.ToDictionary(
            name => name,
            name => material.GetRequired(name),
            StringComparer.Ordinal);

    private static MutationDispatchResult Unsupported(
        MutationOperationSnapshot operation) =>
        new(
            MutationDispatchOutcome.CapabilityUnsupported,
            operation,
            "mutation_handler_not_admitted");

    private static MutationDispatchResult InvalidMaterial(
        MutationOperationSnapshot operation) =>
        new(
            MutationDispatchOutcome.InvalidExecutionMaterial,
            operation,
            "execution_material_invalid");

    private static MutationDispatchResult Executed(
        MutationOperationSnapshot operation) =>
        new(
            MutationDispatchOutcome.Executed,
            operation,
            operation.ResultCode);

    private static bool IsAdmittedWithoutExecutionMaterial(
        MutationOperationKind kind) =>
        kind is
            MutationOperationKind.TopicCreate or
            MutationOperationKind.TopicAlter or
            MutationOperationKind.TopicIncreasePartitions or
            MutationOperationKind.TopicDelete or
            MutationOperationKind.ConsumerOffsetAlter or
            MutationOperationKind.ConsumerDelete or
            MutationOperationKind.SchemaAlter or
            MutationOperationKind.SchemaDelete or
            MutationOperationKind.RecordsPurge;
}
