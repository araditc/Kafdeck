using System.Data.Common;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Infrastructure.Persistence;

/// <summary>Implemented by the owning catalog store; policy never issues cross-module SQL.</summary>
public interface IPolicyMetadataTransactionReader
{
    Task<IReadOnlyDictionary<string, string>?> ReadAsync(DbConnection connection, DbTransaction transaction,
        string clusterId, string resourceId, CancellationToken token);
}

public interface IGovernancePolicyTransactionGate
{
    Task ValidateDispatchAsync(DbConnection connection, DbTransaction transaction, MutationOperationSnapshot operation, CancellationToken token);
}

public interface IMutationOperationTransactionReader
{
    Task<MutationOperationSnapshot?> ReadAsync(DbConnection connection,DbTransaction transaction,Guid operationId,CancellationToken token);
}
