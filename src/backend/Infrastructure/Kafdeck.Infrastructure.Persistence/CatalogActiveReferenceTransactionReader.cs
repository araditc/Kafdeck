using System.Data.Common;
using Kafdeck.Core.Catalog;

namespace Kafdeck.Infrastructure.Persistence;

/// <summary>Catalog owns reference interpretation. Callers supply their owning transaction;
/// no second connection or cross-module catalog SQL is permitted.</summary>
public interface ICatalogActiveReferenceTransactionReader
{
    Task<bool> IsActiveReferenceAsync(DbConnection connection,DbTransaction transaction,string clusterId,
        CatalogEntityKind kind,string id,CancellationToken token);
}
