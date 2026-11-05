using System.Data.Common;
using Kafdeck.Core.Governance;
using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Infrastructure.Persistence;

/// <summary>Pure current configuration/registration checks; no stored claims are rehydrated.</summary>
public interface IServiceRequestCurrentAuthority
{
    bool IsCurrentRequesterAllowed(string principalId,AuthorizationRequest request);
    bool IsCurrentHumanDecisionAllowed(string principalId,AuthorizationRequest request);
}
public interface IApprovedServiceRequestTransactionReader
{
    Task<GovernanceApprovedRequestFact?> ReadApprovedAsync(DbConnection db,DbTransaction tx,
        MutationOperationSnapshot operation,CancellationToken token);
}
