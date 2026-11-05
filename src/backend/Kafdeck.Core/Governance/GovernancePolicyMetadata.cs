using System.Globalization;
using Kafdeck.Core.Catalog;

namespace Kafdeck.Core.Governance;

/// <summary>One catalog-owned metadata projection for explain and transactional execution.
/// An unset fact is omitted; it is never manufactured as an empty observed value.</summary>
public static class GovernancePolicyMetadata
{
    public static IReadOnlyDictionary<string,string>? FromCatalog(CatalogRecord? row)
    {
        var identity=row?.TopicIdentity;
        if(row is null||row.Archived||identity is not {Length:64})return null;
        var result=new Dictionary<string,string>(StringComparer.Ordinal)
        {
            ["topicIdentity"]=identity,["revision"]=row.Revision.ToString(CultureInfo.InvariantCulture),
        };
        if(row.Classification is not null)result["classification"]=row.Classification;
        if(row.Domain is not null)result["domain"]=row.Domain;
        if(row.TeamId is not null)result["teamId"]=row.TeamId;
        if(row.ApplicationId is not null)result["applicationId"]=row.ApplicationId;
        return result;
    }
}
