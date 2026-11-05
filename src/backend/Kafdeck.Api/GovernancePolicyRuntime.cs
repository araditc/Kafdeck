using Kafdeck.Core.Catalog;
using Kafdeck.Core.Governance;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Api;

/// <summary>Current persisted policy for initial dispatch and every resumable worker effect; no claims rehydration.</summary>
public sealed class GovernancePolicyRuntime(AdoGovernancePolicyStore store,ICatalogTopicIdentityPort topics,ICatalogStore catalog)
{
    public async Task<MutationPreDispatchGuardResult> ValidateAsync(MutationOperationSnapshot operation,CancellationToken token=default)
    {
        try
        {
            foreach(var cluster in operation.AuthorizationTargets.Select(t=>t.ClusterId).Distinct(StringComparer.Ordinal))
            {
                var state=await store.GetStateAsync(cluster,token).ConfigureAwait(false);
                if(!state.Enabled)continue;
                var version=await store.GetVersionAsync(cluster,state.VersionId!,false,token).ConfigureAwait(false);
                if(version is null) return Denied("GovernanceUnavailable");
                var policy=GovernancePolicyCompiler.Compile(version.Document);
                foreach(var target in operation.AuthorizationTargets.Where(t=>t.ClusterId==cluster && AdoGovernancePolicyStore.ResourceKind(t.Action.ToString())=="topic"))
                {
                    if(!policy.Document.Rules.Any(r=>r.Scope.Action.Equals(target.Action.ToString(),StringComparison.OrdinalIgnoreCase)&&r.Scope.ResourceIds.Contains(target.ResourceName,StringComparer.Ordinal)&&r.Conditions.Any(c=>c.Field is not ("risk" or "requestApproved"))))continue;
                    var metadata=await catalog.FindTopicAsync(cluster,target.ResourceName,token).ConfigureAwait(false);
                    var observed=await topics.ObserveAsync(cluster,target.ResourceName,token).ConfigureAwait(false);
                    if(metadata is null||metadata.Archived||observed is null||metadata.TopicIdentity!=observed.Fingerprint)return Denied("GovernanceEvidenceUnavailable");
                    store.CaptureTopicIdentity(operation.OperationId,cluster,target.ResourceName,observed.Fingerprint);
                }
            }
            await store.ValidateOperationAsync(operation,token).ConfigureAwait(false);
            return MutationPreDispatchGuardResult.Allowed;
        }
        catch(GovernanceEffectRejectedException ex){return new(ex.Stale?MutationPreDispatchGuardOutcome.StalePreview:MutationPreDispatchGuardOutcome.AuthorizationDenied,ex.Code);}
        catch(OperationCanceledException){throw;}
        catch(Exception){return Denied("GovernanceUnavailable");}
    }
    private static MutationPreDispatchGuardResult Denied(string code)=>new(MutationPreDispatchGuardOutcome.AuthorizationDenied,code);
}
