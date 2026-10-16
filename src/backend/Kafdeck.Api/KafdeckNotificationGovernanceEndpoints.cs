using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Serialization;
using Kafdeck.Core.Notifications;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Microsoft.AspNetCore.Mvc;

namespace Kafdeck.Api;

public static class KafdeckNotificationGovernanceEndpoints
{
    public static WebApplication MapKafdeckNotificationGovernance(this WebApplication app)
    {
        app.MapGet("/api/v1/notifications/management/destinations",async (HttpContext context,
            KafdeckAuthorizationService authorization,NotificationGovernanceOptions options,INotificationGovernanceStore store) =>
        {
            var access=authorization.AuthorizeCollection(context.User,AuthorizationAction.NotificationManage,null);
            if(access!=KafdeckAuthorizationOutcome.Allowed) return Denied(access);
            var result=new List<NotificationManagedDestinationData>();
            foreach(var group in options.Candidates.GroupBy(c=>c.DestinationId).OrderBy(c=>c.Key,StringComparer.Ordinal))
            {
                if(!Allowed(context,authorization,group.Key)) continue;
                var state=await store.GetAsync(group.Key,context.RequestAborted).ConfigureAwait(false);
                var selected=state is null?null:options.Find(group.Key,state.CandidateId);
                result.Add(new(group.Key,state?.Revision??0,state?.CandidateId,state?.ProfileEnabled??false,
                    state?.CredentialEnabled??false,state is not null && (selected is null || selected.Fingerprint!=state.CandidateFingerprint),
                    group.Select(c=>new NotificationManagedCandidateData(c.CandidateId,c.Profile.Provider.ToString(),c.Profile.CredentialBindingId is not null)).ToArray()));
            }
            return Results.Ok(new NotificationManagedDestinationList(result));
        }).WithName("v08-notification-governance-destinations");

        app.MapPut("/api/v1/notifications/management/destinations/{destinationId}",async (string destinationId,
            NotificationManagedDestinationChangeRequest request,HttpContext context,KafdeckAuthorizationService authorization,
            NotificationGovernanceOptions options,INotificationGovernanceStore store,ISecurityAuditSink audit) =>
        {
            if(!Allowed(context,authorization,destinationId)) return Denied(KafdeckAuthorizationOutcome.Forbidden);
            var candidate=options.Find(destinationId,request.CandidateId);
            if(candidate is null) return Invalid();
            try
            {
                var actor=Actor(context);
                if(actor is null) return Denied(KafdeckAuthorizationOutcome.Unauthenticated);
                var change=new NotificationGovernanceChange(destinationId,request.CandidateId,candidate.Fingerprint,request.ExpectedRevision,
                    request.ProfileEnabled,request.CredentialEnabled,actor,authorization.PolicyFingerprint);
                if(!Allowed(context,authorization,destinationId)) return Denied(KafdeckAuthorizationOutcome.Forbidden);
                var state=await store.ChangeAsync(change,context.RequestAborted).ConfigureAwait(false);
                if(state is null) return Results.Problem(statusCode:409,title:"Notification governance revision conflict",
                    type:"urn:kafdeck:problem:notification-governance-conflict");
                await audit.WriteAsync(new SecurityAuditEvent(state.UpdatedAtUtc,SecurityAuditEventType.NotificationManagementChanged,
                    actor,null,null,destinationId,SecurityAuditOutcome.Succeeded,"notification_governance_changed"),context.RequestAborted).ConfigureAwait(false);
                return Results.Ok(new NotificationManagedDestinationReceipt(destinationId,state.Revision,state.ProfileEnabled,state.CredentialEnabled));
            }
            catch(ArgumentException) { return Invalid(); }
        }).WithName("v08-notification-governance-change")
          .RequireKafdeckAuthorization(AuthorizationAction.NotificationManage,resourceRouteKey:"destinationId")
          .RequireKafdeckAntiforgery();

        app.MapGet("/api/v1/notifications/management/destinations/{destinationId}/audit",async (string destinationId,
            [FromQuery] long? afterRevision,[FromQuery] int? maxResults,HttpContext context,KafdeckAuthorizationService authorization,
            NotificationGovernanceOptions options,INotificationGovernanceStore store) =>
        {
            if(!Allowed(context,authorization,destinationId)) return Denied(KafdeckAuthorizationOutcome.Forbidden);
            if(!options.Candidates.Any(c=>c.DestinationId==destinationId)) return Results.NotFound();
            try
            {
                var rows=await store.ReadAuditAsync(destinationId,afterRevision??0,maxResults??50,context.RequestAborted).ConfigureAwait(false);
                return Results.Ok(new NotificationManagedAuditList(rows.Select(r=>new NotificationManagedAuditData(r.Revision,r.CandidateId,
                    r.ProfileEnabled,r.CredentialEnabled,Hash(r.Actor),r.PolicyFingerprint,r.UpdatedAtUtc)).ToArray(),rows.LastOrDefault()?.Revision));
            }
            catch(ArgumentException) { return Invalid(); }
        }).WithName("v08-notification-governance-audit")
          .RequireKafdeckAuthorization(AuthorizationAction.NotificationManage,resourceRouteKey:"destinationId");
        return app;
    }
    private static string Hash(string value)=>Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value))).ToLowerInvariant();
    private static string? Actor(HttpContext context)=>context.User.Identity is NonBrowserReadIdentity { Kind:NonBrowserPrincipalKind.Human } bearer?bearer.AuditId:
        OperatorSessionContextFactory.TryCreate(context.User,out var session)&&session is not null?SecurityAuditPrincipal.FromOperator(session.Identity):null;
    private static bool Allowed(HttpContext context,KafdeckAuthorizationService authorization,string id)=>
        authorization.Authorize(context.User,new(AuthorizationAction.NotificationManage,ResourceName:id))==KafdeckAuthorizationOutcome.Allowed;
    private static IResult Denied(KafdeckAuthorizationOutcome outcome)=>Results.Problem(statusCode:outcome==KafdeckAuthorizationOutcome.Unauthenticated?401:403,
        type:"urn:kafdeck:problem:operator-authorization-denied",title:"Notification management is not authorized");
    private static IResult Invalid()=>Results.Problem(statusCode:400,type:"urn:kafdeck:problem:invalid-notification-governance",title:"Invalid notification governance request");
}
[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public sealed record NotificationManagedDestinationChangeRequest(string CandidateId,long ExpectedRevision,bool ProfileEnabled,bool CredentialEnabled);
public sealed record NotificationManagedCandidateData(string CandidateId,string Provider,bool RequiresCredential);
public sealed record NotificationManagedDestinationData(string DestinationId,long Revision,string? CandidateId,bool ProfileEnabled,
    bool CredentialEnabled,bool ConfigurationMismatch,IReadOnlyList<NotificationManagedCandidateData> Candidates);
public sealed record NotificationManagedDestinationList(IReadOnlyList<NotificationManagedDestinationData> Items);
public sealed record NotificationManagedDestinationReceipt(string DestinationId,long Revision,bool ProfileEnabled,bool CredentialEnabled);
public sealed record NotificationManagedAuditData(long Revision,string CandidateId,bool ProfileEnabled,bool CredentialEnabled,
    string ActorFingerprint,string PolicyFingerprint,DateTimeOffset UpdatedAtUtc);
public sealed record NotificationManagedAuditList(IReadOnlyList<NotificationManagedAuditData> Items,long? NextRevision);
