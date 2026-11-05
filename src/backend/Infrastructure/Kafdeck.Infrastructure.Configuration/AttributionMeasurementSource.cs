using System.Net.Http.Headers;
using System.Text.Json;
using Kafdeck.Core.Attribution;
using Kafdeck.Core.Notifications;

namespace Kafdeck.Infrastructure.Configuration;

public sealed class AttributionSourceRegistration
{
    public string SourceId{get;set;}="";
    public string ClusterId{get;set;}="";
    public string Provider{get;set;}="KafdeckMeasuredCountersV1";
    public string Endpoint{get;set;}="";
    public string? AccessTokenReference{get;set;}
    public void Validate()
    {
        AttributionContract.Text(SourceId,128);AttributionContract.Text(ClusterId,256);
        if(Provider!="KafdeckMeasuredCountersV1")throw new KafdeckConfigurationException("Unregistered attribution measurement profile.");
        _=NotificationHttpsEndpointPolicy.ValidateAndNormalize(new Uri(Endpoint,UriKind.Absolute));
        if(AccessTokenReference is not null)_=SecretReference.Parse(AccessTokenReference);
    }
}
public sealed class AttributionRuntimeOptions
{
    public List<AttributionSourceRegistration> Sources{get;set;}=[];
    public int PollSeconds{get;set;}=60;
    public int RetentionDays{get;set;}=90;
    public void Validate()
    {
        if(PollSeconds is <60 or >3600||RetentionDays is <1 or >366||Sources is null||Sources.Count>32||Sources.Any(s=>s is null)||Sources.Select(s=>s.SourceId).Distinct(StringComparer.Ordinal).Count()!=Sources.Count)throw new KafdeckConfigurationException("Attribution source/retention limits are invalid.");
        foreach(var s in Sources)s.Validate();
    }
}

/// <summary>One operator-selected immutable HTTPS destination, fixed registered counter profile.
/// No query language, caller URL, record samples or broker-billing inference.</summary>
public sealed class ConfiguredAttributionMeasurementSource:IAttributionMeasurementSource
{
    private readonly AttributionSourceRegistration registration;
    private readonly INotificationEndpointResolutionPort resolution;
    private readonly NotificationPinnedHttpHandlerFactory handlers;
    private readonly SecretResolver secrets;
    private readonly TimeProvider clock;
    public AttributionSourceIdentity Identity{get;}
    public ConfiguredAttributionMeasurementSource(AttributionSourceRegistration source,INotificationEndpointResolutionPort resolver,
        NotificationPinnedHttpHandlerFactory factory,SecretResolver secretResolver,TimeProvider? timeProvider=null)
    {
        ArgumentNullException.ThrowIfNull(source);source.Validate();registration=new(){SourceId=source.SourceId,ClusterId=source.ClusterId,Provider=source.Provider,Endpoint=source.Endpoint,AccessTokenReference=source.AccessTokenReference};
        resolution=resolver;handlers=factory;secrets=secretResolver;clock=timeProvider??TimeProvider.System;Identity=new(source.SourceId,source.ClusterId,source.Provider);
    }
    public async Task<IReadOnlyList<MeasurementWindow>> AcquireAsync(CancellationToken token)
    {
        using var timeout=CancellationTokenSource.CreateLinkedTokenSource(token);timeout.CancelAfter(TimeSpan.FromSeconds(15));
        var endpoint=new Uri(registration.Endpoint,UriKind.Absolute);
        var pinned=await resolution.ResolveForConnectionAsync(endpoint,timeout.Token).ConfigureAwait(false);
        using var client=new HttpClient(handlers.CreateGuardedHandler(pinned),true){Timeout=Timeout.InfiniteTimeSpan};
        using var request=new HttpRequestMessage(HttpMethod.Get,endpoint);
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        if(registration.AccessTokenReference is not null)
        {
            var secret=secrets.ResolveBounded(SecretReference.Parse(registration.AccessTokenReference),8192).Reveal();
            if(secret.Any(char.IsWhiteSpace)||secret.Any(char.IsControl))throw new KafdeckConfigurationException("Invalid source credential.");
            request.Headers.Authorization=new AuthenticationHeaderValue("Bearer",secret);
        }
        using var response=await client.SendAsync(request,HttpCompletionOption.ResponseHeadersRead,timeout.Token).ConfigureAwait(false);
        if(!response.IsSuccessStatusCode||response.Content.Headers.ContentType?.MediaType!="application/json")throw new InvalidOperationException("MeasurementSourceUnavailable");
        if(response.Content.Headers.ContentLength>1024*1024)throw new InvalidOperationException("MeasurementSourceBoundExceeded");
        await using var body=await response.Content.ReadAsStreamAsync(timeout.Token).ConfigureAwait(false);using var bytes=new MemoryStream();var buffer=new byte[8192];
        while(true){var n=await body.ReadAsync(buffer,timeout.Token).ConfigureAwait(false);if(n==0)break;if(bytes.Length+n>1024*1024)throw new InvalidOperationException("MeasurementSourceBoundExceeded");await bytes.WriteAsync(buffer.AsMemory(0,n),timeout.Token).ConfigureAwait(false);}
        using var document=JsonDocument.Parse(bytes.ToArray(),new(){MaxDepth=12});RejectDuplicateProperties(document.RootElement);
        var batch=document.RootElement.Deserialize<MeasuredCounterBatch>(AttributionContract.Json)??throw new ArgumentException("Invalid measured batch.");
        if(batch.SchemaVersion!=1||batch.Windows is not{Count:>=1 and <=500})throw new ArgumentException("Invalid measured batch.");
        var now=clock.GetUtcNow();var provenance=AttributionContract.Hash(Identity.SourceId+"\n"+Identity.Provider+"\n"+Identity.ClusterId+"\n"+pinned.Endpoint.AbsoluteUri+"\n"+pinned.AddressPolicyFingerprint);
        var rows=batch.Windows.Select(w=>AttributionContract.Counter(Identity,w,now,provenance)).ToArray();
        if(rows.Select(r=>r.WindowId).Distinct(StringComparer.Ordinal).Count()!=rows.Length)throw new ArgumentException("Duplicate measured window.");
        return rows;
    }
    private static void RejectDuplicateProperties(JsonElement element)
    {
        if(element.ValueKind==JsonValueKind.Object){var keys=new HashSet<string>(StringComparer.Ordinal);foreach(var p in element.EnumerateObject()){if(!keys.Add(p.Name))throw new ArgumentException("Duplicate source field.");RejectDuplicateProperties(p.Value);}}
        else if(element.ValueKind==JsonValueKind.Array)foreach(var item in element.EnumerateArray())RejectDuplicateProperties(item);
    }
}
