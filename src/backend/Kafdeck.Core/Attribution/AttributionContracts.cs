using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Kafdeck.Core.Attribution;

[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public sealed record AttributionWeight(string? TeamId,string? ApplicationId,string Weight);
[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public sealed record RateCardDocument(string MetricKind,string Unit,string Currency,string Amount,
    DateTimeOffset EffectiveFromUtc,DateTimeOffset EffectiveToUtc,IReadOnlyList<AttributionWeight> Weights);
public sealed record RateCardVersion(string VersionId,string ClusterId,string ContentHash,RateCardDocument Document,
    string CreatedBy,DateTimeOffset CreatedAtUtc,bool Committed);
public sealed record RateCardState(string ClusterId,long Revision,string? VersionId,bool Enabled);
public sealed record MeasurementOwnership(long? Revision,string? TeamId,string? ApplicationId,string? TopicIdentity,string Quality);
public sealed record MeasurementWindow(string WindowId,string SourceId,string ClusterId,string Provider,string MetricKind,string Unit,
    string ResourceId,DateTimeOffset WindowStartUtc,DateTimeOffset WindowEndUtc,DateTimeOffset CollectedAtUtc,
    string? Quantity,string Quality,IReadOnlyList<string> Reasons,MeasurementOwnership Ownership,string ProvenanceHash,string? ResourceIdentity=null);
public sealed record AttributionAllocation(string? TeamId,string? ApplicationId,string Quantity,string Quality);
public sealed record AttributionWindow(MeasurementWindow Measurement,IReadOnlyList<AttributionAllocation> Allocations,
    string? ResidualQuantity,string? RateCardHash,string? Currency,string? UnroundedAmount,string? RoundedAmount,string Quality,IReadOnlyList<string> Reasons);
public sealed record AttributionTotals(string? Currency,string? UnroundedAmount,string? RoundedAmount,string Status,bool Complete);
public sealed record AttributionPage<T>(IReadOnlyList<T> Items,string? NextCursor,DateTimeOffset FromUtc,DateTimeOffset ToUtc,
    string Coverage,string Status,AttributionTotals? Totals=null);
public sealed record MeasurementQuery(string ClusterId,DateTimeOffset FromUtc,DateTimeOffset ToUtc,string? ResourceId,int PageSize,string? AfterId=null);
public sealed record MeasurementAccessScope(string ScopeId,string ClusterId,string ResourceId,string? TeamId,string? ApplicationId);
public sealed record MeasuredCounterWindow(string ResourceId,string MetricKind,string Unit,DateTimeOffset StartUtc,
    DateTimeOffset EndUtc,string StartValue,string EndValue,string Coverage,string SourceRevision,string? ResourceIdentity=null);
public sealed record MeasuredCounterBatch(int SchemaVersion,IReadOnlyList<MeasuredCounterWindow> Windows);
public sealed record AttributionSourceIdentity(string SourceId,string ClusterId,string Provider);
public interface IAttributionMeasurementSource
{
    AttributionSourceIdentity Identity { get; }
    Task<IReadOnlyList<MeasurementWindow>> AcquireAsync(CancellationToken token);
}
public interface IAttributionStore
{
    Task InitializeAsync(CancellationToken token=default);
    Task AppendAsync(IReadOnlyList<MeasurementWindow> windows,CancellationToken token=default);
    Task<IReadOnlyList<MeasurementWindow>> QueryAuthorizedAsync(MeasurementQuery query,Func<MeasurementAccessScope,bool> allowed,CancellationToken token=default);
    Task<RateCardVersion> StageRateAsync(string clusterId,RateCardDocument document,string actor,CancellationToken token=default);
    Task<RateCardVersion?> GetRateAsync(string clusterId,string versionId,bool includeDraft,CancellationToken token=default);
    Task<RateCardState> GetStateAsync(string clusterId,CancellationToken token=default);
    Task<IReadOnlyList<RateCardVersion>> ListRatesAuthorizedAsync(string clusterId,string? afterId,int take,Func<RateCardVersion,bool> allowed,CancellationToken token=default);
    Task<RateCardVersion?> FindActiveRateAsync(string clusterId,string metricKind,DateTimeOffset fromUtc,DateTimeOffset toUtc,CancellationToken token=default);
    Task<RateCardVersion?> FindActiveRateAuthorizedAsync(string clusterId,string metricKind,DateTimeOffset fromUtc,DateTimeOffset toUtc,Func<RateCardVersion,bool> allowed,CancellationToken token=default);
}
public sealed class AttributionConflictException(string code):InvalidOperationException(code){public string Code{get;}=code;}

public static class AttributionContract
{
    public const int MaximumVisibilityScopesPerCluster=10_000;
    public static readonly JsonSerializerOptions Json=new(JsonSerializerDefaults.Web){UnmappedMemberHandling=JsonUnmappedMemberHandling.Disallow,MaxDepth=12};
    public static string Hash(string text)=>Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(text))).ToLowerInvariant();
    public static string Number(decimal value)=>value.ToString("0.############################",CultureInfo.InvariantCulture);
    public static decimal Decimal(string value)
    {
        ArgumentException.ThrowIfNullOrEmpty(value);
        if(value.Length>64||value.Trim()!=value||!decimal.TryParse(value,NumberStyles.AllowLeadingSign|NumberStyles.AllowDecimalPoint,CultureInfo.InvariantCulture,out var result))throw new ArgumentException("Invalid invariant decimal.");
        return result;
    }
    public static string Text(string value,int maximum)
    {if(string.IsNullOrWhiteSpace(value)||value.Length>maximum||value!=value.Trim()||value.Any(char.IsControl))throw new ArgumentException("Invalid bounded attribution text.");return value;}
    public static void Metric(string kind,string unit)
    {if((kind,unit) is not (("trafficBytes","bytes") or ("storageByteSeconds","byteSeconds")))throw new ArgumentException("AttributionUnsupported");}
    public static void Query(MeasurementQuery q)
    {
        Text(q.ClusterId,256);if(q.ResourceId is not null)Text(q.ResourceId,512);
        if(q.FromUtc==default||q.ToUtc<=q.FromUtc||q.ToUtc-q.FromUtc>TimeSpan.FromDays(31)||q.PageSize is <1 or >500)throw new ArgumentException("Invalid bounded measurement query.");
        if(q.AfterId is not null)Digest(q.AfterId);
    }
    public static string Digest(string value)
    {if(value is null||value.Length!=64||value.Any(c=>!char.IsAsciiHexDigit(c))||value!=value.ToLowerInvariant())throw new ArgumentException("Invalid attribution identity.");return value;}
    public static RateCardDocument Rate(RateCardDocument d)
    {
        ArgumentNullException.ThrowIfNull(d);
        Metric(d.MetricKind,d.Unit);if(d.Currency is null||d.Currency.Length!=3||d.Currency.Any(c=>c is <'A' or >'Z')||Decimal(d.Amount)<0||d.EffectiveFromUtc==default||d.EffectiveToUtc<=d.EffectiveFromUtc)throw new ArgumentException("Invalid rate card.");
        if(d.Weights is null||d.Weights.Count>100)throw new ArgumentException("Invalid allocation weights.");
        var ids=new HashSet<string>(StringComparer.Ordinal);decimal sum=0;
        foreach(var w in d.Weights)
        {
            ArgumentNullException.ThrowIfNull(w);
            if(w.TeamId is null&&w.ApplicationId is null)throw new ArgumentException("Allocation owner required.");
            foreach(var id in new[]{w.TeamId,w.ApplicationId})if(id is not null)Kafdeck.Core.Catalog.CatalogContract.Id(id);
            if(!ids.Add((w.TeamId??"")+"/"+(w.ApplicationId??""))||Decimal(w.Weight)<=0||Decimal(w.Weight)>1)throw new ArgumentException("Invalid allocation weight.");
            sum=checked(sum+Decimal(w.Weight));
        }
        if(sum>1)throw new ArgumentException("Allocation weights exceed the measured quantity.");
        return d with{Amount=Number(Decimal(d.Amount)),EffectiveFromUtc=d.EffectiveFromUtc.ToUniversalTime(),EffectiveToUtc=d.EffectiveToUtc.ToUniversalTime(),Weights=d.Weights.Select(w=>w with{Weight=Number(Decimal(w.Weight))}).OrderBy(w=>w.TeamId,StringComparer.Ordinal).ThenBy(w=>w.ApplicationId,StringComparer.Ordinal).ToArray()};
    }
    public static void Window(MeasurementWindow w)
    {
        ArgumentNullException.ThrowIfNull(w);ArgumentNullException.ThrowIfNull(w.Ownership);ArgumentNullException.ThrowIfNull(w.Reasons);
        foreach(var id in new[]{w.Ownership.TeamId,w.Ownership.ApplicationId})if(id is not null)Kafdeck.Core.Catalog.CatalogContract.Id(id);
        Digest(w.WindowId);Digest(w.ProvenanceHash);Text(w.SourceId,128);Text(w.ClusterId,256);Text(w.Provider,128);Text(w.ResourceId,512);Metric(w.MetricKind,w.Unit);
        if(w.WindowEndUtc-w.WindowStartUtc<TimeSpan.FromMinutes(1)||w.WindowEndUtc-w.WindowStartUtc>TimeSpan.FromDays(31)||w.CollectedAtUtc<w.WindowEndUtc||w.Quantity is not null&&Decimal(w.Quantity)<0||w.Reasons.Count>10)throw new ArgumentException("Invalid measurement window.");
        if(w.Quality is not("Observed" or "Missing" or "CounterReset" or "Incomplete" or "Stale" or "OwnershipAmbiguous"))throw new ArgumentException("Invalid measurement quality.");
        foreach(var r in w.Reasons)Text(r,64);
    }
    public static MeasurementAccessScope AccessScope(MeasurementWindow w)=>new(Hash(w.ClusterId+"\n"+w.ResourceId+"\n"+(w.Ownership.TeamId??"")+"\n"+(w.Ownership.ApplicationId??"")),w.ClusterId,w.ResourceId,w.Ownership.TeamId,w.Ownership.ApplicationId);
    public static MeasurementWindow Counter(AttributionSourceIdentity source,MeasuredCounterWindow sample,DateTimeOffset collectedUtc,string provenance)
    {
        ArgumentNullException.ThrowIfNull(source);ArgumentNullException.ThrowIfNull(sample);
        sample=sample with{StartUtc=sample.StartUtc.ToUniversalTime(),EndUtc=sample.EndUtc.ToUniversalTime()};
        Metric(sample.MetricKind,sample.Unit);var start=Decimal(sample.StartValue);var end=Decimal(sample.EndValue);if(start<0||end<0)throw new ArgumentException("Negative counter.");
        Text(sample.SourceRevision,128);var quality="Observed";string? quantity=null;var reasons=new List<string>();
        if(end<start){quality="CounterReset";reasons.Add("CounterReset");}
        else if(sample.Coverage!="Complete"){quality="Incomplete";reasons.Add("EvidenceIncomplete");}
        else if(collectedUtc-sample.EndUtc>TimeSpan.FromMinutes(5)){quality="Stale";reasons.Add("MeasurementStale");}
        else quantity=Number(checked(end-start));
        var key=Hash(source.SourceId+"\n"+source.ClusterId+"\n"+sample.ResourceId+"\n"+sample.MetricKind+"\n"+sample.StartUtc.ToString("O")+"\n"+sample.EndUtc.ToString("O"));
        if(sample.ResourceIdentity is not null)Digest(sample.ResourceIdentity);
        var result=new MeasurementWindow(key,source.SourceId,source.ClusterId,source.Provider,sample.MetricKind,sample.Unit,sample.ResourceId,sample.StartUtc,sample.EndUtc,collectedUtc,quantity,quality,reasons,new(null,null,null,null,"Missing"),Hash(provenance+"\n"+JsonSerializer.Serialize(sample,Json)),sample.ResourceIdentity);
        Window(result);return result;
    }
}

public static class AttributionCalculator
{
    public static AttributionWindow Allocate(MeasurementWindow w,RateCardVersion? card,DateTimeOffset now)
    {
        AttributionContract.Window(w);var reasons=w.Reasons.ToList();var quality=w.Quality;var allocations=new List<AttributionAllocation>();
        if(w.Quantity is null)return new(w,allocations,null,card?.ContentHash,card?.Document.Currency,null,null,quality,reasons);
        var quantity=AttributionContract.Decimal(w.Quantity);decimal allocated=0;
        if(w.Ownership.Quality=="Observed"&&(w.Ownership.TeamId is not null||w.Ownership.ApplicationId is not null))
        {allocations.Add(new(w.Ownership.TeamId,w.Ownership.ApplicationId,w.Quantity,"Observed"));allocated=quantity;}
        else if(w.Ownership.Quality=="Ambiguous")
        {quality="OwnershipAmbiguous";reasons.Add("OwnershipAmbiguous");}
        else if(card is not null&&card.Document.Weights.Count>0)
        {
            foreach(var weight in card.Document.Weights){var amount=checked(quantity*AttributionContract.Decimal(weight.Weight));allocated=checked(allocated+amount);allocations.Add(new(weight.TeamId,weight.ApplicationId,AttributionContract.Number(amount),"Estimated"));}
            quality="Estimated";reasons.Add("ConfiguredAllocation");
        }
        else {quality="Incomplete";reasons.Add("OwnershipMissing");}
        var residual=checked(quantity-allocated);
        if(residual<0)throw new ArgumentException("AllocationPrecisionExceeded");
        string? unrounded=null;string? rounded=null;
        if(card is null){reasons.Add("RateCardMissing");quality=quality=="Observed"?"Incomplete":quality;}
        else
        {
            var d=AttributionContract.Rate(card.Document);
            if(d.MetricKind!=w.MetricKind||d.Unit!=w.Unit||d.EffectiveFromUtc>w.WindowStartUtc||d.EffectiveToUtc<w.WindowEndUtc)throw new ArgumentException("Rate card differs from measurement.");
            var amount=checked(quantity*AttributionContract.Decimal(d.Amount));unrounded=AttributionContract.Number(amount);rounded=AttributionContract.Number(decimal.Round(amount,2,MidpointRounding.ToEven));
        }
        return new(w,allocations,AttributionContract.Number(residual),card?.ContentHash,card?.Document.Currency,unrounded,rounded,quality,reasons.Distinct(StringComparer.Ordinal).ToArray());
    }
    public static AttributionTotals Totals(IReadOnlyList<AttributionWindow> windows,bool complete)
    {
        var currencies=windows.Where(w=>w.Currency is not null).Select(w=>w.Currency!).Distinct(StringComparer.Ordinal).ToArray();
        if(currencies.Length>1)throw new ArgumentException("MixedCurrencyTotals");
        foreach(var group in windows.GroupBy(w=>(w.Measurement.ClusterId,w.Measurement.ResourceId,w.Measurement.MetricKind)))
        {
            DateTimeOffset? previousEnd=null;foreach(var row in group.OrderBy(w=>w.Measurement.WindowStartUtc))
            {if(previousEnd>row.Measurement.WindowStartUtc)return new(currencies.SingleOrDefault(),null,null,"EvidenceIncomplete",false);previousEnd=row.Measurement.WindowEndUtc;}
        }
        if(windows.Count==0||windows.Any(w=>w.UnroundedAmount is null))return new(currencies.SingleOrDefault(),null,null,"EvidenceIncomplete",false);
        decimal total=0;foreach(var w in windows)total=checked(total+AttributionContract.Decimal(w.UnroundedAmount!));
        var incomplete=windows.Any(w=>w.Quality is not("Observed" or "Estimated"));
        var status=incomplete?"EvidenceIncomplete":!complete?"Partial":windows.Any(w=>w.Quality=="Estimated")?"Estimated":"Complete";
        return new(currencies.SingleOrDefault(),AttributionContract.Number(total),AttributionContract.Number(decimal.Round(total,2,MidpointRounding.ToEven)),status,complete&&!incomplete);
    }
}
