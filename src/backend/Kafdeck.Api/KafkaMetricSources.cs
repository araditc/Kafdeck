using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Mvc;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;

namespace Kafdeck.Api;

internal sealed record BrokerSource(string ClusterId, string Instance, byte[] Credential);
internal sealed record JmxPoint(string Metric, SortedDictionary<string, string> Labels, double Value);
internal sealed record JmxBatch(bool Partial, Guid Cycle, int Part, bool Last, JmxPoint[] Samples);
internal sealed record MonitoringSettings(string Database, BrokerSource[] Sources)
{
    internal const string MetadataInstance = "kafdeck-metadata";
    internal string[] MetadataClusters { get; init; } = [];
    internal static MonitoringSettings Load(IConfiguration config,string? managedDatabase)
    {
        var section = config.GetSection("Kafdeck:Monitoring");
        var sources = new List<BrokerSource>();
        foreach (var row in section.GetSection("Sources").GetChildren())
        {
            var cluster = row["ClusterId"];
            var instance = row["Instance"];
            var reference = row["Token"];
            if (string.IsNullOrWhiteSpace(cluster) || cluster.Length > 160 || string.IsNullOrWhiteSpace(instance) ||
                instance.Length > 160 || sources.Count >= 128 || reference is null ||
                !(reference.StartsWith("file:", StringComparison.Ordinal) || reference.StartsWith("env:", StringComparison.Ordinal)))
                throw new InvalidOperationException("Invalid Kafdeck monitoring source configuration.");
            var token = new SecretResolver().Resolve(SecretReference.Parse(reference)).Reveal().Trim();
            if (token.Length is < 32 or > 256 || token.Any(char.IsWhiteSpace)) throw new InvalidOperationException("Invalid monitoring credential.");
            var hash = SHA256.HashData(Encoding.UTF8.GetBytes(token));
            if (sources.Any(s => s.ClusterId == cluster && s.Instance == instance || CryptographicOperations.FixedTimeEquals(s.Credential,hash)))
                throw new InvalidOperationException("Monitoring source identities and credentials must be unique.");
            sources.Add(new(cluster, instance, hash));
        }
        var ids=section.GetSection("MetadataClusters").Get<string[]>() ?? [];
        if(ids.Length>16 || ids.Any(id=>string.IsNullOrWhiteSpace(id) || id.Length>160) || ids.Distinct().Count()!=ids.Length ||
            sources.Any(s=>s.Instance==MetadataInstance)) throw new InvalidOperationException("Invalid metadata monitoring targets.");
        var db = section["DatabasePath"] ?? managedDatabase ?? "";
        if ((ids.Length>0 || sources.Count > 0 || db.Length > 0) && !Path.IsPathFullyQualified(db)) throw new InvalidOperationException("Monitoring needs an absolute DatabasePath.");
        return new(db, sources.ToArray()){MetadataClusters=ids};
    }
}

internal static class KafkaMetricSources
{
    internal sealed record Enrollment(string Instance,string Action);
    internal sealed record SamplingChange(int Seconds);
    internal sealed record TopicChange(string Topic,bool Enabled);
    internal sealed record RuleChange(string Name, string? Metric, double Threshold, bool Below, int Seconds, string? Severity, bool Remove);
    private static (BrokerSource? Source,IResult? Error) Authenticate(HttpContext context, MonitoringStore store)
    {
        if (!context.Request.IsHttps && (context.Connection.RemoteIpAddress is not { } remote || !System.Net.IPAddress.IsLoopback(remote)))
            return (null,Results.StatusCode(403));
        var auth=context.Request.Headers["X-Kafdeck-Agent-Token"];
        if (auth.Count!=1 || auth[0] is not {Length: >=32 and <=256} token) return (null,Results.Unauthorized());
        var hash=SHA256.HashData(Encoding.UTF8.GetBytes(token));
        var source=store.Sources().FirstOrDefault(s=>CryptographicOperations.FixedTimeEquals(s.Credential,hash));
        if (source is null) return (null,Results.Unauthorized());
        if (!context.Request.HasJsonContentType()) return (null,Results.StatusCode(415));
        if (context.Request.ContentLength>2_000_000) return (null,Results.StatusCode(413));
        return (source,null);
    }
    internal static void Map(WebApplication app, KafdeckOptions options)
    {
        app.MapPost("/monitoring/ingest", async (HttpContext context, MonitoringStore store, CancellationToken ct) =>
        {
            var (source,error)=Authenticate(context,store);
            if (error is not null) return error;
            JmxBatch? batch;
            try { batch = await context.Request.ReadFromJsonAsync<JmxBatch>(ct); }
            catch (JsonException) { return Results.BadRequest(); }
            if (batch?.Samples is not { Length: <= 8192 } || batch.Cycle==Guid.Empty || batch.Part is < 0 or >= 64 || batch.Samples.Any(p => p is null || string.IsNullOrWhiteSpace(p.Metric) ||
                p.Metric.Length > 512 || p.Metric.Any(char.IsControl) || !double.IsFinite(p.Value) || p.Labels is null ||
                p.Labels.Count > 24 || p.Labels.Any(l => l.Key.Length is 0 or >100 || l.Key.Any(char.IsControl) ||
                    l.Value is null || l.Value.Length > 512 || l.Value.Any(char.IsControl) || l.Key is "instance" or "kafdeck_cluster")))
                return Results.BadRequest();
            if (batch.Last && store.SamplingOverride(source!.ClusterId) is int seconds)
                context.Response.Headers["X-Kafdeck-Interval-Seconds"]=seconds.ToString(System.Globalization.CultureInfo.InvariantCulture);
            if (batch.Last && store.SelectedTopics(source!.ClusterId) is { } topics)
                context.Response.Headers["X-Kafdeck-Topics"]="v1:"+string.Join(',',topics);
            return store.Ingest(source!,batch,ct,context.Request.Headers["X-Kafdeck-Topic-Only"]=="1");
        }).WithMetadata(new RequestSizeLimitAttribute(2_000_000));
        app.MapPost("/monitoring/client-stats", async (HttpContext context,MonitoringStore store,TimeProvider clock,CancellationToken ct) =>
        {
            var (source,error)=Authenticate(context,store);
            if (error is not null) return error;
            try
            {
                using var doc=await JsonDocument.ParseAsync(context.Request.Body,new JsonDocumentOptions {MaxDepth=12},ct);
                var batch=KafkaClientStatistics.Read(doc.RootElement,clock.GetUtcNow().ToUnixTimeSeconds());
                return batch is null ? Results.BadRequest() : store.Ingest(source!,batch,ct);
            }
            catch (JsonException) {return Results.BadRequest();}
        }).WithMetadata(new RequestSizeLimitAttribute(2_000_000));
        if (options.Deployment.Mode == AccessMode.Oidc && app.Services.GetService<ManagedIdentityStore>() is { } identity)
        {
            bool Owner(HttpContext ctx) => ctx.User.Identity is not NonBrowserReadIdentity &&
                OperatorSessionContextFactory.TryCreate(ctx.User,out var session) && session is not null && ManagedIdentityEndpoints.IsOwner(identity,session);
            app.MapPost("/api/v1/clusters/{clusterId}/monitoring/sampling",(string clusterId, SamplingChange input,HttpContext ctx,MonitoringStore store)=>
            {
                if(!Owner(ctx)) return Results.StatusCode(403);
                if(!options.Clusters.Any(c=>c.Id==clusterId)) return Results.NotFound();
                return store.SetSampling(clusterId,input.Seconds);
            }).WithMetadata(new RequestSizeLimitAttribute(512)).RequireKafdeckAntiforgery();
            app.MapPost("/api/v1/clusters/{clusterId}/monitoring/topic-selection",(string clusterId, TopicChange input,HttpContext ctx,MonitoringStore store)=>
            {
                if(!Owner(ctx)) return Results.StatusCode(403);
                if(!options.Clusters.Any(c=>c.Id==clusterId)) return Results.NotFound();
                return store.SetTopic(clusterId,input.Topic,input.Enabled);
            }).WithMetadata(new RequestSizeLimitAttribute(1024)).RequireKafdeckAntiforgery();
            app.MapPost("/api/v1/clusters/{clusterId}/monitoring/agents", (string clusterId, Enrollment input, HttpContext ctx, MonitoringStore store) =>
            {
                ctx.Response.Headers.CacheControl="no-store";
                if (!Owner(ctx)) return Results.StatusCode(403);
                if (!options.Clusters.Any(c=>c.Id==clusterId)) return Results.NotFound();
                return store.Enroll(clusterId,input.Instance,input.Action);
            }).WithMetadata(new RequestSizeLimitAttribute(2048)).RequireKafdeckAntiforgery();
            app.MapPost("/api/v1/clusters/{clusterId}/monitoring/rules", (string clusterId, RuleChange input, HttpContext ctx, MonitoringStore store) =>
            {
                if (!Owner(ctx)) return Results.StatusCode(403);
                if (!options.Clusters.Any(c=>c.Id==clusterId)) return Results.NotFound();
                return store.ChangeRule(clusterId,input);
            }).WithMetadata(new RequestSizeLimitAttribute(4096)).RequireKafdeckAntiforgery();
        }
        app.MapGet("/api/v1/clusters/{clusterId}/monitoring/agent.jar", (string clusterId) =>
            options.Clusters.Any(c=>c.Id==clusterId) ? Results.File(typeof(KafkaMetricSources).Assembly
                .GetManifestResourceStream("Kafdeck.Monitoring.Agent.jar")!, "application/java-archive", "kafdeck-kafka-agent.jar") : Results.NotFound())
            .RequireKafdeckAuthorization(AuthorizationAction.ClusterRead, "clusterId")
            .RequireKafdeckAuthorization(AuthorizationAction.MonitoringRead, "clusterId");
        app.MapGet("/api/v1/clusters/{clusterId}/monitoring/{kind}", (string clusterId, string kind, string? metric, string? search,
            int? minutes, string? instance, string? filters, string? after, string? topic, int? partition, MonitoringStore store, CancellationToken ct) =>
            options.Clusters.Any(c=>c.Id==clusterId) ? store.Read(clusterId, kind, metric, search, minutes ?? 60, instance, filters, ct,after,topic,partition) : Results.NotFound())
            .RequireKafdeckAuthorization(AuthorizationAction.ClusterRead, "clusterId")
            .RequireKafdeckAuthorization(AuthorizationAction.MonitoringRead, "clusterId");
    }
}
