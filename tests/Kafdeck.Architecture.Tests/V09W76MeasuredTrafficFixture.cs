using System.Globalization;
using System.Diagnostics;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Json;
using Kafdeck.Core.Attribution;
using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Configuration;

namespace Kafdeck.Architecture.Tests;

/// <summary>Actual TLS body-byte meter. Fixture DNS/public-IP dialing is virtualized; certificate,
/// hostname, HTTP, observed bytes and the >=one-minute wall-clock interval are real.
/// This is the registered adapter profile, not measurement of Kafka billable traffic.</summary>
internal sealed class V09W76MeasuredTrafficFixture:IAsyncDisposable
{
    private readonly V08W66HostedWebhookTlsTests.TlsProvider tls;
    private long bytes;
    private long startBytes;
    private DateTimeOffset started;
    private long startedTimestamp;
    private MeasuredCounterWindow? closed;
    private string resource="";
    private string identity="";
    internal int MetricsReads{get;private set;}
    internal long PhysicalBytes=>Interlocked.Read(ref bytes);
    internal DateTimeOffset Started=>started;
    internal MeasuredCounterWindow Window=>closed??throw new InvalidOperationException("Physical meter interval is not complete.");
    internal V09W76MeasuredTrafficFixture()=>tls=new(session:Serve);
    internal Uri Endpoint=>new($"https://hooks.example.org:{tls.Port}/measured-windows");
    private NotificationPinnedHttpHandlerFactory Factory()=>new(tls,NotificationAddressPolicy.NoConfiguredNat64,trustRoots:new X509Certificate2Collection(tls.Root));
    internal ConfiguredAttributionMeasurementSource Source(string cluster)=>new(new(){SourceId="controlled-resource-meter",ClusterId=cluster,Provider="KafdeckMeasuredCountersV1",Endpoint=Endpoint.AbsoluteUri},
        new NotificationDnsEndpointResolver(tls,NotificationAddressPolicy.NoConfiguredNat64),Factory(),new SecretResolver());
    internal void Begin(string resourceId,string resourceIdentity)
    {resource=resourceId;identity=resourceIdentity;started=DateTimeOffset.UtcNow;startedTimestamp=Stopwatch.GetTimestamp();startBytes=PhysicalBytes;}
    internal void ResetCounter()=>Interlocked.Exchange(ref bytes,0);
    internal async Task SendTrafficAsync(int count,CancellationToken token)
    {
        var pinned=await new NotificationDnsEndpointResolver(tls,NotificationAddressPolicy.NoConfiguredNat64).ResolveForConnectionAsync(Endpoint,token);
        using var client=new HttpClient(Factory().CreateGuardedHandler(pinned));
        using var response=await client.PostAsync(Endpoint,new ByteArrayContent(Enumerable.Repeat((byte)'x',count).ToArray()),token);
        response.EnsureSuccessStatusCode();
    }
    internal async Task CloseAsync(CancellationToken token)
    {
        // Task.Delay may finish fractionally early on some timer implementations. Observe the actual
        // UTC and monotonic deadlines rather than treating one completed delay as a measured interval.
        var minimum=TimeSpan.FromSeconds(61);
        DateTimeOffset ended;
        while(true)
        {
            token.ThrowIfCancellationRequested();ended=DateTimeOffset.UtcNow;
            var wallRemaining=minimum-(ended-started);var elapsedRemaining=minimum-Stopwatch.GetElapsedTime(startedTimestamp);
            var remaining=wallRemaining>elapsedRemaining?wallRemaining:elapsedRemaining;
            if(remaining<=TimeSpan.Zero)break;
            await Task.Delay(TimeSpan.FromMilliseconds(Math.Clamp(Math.Ceiling(remaining.TotalMilliseconds)+10,10,1000)),token);
        }
        closed=new(resource,"trafficBytes","bytes",started,ended,startBytes.ToString(CultureInfo.InvariantCulture),PhysicalBytes.ToString(CultureInfo.InvariantCulture),"Complete","physical-body-counter-v1",identity);
    }
    private async Task Serve(SslStream stream,CancellationToken token)
    {
        using var content=new MemoryStream();var buffer=new byte[8192];string? method=null;var headerEnd=-1;var length=0;
        while(content.Length<2*1024*1024)
        {
            var n=await stream.ReadAsync(buffer,token);if(n==0)return;content.Write(buffer,0,n);
            if(headerEnd<0)
            {
                var text=Encoding.ASCII.GetString(content.ToArray());headerEnd=text.IndexOf("\r\n\r\n",StringComparison.Ordinal);if(headerEnd<0)continue;
                var lines=text[..headerEnd].Split("\r\n");method=lines[0].Split(' ')[0];
                if(lines[0].Split(' ')[1]!="/measured-windows")throw new InvalidOperationException("Unexpected physical meter path.");
                var item=lines.FirstOrDefault(l=>l.StartsWith("Content-Length:",StringComparison.OrdinalIgnoreCase));
                length=item is null?0:int.Parse(item[(item.IndexOf(':')+1)..].Trim(),CultureInfo.InvariantCulture);
                if(length is <0 or >1024*1024)throw new InvalidOperationException("Physical meter fixture bound exceeded.");
            }
            if(content.Length<headerEnd+4+length)continue;
            byte[] response;
            if(method=="POST"){Interlocked.Add(ref bytes,length);response="{}"u8.ToArray();}
            else if(method=="GET"){MetricsReads++;response=JsonSerializer.SerializeToUtf8Bytes(new MeasuredCounterBatch(1,[Window]),AttributionContract.Json);}
            else throw new InvalidOperationException("Unexpected physical meter verb.");
            await stream.WriteAsync(Encoding.ASCII.GetBytes($"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {response.Length}\r\nConnection: close\r\n\r\n"),token);
            await stream.WriteAsync(response,token);return;
        }
        throw new InvalidOperationException("Physical meter fixture request bound exceeded.");
    }
    public ValueTask DisposeAsync()=>tls.DisposeAsync();
}
