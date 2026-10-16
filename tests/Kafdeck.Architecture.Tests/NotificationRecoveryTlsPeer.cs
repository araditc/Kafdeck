using System.Collections.Concurrent;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Certificates = Kafdeck.Architecture.Tests.V08W66HostedWebhookTlsTests.TlsProvider;

namespace Kafdeck.Architecture.Tests;

// Concurrent accepts matter: a held first response must not hide an accidental
// second POST. The peer retains at most eight connection tasks and bounded bodies.
internal sealed class NotificationRecoveryTlsPeer(bool holdReceipt) : IAsyncDisposable
{
    private readonly Certificates _certificates = new();
    private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
    private readonly CancellationTokenSource _stop = new();
    private readonly ConcurrentBag<Task> _connections = new();
    private Task? _accept;
    private int _posts;
    private int _accepted;
    private int _invalid;
    internal int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;
    internal int PhysicalPosts => Volatile.Read(ref _posts);
    internal int AcceptedConnections => Volatile.Read(ref _accepted);
    internal bool InvalidWire => Volatile.Read(ref _invalid) != 0;
    internal TaskCompletionSource<Guid> CompletePost { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    internal string Endpoint => $"https://hooks.example.org:{Port}/notify";
    internal byte[] PublicCa => _certificates.Root.Export(System.Security.Cryptography.X509Certificates.X509ContentType.Cert);

    internal void Start() { _listener.Start(); _accept = AcceptAsync(); }
    private async Task AcceptAsync()
    {
        try
        {
            while (!_stop.IsCancellationRequested)
            {
                var client = await _listener.AcceptTcpClientAsync(_stop.Token);
                if (Interlocked.Increment(ref _accepted) > 8)
                { Volatile.Write(ref _invalid, 1); client.Dispose(); return; }
                _connections.Add(HandleAsync(client));
            }
        }
        catch (OperationCanceledException) when (_stop.IsCancellationRequested) { }
    }
    private async Task HandleAsync(TcpClient client)
    {
        using (client)
        await using (var stream = new SslStream(client.GetStream()))
        {
            try
            {
                await stream.AuthenticateAsServerAsync(new SslServerAuthenticationOptions
                {
                    ServerCertificate = _certificates.ServerCertificate,
                    EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
                }, _stop.Token);
                using var data = new MemoryStream();
                var buffer = new byte[4096];
                while (data.Length < 65_536)
                {
                    var count = await stream.ReadAsync(buffer, _stop.Token);
                    if (count == 0) return;
                    data.Write(buffer, 0, count);
                    var text = Encoding.UTF8.GetString(data.ToArray());
                    var boundary = text.IndexOf("\r\n\r\n", StringComparison.Ordinal);
                    if (boundary < 0) continue;
                    var headers = text[..boundary].Split("\r\n");
                    var lengthLine = headers.Single(h => h.StartsWith("Content-Length: ", StringComparison.OrdinalIgnoreCase));
                    var length = int.Parse(lengthLine[16..], System.Globalization.CultureInfo.InvariantCulture);
                    if (length is < 1 or > 32768) throw new IOException("Fixture body budget exceeded.");
                    if (data.Length < boundary + 4 + length) continue;
                    if (headers[0] != "POST /notify HTTP/1.1" ||
                        !headers.Contains($"Host: hooks.example.org:{Port}", StringComparer.OrdinalIgnoreCase))
                        throw new IOException("Unexpected fixture identity.");
                    using var body = JsonDocument.Parse(text[(boundary + 4)..]);
                    var id = body.RootElement.GetProperty("eventId").GetGuid();
                    Interlocked.Increment(ref _posts);
                    CompletePost.TrySetResult(id);
                    if (holdReceipt) await Task.Delay(Timeout.InfiniteTimeSpan, _stop.Token);
                    else await stream.WriteAsync(Encoding.ASCII.GetBytes("HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"), _stop.Token);
                    return;
                }
                throw new IOException("Fixture request budget exceeded.");
            }
            catch (OperationCanceledException) when (_stop.IsCancellationRequested) { }
            catch (Exception error) when (error is IOException or AuthenticationException or JsonException or InvalidOperationException or FormatException)
            { Volatile.Write(ref _invalid, 1); }
        }
    }
    public async ValueTask DisposeAsync()
    {
        await _stop.CancelAsync();
        if (_accept is not null) await _accept;
        _listener.Stop();
        await Task.WhenAll(_connections);
        await _certificates.DisposeAsync();
        _stop.Dispose();
    }
}
