using System.Net;
using System.Text.Json;

using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(90));
using var handler = new SocketsHttpHandler { AllowAutoRedirect = false, UseProxy = false, ConnectTimeout = TimeSpan.FromSeconds(2) };
using var client = new HttpClient(handler) { BaseAddress = new Uri("http://127.0.0.1:8080"), Timeout = TimeSpan.FromSeconds(3) };
while (!deadline.IsCancellationRequested)
{
    try
    {
        using var health = await client.GetAsync("/healthz", deadline.Token);
        using var document = JsonDocument.Parse(await health.Content.ReadAsByteArrayAsync(deadline.Token));
        if (health.StatusCode == HttpStatusCode.OK && document.RootElement.GetProperty("status").GetString() == "ok")
        {
            using var index = await client.GetAsync("/", deadline.Token);
            if (index.StatusCode != HttpStatusCode.OK || !(await index.Content.ReadAsStringAsync(deadline.Token)).Contains("<html", StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException("Offline frontend unavailable.");
            Console.WriteLine("{\"networkNone\":true,\"health\":true,\"staticFrontend\":true,\"scope\":\"isolated startup only; external brokers and identity require separate fixtures\"}");
            return 0;
        }
    }
    catch (Exception error) when (error is HttpRequestException or OperationCanceledException or JsonException) { }
    if (!deadline.IsCancellationRequested) await Task.Delay(500, deadline.Token);
}
Console.Error.WriteLine("Offline candidate did not become ready within the bounded startup window.");
return 1;
