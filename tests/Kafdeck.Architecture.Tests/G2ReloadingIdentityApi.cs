using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;

namespace Kafdeck.Architecture.Tests;

/// <summary>Shipped Program.cs, real TCP, externally reloadable registration file.</summary>
internal sealed class G2ReloadingIdentityApi : IAsyncDisposable
{
    private readonly string _directory;
    private readonly Dictionary<string, string?> _settings;
    private Process? _process;
    private Task<string>? _stdout;
    private Task<string>? _stderr;
    internal HttpClient Http { get; }
    private string SettingsPath => Path.Combine(_directory, "appsettings.json");

    internal G2ReloadingIdentityApi(G2KeycloakLifecycleClient identity, string directory)
    {
        _directory = directory;
        Directory.CreateDirectory(directory);
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        Http = new HttpClient { BaseAddress = new Uri("http://127.0.0.1:" + port), Timeout = TimeSpan.FromSeconds(5) };
        _settings = new()
        {
            ["Kafdeck:Deployment:ListenUrls:0"] = Http.BaseAddress.ToString().TrimEnd('/'),
            ["Kafdeck:NonBrowserRead:Enabled"] = "true",
            ["Kafdeck:NonBrowserRead:Issuer"] = identity.Issuer,
            ["Kafdeck:NonBrowserRead:ApiAudience"] = identity.Audience,
            ["Kafdeck:NonBrowserRead:TrustedCaPemFile"] = identity.CaPath,
            ["Kafdeck:NonBrowserRead:Registrations:0:Id"] = "lifecycle-service",
            ["Kafdeck:NonBrowserRead:Registrations:0:OwnerId"] = "g2-lab-owner",
            ["Kafdeck:NonBrowserRead:Registrations:0:Subject"] = identity.Subject,
            ["Kafdeck:NonBrowserRead:Registrations:0:ClientId"] = identity.ClientId,
            ["Kafdeck:NonBrowserRead:Registrations:0:Enabled"] = "true",
            ["Kafdeck:NonBrowserRead:Registrations:0:ExpiresAtUtc"] = DateTimeOffset.UtcNow.AddMinutes(10).ToString("O"),
            ["Kafdeck:NonBrowserRead:Registrations:0:ReadActions:0"] = "SystemRead",
            ["Kafdeck:Authorization:Roles:0:Id"] = "lifecycle-reader",
            ["Kafdeck:Authorization:Roles:0:Permissions:0:Action"] = "SystemRead",
            ["Kafdeck:Authorization:SubjectBindings:0:Issuer"] = identity.Issuer,
            ["Kafdeck:Authorization:SubjectBindings:0:Subject"] = identity.Subject,
            ["Kafdeck:Authorization:SubjectBindings:0:RoleIds:0"] = "lifecycle-reader",
            ["Logging:LogLevel:Default"] = "Warning",
        };
    }
    internal void RegisterHuman(string subject, string client)
    {
        const string prefix = "Kafdeck:NonBrowserRead:HumanRegistrations:0:";
        _settings[prefix+"Id"]="lifecycle-human"; _settings[prefix+"OwnerId"]="g2-lab-owner";
        _settings[prefix+"Subject"]=subject; _settings[prefix+"ClientId"]=client;
        _settings[prefix+"Enabled"]="true"; _settings[prefix+"ExpiresAtUtc"]=DateTimeOffset.UtcNow.AddMinutes(10).ToString("O");
        _settings[prefix+"ReadActions:0"]="SystemRead";
        _settings["Kafdeck:Authorization:SubjectBindings:1:Issuer"]=_settings["Kafdeck:NonBrowserRead:Issuer"];
        _settings["Kafdeck:Authorization:SubjectBindings:1:Subject"]=subject;
        _settings["Kafdeck:Authorization:SubjectBindings:1:RoleIds:0"]="lifecycle-reader";
    }
    internal async Task StartAsync(CancellationToken token)
    {
        await SaveAsync(token);
        var repository = new DirectoryInfo(AppContext.BaseDirectory);
        while (!File.Exists(Path.Combine(repository.FullName, "Kafdeck.slnx"))) repository = repository.Parent!;
        var configuration = new DirectoryInfo(AppContext.BaseDirectory).Parent!.Name;
        var dll = Path.Combine(repository.FullName, "src/backend/Kafdeck.Api/bin", configuration, "net10.0/Kafdeck.Api.dll");
        var start = new ProcessStartInfo("dotnet") { WorkingDirectory = _directory, UseShellExecute = false,
            CreateNoWindow = true, RedirectStandardOutput = true, RedirectStandardError = true };
        start.ArgumentList.Add(dll);
        start.Environment["ASPNETCORE_CONTENTROOT"] = _directory;
        start.Environment["DOTNET_USE_POLLING_FILE_WATCHER"] = "1";
        _process = Process.Start(start) ?? throw new InvalidOperationException("Lifecycle API did not start.");
        _stdout = _process.StandardOutput.ReadToEndAsync(token);
        _stderr = _process.StandardError.ReadToEndAsync(token);
        for (var i = 0; i < 120 && !_process.HasExited; i++)
        {
            try { using var response = await Http.GetAsync("/healthz", token); if (response.IsSuccessStatusCode) return; }
            catch (HttpRequestException) { }
            await Task.Delay(100, token);
        }
        throw new InvalidOperationException("Actual lifecycle API health readiness failed; private output withheld.");
    }
    internal async Task ChangeAsync(string key, string? value, CancellationToken token)
    {
        if (value is null) _settings.Remove(key); else _settings[key] = value;
        await SaveAsync(token);
    }
    private async Task SaveAsync(CancellationToken token)
    {
        var temporary = SettingsPath + ".new";
        await File.WriteAllBytesAsync(temporary, JsonSerializer.SerializeToUtf8Bytes(_settings), token);
        File.Move(temporary, SettingsPath, true);
    }
    internal async Task<int> GetAsync(string bearer, CancellationToken token)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, "/api/v1/system/info");
        request.Headers.Authorization = new("Bearer", bearer);
        using var response = await Http.SendAsync(request, token);
        return (int)response.StatusCode;
    }
    internal async Task WaitStatusAsync(string bearer, int expected, CancellationToken token)
    {
        for (var i = 0; i < 120; i++)
        {
            if (await GetAsync(bearer, token) == expected) return;
            await Task.Delay(100, token);
        }
        throw new InvalidOperationException("Actual API failed to enforce current identity lifecycle state.");
    }
    public async ValueTask DisposeAsync()
    {
        if (_process is not null)
        {
            if (!_process.HasExited) { _process.Kill(true); await _process.WaitForExitAsync(); }
            var output = (await _stdout!) + (await _stderr!);
            if (output.Contains("Bearer eyJ", StringComparison.Ordinal) || output.Contains("access_token\":", StringComparison.Ordinal))
                throw new InvalidOperationException("Lifecycle host output contained credential material; output withheld.");
            _process.Dispose();
        }
        Http.Dispose();
    }
}
