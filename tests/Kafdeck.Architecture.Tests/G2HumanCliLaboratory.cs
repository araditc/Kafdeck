using System.Diagnostics;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using System.Text.RegularExpressions;
using Kafdeck.Cli;
using Microsoft.AspNetCore.WebUtilities;
using Xunit;

namespace Kafdeck.Architecture.Tests;

/// <summary>Actual shipped API and CLI with real HTTPS Keycloak authorization codes.
/// Configuration is private laboratory data; credentials never become CLI output.</summary>
internal sealed class G2HumanCliLaboratory : IAsyncDisposable
{
    private readonly JsonDocument _identity;
    private readonly string _directory;
    private readonly string _profile;
    private readonly ProcessStartInfo _start;
    private Process? _process;
    private Task<string>? _stdout;
    private Task<string>? _stderr;
    internal Dictionary<string,string> Settings { get; } = new();
    internal Uri Api { get; }
    internal bool Approver { get; set; }
    internal string ConfigurationFile => Path.Combine(_directory,"appsettings.json");
    internal string Output { get; private set; } = "";
    internal int CompletedLogins { get; private set; }
    internal string Field(string name) => _identity.RootElement.GetProperty(name).GetString()!;

    internal G2HumanCliLaboratory(string directory)
    {
        _identity=JsonDocument.Parse(File.ReadAllText(Environment.GetEnvironmentVariable("KAFDECK_KEYCLOAK_G2_LAB")!));
        _directory=directory; Directory.CreateDirectory(directory);
        using var reservation=new TcpListener(IPAddress.Loopback,0); reservation.Start();
        Api=new Uri("http://127.0.0.1:"+((IPEndPoint)reservation.LocalEndpoint).Port); reservation.Stop();
        _profile=Path.Combine(directory,"human-public-profile.json");
        File.WriteAllBytes(_profile,JsonSerializer.SerializeToUtf8Bytes(new {issuer=Field("issuer"),audience=Field("audience"),
            clientId=Field("cliClientId"),trustedCaPemFile=Field("caPemFile")}));
        var root=new DirectoryInfo(AppContext.BaseDirectory);
        while(!File.Exists(Path.Combine(root.FullName,"Kafdeck.slnx"))) root=root.Parent!;
        var build=typeof(G2HumanCliLaboratory).Assembly.GetCustomAttribute<AssemblyConfigurationAttribute>()?.Configuration??"Debug";
        var dll=Path.Combine(root.FullName,"src/backend/Kafdeck.Api/bin",build,"net10.0/Kafdeck.Api.dll");
        _start=new("dotnet") {WorkingDirectory=directory,UseShellExecute=false,CreateNoWindow=true,
            RedirectStandardOutput=true,RedirectStandardError=true};
        _start.ArgumentList.Add(dll); _start.Environment["ASPNETCORE_CONTENTROOT"]=directory;
        Settings["Kafdeck:Deployment:ListenUrls:0"]=Api.AbsoluteUri;
        Settings["Kafdeck:Deployment:AccessMode"]="Oidc";
        Settings["Kafdeck:Deployment:Oidc:Issuer"]=Field("issuer");
        Settings["Kafdeck:Deployment:Oidc:ClientId"]=Field("cliClientId");
        Settings["Kafdeck:Deployment:Oidc:Scopes:0"]="openid";
        Settings["Kafdeck:Deployment:Oidc:TrustedCaPemFile"]=Field("caPemFile");
        Settings["Kafdeck:NonBrowserRead:Enabled"]="true";
        Settings["Kafdeck:NonBrowserRead:Issuer"]=Field("issuer");
        Settings["Kafdeck:NonBrowserRead:ApiAudience"]=Field("audience");
        Settings["Kafdeck:NonBrowserRead:TrustedCaPemFile"]=Field("caPemFile");
        Settings["Kafdeck:NonBrowserRead:GovernedMutationsEnabled"]="true";
        Settings["Kafdeck:NonBrowserRead:GovernedApprovalsEnabled"]="true";
        Settings["Logging:LogLevel:Default"]="Warning";
    }

    internal void RegisterHumans(IReadOnlyList<string> mutationActions,IReadOnlyList<string> readActions,
        IReadOnlyList<string> approvalActions,string? cluster=null,string? resource=null)
    {
        for(var person=0;person<2;person++)
        {
            var prefix=$"Kafdeck:NonBrowserRead:HumanRegistrations:{person}:";
            Settings[prefix+"Id"]=person==0?"family-requester":"family-approver";
            Settings[prefix+"OwnerId"]="g2-owned-laboratory";
            Settings[prefix+"Subject"]=Field(person==0?"humanSubject":"approverSubject");
            Settings[prefix+"ClientId"]=Field("cliClientId"); Settings[prefix+"Enabled"]="true";
            Settings[prefix+"ExpiresAtUtc"]=DateTimeOffset.UtcNow.AddHours(1).ToString("O");
            foreach(var (action,index) in readActions.Select((value,index)=>(value,index))) Settings[prefix+$"ReadActions:{index}"]=action;
            foreach(var (action,index) in mutationActions.Select((value,index)=>(value,index))) Settings[prefix+$"MutationActions:{index}"]=action;
            foreach(var (action,index) in approvalActions.Select((value,index)=>(value,index))) Settings[prefix+$"ApprovalActions:{index}"]=action;
            var role=$"family-human-{person}";
            Settings[$"Kafdeck:Authorization:Roles:{person}:Id"]=role;
            var all=readActions.Concat(mutationActions).Concat(approvalActions).Append("MutationApprove").Distinct(StringComparer.Ordinal).ToArray();
            for(var index=0;index<all.Length;index++)
            {
                var permission=$"Kafdeck:Authorization:Roles:{person}:Permissions:{index}:";
                Settings[permission+"Action"]=all[index];
                if(cluster is not null&&all[index]!="SystemRead") Settings[permission+"ClusterIds:0"]=cluster;
                if(resource is not null&&all[index]!="SystemRead") Settings[permission+"ResourcePatterns:0"]=resource;
            }
            Settings[$"Kafdeck:Authorization:SubjectBindings:{person}:Issuer"]=Field("issuer");
            Settings[$"Kafdeck:Authorization:SubjectBindings:{person}:Subject"]=Field(person==0?"humanSubject":"approverSubject");
            Settings[$"Kafdeck:Authorization:SubjectBindings:{person}:RoleIds:0"]=role;
        }
    }
    internal void Secret(string name,string value)=>_start.Environment[name]=value;
    internal async Task StartAsync(CancellationToken token)
    {
        if(_process is not null) throw new InvalidOperationException("Stop the owned API before starting it again.");
        await File.WriteAllBytesAsync(ConfigurationFile,JsonSerializer.SerializeToUtf8Bytes(Settings),token);
        _process=Process.Start(_start)??throw new InvalidOperationException("Owned API process did not start.");
        _stdout=_process.StandardOutput.ReadToEndAsync(); _stderr=_process.StandardError.ReadToEndAsync();
        using var client=new HttpClient {BaseAddress=Api,Timeout=TimeSpan.FromSeconds(3)};
        for(var attempt=0;attempt<150&&!_process.HasExited;attempt++)
        {
            try {using var response=await client.GetAsync("/healthz",token); if(response.IsSuccessStatusCode)return;}
            catch(HttpRequestException) {} catch(TaskCanceledException) when(!token.IsCancellationRequested) {}
            await Task.Delay(100,token);
        }
        if(_process.HasExited) await PersistPrivateConsoleAsync();
        throw new InvalidOperationException("Owned actual API failed health readiness; private console is retained locally, configuration and output withheld.");
    }
    internal async Task<string> RequestFileAsync(object body,CancellationToken token)
    {
        var path=Path.Combine(_directory,"request-"+Guid.NewGuid().ToString("N")+".json");
        await File.WriteAllBytesAsync(path,JsonSerializer.SerializeToUtf8Bytes(body),token); return path;
    }
    internal async Task<int> RunAsync(CancellationToken token,params string[] command)
    {
        using var output=new StringWriter(); using var error=new StringWriter();
        var args=new[]{"--url",Api.AbsoluteUri,"--oidc-profile",_profile}.Concat(command).ToArray();
        var result=await CliApplication.RunAsync(args,output,error,token,OpenBrowserAsync);
        Output=output.ToString();
        var phase=string.Join('.',command.Take(2).Select(value=>Regex.IsMatch(value,"^[a-z-]{1,40}$",RegexOptions.CultureInvariant,TimeSpan.FromSeconds(1))?value:"argument"));
        Console.WriteLine($"G2_CLI phase={phase}; exit={result}");
        var transcript=Output+error;
        Assert.False(new[]{"\"access_token\"","\"id_token\"","\"code_verifier\"","Bearer eyJ"}.Any(marker=>transcript.Contains(marker,StringComparison.OrdinalIgnoreCase)),"CLI exposed credential material.");
        foreach(var field in new[]{"humanPasswordFile","approverPasswordFile"})
            Assert.False(transcript.Contains(await File.ReadAllTextAsync(Field(field),token),StringComparison.Ordinal),"CLI exposed a private laboratory password.");
        return result;
    }
    private async Task OpenBrowserAsync(Uri authorization,CancellationToken token)
    {
        var query=QueryHelpers.ParseQuery(authorization.Query);
        Assert.Equal("S256",query["code_challenge_method"].ToString());
        Assert.Equal("code",query["response_type"].ToString());
        var callback=new Uri(query["redirect_uri"].ToString());
        Assert.Equal("127.0.0.1",callback.Host); Assert.Equal("/oauth/callback",callback.AbsolutePath);
        using var ca=X509Certificate2.CreateFromPem(await File.ReadAllTextAsync(Field("caPemFile"),token));
        using var handler=new SocketsHttpHandler {AllowAutoRedirect=false,UseProxy=false,CookieContainer=new CookieContainer(),
            SslOptions=new SslClientAuthenticationOptions {CertificateChainPolicy=new X509ChainPolicy {
                TrustMode=X509ChainTrustMode.CustomRootTrust,RevocationMode=X509RevocationMode.NoCheck,CustomTrustStore={ca}}}};
        using var browser=new HttpClient(handler) {Timeout=TimeSpan.FromSeconds(20)};
        using var login=await browser.GetAsync(authorization,token); Assert.True(login.IsSuccessStatusCode,"Genuine Keycloak authorization failed.");
        var html=await login.Content.ReadAsStringAsync(token); Assert.InRange(html.Length,1,256*1024);
        var match=Regex.Match(html,"<form\\b[^>]*\\baction=\"([^\"]+)\"",RegexOptions.CultureInvariant,TimeSpan.FromSeconds(1));
        Assert.True(match.Success,"Pinned genuine Keycloak login form is missing.");
        var form=new Uri(WebUtility.HtmlDecode(match.Groups[1].Value));
        Assert.Equal(new Uri(Field("issuer")).GetLeftPart(UriPartial.Authority),form.GetLeftPart(UriPartial.Authority));
        using var submitted=await browser.PostAsync(form,new FormUrlEncodedContent(new Dictionary<string,string> {
            ["username"]=Field(Approver?"approverUsername":"humanUsername"),
            ["password"]=await File.ReadAllTextAsync(Field(Approver?"approverPasswordFile":"humanPasswordFile"),token),["credentialId"]=""}),token);
        Assert.Equal(HttpStatusCode.Found,submitted.StatusCode); var returned=submitted.Headers.Location!;
        Assert.Equal(callback.AbsoluteUri,returned.GetLeftPart(UriPartial.Path));
        var fields=QueryHelpers.ParseQuery(returned.Query);
        Assert.True(fields.ContainsKey("code")&&fields["state"].ToString()==query["state"].ToString(),"Real authorization code/state did not match.");
        using var accepted=await browser.GetAsync(returned,token); Assert.True(accepted.IsSuccessStatusCode,"Bound actual CLI callback rejected the real code.");
        CompletedLogins++;
    }
    public async ValueTask DisposeAsync()
    {
        await StopAsync(CancellationToken.None);
        _identity.Dispose();
    }
    internal async Task RestartAsync(CancellationToken token)
    { await StopAsync(token);await StartAsync(token); }
    internal async Task StopAsync(CancellationToken token)
    {
        if(_process is not null)
        {
            if(!_process.HasExited) {_process.Kill(true);await _process.WaitForExitAsync(token);}
            var logs=await PersistPrivateConsoleAsync();
            Assert.DoesNotContain("Bearer eyJ",logs,StringComparison.Ordinal); _process.Dispose();
            _process=null;_stdout=null;_stderr=null;
        }
    }
    private async Task<string> PersistPrivateConsoleAsync()
    {
        var logs=(_stdout is null?"":await _stdout)+(_stderr is null?"":await _stderr);
        // Only the owned ignored lab directory. CI must never upload this raw log.
        await File.WriteAllTextAsync(Path.Combine(_directory,"api-private-console.log"),logs);
        return logs;
    }
}
