using System.Diagnostics;
using System.Net;
using System.Net.Http.Json;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using System.Text.RegularExpressions;
using Kafdeck.Api;
using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.AspNetCore.WebUtilities;
using Xunit;
using TlsProvider = Kafdeck.Architecture.Tests.V08W66HostedWebhookTlsTests.TlsProvider;

namespace Kafdeck.Architecture.Tests;

// The actual API captures/routes the real Kafka mutation, with sender off.
// A second production sender host consumes that same durable queue after the
// API process stops. Only the test dialer maps an approved public pin locally.
internal sealed class G2KafkaProductNotificationBridge : IAsyncDisposable
{
    private readonly TlsProvider _tls = new();
    private readonly string _path;
    private readonly NotificationDestinationProfile _profile;
    private readonly NotificationDestinationProfile _sourceProfile;
    private readonly NotificationGovernanceOptions _governance;
    private readonly INotificationDeliveryDbConnectionFactory _connections;
    private readonly string? _postgresConnection;
    private NotificationDestinationProfile? _selectedProfile;
    private bool _operatorApiConfigured;
    private bool _browserParUsed;
    private readonly AdoNotificationRoutingStore _routing;
    private readonly AdoNotificationDeliveryStore _deliveries;

    private G2KafkaProductNotificationBridge(string directory,string? postgresConnection)
    {
        _path = Path.GetFullPath(Path.Combine(directory, "product-notifications-" + Guid.NewGuid().ToString("N") + ".db"));
        _profile = new("g2-product-effect", NotificationProviderKind.Webhook, "Real product effect fixture",
            [NotificationEventClass.Mutation], new Uri($"https://hooks.example.org:{_tls.Port}/notify"));
        _sourceProfile=new("g2-product-source-v1",_profile.Provider,_profile.DisplayName,_profile.EnabledEvents,_profile.ConfiguredEndpoint);
        _governance=new([new("product-v1",_profile.DestinationId,_sourceProfile,null)]);
        _postgresConnection=postgresConnection;
        _connections = postgresConnection is null?new SqliteNotificationDeliveryDbConnectionFactory(_path):new PostgreSqlNotificationDeliveryDbConnectionFactory(postgresConnection);
        _routing = new(_connections);
        _deliveries = new(_connections);
    }

    internal static async Task<G2KafkaProductNotificationBridge> PrepareAsync(string directory,
        IDictionary<string, string> configuration,string? postgresConnection=null)
    {
        var bridge = new G2KafkaProductNotificationBridge(directory,postgresConnection);
        try
        {
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await bridge._routing.InitializeAsync(deadline.Token);
            await bridge._deliveries.InitializeAsync(deadline.Token);
            using var lab=JsonDocument.Parse(await File.ReadAllTextAsync(Environment.GetEnvironmentVariable("KAFDECK_KEYCLOAK_G2_LAB")!,deadline.Token));
            if(!lab.RootElement.TryGetProperty("browserClientId",out var browserClient) || string.IsNullOrWhiteSpace(browserClient.GetString()))
                throw new InvalidOperationException("The composed notification proof requires a fresh browser-enabled Keycloak laboratory.");
            configuration["Kafdeck:Deployment:Oidc:ClientId"]=browserClient.GetString()!;
            configuration["Kafdeck:Deployment:Oidc:TrustedCaPemFile"]=lab.RootElement.GetProperty("caPemFile").GetString()!;
            // This disposable client admits only openid plus an exact subject
            // mapper. Do not request an unprovisioned optional profile scope.
            configuration["Kafdeck:Deployment:Oidc:Scopes:0"]="openid";
            configuration["Kafdeck:Notifications:Enabled"] = "true";
            configuration["Kafdeck:Notifications:Provider"] = postgresConnection is null?"Sqlite":"PostgreSql";
            configuration["Kafdeck:Notifications:ExecutionMode"] = "Standalone";
            if(postgresConnection is null) configuration["Kafdeck:Notifications:SqliteDatabasePath"] = bridge._path;
            else configuration["Kafdeck:Notifications:ConnectionString"]="env:KAFDECK_G2_CLI_DATABASE_CONNECTION";
            configuration["Kafdeck:Notifications:ManagementEnabled"]="true";
            configuration["Kafdeck:Notifications:ProductEventsEnabled"] = "true";
            configuration["Kafdeck:Notifications:WebhookDeliveryEnabled"] = "false";
            configuration["Kafdeck:Notifications:HttpProviderDeliveryEnabled"] = "false";
            configuration["Kafdeck:Notifications:EmailDeliveryEnabled"] = "false";
            configuration["Kafdeck:Notifications:Destinations:0:DestinationId"] = bridge._sourceProfile.DestinationId;
            configuration["Kafdeck:Notifications:Destinations:0:Provider"] = "Webhook";
            configuration["Kafdeck:Notifications:Destinations:0:DisplayName"] = bridge._profile.DisplayName;
            configuration["Kafdeck:Notifications:Destinations:0:EnabledEvents:0"] = "Mutation";
            configuration["Kafdeck:Notifications:Destinations:0:ConfiguredEndpoint"] = bridge._profile.ConfiguredEndpoint!.AbsoluteUri;
            configuration["Kafdeck:Notifications:Destinations:0:WebhookAuthentication"] = "Anonymous";
            configuration["Kafdeck:Notifications:Governance:Enabled"]="true";
            configuration["Kafdeck:Notifications:Governance:Candidates:0:CandidateId"]="product-v1";
            configuration["Kafdeck:Notifications:Governance:Candidates:0:DestinationId"]=bridge._profile.DestinationId;
            configuration["Kafdeck:Notifications:Governance:Candidates:0:ProfileId"]=bridge._sourceProfile.DestinationId;
            foreach(var (index,action) in new[]{(20,"NotificationManage"),(21,"NotificationRead")})
            {
                configuration[$"Kafdeck:Authorization:Roles:0:Permissions:{index}:Action"]=action;
                configuration[$"Kafdeck:Authorization:Roles:0:Permissions:{index}:ResourcePatterns:0"]=bridge._profile.DestinationId;
                configuration[$"Kafdeck:Authorization:Roles:0:Permissions:{index}:ResourcePatterns:1"]="g2-product-effect-sub";
            }
            return bridge;
        }
        catch { await bridge.DisposeAsync(); throw; }
    }

    internal async Task ConfigureViaOperatorApiAsync(Uri api,JsonElement identity,CancellationToken token)
    {
        string Field(string name)=>identity.GetProperty(name).GetString()!;
        if(!identity.TryGetProperty("browserClientId",out var browserClient))
            throw new InvalidOperationException("The composed notification proof requires a fresh browser-enabled Keycloak laboratory.");
        using var ca=X509Certificate2.CreateFromPem(await File.ReadAllTextAsync(Field("caPemFile"),token));
        using var handler=new SocketsHttpHandler {AllowAutoRedirect=false,UseProxy=false,UseCookies=true,CookieContainer=new CookieContainer(),
            SslOptions=new SslClientAuthenticationOptions {CertificateChainPolicy=new X509ChainPolicy {
                TrustMode=X509ChainTrustMode.CustomRootTrust,RevocationMode=X509RevocationMode.NoCheck,CustomTrustStore={ca}}}};
        using var client=new HttpClient(handler) {BaseAddress=api,Timeout=TimeSpan.FromSeconds(15)};
        using var challenge=await client.GetAsync("/api/v1/auth/login?returnUrl=%2F",token);
        Assert.Equal(HttpStatusCode.Found,challenge.StatusCode);
        var authorization=challenge.Headers.Location!;
        Assert.True(authorization.IsAbsoluteUri&&authorization.GetLeftPart(UriPartial.Authority)==new Uri(Field("issuer")).GetLeftPart(UriPartial.Authority),"Browser challenge must use the admitted Keycloak origin.");
        var query=QueryHelpers.ParseQuery(authorization.Query);
        Assert.Equal(browserClient.GetString(),query["client_id"].ToString());
        var callback=new Uri(api,"/signin-oidc");
        _browserParUsed=query.TryGetValue("request_uri",out var pushed);
        if(_browserParUsed)
        {
            var reference=pushed.ToString();Assert.InRange(reference.Length,1,2048);
            Assert.StartsWith("urn:ietf:params:oauth:request_uri:",reference);
            // Production PAR carries redirect/state/PKCE in the authenticated
            // backchannel request, not in this browser redirect query.
        }
        else
        {
            Assert.Equal("S256",query["code_challenge_method"].ToString());
            Assert.Equal(callback.AbsoluteUri,query["redirect_uri"].ToString());
        }
        var html=await ReadKeycloakLoginFormAsync(client,authorization,new Uri(Field("issuer")),callback,token);
        var match=Regex.Match(html,"<form\\b[^>]*\\baction=\"([^\"]+)\"",RegexOptions.CultureInvariant,TimeSpan.FromSeconds(1));
        Assert.True(match.Success,"Pinned Keycloak login form is required.");
        var form=new Uri(WebUtility.HtmlDecode(match.Groups[1].Value));
        Assert.Equal(new Uri(Field("issuer")).GetLeftPart(UriPartial.Authority),form.GetLeftPart(UriPartial.Authority));
        Assert.InRange(new FileInfo(Field("humanPasswordFile")).Length,1,4096);
        var password=await File.ReadAllTextAsync(Field("humanPasswordFile"),token);
        using var submitted=await client.PostAsync(form,new FormUrlEncodedContent(new Dictionary<string,string>
            { ["username"]=Field("humanUsername"),["password"]=password,["credentialId"]="" }),token);
        Assert.Equal(HttpStatusCode.Found,submitted.StatusCode);
        var location=submitted.Headers.Location!;
        Assert.True(location.GetLeftPart(UriPartial.Path)==callback.AbsoluteUri,"The genuine code must return to this API's exact callback.");
        var returned=QueryHelpers.ParseQuery(location.Query);
        Assert.True(returned.ContainsKey("code")&&returned.TryGetValue("state",out var returnedState)&&!string.IsNullOrWhiteSpace(returnedState),
            "The genuine code must return with bound state for production middleware validation.");
        if(!_browserParUsed) Assert.Equal(query["state"].ToString(),returned["state"].ToString());
        var badState=QueryHelpers.AddQueryString(callback.AbsoluteUri,new Dictionary<string,string?> {
            ["code"]=returned["code"].ToString(),["state"]="invalid-protected-state"});
        using(var denied=await client.GetAsync(badState,token))
            Assert.True((int)denied.StatusCode is 400 or 401 or 500,"A real code with invalid protected state must fail before authentication.");
        using(var stillAnonymous=await client.GetAsync("/api/v1/notifications/management/destinations",token))
            Assert.Equal(HttpStatusCode.Unauthorized,stillAnonymous.StatusCode);
        using(var noCookies=new HttpClient(new SocketsHttpHandler {UseCookies=false,UseProxy=false,AllowAutoRedirect=false}) {BaseAddress=api})
        {
            using(var denied=await noCookies.GetAsync(location,token))
                Assert.True((int)denied.StatusCode is 400 or 401 or 500,"A real protected state without its bound correlation cookie must fail.");
            using var anonymous=await noCookies.GetAsync("/api/v1/notifications/management/destinations",token);
            Assert.Equal(HttpStatusCode.Unauthorized,anonymous.StatusCode);
        }
        using var accepted=await client.GetAsync(location,token);Assert.Equal(HttpStatusCode.Found,accepted.StatusCode);
        using var session=await client.GetAsync("/api/v1/auth/session",token);Assert.Equal(HttpStatusCode.OK,session.StatusCode);
        const string target="/api/v1/notifications/management/destinations/g2-product-effect";
        using(var noCsrf=await client.PutAsJsonAsync(target,new NotificationManagedDestinationChangeRequest("product-v1",0,true,true),token))
            Assert.Equal(HttpStatusCode.Forbidden,noCsrf.StatusCode);
        using(var csrf=JsonDocument.Parse(await client.GetStringAsync("/api/v1/auth/csrf",token)))
            client.DefaultRequestHeaders.Add(csrf.RootElement.GetProperty("headerName").GetString()!,csrf.RootElement.GetProperty("requestToken").GetString());
        using(var selected=await client.PutAsJsonAsync(target,new NotificationManagedDestinationChangeRequest("product-v1",0,true,true),token))
            Assert.Equal(HttpStatusCode.OK,selected.StatusCode);
        using(var conflict=await client.PutAsJsonAsync(target,new NotificationManagedDestinationChangeRequest("product-v1",0,true,true),token))
            Assert.Equal(HttpStatusCode.Conflict,conflict.StatusCode);
        using(var subscription=await client.PutAsJsonAsync("/api/v1/notifications/subscriptions/g2-product-effect-sub",new {
            destinationId=_profile.DestinationId,state="active",eventClasses=new[]{"mutation"},eventTypes=new[]{"mutation.state-changed"},expectedRevision=(long?)null},token))
            Assert.Equal(HttpStatusCode.Created,subscription.StatusCode);
        using(var visible=await client.GetAsync("/api/v1/notifications/subscriptions/g2-product-effect-sub",token))
            Assert.Equal(HttpStatusCode.OK,visible.StatusCode);
        using(var denied=await client.PutAsJsonAsync("/api/v1/notifications/management/destinations/ungranted",new NotificationManagedDestinationChangeRequest("product-v1",0,true,true),token))
            Assert.Equal(HttpStatusCode.Forbidden,denied.StatusCode);
        var governanceStore=new AdoNotificationGovernanceStore(_connections);
        var state=await governanceStore.GetAsync(_profile.DestinationId,token);Assert.NotNull(state);Assert.Equal(1,state.Revision);
        var audit=await governanceStore.ReadAuditAsync(_profile.DestinationId,0,50,token);
        var entry=Assert.Single(audit);Assert.Equal("oidc:"+Field("issuer")+"|"+Field("humanSubject"),entry.Actor);
        var catalog=new GovernedNotificationCatalog(_governance,governanceStore,new SecretResolver());
        _selectedProfile=await catalog.GetAsync(_profile.DestinationId,token);Assert.NotNull(_selectedProfile);Assert.True(_selectedProfile.Enabled);
        var saved=await _routing.GetSubscriptionAsync("g2-product-effect-sub",token);Assert.NotNull(saved);Assert.Equal(1,saved.Revision);
        _operatorApiConfigured=true;
    }

    private static async Task<string> ReadKeycloakLoginFormAsync(HttpClient client,Uri initial,Uri issuer,Uri callback,CancellationToken token)
    {
        var next=initial;
        for(var redirects=0;redirects<=5;redirects++)
        {
            using var response=await client.GetAsync(next,token);
            if(response.IsSuccessStatusCode)
            {
                var html=await response.Content.ReadAsStringAsync(token);Assert.InRange(html.Length,1,256*1024);return html;
            }
            var location=response.Headers.Location is { } supplied?new Uri(next,supplied):null;
            var error=location is not null&&QueryHelpers.ParseQuery(location.Query).TryGetValue("error",out var value)
                ? value.ToString() switch {"invalid_scope"=>"invalid_scope","invalid_request"=>"invalid_request","access_denied"=>"access_denied",_=>"unclassified"}:"none";
            Assert.True(response.StatusCode is HttpStatusCode.Found or HttpStatusCode.SeeOther or HttpStatusCode.TemporaryRedirect or HttpStatusCode.PermanentRedirect or HttpStatusCode.MovedPermanently,
                $"Real browser authorization did not return its login form. HTTP={(int)response.StatusCode}; OAuthError={error}.");
            Assert.True(location is not null&&location.GetLeftPart(UriPartial.Authority)==issuer.GetLeftPart(UriPartial.Authority)&&
                location.AbsolutePath.StartsWith(issuer.AbsolutePath+"/",StringComparison.Ordinal)&&location.GetLeftPart(UriPartial.Path)!=callback.AbsoluteUri,
                $"Keycloak login redirect must remain inside its exact admitted realm before form submission. OAuthError={error}.");
            next=location!;
        }
        throw new InvalidOperationException("Keycloak login exceeded the five-redirect laboratory bound.");
    }

    internal async Task DeliverAfterApiRestartAsync(Process api, string mutationPath,
        MutationOperationSnapshot mutation, string rawTopic, string evidenceDirectory, CancellationToken cancellationToken)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(TimeSpan.FromSeconds(30));
        cancellationToken = deadline.Token;
        Assert.Equal(MutationOperationState.AppliedVerified, mutation.State);
        Assert.True(_operatorApiConfigured,"Real browser cookie and CSRF management must precede the Kafka effect.");
        var activeProfile=_selectedProfile!;
        var source = new MutationNotificationOutbox(_postgresConnection is null?new SqliteMutationDbConnectionFactory(mutationPath):new PostgreSqlMutationDbConnectionFactory(_postgresConnection));
        NotificationDeliveryRecord pending;
        NotificationProductEventSourceStatus sourceStatus;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var queued = await _deliveries.ListDueAsync(new(DateTimeOffset.UtcNow, maxResults: 2), cancellationToken);
            sourceStatus = await source.GetStatusAsync(cancellationToken);
            if (queued.Items.Count == 1 && sourceStatus.Pending == 0 && sourceStatus.Routed == 1)
            {
                Assert.False(queued.Truncated);
                pending = Assert.Single(queued.Items);
                break;
            }
            await Task.Delay(25, cancellationToken);
        }
        Assert.Equal(NotificationDeliveryState.Pending, pending.Snapshot.State);
        Assert.Equal(0, pending.Snapshot.AttemptCount);
        Assert.Equal(0, _tls.RequestCount);
        Assert.Equal(0, sourceStatus.Expired);
        Assert.Equal(0, sourceStatus.Overflowed);
        var routed = await _routing.GetEventAsync(pending.Snapshot.NotificationId, cancellationToken);
        Assert.NotNull(routed);
        Assert.Equal(NotificationEventClass.Mutation, routed.Event.EventClass);
        Assert.Equal("mutation.state-changed", routed.Event.EventType);
        Assert.Equal(mutation.UpdatedAtUtc, routed.Event.OccurredAtUtc);

        api.Kill(entireProcessTree: true);
        await api.WaitForExitAsync(cancellationToken);
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton<SecretResolver>();
        builder.Services.AddSingleton<INotificationDeliveryDbConnectionFactory>(_connections);
        builder.Services.AddSingleton<INotificationHostAddressLookup>(_tls);
        builder.Services.AddSingleton<INotificationPinnedTcpDialer>(_tls);
        builder.Services.AddSingleton(sp=>new NotificationPinnedHttpHandlerFactory(_tls, NotificationAddressPolicy.NoConfiguredNat64,
            trustRoots: new X509Certificate2Collection(_tls.Root),revisionGate:sp.GetRequiredService<INotificationPhysicalRevisionGate>()));
        builder.Services.AddSingleton<INotificationRoutingStore>(_routing);
        builder.Services.AddSingleton<INotificationDeliveryStore>(_deliveries);
        builder.Services.AddKafdeckWebhookDelivery(new(true,_postgresConnection is null?NotificationPersistenceProvider.Sqlite:NotificationPersistenceProvider.PostgreSql,
            NotificationExecutionMode.Standalone,_postgresConnection is null?_path:null,
            _postgresConnection is null?null:SecretReference.Parse("env:KAFDECK_G2_CLI_DATABASE_CONNECTION"),DestinationProfiles: [_sourceProfile], WebhookDeliveryEnabled: true));
        builder.Services.AddKafdeckNotificationGovernance(_governance);
        using var sender = builder.Build();
        Assert.Equal(activeProfile.RevisionFingerprint,(await sender.Services.GetRequiredService<INotificationDestinationProfileCatalog>()
            .GetAsync(_profile.DestinationId,cancellationToken))!.RevisionFingerprint);
        await sender.StartAsync(cancellationToken);
        var request = await _tls.Received.Task.WaitAsync(cancellationToken);
        NotificationDeliveryRecord? receipt;
        while ((receipt = await _deliveries.GetAsync(pending.Snapshot.NotificationId, _profile.DestinationId,
                   cancellationToken))?.Snapshot.State != NotificationDeliveryState.Delivered)
            await Task.Delay(25, cancellationToken);
        await sender.Services.GetRequiredService<NotificationDeliveryWorker>().RunDueCycleAsync(cancellationToken);
        await sender.StopAsync(cancellationToken);
        Assert.Equal(1, _tls.RequestCount);
        Assert.Equal(1, receipt.Snapshot.AttemptCount);
        Assert.DoesNotContain(rawTopic, request, StringComparison.Ordinal);
        Assert.DoesNotContain(mutation.OperationId.ToString("D"), request, StringComparison.Ordinal);
        Assert.DoesNotContain(mutation.RequesterPrincipalId, request, StringComparison.Ordinal);
        var reopened = new AdoNotificationDeliveryStore(_connections);
        var durable = await reopened.GetAsync(pending.Snapshot.NotificationId, _profile.DestinationId, cancellationToken);
        Assert.NotNull(durable);
        Assert.Equal(NotificationDeliveryState.Delivered, durable.Snapshot.State);
        Assert.Equal(1, durable.Snapshot.AttemptCount);
        Assert.Equal(1, (await source.GetStatusAsync(cancellationToken)).Routed);
        await File.WriteAllTextAsync(Path.Combine(evidenceDirectory, _postgresConnection is null?"human-kafka-product-notification-evidence.json":"postgres-human-kafka-product-notification-evidence.json"),
            JsonSerializer.Serialize(new
            {
                observedAtUtc = DateTimeOffset.UtcNow,
                flow = "genuine browser Keycloak code/cookie -> actual CSRF profile selection and subscription API -> genuine human CLI PKCE -> verified Kafka TopicCreate -> atomic product outbox -> product pump -> governed subscription -> durable Pending -> API process stop -> second production sender -> physical TLS204 -> reopened Delivered",
                mutationState = mutation.State.ToString(), productSourceEnabledInActualApi = true,
                sourceRouted = sourceStatus.Routed, sourcePending = sourceStatus.Pending,
                queuedBeforeRestart = 1, attemptsBeforeRestart = 0, physicalPostsBeforeRestart = 0,
                physicalProviderPosts = _tls.RequestCount, physicalHttpStatus = 204,
                durableState = durable.Snapshot.State.ToString(), durableAttemptCount = durable.Snapshot.AttemptCount,
                rawTopicOperationAndPrincipalLeaks = 0, persistence = _postgresConnection is null?"SQLite":"PostgreSQL",
                realBrowserCookieSession=true,realCsrfProfileAndSubscriptionWrites=true,missingCsrfDenied=true,scopedManagementDenied=true,
                staleSelectionCasDenied=true,durableExactGovernanceActor=true,governedGenerationPreservedAcrossHostHandoff=true,
                browserAuthorizationProtocol=_browserParUsed?"PAR":"authorization-code-S256",productionBrowserUsePkce=true,
                inspectedBrowserRedirectS256=!_browserParUsed,callbackStateValidatedByProductionMiddleware=true,
                genuineCodeWrongStateDenied=true,genuineCodeMissingCorrelationDenied=true,
                caveat = "One low-risk TopicCreate, HTTP browser-form driver and local TLS fixture; no external SaaS, product-source HA, other event family, database restore or broker failover acceptance."
            }, new JsonSerializerOptions { WriteIndented = true }), cancellationToken);
    }

    public ValueTask DisposeAsync() => _tls.DisposeAsync();
}
