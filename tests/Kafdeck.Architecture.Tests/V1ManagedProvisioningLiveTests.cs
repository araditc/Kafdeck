using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Nodes;
using Kafdeck.Api;
using Kafdeck.Core.Security;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class ManagedProvisioningLiveTheoryAttribute : TheoryAttribute
{
    public ManagedProvisioningLiveTheoryAttribute()
    {
        if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable("KAFDECK_MANAGED_PROVISIONING_LAB")))
            Skip = "Requires the fresh owned HTTPS Keycloak managed-provisioning fixture.";
    }
}

public sealed class V1ManagedProvisioningLiveTests
{
    [ManagedProvisioningLiveTheory]
    [InlineData("lost-reply")]
    [InlineData("local-write")]
    [InlineData("foreign-account")]
    public async Task Actual_Keycloak_creation_and_recovery_have_one_positive_bound_effect(string scenario)
    {
        var configPath = Environment.GetEnvironmentVariable("KAFDECK_MANAGED_PROVISIONING_LAB")!;
        using var config = JsonDocument.Parse(await File.ReadAllTextAsync(configPath));
        string Setting(string key) => config.RootElement.GetProperty(key).GetString()!;
        var issuer = Setting("issuer"); var users = issuer[..issuer.LastIndexOf("/realms/", StringComparison.Ordinal)] +
            "/admin" + issuer[issuer.LastIndexOf("/realms/", StringComparison.Ordinal)..] + "/users";
        using var control = new HttpClient(OidcTrustedCaHandler.Create(Setting("caFile"))) { Timeout = TimeSpan.FromSeconds(15) };
        using var token = await control.PostAsync(issuer + "/protocol/openid-connect/token", new FormUrlEncodedContent(new Dictionary<string,string>
        { ["grant_type"]="client_credentials",["client_id"]=Setting("controlClientId"),["client_secret"]=Setting("controlSecret") }));
        token.EnsureSuccessStatusCode();
        using var tokenBody = JsonDocument.Parse(await token.Content.ReadAsStringAsync());
        control.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", tokenBody.RootElement.GetProperty("access_token").GetString());
        var profile = JsonNode.Parse(await control.GetStringAsync(users + "/profile"))!.AsObject();
        var attributes = profile["attributes"]!.AsArray();
        if (!attributes.Any(attribute => attribute?["name"]?.GetValue<string>() == ManagedKeycloakUsers.ProvisioningAttribute))
        {
            attributes.Add(JsonSerializer.SerializeToNode(new { name=ManagedKeycloakUsers.ProvisioningAttribute, multivalued=false,
                permissions=new { view=new[]{"admin"}, edit=new[]{"admin"} } }));
            using var update = await control.PutAsJsonAsync(users + "/profile", profile); update.EnsureSuccessStatusCode();
        }
        // Only the laboratory control identity can configure the realm. The
        // production adapter below has manage-users, never manage-realm.
        var directory = Path.Combine(Path.GetTempPath(), "kafdeck-g01-live-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory); var statePath = Path.Combine(directory,"identity.json");
        var store = new ManagedIdentityStore(statePath, issuer); var ownerId = Guid.NewGuid();
        store.Write(new(issuer,ownerId,false,null,default,[new(ownerId,"owner@example.test","owner",[],true,default)]));
        var owner = new OperatorSessionContext(new(new(issuer,ownerId.ToString("D"))),new SessionId(Guid.NewGuid()),DateTimeOffset.UtcNow);
        var email = "g01-" + Guid.NewGuid().ToString("N") + "@example.test";
        var input = new ManagedIdentityEndpoints.NewUserInput(email,"Temporary-password-123!","viewer",[],Guid.NewGuid());
        var handler = new CommittedReplyFaultHandler(OidcTrustedCaHandler.Create(Setting("caFile")), users, () =>
        {
            if (scenario=="local-write") { File.Move(statePath,statePath+".saved"); Directory.CreateDirectory(statePath); }
        }, scenario=="lost-reply");
        using var client = new ManagedKeycloakUsers(new HttpClient(handler) { Timeout=TimeSpan.FromSeconds(15) },issuer,Setting("clientId"),Setting("secret"));
        var audit = new ManagedAuditCapture();
        try
        {
            if (scenario=="foreign-account")
            {
                using var foreign = await control.PostAsJsonAsync(users,new { username=email,email,enabled=true,
                    attributes=new Dictionary<string,string[]> { [ManagedKeycloakUsers.ProvisioningAttribute]=["unrelated-request"] } });
                foreign.EnsureSuccessStatusCode();
            }
            var result = await new ManagedUserProvisioningService(store,client,audit,[]).Create(owner,input);
            if (scenario=="foreign-account")
            {
                Assert.Equal("identity-duplicate",result.Code); Assert.Single(store.Read().Users); Assert.Empty(audit.Events);
            }
            else
            {
                Assert.Equal("identity-provisioning-pending",result.Code);
                if (scenario=="local-write") { Directory.Delete(statePath); File.Move(statePath+".saved",statePath); }
                var recovered = await new ManagedUserProvisioningService(new(statePath,issuer),client,audit,[]).Recover(owner,result.OperationId!.Value);
                Assert.Equal(201,recovered.Status); Assert.Equal(2,store.Read().Users.Length); Assert.Single(audit.Events);
            }
            Assert.Equal(1,handler.CreateCount);
        }
        finally
        {
            using var found = JsonDocument.Parse(await control.GetStringAsync(users+"?username="+Uri.EscapeDataString(email)+"&exact=true&max=2"));
            foreach(var user in found.RootElement.EnumerateArray())
            { using var removed=await control.DeleteAsync(users+"/"+user.GetProperty("id").GetString()); removed.EnsureSuccessStatusCode(); }
            if (Directory.Exists(statePath)) { Directory.Delete(statePath); File.Move(statePath+".saved",statePath); }
            if (!Path.GetFullPath(directory).StartsWith(Path.GetFullPath(Path.GetTempPath()),StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException("Unexpected fixture path.");
            Directory.Delete(directory,recursive:true);
        }
    }

    private sealed class CommittedReplyFaultHandler(HttpMessageHandler inner,string users,Action committed,bool loseReply) : DelegatingHandler(inner)
    {
        internal int CreateCount;
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,CancellationToken token)
        {
            var creating=request.Method==HttpMethod.Post && request.RequestUri!.AbsoluteUri==users;
            if(creating)CreateCount++;
            var response=await base.SendAsync(request,token);
            if(creating && response.StatusCode==HttpStatusCode.Created)
            { committed(); if(loseReply){response.Dispose();throw new HttpRequestException("Fixture lost a committed reply.");} }
            return response;
        }
    }
}
