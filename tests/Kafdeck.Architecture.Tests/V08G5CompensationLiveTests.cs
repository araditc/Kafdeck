using System.Text.Json;
using Kafdeck.Cli;
using Kafdeck.Core.Notifications;
using Kafdeck.Modules.Administration;
using Xunit;
using Fixture=Kafdeck.Architecture.Tests.V08G3DeclarativeCliLiveTests.Fixture;
using Artifact=Kafdeck.Architecture.Tests.V08G3DeclarativeCliLiveTests.Artifact;

namespace Kafdeck.Architecture.Tests;

public sealed class V08G5CompensationLiveTests
{
    [G3DeclarativeLiveTheory]
    [InlineData(false)] [InlineData(true)]
    public async Task Real_Keycloak_CLI_safe_inverse_is_a_new_independently_approved_fenced_operation(bool postgres)
    {
        using var deadline=new CancellationTokenSource(TimeSpan.FromMinutes(12));var token=deadline.Token;
        if(postgres)Assert.False(string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES")));
        var evidence=Environment.GetEnvironmentVariable("KAFDECK_G2_EVIDENCE_DIRECTORY")??Path.Combine(Path.GetTempPath(),"g5-live-"+Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(evidence);
        await using var f=await Fixture.CreateAsync(evidence,postgres,token);f.EnableService();
        var initial=await f.CommitAsync("g5-safe-inverse","Paused",token);
        f.Lab.Settings[f.RegistrationPrefix(initial.Resource)+"AllowRevisionAdvance"]="true";
        await f.Lab.StartAsync(token);
        Assert.Equal(CliApplication.Success,await f.Lab.RunAsync(token,"declarative","compensation-catalog"));
        var create=await f.PlanAsync(initial,0,token);await f.ApproveAsync(create,token);Assert.Equal(CliApplication.Success,await f.ApplyAsync(create,token));
        var source=await f.SourceAsync(initial,token);var changed=await f.CommitAsync(initial.Resource,"Active",token);
        Assert.Equal(CliApplication.Success,await f.AdvanceAsync(changed,source,token));
        var forward=await f.PlanAsync(changed,1,token);await f.ApproveAsync(forward,token);Assert.Equal(CliApplication.Success,await f.ApplyAsync(forward,token));
        Assert.Equal(2,(await f.Routing.GetSubscriptionAsync(initial.Resource,token))!.Revision);
        Assert.Equal(CliApplication.Success,await f.Lab.RunAsync(token,"declarative","compensation",forward.Id.ToString("D")));
        using(var eligibility=JsonDocument.Parse(f.Lab.Output)) Assert.Equal("RequiresFreshPlan",eligibility.RootElement.GetProperty("eligibility").GetString());
        source=await f.SourceAsync(changed,token);var inverse=await f.CommitAsync(initial.Resource,"Paused",token);
        Assert.Equal(CliApplication.Success,await f.AdvanceAsync(inverse,source,token));
        Assert.Equal(CliApplication.Success,await Preview(inverse,2));var compensation=f.Preview();
        Assert.NotEqual(forward.Id,compensation.Id);
        Assert.Equal(CliApplication.Conflict,await f.ApplyAsync(compensation,token));
        await f.ApproveAsync(compensation,token);
        Assert.Equal(CliApplication.Success,await f.ApplyAsync(compensation,token));
        var current=await f.Routing.GetSubscriptionAsync(initial.Resource,token);
        Assert.Equal(NotificationSubscriptionState.Paused,current!.State);Assert.Equal(3,current.Revision);
        Assert.Equal(forward.Id,(await f.Store.GetPlanAsync(compensation.Id,token))!.CompensatesPlanId);
        var operation=await f.Operations.GetAsync(compensation.Id,token);Assert.NotNull(operation);
        Assert.Equal(MutationOperationState.AppliedVerified,operation.State);
        Assert.NotEqual(operation.RequesterPrincipalId,operation.ApprovedByPrincipalId);
        Assert.Equal(CliApplication.Success,await f.ApplyAsync(compensation,token));
        Assert.Equal(3,(await f.Routing.GetSubscriptionAsync(initial.Resource,token))!.Revision);
        Assert.NotEqual(CliApplication.Success,await Preview(inverse,3));
        Assert.Equal(3,(await f.Routing.GetSubscriptionAsync(initial.Resource,token))!.Revision);
        await File.WriteAllBytesAsync(Path.Combine(evidence,(postgres?"postgres":"sqlite")+"-g5-compensation-evidence.json"),JsonSerializer.SerializeToUtf8Bytes(new
        { actualKeycloakService=true,actualCli=true,distinctHumanApproval=true,newImmutableOperation=true,parentOperationLink=true,
            metadataInverseOnly=true,threeVerifiedSubscriptionRevisions=true,terminalReplayWithoutDuplicateEffect=true,staleOriginalCannotCompensateAgain=true,
            deliveredNotificationsNeverReversed=true,database=postgres?"PostgreSQL":"SQLite" }),token);

        Task<int> Preview(Artifact file,long ownership)=>f.Lab.RunAsync(token,"declarative","compensation-preview",forward.Id.ToString("D"),
            "--manifest-file",file.File,"--source",file.SourceRef,"--source-revision",file.Manifest.Fingerprint(),"--ownership-revision",ownership.ToString(System.Globalization.CultureInfo.InvariantCulture),
            "--idempotency-key","g5-"+Guid.NewGuid().ToString("N"));
    }
}
