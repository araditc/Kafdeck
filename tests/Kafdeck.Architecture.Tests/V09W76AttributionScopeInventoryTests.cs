using System.Data.Common;
using System.Globalization;
using System.Text.Json;
using Kafdeck.Core.Attribution;
using Kafdeck.Infrastructure.Persistence;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Kafdeck.Architecture.Tests;

// A bounded bulk SQL fixture supplies historical rows without 50,000 HTTP/mutation calls.
// It is visibility/capacity evidence, not acquisition or actual metered-traffic evidence.
public sealed class V09W76AttributionScopeInventoryTests
{
    [Fact]
    public async Task SQLite_hidden_history_cannot_consume_page_budget_or_create_a_cursor_and_scope_capacity_is_enforced()
    {
        var path=Path.Combine(Path.GetTempPath(),"w76-scope-inventory-"+Guid.NewGuid().ToString("N")+".db");
        try{await Scenario(new SqliteMutationDbConnectionFactory(path));}
        finally{SqliteConnection.ClearAllPools();File.Delete(path);File.Delete(path+"-wal");File.Delete(path+"-shm");}
    }
    [G2PostgreSqlFact]
    public async Task PostgreSQL_hidden_history_cannot_consume_page_budget_or_create_a_cursor_and_scope_capacity_is_enforced()
    {await using var database=await G2CliPostgreSqlDatabase.CreateAsync();await Scenario(new PostgreSqlMutationDbConnectionFactory(database.ConnectionString));}
    private static async Task Scenario(IMutationDbConnectionFactory factory)
    {
        using var timeout=new CancellationTokenSource(TimeSpan.FromMinutes(2));var token=timeout.Token;
        var store=new AdoAttributionStore(factory);await store.InitializeAsync(token);var now=DateTimeOffset.UtcNow;
        var allowedTeam=Guid.NewGuid().ToString("N");var hiddenTeam=Guid.NewGuid().ToString("N");
        var first=V09W76AttributionTests.Window("1") with{WindowId=new string('b',64),WindowStartUtc=now.AddMinutes(-2),WindowEndUtc=now.AddMinutes(-1),CollectedAtUtc=now,Ownership=new(1,allowedTeam,null,new string('a',64),"Observed")};
        var second=first with{WindowId=new string('d',64),WindowStartUtc=now.AddMinutes(-1),WindowEndUtc=now};
        var start=now.AddDays(-30);var hidden=first with{WindowId=new string('a',64),SourceId="fixture-seed",WindowStartUtc=start.AddMinutes(-1),WindowEndUtc=start,Ownership=new(2,hiddenTeam,null,new string('a',64),"Observed")};
        var hiddenTail=hidden with{WindowId=new string('f',64),WindowStartUtc=now.AddMinutes(-2),WindowEndUtc=now.AddMinutes(-1)};
        await store.AppendAsync([first,second,hidden,hiddenTail],token);
        await BulkHiddenHistory(factory,hidden,start,token);
        var query=new MeasurementQuery("permitted",start.AddMinutes(-1),now,null,1);
        var seenScopes=0;
        bool Allowed(MeasurementAccessScope scope){seenScopes++;return scope.ResourceId=="orders"&&scope.TeamId==allowedTeam;}
        var page=await store.QueryAuthorizedAsync(query,Allowed,token);
        Assert.Equal(2,seenScopes);Assert.Equal(new[]{first.WindowId,second.WindowId},page.Select(w=>w.WindowId));
        var continuation=await store.QueryAuthorizedAsync(query with{AfterId=page[0].WindowId},Allowed,token);
        Assert.Equal(second.WindowId,Assert.Single(continuation).WindowId);
        Assert.Empty(await store.QueryAuthorizedAsync(query with{AfterId=second.WindowId},Allowed,token));
        Assert.Empty(await store.QueryAuthorizedAsync(query,_=>false,token));
        Assert.Empty(await store.QueryAuthorizedAsync(query with{ResourceId="invisible-resource"},_=>true,token));
        // Exhaust only the bounded metadata inventory, not the measurement-row budget.
        await FillInventory(factory,AttributionContract.MaximumVisibilityScopesPerCluster-2,token);
        var blocked=first with{WindowId=new string('1',64),ResourceId="new-scope",Ownership=new(1,null,null,null,"Missing")};
        Assert.Equal("AttributionScopeCapacityExceeded",(await Assert.ThrowsAsync<AttributionConflictException>(()=>store.AppendAsync([blocked],token))).Code);
        Assert.Empty(await store.QueryAuthorizedAsync(query with{ResourceId="new-scope"},_=>true,token));
        var existing=first with{WindowId=new string('e',64),WindowStartUtc=now.AddMinutes(-5),WindowEndUtc=now.AddMinutes(-4)};
        await store.AppendAsync([existing],token);
        Assert.Equal(0,await store.DeleteExpiredMeasurementsAsync(90,now,token));
        await store.AppendAsync([blocked],token); // Expired/unreferenced inventory slots may be reclaimed without evicting live evidence.
    }
    private static async Task BulkHiddenHistory(IMutationDbConnectionFactory factory,MeasurementWindow template,DateTimeOffset start,CancellationToken token)
    {
        var scope=AttributionContract.AccessScope(template);await using var db=await factory.OpenAsync(token);await using var cmd=db.CreateCommand();cmd.CommandTimeout=60;
        if(factory.SupportsSelectForUpdate)
            cmd.CommandText="""
              WITH numbered AS(SELECT generate_series(1,50001) AS n), numbered_windows AS(
                SELECT lpad(to_hex(n),64,'0') AS id,
                to_char((CAST(@base AS timestamptz)+((n-1)/2)*interval '1 minute') AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US')||'0+00:00' AS first_at,
                to_char((CAST(@base AS timestamptz)+(((n-1)/2)+1)*interval '1 minute') AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US')||'0+00:00' AS last_at,
                CASE WHEN n%2=0 THEN 'fixture-even' ELSE 'fixture-odd' END AS source FROM numbered)
              INSERT INTO attribution_measurements(window_id,cluster_id,resource_id,starts_utc,ends_utc,source_id,content_hash,body,scope_id)
              SELECT id,@cluster,@resource,first_at,last_at,source,@hash,
                jsonb_set(jsonb_set(jsonb_set(jsonb_set(CAST(@body AS jsonb),'{windowId}',to_jsonb(id)),'{windowStartUtc}',to_jsonb(first_at)),'{windowEndUtc}',to_jsonb(last_at)),'{sourceId}',to_jsonb(source))::text,@scope FROM numbered_windows
              """;
        else
            cmd.CommandText="""
              WITH RECURSIVE numbered(n) AS(SELECT 1 UNION ALL SELECT n+1 FROM numbered WHERE n<50001), numbered_windows AS(
                SELECT printf('%064x',n) AS id,
                strftime('%Y-%m-%dT%H:%M:%f',@base,'+'||CAST((n-1)/2 AS TEXT)||' minutes')||'0000+00:00' AS first_at,
                strftime('%Y-%m-%dT%H:%M:%f',@base,'+'||CAST(((n-1)/2)+1 AS TEXT)||' minutes')||'0000+00:00' AS last_at,
                CASE WHEN n%2=0 THEN 'fixture-even' ELSE 'fixture-odd' END AS source FROM numbered)
              INSERT INTO attribution_measurements(window_id,cluster_id,resource_id,starts_utc,ends_utc,source_id,content_hash,body,scope_id)
              SELECT id,@cluster,@resource,first_at,last_at,source,@hash,
                json_set(@body,'$.windowId',id,'$.windowStartUtc',first_at,'$.windowEndUtc',last_at,'$.sourceId',source),@scope FROM numbered_windows
              """;
        Add(cmd,"@base",start.ToUniversalTime().ToString("O",CultureInfo.InvariantCulture));Add(cmd,"@cluster",template.ClusterId);Add(cmd,"@resource",template.ResourceId);
        Add(cmd,"@hash",new string('a',64));Add(cmd,"@body",JsonSerializer.Serialize(template,AttributionContract.Json));Add(cmd,"@scope",scope.ScopeId);
        Assert.Equal(50001,await cmd.ExecuteNonQueryAsync(token));
    }
    private static async Task FillInventory(IMutationDbConnectionFactory factory,int count,CancellationToken token)
    {
        await using var db=await factory.OpenAsync(token);await using var tx=await db.BeginTransactionAsync(token);
        for(var start=0;start<count;start+=500)
        {
            await using var cmd=db.CreateCommand();cmd.Transaction=tx;var rows=new List<string>();Add(cmd,"@cluster","permitted");
            for(var index=start;index<Math.Min(start+500,count);index++)
            {
                var resource="capacity-fixture-"+index.ToString(CultureInfo.InvariantCulture);var id=AttributionContract.Hash("permitted\n"+resource+"\n\n");
                var name="@id"+index;var value="@resource"+index;rows.Add("(@cluster,"+name+","+value+",NULL,NULL)");Add(cmd,name,id);Add(cmd,value,resource);
            }
            cmd.CommandText="INSERT INTO attribution_visibility_scopes(cluster_id,scope_id,resource_id,team_id,application_id) VALUES "+string.Join(',',rows);await cmd.ExecuteNonQueryAsync(token);
        }
        await tx.CommitAsync(token);
    }
    private static void Add(DbCommand command,string name,object value){var p=command.CreateParameter();p.ParameterName=name;p.Value=value;command.Parameters.Add(p);}
}
