using System.Globalization;
using System.Text.Json;
using Microsoft.Data.Sqlite;

namespace Kafdeck.Api;

internal sealed record MonitorRule(string Name, string Metric, double Threshold, bool Below, int Seconds, string Severity);

internal sealed class MonitoringStore
{
    private readonly MonitoringSettings _settings;
    // Internal samples are admitted only in process. An empty credential cannot match a hashed HTTP credential.
    private readonly BrokerSource[] _metadataSources;
    private readonly string _connection;
    private readonly MonitorRule[] _rules;
    private readonly ILogger<MonitoringStore> _logger;
    private readonly TimeProvider _clock;
    private long _lastCleanup;
    private readonly object _writes = new();
    private readonly Dictionary<(string,string),(Guid Cycle,int Next,long Started)> _cycles = [];
    private const string Heartbeat = "kafdeck:collector/available";
    public MonitoringStore(MonitoringSettings settings, IConfiguration configuration, ILogger<MonitoringStore> logger, TimeProvider clock)
    {
        _settings = settings;
        _metadataSources = settings.MetadataClusters.Select(cluster=>new BrokerSource(cluster,MonitoringSettings.MetadataInstance,[])).ToArray();
        _logger = logger;
        _clock = clock;
        _connection = new SqliteConnectionStringBuilder { DataSource=settings.Database, DefaultTimeout=3 }.ToString();
        var section = configuration.GetSection("Kafdeck:Monitoring");
        _rules = [
            new("KafkaBrokerCpuHigh","kafdeck.jvm/cpu_percent",85,false,300,"warning"),
            new("KafkaBrokerGcHigh","kafdeck.jvm/gc_time_percent",10,false,120,"warning"),
            new("KafkaRequestQueueSlow","kafka.network:RequestMetrics/RequestQueueTimeMs/99thPercentile",100,false,120,"warning"),
            new("KafkaMetadataOfflinePartitions", "kafka.metadata/offline_partitions", 0,false,0,"critical"),
            new("KafkaMetadataUnderReplicated", "kafka.metadata/under_replicated_partitions", 0,false,120,"warning"),
            new("KafkaOfflinePartitions", "kafka.controller:KafkaController/OfflinePartitionsCount/Value", 0,false,0,"critical"),
            new("KafkaUnderReplicated", "kafka.server:ReplicaManager/UnderReplicatedPartitions/Value", 0,false,120,"warning"),
            new("KafkaHandlerBusy", "kafka.server:KafkaRequestHandlerPool/RequestHandlerAvgIdlePercent/OneMinuteRate", 0.15,true,300,"warning"),
            new("KafkaBrokerHandlerBusy", "kafka.server:KafkaRequestHandlerPool/BrokerRequestHandlerAvgIdlePercent/OneMinuteRate", 0.15,true,300,"warning"),
            new("KafkaRequestSlow", "kafka.network:RequestMetrics/TotalTimeMs/99thPercentile", 250,false,300,"warning"),
            .. section.GetSection("Rules").GetChildren().Select(row => new MonitorRule(
                row["Name"] ?? "", row["Metric"] ?? "", row.GetValue<double>("Threshold"),
                row["Comparison"] switch { "below" => true, "above" => false, _ => throw new InvalidOperationException("Invalid monitoring rule comparison.") },
                row.GetValue<int>("ForSeconds"), row["Severity"] ?? "warning"))
        ];
        if (_rules.Length > 64 || _rules.Any(r => !double.IsFinite(r.Threshold) || r.Name.Length is 0 or > 100 ||
            r.Name == "KafkaTargetDown" || r.Metric.Length is 0 or > 512 || r.Seconds is < 0 or > 86400 ||
            r.Severity is not ("warning" or "critical")) || _rules.DistinctBy(r=>r.Name).Count()!=_rules.Length)
            throw new InvalidOperationException("Invalid monitoring rule configuration.");
        if (settings.Database.Length == 0) return;
        using var db = Open();
        using var init = db.CreateCommand();
        init.CommandText = """
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS monitor_enrollment(cluster TEXT, instance TEXT, hash BLOB UNIQUE, PRIMARY KEY(cluster,instance));
            CREATE TABLE IF NOT EXISTS monitor_source(cluster TEXT, instance TEXT, since INTEGER, seen INTEGER, partial INTEGER, PRIMARY KEY(cluster,instance));
            CREATE TABLE IF NOT EXISTS monitor_series(id INTEGER PRIMARY KEY, cluster TEXT, instance TEXT, metric TEXT, labels TEXT, UNIQUE(cluster,instance,metric,labels));
            CREATE INDEX IF NOT EXISTS monitor_names ON monitor_series(cluster,metric);
            CREATE TABLE IF NOT EXISTS monitor_sample(series INTEGER, time INTEGER, value REAL, PRIMARY KEY(series,time)) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS monitor_expiry ON monitor_sample(time);
            CREATE TABLE IF NOT EXISTS monitor_alert(cluster TEXT, instance TEXT, name TEXT, metric TEXT, labels TEXT, since INTEGER, seen INTEGER, value REAL, threshold REAL, state TEXT, severity TEXT, PRIMARY KEY(cluster,instance,name,labels));
            CREATE TABLE IF NOT EXISTS monitor_rule(cluster TEXT, name TEXT, metric TEXT, threshold REAL, below INTEGER, seconds INTEGER, severity TEXT, PRIMARY KEY(cluster,name));
            CREATE TABLE IF NOT EXISTS monitor_sampling(cluster TEXT PRIMARY KEY, seconds INTEGER NOT NULL CHECK(seconds BETWEEN 10 AND 60));
            CREATE TABLE IF NOT EXISTS monitor_incident(id INTEGER PRIMARY KEY, cluster TEXT, instance TEXT, name TEXT, metric TEXT, labels TEXT, since INTEGER, fired INTEGER, value REAL, threshold REAL, severity TEXT, ended INTEGER, reason TEXT);
            CREATE UNIQUE INDEX IF NOT EXISTS monitor_incident_open ON monitor_incident(cluster,instance,name,labels) WHERE ended IS NULL;
            CREATE INDEX IF NOT EXISTS monitor_incident_time ON monitor_incident(cluster,fired);
            """;
        init.ExecuteNonQuery();
        foreach (var source in settings.Sources.Concat(_metadataSources))
            Exec(db,"INSERT OR IGNORE INTO monitor_source VALUES($c,$i,$t,NULL,0)",("$c",source.ClusterId),("$i",source.Instance),("$t",Now));
    }

    private long Now => _clock.GetUtcNow().ToUnixTimeSeconds();
    internal int? SamplingOverride(string cluster)
    {
        if (_settings.Database.Length==0) return null;
        using var db=Open();
        using var cmd=Sql(db,"SELECT seconds FROM monitor_sampling WHERE cluster=$c",("$c",cluster));
        return cmd.ExecuteScalar() is long value ? (int)value : null;
    }
    internal IResult SetSampling(string cluster,int seconds)
    {
        if(seconds is <10 or >60) return Results.BadRequest();
        if(_settings.Database.Length==0) return Results.Conflict();
        lock(_writes)
        {
            using var db=Open();
            Exec(db,"INSERT INTO monitor_sampling VALUES($c,$v) ON CONFLICT(cluster) DO UPDATE SET seconds=excluded.seconds",("$c",cluster),("$v",seconds));
        }
        return Results.Ok(new {seconds});
    }
    private static List<MonitorRule> ClusterRules(SqliteConnection db,string cluster)
    {
        using var cmd=Sql(db,"SELECT name,metric,threshold,below,seconds,severity FROM monitor_rule WHERE cluster=$c ORDER BY name",("$c",cluster));
        using var reader=cmd.ExecuteReader();
        var rules=new List<MonitorRule>();
        while(reader.Read()) rules.Add(new(reader.GetString(0),reader.GetString(1),reader.GetDouble(2),
            reader.GetBoolean(3),reader.GetInt32(4),reader.GetString(5)));
        return rules;
    }

    internal IResult ChangeRule(string cluster,KafkaMetricSources.RuleChange input)
    {
        if (_settings.Database.Length==0) return Results.Conflict();
        if (string.IsNullOrWhiteSpace(input.Name) || input.Name.Length>100 || input.Name.Any(char.IsControl) ||
            input.Name=="KafkaTargetDown" || _rules.Any(r=>r.Name==input.Name)) return Results.BadRequest();
        if (!input.Remove && (string.IsNullOrWhiteSpace(input.Metric) || input.Metric.Length>512 ||
            input.Metric.Any(char.IsControl) || !double.IsFinite(input.Threshold) || input.Seconds is <0 or >86400 ||
            input.Severity is not ("warning" or "critical"))) return Results.BadRequest();
        lock (_writes)
        {
            using var db=Open();
            using var tx=db.BeginTransaction();
            var rules=ClusterRules(db,cluster);
            bool exists=rules.Any(r=>r.Name==input.Name);
            if (input.Remove && !exists) return Results.NotFound();
            if (!input.Remove && !exists && rules.Count>=64) return Results.Conflict();
            if (input.Remove)
                Exec(db,"DELETE FROM monitor_rule WHERE cluster=$c AND name=$n",("$c",cluster),("$n",input.Name));
            else
                Exec(db,"INSERT OR REPLACE INTO monitor_rule VALUES($c,$n,$m,$v,$b,$s,$level)",("$c",cluster),("$n",input.Name),
                    ("$m",input.Metric!),("$v",input.Threshold),("$b",input.Below),("$s",input.Seconds),("$level",input.Severity!));
            Exec(db,"DELETE FROM monitor_alert WHERE cluster=$c AND name=$n",("$c",cluster),("$n",input.Name));
            Exec(db,"UPDATE monitor_incident SET ended=$t,reason='ruleChanged' WHERE cluster=$c AND name=$n AND ended IS NULL",
                ("$c",cluster),("$n",input.Name),("$t",Now));
            tx.Commit();
            return Results.Ok(new {name=input.Name});
        }
    }
    internal BrokerSource[] Sources()
    {
        if (_settings.Database.Length==0) return [];
        using var db=Open();using var cmd=Sql(db,"SELECT cluster,instance,hash FROM monitor_enrollment");
        using var r=cmd.ExecuteReader();
        var rows=_settings.Sources.Concat(_metadataSources).ToList();
        while (r.Read()) rows.Add(new(r.GetString(0),r.GetString(1),(byte[])r[2]));
        return rows.ToArray();
    }

    internal IResult Enroll(string cluster,string instance,string action)
    {
        if (_settings.Database.Length==0) return Results.Problem(statusCode:409,title:"Configure a monitoring DatabasePath on the Kafdeck server.");
        if (string.IsNullOrWhiteSpace(instance) || instance==MonitoringSettings.MetadataInstance || instance.Length>160 || instance.Any(char.IsControl) ||
            action is not ("create" or "rotate" or "revoke")) return Results.BadRequest();
        lock (_writes)
        {
            if (_settings.Sources.Any(s=>s.ClusterId==cluster && s.Instance==instance)) return Results.Conflict();
            using var db=Open();using var tx=db.BeginTransaction();
            var scope=new (string,object)[]{("$c",cluster),("$i",instance)};
            using var count=Sql(db,"SELECT count(*) FROM monitor_enrollment WHERE cluster=$c AND instance=$i",scope);
            bool exists=(long)count.ExecuteScalar()!>0;
            if (action=="create" && exists) return Results.Conflict();
            if (action!="create" && !exists) return Results.NotFound();
            if (action=="revoke")
            {
                Exec(db,"DELETE FROM monitor_enrollment WHERE cluster=$c AND instance=$i",scope);
                Exec(db,"DELETE FROM monitor_alert WHERE cluster=$c AND instance=$i",scope);
                Exec(db,"DELETE FROM monitor_incident WHERE cluster=$c AND instance=$i",scope);
                Exec(db,"DELETE FROM monitor_source WHERE cluster=$c AND instance=$i",scope);
                Exec(db,"DELETE FROM monitor_sample WHERE series IN (SELECT id FROM monitor_series WHERE cluster=$c AND instance=$i)",scope);
                Exec(db,"DELETE FROM monitor_series WHERE cluster=$c AND instance=$i",scope);
                tx.Commit();_cycles.Remove((cluster,instance));return Results.Ok(new {instance});
            }
            using var total=Sql(db,"SELECT count(*) FROM monitor_enrollment");
            if (!exists && (long)total.ExecuteScalar()!+_settings.Sources.Length>=128) return Results.Conflict();
            var secret=Convert.ToHexString(System.Security.Cryptography.RandomNumberGenerator.GetBytes(32));
            var digest=System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(secret));
            Exec(db,"INSERT OR REPLACE INTO monitor_enrollment VALUES($c,$i,$hash)",[..scope,("$hash",digest)]);
            Exec(db,"INSERT OR IGNORE INTO monitor_source VALUES($c,$i,$t,NULL,0)",[..scope,("$t",Now)]);
            tx.Commit();return Results.Ok(new {instance,token=secret});
        }
    }
    private SqliteConnection Open() { var db = new SqliteConnection(_connection); db.Open(); return db; }
    private static SqliteCommand Sql(SqliteConnection db,string text, params (string Key,object Value)[] args)
    {
        var cmd=db.CreateCommand(); cmd.CommandText=text;
        foreach(var (key,value) in args) cmd.Parameters.AddWithValue(key,value);
        return cmd;
    }
    private static void Exec(SqliteConnection db,string text,params (string Key,object Value)[] args)
    { using var cmd=Sql(db,text,args);cmd.ExecuteNonQuery(); }

    internal IResult Ingest(BrokerSource src,JmxBatch batch,CancellationToken ct)
    {
        lock (_writes)
        {
            if (!Sources().Any(s=>s.ClusterId==src.ClusterId && s.Instance==src.Instance && System.Security.Cryptography.CryptographicOperations.FixedTimeEquals(s.Credential,src.Credential))) return Results.Unauthorized();
            using var db=Open();
            var now=Now;
            using var recent=Sql(db,"SELECT seen FROM monitor_source WHERE cluster=$c AND instance=$i",("$c",src.ClusterId),("$i",src.Instance));
            var key=(src.ClusterId,src.Instance);
            var active=_cycles.GetValueOrDefault(key);
            if (batch.Part==0 && (now-active.Started<5 || recent.ExecuteScalar() is long seen && now-seen<5)) return Results.StatusCode(429);
            if (batch.Part>0 && (active.Cycle!=batch.Cycle || active.Next!=batch.Part)) return Results.Conflict();
            using var tx=db.BeginTransaction();
            using var insert=Sql(db,"INSERT OR IGNORE INTO monitor_series(cluster,instance,metric,labels) VALUES($c,$i,$m,$l)",
                ("$c",src.ClusterId),("$i",src.Instance),("$m",""),("$l",""));
            using var write=Sql(db,"INSERT OR REPLACE INTO monitor_sample SELECT id,$t,$v FROM monitor_series WHERE cluster=$c AND instance=$i AND metric=$m AND labels=$l",
                ("$c",src.ClusterId),("$i",src.Instance),("$m",""),("$l",""),("$t",now),("$v",0d));
            insert.Transaction=tx;write.Transaction=tx;
            var rules=_rules.Concat(ClusterRules(db,src.ClusterId)).ToLookup(rule=>rule.Metric);
            var ids=new HashSet<(string,string)>();
            foreach(var p in batch.Samples.Append(new JmxPoint(Heartbeat,[],1)))
            {
                ct.ThrowIfCancellationRequested();
                var labels=JsonSerializer.Serialize(p.Labels);
                if (!ids.Add((p.Metric,labels))) return Results.BadRequest();
                insert.Parameters["$m"].Value=write.Parameters["$m"].Value=p.Metric;
                insert.Parameters["$l"].Value=write.Parameters["$l"].Value=labels;
                write.Parameters["$v"].Value=p.Value;
                insert.ExecuteNonQuery();write.ExecuteNonQuery();
                foreach(var rule in rules[p.Metric])
                    Evaluate(db,src,rule,labels,p.Value,now);
            }
            Exec(db,"UPDATE monitor_source SET seen=$t,partial=$p WHERE cluster=$c AND instance=$i",("$t",now),("$p",batch.Partial || !batch.Last),("$c",src.ClusterId),("$i",src.Instance));
            Exec(db,"DELETE FROM monitor_alert WHERE cluster=$c AND instance=$i AND name='KafkaTargetDown'",("$c",src.ClusterId),("$i",src.Instance));
            Exec(db,"UPDATE monitor_incident SET ended=$t,reason='recovered' WHERE cluster=$c AND instance=$i AND name='KafkaTargetDown' AND ended IS NULL",
                ("$c",src.ClusterId),("$i",src.Instance),("$t",now));
            tx.Commit();
            _cycles[key]=(batch.Cycle,batch.Last ? -1 : batch.Part+1,batch.Part==0 ? now : active.Started);
            return Results.NoContent();
        }
    }

    private void Evaluate(SqliteConnection db,BrokerSource src,MonitorRule rule,string labels,double value,long now)
    {
        var args=new (string,object)[]{("$c",src.ClusterId),("$i",src.Instance),("$n",rule.Name),("$l",labels)};
        const string match="cluster=$c AND instance=$i AND name=$n AND labels=$l";
        if (!(rule.Below ? value<rule.Threshold : value>rule.Threshold))
        {
            Exec(db,"DELETE FROM monitor_alert WHERE "+match,args);
            Exec(db,"UPDATE monitor_incident SET ended=$t,reason='recovered' WHERE ended IS NULL AND "+match,[..args,("$t",now)]);
            return;
        }
        using var previous=Sql(db,"SELECT since,seen,state FROM monitor_alert WHERE "+match,args);
        long since=now;
        bool alreadyFiring=false;
        using(var reader=previous.ExecuteReader())
            if(reader.Read() && now-reader.GetInt64(1)<=120)
            {since=reader.GetInt64(0);alreadyFiring=reader.GetString(2)=="firing";}
        var state=now-since>=rule.Seconds ? "firing" : "pending";
        Exec(db,"INSERT OR REPLACE INTO monitor_alert VALUES($c,$i,$n,$m,$l,$since,$t,$v,$threshold,$state,$severity)",
            [..args,("$m",rule.Metric),("$since",since),("$t",now),("$v",value),("$threshold",rule.Threshold),("$state",state),("$severity",rule.Severity)]);
        if(state=="firing" && !alreadyFiring)
        {
            Exec(db,"INSERT OR IGNORE INTO monitor_incident(cluster,instance,name,metric,labels,since,fired,value,threshold,severity) VALUES($c,$i,$n,$m,$l,$since,$t,$v,$threshold,$severity)",
                [..args,("$m",rule.Metric),("$since",since),("$t",now),("$v",value),("$threshold",rule.Threshold),("$severity",rule.Severity)]);
            _logger.LogWarning("Monitoring alert {Rule} on {Cluster}/{Instance}: {Value}",rule.Name,src.ClusterId,src.Instance,value);
        }
    }

    internal void Maintain()
    {
        if (_settings.Database.Length==0) return;
        lock(_writes)
        {
            var now=Now;
            using var db=Open();
            using var tx=db.BeginTransaction();
            foreach(var src in Sources())
            {
                using var last=Sql(db,"SELECT COALESCE(seen,since) FROM monitor_source WHERE cluster=$c AND instance=$i",("$c",src.ClusterId),("$i",src.Instance));
                if (now-Convert.ToInt64(last.ExecuteScalar(),CultureInfo.InvariantCulture)<120) continue;
                Evaluate(db,src,new("KafkaTargetDown",Heartbeat,1,true,0,"critical"),"{}",0,now);
                Exec(db,"INSERT OR IGNORE INTO monitor_series(cluster,instance,metric,labels) VALUES($c,$i,$m,'{}')",("$c",src.ClusterId),("$i",src.Instance),("$m",Heartbeat));
                Exec(db,"INSERT OR REPLACE INTO monitor_sample SELECT id,$t,0 FROM monitor_series WHERE cluster=$c AND instance=$i AND metric=$m",("$c",src.ClusterId),("$i",src.Instance),("$m",Heartbeat),("$t",now));
            }
            Exec(db,"DELETE FROM monitor_alert WHERE seen<$t",("$t",now-120));
            Exec(db,"""
                UPDATE monitor_incident AS i SET ended=$t,reason='noData' WHERE ended IS NULL AND NOT EXISTS
                (SELECT 1 FROM monitor_alert a WHERE a.cluster=i.cluster AND a.instance=i.instance AND a.name=i.name AND a.labels=i.labels AND a.state='firing')
                """,("$t",now));
            if(now-_lastCleanup>=300) {
                Exec(db,"DELETE FROM monitor_sample WHERE time<$t",("$t",now-86400));
                Exec(db,"DELETE FROM monitor_incident WHERE ended<$t",("$t",now-86400));
                Exec(db,"DELETE FROM monitor_series WHERE NOT EXISTS(SELECT 1 FROM monitor_sample WHERE series=monitor_series.id)");
                _lastCleanup=now;
            }
            tx.Commit();
        }
    }

    internal IResult Read(string cluster,string kind,string? metric,string? search,int minutes,string? instance,string? filters,CancellationToken ct)
    {
        if(kind=="sampling") return Results.Ok(new {seconds=SamplingOverride(cluster)??30});
        var sources=Sources().Where(s=>s.ClusterId==cluster).Select(s=>s.Instance).ToArray();
        if (sources.Length==0) return Results.Ok(new {state="unconfigured"});
        if (minutes is not (15 or 60 or 360 or 1440) || metric?.Length>512 || search?.Length>100 || instance?.Length>160 || filters?.Length>2500) return Results.BadRequest();
        var now=Now;
        using var db=Open();
        // Configuration removals take effect on reads even while the old rows are awaiting retention.
        var args=new List<(string,object)>{("$c",cluster)};
        for(var i=0;i<sources.Length;i++) args.Add(("$s"+i,sources[i]));
        var scope="cluster=$c AND instance IN ("+string.Join(',',Enumerable.Range(0,sources.Length).Select(i=>"$s"+i))+")";
        using var status=Sql(db,"SELECT count(*) FROM monitor_source WHERE "+scope+" AND (seen IS NULL OR seen<$age OR partial=1)",[..args,("$age",now-120)]);
        bool warning=(long)status.ExecuteScalar()!>0;
        if(kind=="sources")
        {
            using var cmd=Sql(db,"SELECT instance,seen,partial FROM monitor_source WHERE "+scope+" ORDER BY instance",args.ToArray());
            using var reader=cmd.ExecuteReader();
            var targets=new List<object>();
            while(reader.Read())
            {
                long? seen=reader.IsDBNull(1)?null:reader.GetInt64(1);
                var health=seen is null ? "waiting" : now-seen>=120 ? "stale" : reader.GetBoolean(2) ? "partial" : "current";
                targets.Add(new {instance=reader.GetString(0),seen,health});
            }
            return Results.Ok(new {state="available",observedAt=now,warning,truncated=false,rows=targets});
        }
        if(kind=="rules")
        {
            var rows=ClusterRules(db,cluster);
            return Results.Ok(new {state="available",observedAt=now,warning,truncated=false,rows});
        }
        if(kind=="metrics")
        {
            using var cmd=Sql(db,"SELECT DISTINCT metric FROM monitor_series WHERE "+scope+" AND instr(lower(metric),lower($q))>0 ORDER BY metric LIMIT 501",[..args,("$q",search??"")]);
            using var reader=cmd.ExecuteReader();
            var names=new List<string>();while(reader.Read()) names.Add(reader.GetString(0));
            return Results.Ok(new {state="available",observedAt=now,warning,truncated=names.Count>500,names=names.Take(500)});
        }
        if(kind=="alerts")
        {
            using var cmd=Sql(db,"SELECT name,metric,labels,instance,since,seen,value,threshold,state,severity FROM monitor_alert WHERE "+scope+" AND seen>=$age ORDER BY state,severity,name,instance LIMIT 201",[..args,("$age",now-120)]);
            using var reader=cmd.ExecuteReader();
            var alerts=new List<object>();
            while(reader.Read()) alerts.Add(new {name=reader.GetString(0),metric=reader.GetString(1),labels=JsonSerializer.Deserialize<Dictionary<string,string>>(reader.GetString(2)),
                instance=reader.GetString(3),since=reader.GetInt64(4),time=reader.GetInt64(5),value=reader.GetDouble(6),threshold=reader.GetDouble(7),state=reader.GetString(8),severity=reader.GetString(9)});
            return Results.Ok(new {state="available",observedAt=now,warning,truncated=alerts.Count>200,rows=alerts.Take(200)});
        }
        if(kind=="incidents")
        {
            using var cmd=Sql(db,"SELECT name,instance,metric,fired,ended,reason,value,threshold,labels FROM monitor_incident WHERE "+scope+" AND (ended IS NULL OR ended>=$cutoff) ORDER BY fired DESC,id DESC LIMIT 51",[..args,("$cutoff",now-86400)]);
            using var reader=cmd.ExecuteReader();
            var rows=new List<object>();
            while(reader.Read()) rows.Add(new {name=reader.GetString(0),instance=reader.GetString(1),metric=reader.GetString(2),fired=reader.GetInt64(3),
                ended=reader.IsDBNull(4)?(long?)null:reader.GetInt64(4),reason=reader.IsDBNull(5)?null:reader.GetString(5),value=reader.GetDouble(6),
                threshold=reader.GetDouble(7),labels=JsonSerializer.Deserialize<Dictionary<string,string>>(reader.GetString(8))});
            return Results.Ok(new {state="available",observedAt=now,warning,truncated=rows.Count>50,rows=rows.Take(50)});
        }
        if(kind!="series" || string.IsNullOrWhiteSpace(metric)) return Results.BadRequest();
        var where=scope+" AND metric=$m";args.Add(("$m",metric));
        if (!string.IsNullOrEmpty(instance)) {where+=" AND instance=$instance"; args.Add(("$instance",instance));}
        if(!string.IsNullOrEmpty(filters))
        {
            Dictionary<string,string>? extra;
            try {extra=JsonSerializer.Deserialize<Dictionary<string,string>>(filters);}
            catch(JsonException){return Results.BadRequest();}
            if(extra is null || extra.Count>8 || extra.Any(k=>k.Key.Length>100 || k.Key is "instance" or "kafdeck_cluster" || k.Value is null || k.Value.Length>512)) return Results.BadRequest();
            var i=0;foreach(var (key,value) in extra){
                where+=" AND EXISTS (SELECT 1 FROM json_each(labels) WHERE key=$k"+i+" AND value=$v"+i+")";
                args.Add(("$k"+i,key));args.Add(("$v"+i,value));i++;
            }
        }
        var start=now-minutes*60;
        using var select=Sql(db,"SELECT id,instance,labels FROM monitor_series WHERE "+where+" AND EXISTS (SELECT 1 FROM monitor_sample WHERE series=monitor_series.id AND time BETWEEN $from AND $to) ORDER BY instance,labels LIMIT 13",[..args,("$from",start),("$to",now)]);
        var ids=new List<(long Id,string Instance,string Labels)>();
        using(var reader=select.ExecuteReader()) while(reader.Read()) ids.Add((reader.GetInt64(0),reader.GetString(1),reader.GetString(2)));
        var series=new List<object>();
        foreach(var entry in ids.Take(12))
        {
            ct.ThrowIfCancellationRequested();
            // Show the last actual measurement in each display bucket, with its own timestamp.
            using var sample=Sql(db,"SELECT MAX(time),value FROM monitor_sample WHERE series=$id AND time BETWEEN $a AND $b GROUP BY (time-$a)/$step ORDER BY MAX(time)",
                ("$id",entry.Id),("$a",start),("$b",now),("$step",Math.Max(1,minutes*60/120)));
            using var reader=sample.ExecuteReader();
            var points=new List<object>();
            while(reader.Read()) points.Add(new{time=reader.GetInt64(0)*1000,value=reader.GetDouble(1)});
            var labels=JsonSerializer.Deserialize<Dictionary<string,string>>(entry.Labels)!;
            labels["instance"]=entry.Instance;series.Add(new{labels,points});
        }
        return Results.Ok(new {state="available",observedAt=now,from=start,to=now,warning,truncated=ids.Count>12,rows=series});
    }
}

internal sealed class MonitoringWatchdog(MonitoringStore store, ILogger<MonitoringWatchdog> log) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer=new PeriodicTimer(TimeSpan.FromSeconds(15));
        while(await timer.WaitForNextTickAsync(stoppingToken)) {
            try {store.Maintain();} catch(SqliteException ex) {log.LogError(ex,"Monitoring maintenance failed");}
        }
    }
}
