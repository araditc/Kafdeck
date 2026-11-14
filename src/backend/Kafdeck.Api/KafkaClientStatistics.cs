using System.Text.Json;

namespace Kafdeck.Api;

// Preserve librdkafka's native field names and units. Dimension values never become metric names.
internal static class KafkaClientStatistics
{
    internal static JmxBatch? Read(JsonElement root,long now)
    {
        if (root.ValueKind!=JsonValueKind.Object || !root.TryGetProperty("type",out var type) ||
            type.ValueKind!=JsonValueKind.String || type.GetString() is not ("producer" or "consumer") ||
            !root.TryGetProperty("time",out var time) || time.ValueKind!=JsonValueKind.Number || !time.TryGetInt64(out var captured) ||
            captured < now-120 || captured > now+30) return null;
        var points=new List<JmxPoint>();
        bool partial=false;
        var dimensions=new SortedDictionary<string,string>(StringComparer.Ordinal) {["role"]=type.GetString()!};
        Visit(root,"librdkafka",dimensions);
        return points.Count==0 ? null : new(partial,Guid.NewGuid(),0,true,points.ToArray());

        void Visit(JsonElement obj,string prefix,SortedDictionary<string,string> labels)
        {
            foreach (var property in obj.EnumerateObject())
            {
                if (points.Count>=8192) {partial=true; return;}
                var key=property.Name;
                var value=property.Value;
                if (key.Length is 0 or >100 || !key.All(c=>char.IsAsciiLetterOrDigit(c) || c=='_')) {partial=true;continue;}
                string path=prefix+"/"+key;
                if (path.Length>512) {partial=true;continue;}
                if (value.ValueKind==JsonValueKind.Number && value.TryGetDouble(out var n) && double.IsFinite(n))
                {
                    if (prefix=="librdkafka" && key is "time" or "ts") continue;
                    points.Add(new(path,labels,n));
                }
                else if (value.ValueKind==JsonValueKind.Object)
                {
                    if (key is "brokers" or "topics" or "partitions" or "req")
                    {
                        string dimension=key switch {"brokers"=>"broker","topics"=>"topic","partitions"=>"partition",_=>"request"};
                        foreach(var item in value.EnumerateObject())
                        {
                            if (points.Count>=8192) {partial=true;return;}
                            if(item.Name.Length is 0 or >512 || item.Name.Any(char.IsControl)) {partial=true;continue;}
                            var child=new SortedDictionary<string,string>(labels,StringComparer.Ordinal){[dimension]=item.Name};
                            if(item.Value.ValueKind==JsonValueKind.Object) Visit(item.Value,path,child);
                            else if(item.Value.ValueKind==JsonValueKind.Number && item.Value.TryGetDouble(out var count) && double.IsFinite(count))
                                points.Add(new(path,child,count));
                        }
                    }
                    else if(key!="toppars") Visit(value,path,labels);
                }
            }
        }
    }
}
