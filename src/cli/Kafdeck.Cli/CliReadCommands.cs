using System.Text.Json;
using Kafdeck.Cli.Contracts;

namespace Kafdeck.Cli;

/// <summary>Every view name and URL template is compiled; a file supplies only its closed query/body fields.</summary>
public static class CliReadCommands
{
    public static CliFamilyInvocation? TryParse(IReadOnlyList<string> args)
    {
        if (args.Count == 0 || args[0] != "views") return null;
        var contract = args.Count >= 2 ? KafdeckReadContracts.Find(args[1]) : null;
        if (contract is null) throw new CliUsageException("Use a compiled versioned view name from capabilities reads.");
        var segments = contract.Path.Split('/').Where(segment => segment.StartsWith('{')).ToArray();
        if (args.Count != segments.Length + 2) throw new CliUsageException("This named view requires its exact positional path identities.");
        var route = contract.Path;
        for (var index = 0; index < segments.Length; index++)
        {
            var value = args[index + 2];
            if (string.IsNullOrWhiteSpace(value) || value.Length > 512 || value != value.Trim() || value.Any(char.IsControl) ||
                value is "." or ".." || value.Contains('/') || value.Contains('\\'))
                throw new CliUsageException("View path identities must be exact bounded segments.");
            route = route.Replace(segments[index], Uri.EscapeDataString(value), StringComparison.Ordinal);
        }
        return new(route, false, contract.Body is not null || contract.QueryKeys.Length > 0 ? CliFamilyPayload.NamedReadRequest : CliFamilyPayload.Empty,
            HumanOnly: contract.HumanOnly, Read: true, Method: contract.Method, ReadContract: contract.Name);
    }

    public static bool Validate(JsonElement value, string name)
    {
        var contract = KafdeckReadContracts.Find(name);
        if (contract is null || value.ValueKind != JsonValueKind.Object) return false;
        if (contract.Method == "get") return Closed(value, contract.QueryKeys.Split(',', StringSplitOptions.RemoveEmptyEntries), Scalar);
        return contract.Body switch
        {
            "SchemaMockRequest" => Closed(value, ["subject", "version", "count", "seed"], Scalar),
            "ControlledSerdeDecodeApiRequest" => Closed(value, ["format", "payloadBase64"], Scalar),
            "ControlledSerdeEncodeApiRequest" => Closed(value, ["format", "structuredValue"], DuplicateFree),
            "ConnectPluginValidationRequest" => Closed(value, ["configuration"], configuration => configuration.ValueKind == JsonValueKind.Object &&
                Closed(configuration, configuration.EnumerateObject().Select(p => p.Name), v => v.ValueKind == JsonValueKind.String)),
            "KsqlQueryRequestBody" => Closed(value, ["statement", "limits"], v => v.ValueKind != JsonValueKind.Object ? Scalar(v) :
                Closed(v, ["maxRows", "maxBytes", "maxDurationSeconds"], Scalar)),
            _ => false,
        };
    }

    public static string BuildRoute(CliFamilyInvocation invocation, JsonDocument? payload)
    {
        if (invocation.Method != "get" || payload is null) return invocation.Route;
        return invocation.Route + (payload.RootElement.EnumerateObject().Any() ? "?" + string.Join("&",
            payload.RootElement.EnumerateObject().Where(p => p.Value.ValueKind != JsonValueKind.Null).Select(p =>
                Uri.EscapeDataString(p.Name) + "=" + Uri.EscapeDataString(p.Value.ValueKind == JsonValueKind.String ? p.Value.GetString()! : p.Value.GetRawText()))) : "");
    }

    public static bool RawOutput(string? contract) => contract is "v03-records-tail" or "v03-records-export";
    public static int Outcome(string body, string? contract)
    {
        if (contract != "v03-records-tail") return CliApplication.Success;
        var completed = false;
        foreach (var line in body.Split('\n').Where(line => line.StartsWith("data: ", StringComparison.Ordinal)))
        {
            using var frame = JsonDocument.Parse(line[6..]);
            if (frame.RootElement.TryGetProperty("kind", out var kind) && kind.GetString() is "error" or "admissionDenied")
                return CliApplication.RemoteError;
            if (frame.RootElement.TryGetProperty("kind", out kind) && kind.GetString() == "completed") completed = true;
        }
        return completed ? CliApplication.Success : CliApplication.RemoteError;
    }
    private static bool Scalar(JsonElement value) => value.ValueKind is not (JsonValueKind.Object or JsonValueKind.Array or JsonValueKind.Undefined) &&
        (value.ValueKind != JsonValueKind.String || value.GetString() is { Length: <= 4096 } text && !text.Any(char.IsControl));
    private static bool Closed(JsonElement value, IEnumerable<string> names, Func<JsonElement, bool> validate)
    {
        if (value.ValueKind != JsonValueKind.Object) return false;
        var allowed = names.ToHashSet(StringComparer.Ordinal); var seen = new HashSet<string>(StringComparer.Ordinal);
        return value.EnumerateObject().Count() <= 1024 && value.EnumerateObject().All(p => allowed.Contains(p.Name) && seen.Add(p.Name) && validate(p.Value));
    }
    private static bool DuplicateFree(JsonElement value) => value.ValueKind == JsonValueKind.Array ? value.EnumerateArray().All(DuplicateFree) :
        value.ValueKind != JsonValueKind.Object || Closed(value, value.EnumerateObject().Select(p => p.Name), DuplicateFree);
}
