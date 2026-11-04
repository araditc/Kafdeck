using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Kafdeck.Core.Notifications;

namespace Kafdeck.Core.Declarative;

/// <summary>Closed desired state. Parsing confers no ownership or execution authority.</summary>
public sealed record DeclarativeSubscriptionManifest
{
    public const string ApiVersion = "kafdeck.io/v0.8";
    public const int MaxInputBytes = 16 * 1024;
    public const int MaxNodes = 256;
    public const int MaxDepth = 6;
    private static readonly UTF8Encoding StrictUtf8 = new(false, true);

    private DeclarativeSubscriptionManifest(string deploymentRef, string ownerRef,
        NotificationSubscriptionDefinition definition, NotificationSubscriptionState state)
    { DeploymentRef = deploymentRef; OwnerRef = ownerRef; Definition = definition; State = state; }

    public string DeploymentRef { get; }
    public string OwnerRef { get; }
    public NotificationSubscriptionDefinition Definition { get; }
    public NotificationSubscriptionState State { get; }

    public static DeclarativeSubscriptionManifest Parse(ReadOnlyMemory<byte> input, string format)
    {
        if (input.Length is < 2 or > MaxInputBytes) throw Invalid();
        try
        {
            // Reject malformed UTF8 independently of either format's parser.
            var text = StrictUtf8.GetString(input.Span);
            var json = format switch
            {
                "json" => input,
                "yaml" => (ReadOnlyMemory<byte>)BoundedSubscriptionYaml.ToJson(text),
                _ => throw Invalid()
            };
            using var document = JsonDocument.Parse(json, new JsonDocumentOptions { MaxDepth = MaxDepth });
            var nodes = 0; ValidateTree(document.RootElement, ref nodes);
            var root = document.RootElement;
            Fields(root, "apiVersion", "kind", "metadata", "spec");
            if (String(root, "apiVersion") != ApiVersion || String(root, "kind") != "NotificationSubscription") throw Invalid();
            var metadata = root.GetProperty("metadata");
            Fields(metadata, "deploymentRef", "name", "ownerRef");
            var deployment = Identifier(metadata, "deploymentRef");
            var owner = Identifier(metadata, "ownerRef");
            var name = Identifier(metadata, "name");
            var spec = root.GetProperty("spec");
            Fields(spec, "destinationRef", "state", "eventClasses", "eventTypes");
            var state = String(spec, "state") switch
            {
                "Active" => NotificationSubscriptionState.Active,
                "Paused" => NotificationSubscriptionState.Paused,
                _ => throw Invalid()
            };
            var classes = Array(spec, "eventClasses", NotificationSubscriptionDefinition.HardMaxEventClasses)
                .Select(value => value switch
                {
                    "Operational" => NotificationEventClass.Operational,
                    "Mutation" => NotificationEventClass.Mutation,
                    "Security" => NotificationEventClass.Security,
                    "Slo" => NotificationEventClass.Slo,
                    "DataQuality" => NotificationEventClass.DataQuality,
                    _ => throw Invalid()
                }).ToArray();
            var types = Array(spec, "eventTypes", NotificationSubscriptionDefinition.HardMaxEventTypes);
            return new(deployment, owner, new NotificationSubscriptionDefinition(name,
                Identifier(spec, "destinationRef"), classes, types), state);
        }
        catch (Exception error) when (error is ArgumentException or JsonException or InvalidOperationException or KeyNotFoundException)
        {
            // No source bytes, unknown keys, values or credential sentinels in diagnostics.
            throw Invalid();
        }
    }

    public byte[] ToCanonicalUtf8() => JsonSerializer.SerializeToUtf8Bytes(new
    {
        apiVersion = ApiVersion,
        kind = "NotificationSubscription",
        metadata = new { deploymentRef = DeploymentRef, name = Definition.SubscriptionId, ownerRef = OwnerRef },
        spec = new { destinationRef = Definition.DestinationId, state = State.ToString(),
            eventClasses = Definition.EventClasses.Select(value => value.ToString()).ToArray(),
            eventTypes = Definition.EventTypes.ToArray() }
    });

    public string Fingerprint() => Convert.ToHexString(SHA256.HashData(ToCanonicalUtf8())).ToLowerInvariant();

    private static string Identifier(JsonElement value, string name)
    {
        var identifier = NotificationSubscriptionDefinition.NormalizeSubscriptionId(String(value, name));
        return identifier is "." or ".." ? throw Invalid() : identifier;
    }
    private static string String(JsonElement value, string name) =>
        value.GetProperty(name).GetString() ?? throw Invalid();
    private static string[] Array(JsonElement value, string name, int limit)
    {
        var array = value.GetProperty(name);
        if (array.ValueKind != JsonValueKind.Array || array.GetArrayLength() > limit) throw Invalid();
        return array.EnumerateArray().Select(item => item.GetString() ?? throw Invalid()).ToArray();
    }
    private static void Fields(JsonElement value, params string[] names)
    {
        if (value.ValueKind != JsonValueKind.Object || value.EnumerateObject().Count() != names.Length ||
            names.Any(name => !value.TryGetProperty(name, out _))) throw Invalid();
    }
    private static void ValidateTree(JsonElement value, ref int nodes)
    {
        if (++nodes > MaxNodes) throw Invalid();
        if (value.ValueKind == JsonValueKind.Object)
        {
            var names = new HashSet<string>(StringComparer.Ordinal);
            foreach (var property in value.EnumerateObject())
            {
                if (++nodes > MaxNodes || !names.Add(property.Name) || property.Name.Length > 128) throw Invalid();
                ValidateTree(property.Value, ref nodes);
            }
        }
        else if (value.ValueKind == JsonValueKind.Array)
            foreach (var item in value.EnumerateArray()) ValidateTree(item, ref nodes);
        else if (value.ValueKind == JsonValueKind.String)
        {
            var text = value.GetString()!;
            if (text.Length > 1024 || text.Any(char.IsSurrogate)) throw Invalid();
        }
        else throw Invalid();
    }
    internal static ArgumentException Invalid() => new("Manifest violates the closed versioned subscription grammar.");
}
