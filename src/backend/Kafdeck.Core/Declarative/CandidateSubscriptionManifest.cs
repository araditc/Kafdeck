using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Kafdeck.Core.Notifications;

namespace Kafdeck.Core.Declarative;

// Candidate, inert contract. Parsing never registers an owner or authorizes an effect.
public sealed record CandidateSubscriptionManifest
{
    public const string ApiVersion = "kafdeck.io/v0.8-proposal";
    public const int MaxInputBytes = 16384;
    public const int MaxNodes = 256;

    private CandidateSubscriptionManifest(string deploymentRef, string ownerRef,
        NotificationSubscriptionDefinition definition, NotificationSubscriptionState state)
    {
        DeploymentRef = deploymentRef;
        OwnerRef = ownerRef;
        Definition = definition;
        State = state;
    }

    public string DeploymentRef { get; }
    public string OwnerRef { get; }
    public NotificationSubscriptionDefinition Definition { get; }
    public NotificationSubscriptionState State { get; }

    // JSON only. YAML (including anchors/aliases) has no parser or expansion path.
    public static CandidateSubscriptionManifest ParseJson(ReadOnlyMemory<byte> utf8)
    {
        if (utf8.Length is < 2 or > MaxInputBytes)
            throw new ArgumentException("Candidate manifest byte limit exceeded.");

        try
        {
            using var document = JsonDocument.Parse(utf8, new JsonDocumentOptions { MaxDepth = 6 });
            var nodes = 0;
            ValidateTree(document.RootElement, ref nodes);
            var root = document.RootElement;
            RequireFields(root, "apiVersion", "kind", "metadata", "spec");
            if (String(root, "apiVersion") != ApiVersion ||
                String(root, "kind") != "NotificationSubscription")
                throw new ArgumentException("Candidate manifest version or family is unsupported.");

            var metadata = root.GetProperty("metadata");
            RequireFields(metadata, "deploymentRef", "name", "ownerRef");
            var deployment = Identifier(metadata, "deploymentRef");
            var owner = Identifier(metadata, "ownerRef");
            var name = Identifier(metadata, "name");
            var spec = root.GetProperty("spec");
            RequireFields(spec, "destinationRef", "state", "eventClasses", "eventTypes");
            var state = String(spec, "state") switch
            {
                "Active" => NotificationSubscriptionState.Active,
                "Paused" => NotificationSubscriptionState.Paused,
                _ => throw new ArgumentException("Candidate manifest lifecycle is unsupported.")
            };
            var classes = spec.GetProperty("eventClasses").EnumerateArray().Select(value =>
                value.GetString() switch
                {
                    "Operational" => NotificationEventClass.Operational,
                    "Mutation" => NotificationEventClass.Mutation,
                    "Security" => NotificationEventClass.Security,
                    "Slo" => NotificationEventClass.Slo,
                    "DataQuality" => NotificationEventClass.DataQuality,
                    _ => throw new ArgumentException("Candidate event class is unsupported.")
                }).ToArray();
            var types = spec.GetProperty("eventTypes").EnumerateArray()
                .Select(value => value.GetString() ?? throw new ArgumentException("Invalid event filter.")).ToArray();
            return new(deployment, owner,
                new NotificationSubscriptionDefinition(name, Identifier(spec, "destinationRef"), classes, types), state);
        }
        catch (Exception exception) when (exception is JsonException or InvalidOperationException or KeyNotFoundException)
        {
            // Never copy source JSON or property values into diagnostics.
            throw new ArgumentException("Candidate manifest is not a valid closed JSON document.");
        }
    }

    public string Fingerprint() => Fingerprint(DeploymentRef, Definition, State);

    internal static string Fingerprint(string deploymentRef, NotificationSubscriptionDefinition definition,
        NotificationSubscriptionState state)
    {
        // Fixed property order and existing domain normalization preserve exact case and set order.
        var canonical = JsonSerializer.SerializeToUtf8Bytes(new
        {
            apiVersion = ApiVersion,
            kind = "NotificationSubscription",
            deploymentRef,
            name = definition.SubscriptionId,
            destinationRef = definition.DestinationId,
            state = state.ToString(),
            eventClasses = definition.EventClasses.Select(value => value.ToString()).ToArray(),
            eventTypes = definition.EventTypes.ToArray()
        });
        return Convert.ToHexString(SHA256.HashData(canonical)).ToLowerInvariant();
    }

    private static string String(JsonElement element, string name) =>
        element.GetProperty(name).GetString() ?? throw new ArgumentException("Candidate field must be a string.");

    private static string Identifier(JsonElement element, string name) =>
        NotificationSubscriptionDefinition.NormalizeSubscriptionId(String(element, name));

    private static void RequireFields(JsonElement element, params string[] fields)
    {
        if (element.ValueKind != JsonValueKind.Object || element.EnumerateObject().Count() != fields.Length ||
            fields.Any(field => !element.TryGetProperty(field, out _)))
            throw new ArgumentException("Candidate manifest has unknown or missing fields.");
    }

    private static void ValidateTree(JsonElement element, ref int nodes)
    {
        if (++nodes > MaxNodes) throw new ArgumentException("Candidate manifest node limit exceeded.");
        if (element.ValueKind == JsonValueKind.Object)
        {
            var names = new HashSet<string>(StringComparer.Ordinal);
            foreach (var property in element.EnumerateObject())
            {
                if (!names.Add(property.Name)) throw new ArgumentException("Duplicate manifest field.");
                ValidateTree(property.Value, ref nodes);
            }
        }
        else if (element.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in element.EnumerateArray()) ValidateTree(item, ref nodes);
        }
        else if (element.ValueKind == JsonValueKind.String)
        {
            var value = element.GetString()!;
            // GetString rejects malformed surrogate escapes; all admitted fields are ASCII later.
            if (Encoding.UTF8.GetByteCount(value) > 1024)
                throw new ArgumentException("Candidate manifest string limit exceeded.");
        }
    }
}
