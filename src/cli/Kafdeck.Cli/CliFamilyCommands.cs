using System.Text.Json;

namespace Kafdeck.Cli;

public sealed record CliFamilyInvocation(string Route, bool Preview, CliFamilyPayload Payload, string? RequestFile = null,
    bool HumanOnly = false, bool Read = false, bool Recovery = false, string Method = "post", bool RuntimeProgress = false, bool Stop = false, bool PolicyStatus = false,
    string? ReadContract = null);
public enum CliFamilyPayload { Empty, RecordsPreview, RecordsExecution, Purge, ConsumerOffsets, ConsumerDelete, SchemaRegister, SchemaCompatibility, SchemaDelete, SchemaExecution, ConnectConfiguration, ConnectControl, RecoveryPreview, RecoveryReview, DataJob, Generator, BatchReconciliation, AutoRestart, AutoRestartApply, ReconciliationProof, NamedReadRequest }

/// <summary>Named family commands only. Structured files have a closed request schema, never a path/handler/actor selector.</summary>
public static class CliFamilyCommands
{
    public const int MaxRequestBytes = 64 * 1024;
    private sealed record Contract(string Family, string Command, string Route, int Arguments, bool Preview, CliFamilyPayload Payload,
        string Method = "post", bool RuntimeProgress = false, bool Stop = false);
    private static readonly Contract[] Contracts =
    [
        new("records", "produce-preview", "clusters/{0}/topics/{1}/mutations/produce/preview", 2, true, CliFamilyPayload.RecordsPreview),
        new("records", "produce-execute", "mutations/{0}/record-production/execute", 1, false, CliFamilyPayload.RecordsExecution),
        new("records", "purge-preview", "clusters/{0}/records/purge/preview", 1, true, CliFamilyPayload.Purge),
        new("consumer-groups", "offsets-preview", "clusters/{0}/consumer-groups/{1}/mutations/offsets/preview", 2, true, CliFamilyPayload.ConsumerOffsets),
        new("consumer-groups", "delete-preview", "clusters/{0}/consumer-groups/{1}/mutations/delete/preview", 2, true, CliFamilyPayload.ConsumerDelete),
        new("schemas", "register-preview", "clusters/{0}/schemas/subjects/{1}/mutations/register/preview", 2, true, CliFamilyPayload.SchemaRegister),
        new("schemas", "compatibility-preview", "clusters/{0}/schemas/subjects/{1}/mutations/compatibility/preview", 2, true, CliFamilyPayload.SchemaCompatibility),
        new("schemas", "global-compatibility-preview", "clusters/{0}/schemas/mutations/compatibility/preview", 1, true, CliFamilyPayload.SchemaCompatibility),
        new("schemas", "delete-preview", "clusters/{0}/schemas/subjects/{1}/mutations/delete/preview", 2, true, CliFamilyPayload.SchemaDelete),
        new("schemas", "register-execute", "mutations/{0}/schema-create/execute", 1, false, CliFamilyPayload.SchemaExecution),
        new("connect", "create-preview", "clusters/{0}/connect/connectors/{1}/mutations/create/preview", 2, true, CliFamilyPayload.ConnectConfiguration),
        new("connect", "update-preview", "clusters/{0}/connect/connectors/{1}/mutations/update/preview", 2, true, CliFamilyPayload.ConnectConfiguration),
        new("connect", "control-preview", "clusters/{0}/connect/connectors/{1}/mutations/control/preview", 2, true, CliFamilyPayload.ConnectControl),
        new("connect", "delete-preview", "clusters/{0}/connect/connectors/{1}/mutations/delete/preview", 2, true, CliFamilyPayload.Empty),
        new("connect", "profile-create-preview", "clusters/{0}/connect/profiles/{1}/connectors/{2}/mutations/create/preview", 3, true, CliFamilyPayload.ConnectConfiguration),
        new("connect", "profile-update-preview", "clusters/{0}/connect/profiles/{1}/connectors/{2}/mutations/update/preview", 3, true, CliFamilyPayload.ConnectConfiguration),
        new("connect", "profile-control-preview", "clusters/{0}/connect/profiles/{1}/connectors/{2}/mutations/control/preview", 3, true, CliFamilyPayload.ConnectControl),
        new("connect", "profile-delete-preview", "clusters/{0}/connect/profiles/{1}/connectors/{2}/mutations/delete/preview", 3, true, CliFamilyPayload.Empty),
        new("connect", "configuration-execute", "mutations/{0}/connect-configuration/execute", 1, false, CliFamilyPayload.ConnectConfiguration),
        new("connect", "control-execute", "mutations/{0}/connect/execute", 1, false, CliFamilyPayload.Empty),
        new("data-jobs", "replay-preview", "data-jobs/replay/preview", 0, true, CliFamilyPayload.DataJob),
        new("data-jobs", "forward-preview", "data-jobs/forward/preview", 0, true, CliFamilyPayload.DataJob),
        new("data-jobs", "reprocess-preview", "data-jobs/reprocess/preview", 0, true, CliFamilyPayload.DataJob),
        new("data-jobs", "dlq-forward-preview", "data-jobs/dlq-forward/preview", 0, true, CliFamilyPayload.DataJob),
        new("data-jobs", "start", "data-jobs/{0}/start", 1, false, CliFamilyPayload.Empty),
        new("data-jobs", "status", "data-jobs/{0}", 1, false, CliFamilyPayload.Empty, "get", true),
        new("data-jobs", "cancel", "data-jobs/{0}/cancel", 1, false, CliFamilyPayload.Empty, RuntimeProgress: true, Stop: true),
        new("data-jobs", "reconcile", "data-jobs/{0}/reconcile", 1, false, CliFamilyPayload.BatchReconciliation, RuntimeProgress: true),
        new("data-jobs", "reconcile-preview", "data-jobs/{0}/reconcile/preview", 1, true, CliFamilyPayload.ReconciliationProof),
        new("generator", "preview", "data-jobs/generator/preview", 0, true, CliFamilyPayload.Generator),
        new("generator", "start", "data-jobs/generator/{0}/start", 1, false, CliFamilyPayload.Empty),
        new("generator", "status", "data-jobs/generator/{0}", 1, false, CliFamilyPayload.Empty, "get", true),
        new("generator", "cancel", "data-jobs/generator/{0}/cancel", 1, false, CliFamilyPayload.Empty, RuntimeProgress: true, Stop: true),
        new("generator", "reconcile", "data-jobs/generator/{0}/reconcile", 1, false, CliFamilyPayload.BatchReconciliation, RuntimeProgress: true),
        new("generator", "reconcile-preview", "data-jobs/generator/{0}/reconcile/preview", 1, true, CliFamilyPayload.ReconciliationProof),
        new("connect", "auto-restart-preview", "clusters/{0}/connect/profiles/{1}/connectors/{2}/restart-policy/preview", 3, true, CliFamilyPayload.AutoRestart, "put"),
        new("connect", "auto-restart-apply", "clusters/{0}/connect/profiles/{1}/connectors/{2}/restart-policy", 3, false, CliFamilyPayload.AutoRestartApply, "put"),
        new("connect", "auto-restart-status", "clusters/{0}/connect/profiles/{1}/connectors/{2}/restart-policy", 3, false, CliFamilyPayload.Empty, "get"),
    ];

    public static CliFamilyInvocation? TryParse(IReadOnlyList<string> args)
    {
        if (args.Count < 2) return null;
        if (args.Count >= 3 && args[0] == "notifications" && args[1] == "recovery")
        {
            var command = args[2];
            if (command is "status" or "preview" && args.Count == 3)
                return new("/api/v1/automation/notifications/recovery/" + command, false,
                    command == "preview" ? CliFamilyPayload.RecoveryPreview : CliFamilyPayload.Empty,
                    HumanOnly: true, Read: command == "status", Recovery: true, Method: command == "status" ? "get" : "post");
            if (command is "get" or "approve" or "apply" && args.Count == 4 && Guid.TryParseExact(args[3], "D", out _))
                return new("/api/v1/automation/notifications/recovery/" + args[3] + (command == "get" ? "" : "/" + command), false,
                    command == "get" ? CliFamilyPayload.Empty : CliFamilyPayload.RecoveryReview,
                    HumanOnly: true, Read: command == "get", Recovery: true, Method: command == "get" ? "get" : "post");
            throw new CliUsageException("Notification recovery requires a named command and its exact arguments.");
        }
        var contract = Contracts.SingleOrDefault(c => c.Family == args[0] && c.Command == args[1]);
        if (contract is null) return null;
        if (args.Count != contract.Arguments + 2) throw new CliUsageException("The named family command requires its exact resource arguments.");
        var resources = args.Skip(2).ToArray();
        if (resources.Any(r => string.IsNullOrWhiteSpace(r) || r.Length > 512 || r != r.Trim() || r.Any(char.IsControl) || r.Contains('/') || r.Contains('\\')))
            throw new CliUsageException("Family resource identifiers must be exact and bounded.");
        if ((!contract.Preview || contract.Command == "reconcile-preview") && contract.Arguments == 1 && !Guid.TryParseExact(resources[0], "D", out _))
            throw new CliUsageException("Material execution requires a canonical operation UUID.");
        var path = string.Format(System.Globalization.CultureInfo.InvariantCulture, contract.Route, resources.Select(Uri.EscapeDataString).Cast<object>().ToArray());
        return new("/api/v1/automation/" + path, contract.Preview, contract.Payload, Read: contract.Method == "get",
            Method: contract.Method, RuntimeProgress: contract.RuntimeProgress, Stop: contract.Stop,
            PolicyStatus: contract.Family == "connect" && contract.Command == "auto-restart-status");
    }

    public static async Task<JsonDocument?> ReadPayloadAsync(CliFamilyInvocation? invocation, CancellationToken token)
    {
        if (invocation is null || invocation.Payload == CliFamilyPayload.Empty) return null;
        if (string.IsNullOrWhiteSpace(invocation.RequestFile)) throw new CliUsageException("This named command requires --request-file.");
        await using var stream = new FileStream(invocation.RequestFile, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.Asynchronous);
        if (stream.Length is < 1 or > MaxRequestBytes) throw new CliUsageException("The closed family request must be 1..65536 bytes.");
        var buffer = new byte[MaxRequestBytes + 1];
        var length = 0;
        try
        {
            while (length < buffer.Length)
            {
                var read = await stream.ReadAsync(buffer.AsMemory(length), token);
                if (read == 0) break;
                length += read;
            }
            if (length > MaxRequestBytes) throw new CliUsageException("The closed family request is too large.");
            JsonDocument document;
            try
            {
                var reader = new Utf8JsonReader(buffer.AsSpan(0, length), new JsonReaderOptions { MaxDepth = 16 });
                document = JsonDocument.ParseValue(ref reader);
                if (reader.Read()) { document.Dispose(); throw new JsonException(); }
            }
            catch (JsonException) { throw new CliUsageException("Invalid closed family request JSON."); }
            if (invocation.ReadContract is { } readName ? !CliReadCommands.Validate(document.RootElement, readName) : !ValidatePayload(document.RootElement, invocation.Payload))
            { document.Dispose(); throw new CliUsageException("The request does not match this named family's closed schema."); }
            return document;
        }
        finally { System.Security.Cryptography.CryptographicOperations.ZeroMemory(buffer); }
    }

    public static bool ValidatePayload(JsonElement value, CliFamilyPayload payload) => payload switch
    {
        CliFamilyPayload.RecordsPreview => Object(value, ("records", Records), ("schemaValidation", v => Object(v, ("validatorId", Scalar), ("schemaIdentity", Scalar)))),
        CliFamilyPayload.RecordsExecution => Object(value, ("records", Records)),
        CliFamilyPayload.Purge => Object(value, ("targets", v => Array(v, p => Object(p, ("topicName", Scalar), ("partition", Scalar),
            ("selector", s => Object(s, ("kind", Scalar), ("beforeOffset", Scalar), ("timestampUtc", Scalar))))))),
        CliFamilyPayload.ConsumerOffsets => Object(value, ("targets", v => Array(v, p => Object(p, ("topicName", Scalar), ("partition", Scalar),
            ("selector", s => Object(s, ("kind", Scalar), ("value", Scalar), ("timestampUtc", Scalar))))))),
        CliFamilyPayload.ConsumerDelete => Object(value, ("mode", Scalar), ("targets", v => Array(v, p => Object(p, ("topicName", Scalar), ("partition", Scalar))))),
        CliFamilyPayload.SchemaRegister => Object(value, ("format", Scalar), ("schema", Scalar), ("references", v => Array(v, r => Object(r, ("name", Scalar), ("subject", Scalar), ("version", Scalar))))),
        CliFamilyPayload.SchemaCompatibility => Object(value, ("requestedMode", Scalar)),
        CliFamilyPayload.SchemaDelete => Object(value, ("version", Scalar), ("permanent", Scalar)),
        CliFamilyPayload.SchemaExecution => Object(value, ("schema", Scalar)),
        CliFamilyPayload.ConnectConfiguration => Object(value, ("configuration", v => Dictionary(v))),
        CliFamilyPayload.ConnectControl => Object(value, ("action", Scalar), ("taskId", Scalar)),
        CliFamilyPayload.RecoveryPreview => Object(value,
            ("command", v => Choice(v, "FenceBatch", "FenceSourceBatch", "AdvanceGeneration", "EnrollEmpty")),
            ("disposition", v => v.ValueKind == JsonValueKind.Null || Choice(v, "ExternalAcceptanceObserved", "ExternalNonAcceptanceObserved", "UnresolvedRiskAccepted")),
            ("proofReferenceSha256", Scalar), ("fencedSendersEvidenceSha256", Scalar), ("expectedDatabaseGeneration", Scalar),
            ("targetGeneration", Scalar), ("expectedAnchorSha256", Scalar), ("maxItems", Scalar)),
        CliFamilyPayload.RecoveryReview => Object(value, ("expectedRevision", Scalar), ("previewHash", Scalar)),
        CliFamilyPayload.DataJob => Object(value, ("sourceClusterId", Scalar), ("sourceProfileVersion", Scalar),
            ("destinationClusterId", Scalar), ("destinationProfileVersion", Scalar),
            ("ranges", v => Array(v, r => Object(r, ("sourceTopic", Scalar), ("sourcePartition", Scalar), ("destinationTopic", Scalar),
                ("destinationPartition", Scalar), ("startInclusive", Scalar), ("endExclusive", Scalar)))),
            ("budget", Budget), ("transform", v => Object(v, ("kind", Scalar), ("serdeFormat", Scalar), ("projectedFields", p => Array(p, Scalar))))),
        CliFamilyPayload.Generator => Object(value, ("destinationClusterId", Scalar), ("destinationProfileVersion", Scalar), ("destinationTopic", Scalar),
            ("destinationPartition", Scalar), ("recordCount", Scalar), ("seed", Scalar), ("budget", Budget),
            ("source", v => Object(v, ("kind", Scalar), ("schemaSubject", Scalar), ("schemaVersion", Scalar), ("template", Scalar)))),
        CliFamilyPayload.BatchReconciliation => Object(value, ("batchId", Scalar), ("disposition", v => Choice(v, "provenNonApplication"))),
        CliFamilyPayload.ReconciliationProof => Object(value, ("batchId", Scalar), ("expectedProgressVersion", Scalar),
            ("proofReferenceSha256", Scalar), ("fencedWorkerEvidenceSha256", Scalar)),
        CliFamilyPayload.AutoRestart => Object(value, ("enabled", Scalar), ("taskId", Scalar), ("maxAttempts", Scalar), ("initialBackoffSeconds", Scalar),
            ("maxBackoffSeconds", Scalar), ("activationLifetimeSeconds", Scalar), ("maxActivePoliciesPerProfile", Scalar), ("jitterBasisPoints", Scalar)),
        CliFamilyPayload.AutoRestartApply => Object(value, ("operationId", Scalar), ("taskId", Scalar)),
        _ => false,
    };
    private static bool Records(JsonElement value) => Array(value, r => Object(r, ("key", Scalar), ("value", Scalar),
        ("headers", h => Array(h, p => Object(p, ("name", Scalar), ("value", Scalar))))));
    private static bool Budget(JsonElement value) => Object(value, ("maxBatchRecords", Scalar), ("maxBatchBytes", Scalar), ("maxTotalRecords", Scalar),
        ("maxTotalBytes", Scalar), ("maxDurationSeconds", Scalar), ("maxRecordsPerSecond", Scalar), ("maxBytesPerSecond", Scalar));
    private static bool Scalar(JsonElement value) => value.ValueKind is not (JsonValueKind.Array or JsonValueKind.Object or JsonValueKind.Undefined);
    private static bool Choice(JsonElement value, params string[] values) => value.ValueKind == JsonValueKind.String && values.Contains(value.GetString(), StringComparer.Ordinal);
    private static bool Array(JsonElement value, Func<JsonElement, bool> element) => value.ValueKind == JsonValueKind.Null ||
        value.ValueKind == JsonValueKind.Array && value.GetArrayLength() <= 1024 && value.EnumerateArray().All(element);
    private static bool Dictionary(JsonElement value) => value.ValueKind == JsonValueKind.Null || value.ValueKind == JsonValueKind.Object &&
        value.EnumerateObject().Select(p => p.Name).Distinct(StringComparer.Ordinal).Count() == value.EnumerateObject().Count() &&
        value.EnumerateObject().All(p => p.Value.ValueKind == JsonValueKind.String);
    private static bool Object(JsonElement value, params (string Name, Func<JsonElement, bool> Validate)[] properties)
    {
        if (value.ValueKind == JsonValueKind.Null) return true;
        if (value.ValueKind != JsonValueKind.Object) return false;
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var property in value.EnumerateObject())
        {
            var admitted = properties.FirstOrDefault(p => p.Name == property.Name);
            if (!seen.Add(property.Name) || admitted.Validate is null || !admitted.Validate(property.Value)) return false;
        }
        return true;
    }
}
