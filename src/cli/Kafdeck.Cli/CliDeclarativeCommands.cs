using System.Globalization;
using System.Net;
using System.Text;
using System.Text.Json;
using Kafdeck.Core.Declarative;

namespace Kafdeck.Cli;

public sealed record CliDeclarativeInvocation(string Command, string? ManifestFile = null, string? Format = null,
    string? SourceRef = null, string? SourceRevision = null, long ExpectedOwnershipRevision = 0,
    Guid? PlanId = null, string? PlanFingerprint = null, bool Yes = false, string? ResourceId = null,
    Guid? ExpectedSourceGeneration = null, long? ExpectedSourceRevision = null, string? ExpectedSourceHash = null);

public static class CliDeclarativeCommands
{
    internal static CliDeclarativeInvocation? TryParse(IReadOnlyList<string> args, string? file, string? format,
        string? source, string? revision, string? ownershipRevision, bool yes, string? resource = null)
    {
        if (args.Count < 1 || args[0] != "declarative")
        {
            if (file is not null || format is not null || source is not null || revision is not null || ownershipRevision is not null || yes || resource is not null)
                throw Invalid();
            return null;
        }
        if (args.Count == 3 && args[1] == "source-show")
        {
            if (!Identifier(args[2]) || !Identifier(resource) || file is not null || format is not null || source is not null || revision is not null || ownershipRevision is not null || yes) throw Invalid();
            return new("source-show", SourceRef: args[2], ResourceId: resource);
        }
        if (args.Count == 6 && args[1] == "source-advance")
        {
            if (!Identifier(args[2]) || !Guid.TryParseExact(args[3], "D", out var generation) ||
                !long.TryParse(args[4], NumberStyles.None, CultureInfo.InvariantCulture, out var expectedSource) || expectedSource < 0 ||
                !Hash(args[5]) || file is null || source is not null || revision is not null || ownershipRevision is not null || yes || resource is not null) throw Invalid();
            format = Format(file, format);
            return new("source-advance", file, format, SourceRef: args[2], ExpectedSourceGeneration: generation,
                ExpectedSourceRevision: expectedSource, ExpectedSourceHash: args[5]);
        }
        if (args.Count == 2 && args[1] == "compensation-catalog" || args.Count == 3 && args[1] == "compensation")
        {
            if (file is not null || format is not null || source is not null || revision is not null || ownershipRevision is not null || yes || resource is not null) throw Invalid();
            if (args[1] == "compensation-catalog") return new("compensation-catalog");
            if (!Guid.TryParseExact(args[2], "D", out var parent) || parent == Guid.Empty) throw Invalid();
            return new("compensation", PlanId: parent);
        }
        if (args.Count == 3 && args[1] == "compensation-preview")
        {
            if (!Guid.TryParseExact(args[2], "D", out var parent) || parent == Guid.Empty || resource is not null || yes || file is null || !Identifier(source) || !Hash(revision) ||
                !long.TryParse(ownershipRevision, NumberStyles.None, CultureInfo.InvariantCulture, out var inverseExpected) || inverseExpected < 0) throw Invalid();
            return new("compensation-preview", file, Format(file,format), source, revision, inverseExpected, PlanId:parent);
        }
        if (resource is not null) throw Invalid();
        if (args.Count == 2 && args[1] == "capabilities" || args.Count == 3 && args[1] == "show")
        {
            if (file is not null || format is not null || source is not null || revision is not null || ownershipRevision is not null || yes) throw Invalid();
            if (args[1] == "capabilities") return new("capabilities");
            if (!Guid.TryParseExact(args[2], "D", out var shown) || shown == Guid.Empty) throw Invalid();
            return new("show", PlanId: shown);
        }
        if (args.Count == 4 && args[1] is "apply" or "recover")
        {
            if (file is not null || format is not null || source is not null || revision is not null || ownershipRevision is not null || yes ||
                !Guid.TryParseExact(args[2], "D", out var id) || id == Guid.Empty || !Hash(args[3])) throw Invalid();
            return new(args[1], PlanId: id, PlanFingerprint: args[3]);
        }
        if (args.Count != 2 || args[1] is not ("validate" or "observe" or "drift" or "import" or "import-preview" or "plan" or "reconcile")) throw Invalid();
        if (file is null || !Identifier(source) || !Hash(revision) ||
            !long.TryParse(ownershipRevision, NumberStyles.None, CultureInfo.InvariantCulture, out var expected) || expected < 0 ||
            yes && args[1] != "reconcile") throw Invalid();
        format = Format(file, format);
        return new(args[1] == "import" ? "import-preview" : args[1], file, format, source, revision, expected, Yes: yes);
    }

    internal static async Task<object> ReadRequestAsync(CliDeclarativeInvocation invocation, CancellationToken token)
    {
        await using var stream = new FileStream(invocation.ManifestFile!, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.Asynchronous);
        if (stream.Length is < 2 or > DeclarativeSubscriptionManifest.MaxInputBytes) throw Invalid();
        var bytes = new byte[DeclarativeSubscriptionManifest.MaxInputBytes + 1]; var count = 0;
        while (count < bytes.Length)
        {
            var read = await stream.ReadAsync(bytes.AsMemory(count), token); if (read == 0) break; count += read;
        }
        DeclarativeSubscriptionManifest manifest;
        try { manifest = DeclarativeSubscriptionManifest.Parse(bytes.AsMemory(0, count), invocation.Format!); }
        catch (ArgumentException) { throw Invalid(); }
        if (invocation.Command == "source-advance") return new { manifest = Encoding.UTF8.GetString(manifest.ToCanonicalUtf8()), format = "json",
            expectedGeneration = invocation.ExpectedSourceGeneration, expectedRevision = invocation.ExpectedSourceRevision,
            expectedSourceHash = invocation.ExpectedSourceHash };
        return new { manifest = Encoding.UTF8.GetString(manifest.ToCanonicalUtf8()), format = "json", sourceRef = invocation.SourceRef,
            sourceRevision = invocation.SourceRevision, expectedOwnershipRevision = invocation.ExpectedOwnershipRevision };
    }

    internal static async Task<int> ExecuteAsync(CliInvocation invocation, TextWriter output, TextWriter error,
        CancellationToken token, Func<Uri, CancellationToken, Task>? openBrowser)
    {
        var command = invocation.Declarative!;
        // Parse and validate before opening credential files, OIDC or any socket.
        var request = command.Command is "apply" or "recover" or "show" or "capabilities" or "source-show" or "compensation" or "compensation-catalog" ? null : await ReadRequestAsync(command, token);
        if (command.Command == "validate")
        {
            var json = JsonSerializer.SerializeToElement(request);
            var manifest = DeclarativeSubscriptionManifest.Parse(Encoding.UTF8.GetBytes(json.GetProperty("manifest").GetString()!), "json");
            await output.WriteLineAsync(JsonSerializer.Serialize(new { valid = true, apiVersion = DeclarativeSubscriptionManifest.ApiVersion,
                kind = "NotificationSubscription", manifestFingerprint = manifest.Fingerprint() }));
            return CliApplication.Success;
        }
        using var client = await KafdeckCliClient.CreateAsync(invocation, token, openBrowser);
        using var requests = CancellationTokenSource.CreateLinkedTokenSource(token);
        requests.CancelAfter(KafdeckCliClient.RequestTimeout); token = requests.Token;
        if (command.Command == "reconcile")
        {
            var observed = await SendAsync(client, "/api/v1/declarative/observe", request, null, error, token);
            if (observed.Body is null) return observed.Exit;
            var decision = ReconcileObservation(observed.Body);
            if (decision is { } exit)
            {
                await output.WriteLineAsync(observed.Body); return exit;
            }
        }
        var path = BuildRoute(command);
        var body = command.Command is "apply" or "recover" ? (object)new { planFingerprint = command.PlanFingerprint } : request;
        var result = await SendAsync(client, path, body, invocation.IdempotencyKey, error, token, command.Command is "show" or "capabilities" or "source-show" or "compensation" or "compensation-catalog");
        if (result.Body is null) return result.Exit;
        if (command.Command == "reconcile" && command.Yes)
        {
            var current = result.Body;
            Guid reviewedId; string reviewedHash;
            using (var preview = JsonDocument.Parse(current))
            {
                var root = preview.RootElement;
                if (!Plan(root, out reviewedId, out reviewedHash)) return CliApplication.RemoteError;
                if (root.GetProperty("state").GetString() == "awaitingConfirmation")
                {
                    var challenge = root.TryGetProperty("confirmationChallenge", out var value) && value.ValueKind == JsonValueKind.String ? value.GetString() : null;
                    var confirmed = await SendAsync(client, $"/api/v1/automation/mutations/{reviewedId:D}/confirm",
                        new { previewHash = reviewedHash, typedTargetChallenge = challenge }, null, error, token);
                    if (confirmed.Body is null) return confirmed.Exit;
                    current = confirmed.Body;
                }
            }
            using var confirmation = JsonDocument.Parse(current);
            var status = confirmation.RootElement;
            // No automatic approval, refresh, retry, or cross-principal takeover.
            if (!Plan(status, out var readyId, out var readyHash) || readyId != reviewedId || readyHash != reviewedHash) return CliApplication.RemoteError;
            if (status.GetProperty("state").GetString() == "ready")
            {
                var applied = await SendAsync(client, $"/api/v1/declarative/plans/{readyId:D}/apply",
                    new { planFingerprint = readyHash }, null, error, token);
                if (applied.Body is null) return applied.Exit;
                current = applied.Body;
            }
            await output.WriteLineAsync(current);
            return CliApplication.MutationOutcomeCategory(current);
        }
        await output.WriteLineAsync(result.Body);
        if (command.Command is "capabilities" or "source-show" or "compensation" or "compensation-catalog" or "source-advance") return CliApplication.Success;
        return command.Command is "compensation-preview" or "plan" or "import-preview" or "apply" or "recover" or "reconcile" or "show"
            ? CliApplication.MutationOutcomeCategory(result.Body) : DriftExit(result.Body);
    }

    private static async Task<(int Exit, string? Body)> SendAsync(KafdeckCliClient client, string path, object? request,
        string? key, TextWriter error, CancellationToken token, bool get = false)
    {
        using var response = get ? await client.GetAsync(path, token) : await client.PostAsync(path, request, key, token);
        if (!response.IsSuccessStatusCode)
        {
            await error.WriteLineAsync($"Kafdeck returned HTTP {(int)response.StatusCode}.");
            return (response.StatusCode switch { HttpStatusCode.Unauthorized or HttpStatusCode.Forbidden => CliApplication.AuthenticationOrAuthorizationError,
                HttpStatusCode.Conflict => CliApplication.Conflict, HttpStatusCode.Gone => CliApplication.Expired, _ => CliApplication.RemoteError }, null);
        }
        try
        {
            var body = await CliResponseBodyReader.ReadAsync(response.Content, token);
            using var document = JsonDocument.Parse(body);
            return (CliApplication.Success, document.RootElement.GetRawText());
        }
        catch (CliResponseTooLargeException) { await error.WriteLineAsync("Declarative response exceeded the bounded CLI contract."); return (CliApplication.RemoteError, null); }
    }
    private static bool Plan(JsonElement root, out Guid id, out string hash)
    {
        id = default; hash = "";
        if (root.ValueKind != JsonValueKind.Object || !root.TryGetProperty("operationId", out var operation) ||
            operation.ValueKind != JsonValueKind.String || !operation.TryGetGuid(out id) || id == Guid.Empty || !root.TryGetProperty("previewHash", out var fingerprint) ||
            fingerprint.ValueKind != JsonValueKind.String || !Hash(fingerprint.GetString()) ||
            !root.TryGetProperty("state", out var state) || state.ValueKind != JsonValueKind.String) return false;
        hash = fingerprint.GetString()!; return true;
    }
    internal static int DriftExit(string json)
    {
        using var document = JsonDocument.Parse(json);
        if (document.RootElement.ValueKind != JsonValueKind.Object || !document.RootElement.TryGetProperty("drift", out var status) || status.ValueKind != JsonValueKind.String) return CliApplication.RemoteError;
        return status.GetString()?.ToLowerInvariant() switch
        {
            "insync" => CliApplication.Success,
            "outofsync" or "missing" or "conflict" => CliApplication.Conflict,
            "inaccessible" => CliApplication.AuthenticationOrAuthorizationError,
            "unknown" => CliApplication.UnknownEffect,
            _ => CliApplication.RemoteError
        };
    }
    // null means a finite newly governed plan is needed. Observed-only current
    // resources always require explicit import; uncertainty never creates.
    internal static int? ReconcileObservation(string json)
    {
        using var document = JsonDocument.Parse(json); var root = document.RootElement;
        if (root.ValueKind != JsonValueKind.Object || !root.TryGetProperty("drift", out var drift) || drift.ValueKind != JsonValueKind.String ||
            !root.TryGetProperty("ownership", out var ownership) || ownership.ValueKind != JsonValueKind.String)
        {
            if (root.ValueKind == JsonValueKind.Object && root.TryGetProperty("drift", out var unavailable) &&
                unavailable.ValueKind == JsonValueKind.String && unavailable.GetString()?.ToLowerInvariant() is "unknown" or "inaccessible" or "conflict" or "unsupported")
                return DriftExit(json);
            return CliApplication.RemoteError;
        }
        var status = drift.GetString()?.ToLowerInvariant(); var managed = ownership.GetString() == "Managed";
        if (status == "insync" && managed)
        {
            if (!root.TryGetProperty("sourceInSync", out var source) || source.ValueKind is not (JsonValueKind.True or JsonValueKind.False))
                return CliApplication.RemoteError;
            return source.GetBoolean() ? CliApplication.Success : null;
        }
        if (status == "missing" && ownership.GetString() == "ObservedOnly" || status == "outofsync" && managed) return null;
        return status == "insync" ? CliApplication.Conflict : DriftExit(json);
    }
    private static bool Identifier(string? text) => text is { Length: >= 1 and <= 128 } && text is not ("." or "..") &&
        text.All(ch => char.IsAsciiLetterOrDigit(ch) || ch is '.' or '_' or '-');
    private static bool Hash(string? text) => text is { Length: 64 } && text.All(ch => ch is >= 'a' and <= 'f' or >= '0' and <= '9');
    private static CliUsageException Invalid() => new("Declarative command requires its exact closed manifest/source/revision or stored plan arguments.");
    internal static string BuildRoute(CliDeclarativeInvocation invocation) => invocation.Command switch
    {
        "compensation-catalog" => "/api/v1/declarative/compensation/catalog",
        "compensation" or "compensation-preview" => $"/api/v1/declarative/plans/{invocation.PlanId:D}/{invocation.Command}",
        "apply" or "recover" => $"/api/v1/declarative/plans/{invocation.PlanId:D}/{invocation.Command}",
        "show" => $"/api/v1/declarative/plans/{invocation.PlanId:D}",
        "capabilities" => "/api/v1/declarative/capabilities",
        "source-show" => $"/api/v1/declarative/sources/{Uri.EscapeDataString(invocation.SourceRef!)}?resourceId={Uri.EscapeDataString(invocation.ResourceId!)}",
        "source-advance" => $"/api/v1/declarative/sources/{Uri.EscapeDataString(invocation.SourceRef!)}/advance",
        "reconcile" => "/api/v1/declarative/plan",
        "validate" or "observe" or "drift" or "import-preview" or "plan" => "/api/v1/declarative/" + invocation.Command,
        _ => throw Invalid()
    };
    private static string Format(string file, string? format)
    {
        format ??= Path.GetExtension(file).ToLowerInvariant() switch { ".json" => "json", ".yaml" or ".yml" => "yaml", _ => null };
        return format is "json" or "yaml" ? format : throw Invalid();
    }
}
