using System.Text.Json;
using Kafdeck.Core.Notifications;

namespace Kafdeck.Core.AgentTools;

public sealed record CandidateNotificationReadRequest
{
    public const int MaxResults = 50;

    public CandidateNotificationReadRequest(int limit = 20, string? afterSubscriptionId = null)
    {
        if (limit is < 1 or > MaxResults) throw new ArgumentOutOfRangeException(nameof(limit));
        Limit = limit;
        AfterSubscriptionId = afterSubscriptionId is null ? null :
            NotificationSubscriptionDefinition.NormalizeSubscriptionId(afterSubscriptionId);
    }

    public int Limit { get; }
    public string? AfterSubscriptionId { get; }
}

// This port may be implemented only over the authenticated, authorization-filtered REST projection.
// It carries no principal, credentials, URL, execution or persistence capability supplied by a tool caller.
public interface IAuthorizedCandidateNotificationReadApi
{
    Task<CandidateNotificationReadPage> ReadAsync(CandidateNotificationReadRequest request,
        CancellationToken cancellationToken);
}

public sealed record CandidateNotificationMetadata(string SubscriptionId,
    NotificationSubscriptionState State, long Revision);

public sealed record CandidateNotificationReadPage(IReadOnlyList<CandidateNotificationMetadata> Items,
    bool Truncated, string? NextSubscriptionId, bool AuthorizationFiltered, bool ContinuationRestricted);

// Inert M2 adapter candidate. No MCP transport/package or HTTP client is registered by this class.
public static class CandidateNotificationReadTool
{
    public const string Name = "kafdeck_notification_subscriptions_read";
    public const int MaxArgumentBytes = 2048;
    public const int MaxResultBytes = 16384;
    public const string RestAction = "GET /api/v1/notifications/subscriptions";
    public const string InputSchema = """
        {"type":"object","additionalProperties":false,"properties":{"limit":{"type":"integer","minimum":1,"maximum":50,"default":20},"afterSubscriptionId":{"type":"string","minLength":1,"maxLength":128,"pattern":"^[A-Za-z0-9._-]+$"}}}
        """;
    public const string Annotations = """
        {"readOnlyHint":true,"destructiveHint":false,"idempotentHint":true,"openWorldHint":false}
        """;
    public const string OutputSchema = """
        {"type":"object","additionalProperties":false,"required":["Items","Truncated","NextSubscriptionId","AuthorizationFiltered","ContinuationRestricted"],"properties":{"Items":{"type":"array","maxItems":50,"items":{"type":"object","additionalProperties":false,"required":["SubscriptionId","State","Revision"],"properties":{"SubscriptionId":{"type":"string","minLength":1,"maxLength":128,"pattern":"^[A-Za-z0-9._-]+$"},"State":{"type":"integer","enum":[1,2,3]},"Revision":{"type":"integer","minimum":1}}}},"Truncated":{"type":"boolean"},"NextSubscriptionId":{"type":["string","null"],"minLength":1,"maxLength":128,"pattern":"^[A-Za-z0-9._-]+$"},"AuthorizationFiltered":{"type":"boolean"},"ContinuationRestricted":{"type":"boolean"}}}
        """;

    public static CandidateNotificationReadRequest ParseArguments(ReadOnlyMemory<byte> utf8)
    {
        if (utf8.Length is < 2 or > MaxArgumentBytes)
            throw new ArgumentException("Tool argument byte limit exceeded.");
        try
        {
            using var document = JsonDocument.Parse(utf8, new JsonDocumentOptions { MaxDepth = 2 });
            if (document.RootElement.ValueKind != JsonValueKind.Object)
                throw new ArgumentException("Tool arguments require an object.");
            var names = new HashSet<string>(StringComparer.Ordinal);
            var limit = 20;
            string? after = null;
            foreach (var property in document.RootElement.EnumerateObject())
            {
                if (!names.Add(property.Name)) throw new ArgumentException("Duplicate tool argument.");
                switch (property.Name)
                {
                    case "limit": limit = property.Value.GetInt32(); break;
                    case "afterSubscriptionId":
                        after = property.Value.GetString() ?? throw new ArgumentException("Invalid cursor.");
                        break;
                    default: throw new ArgumentException("Unknown tool argument.");
                }
            }
            return new(limit, after);
        }
        catch (Exception exception) when (exception is JsonException or InvalidOperationException or FormatException)
        {
            throw new ArgumentException("Invalid typed read tool arguments.");
        }
    }

    public static async Task<byte[]> ReadAsync(IAuthorizedCandidateNotificationReadApi api,
        CandidateNotificationReadRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(api);
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();
        // Exactly one bounded API page. Denials/failures propagate; no synthetic empty success.
        var page = await api.ReadAsync(request, cancellationToken).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        ValidatePage(page, request);
        var output = JsonSerializer.SerializeToUtf8Bytes(page);
        if (output.Length > MaxResultBytes)
            throw new InvalidOperationException("Typed read tool result byte limit exceeded.");
        return output;
    }

    private static void ValidatePage(CandidateNotificationReadPage page, CandidateNotificationReadRequest request)
    {
        if (page is null || page.Items is null || page.Items.Count > request.Limit ||
            page.Items.Any(item => item is null || item.Revision < 1 || !Enum.IsDefined(item.State)) ||
            page.Items.Select(item => item.SubscriptionId).Distinct(StringComparer.Ordinal).Count() != page.Items.Count)
            throw new InvalidOperationException("Invalid bounded typed API read projection.");
        foreach (var item in page.Items)
            NotificationSubscriptionDefinition.NormalizeSubscriptionId(item.SubscriptionId);
        if (page.NextSubscriptionId is not null)
            NotificationSubscriptionDefinition.NormalizeSubscriptionId(page.NextSubscriptionId);
        if ((page.AuthorizationFiltered && page.NextSubscriptionId is not null) ||
            page.ContinuationRestricted != (page.Truncated && page.AuthorizationFiltered) ||
            (!page.Truncated && page.NextSubscriptionId is not null) ||
            (page.Truncated && !page.AuthorizationFiltered &&
             (page.Items.Count == 0 || page.NextSubscriptionId != page.Items[^1].SubscriptionId)))
            throw new InvalidOperationException("Invalid authorization-safe API continuation.");
        if (request.AfterSubscriptionId is not null && page.Items.Any(item =>
                string.CompareOrdinal(item.SubscriptionId, request.AfterSubscriptionId) <= 0))
            throw new InvalidOperationException("Typed API read continuation did not advance.");
    }
}
