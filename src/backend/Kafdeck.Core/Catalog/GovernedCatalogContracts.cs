using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Kafdeck.Core.Catalog;

public enum CatalogEntityKind { Team = 1, Application = 2, Topic = 3 }

[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public sealed record CatalogChange(CatalogEntityKind Kind, string? Id, long ExpectedRevision, string Name,
    string? Description = null, bool Archived = false, string? TeamId = null, string? ApplicationId = null,
    string? TopicName = null, string? Owner = null, string? Domain = null, string? Classification = null,
    string? DocumentationReference = null, IReadOnlyList<string>? Tags = null, bool RebindTopicIdentity = false);

public sealed record CatalogPlannedChange(CatalogChange Change, string Id, string? TopicIdentity);
public sealed record CatalogRecord(CatalogEntityKind Kind, string Id, string ClusterId, long Revision, string Name,
    string? Description, bool Archived, string? TeamId, string? ApplicationId, string? TopicName, string? TopicIdentity,
    string? Owner, string? Domain, string? Classification, string? DocumentationReference, IReadOnlyList<string> Tags,
    string CreatedBy, DateTimeOffset CreatedAtUtc, DateTimeOffset UpdatedAtUtc, bool? TopicIdentityCurrent = null);
public sealed record CatalogHistoryItem(CatalogRecord Record, string ActorFingerprint, string AuthorityFingerprint,
    Guid OperationId, DateTimeOffset EffectiveFromUtc, DateTimeOffset? EffectiveToUtc);
public sealed record CatalogPlan(Guid PlanId, string ClusterId, string RequesterPrincipalId, string AuthorityFingerprint,
    string DeploymentGeneration, string ContentHash, IReadOnlyList<CatalogPlannedChange> Changes,
    DateTimeOffset CreatedAtUtc, DateTimeOffset ExpiresAtUtc);
public sealed record CatalogCommitReceipt(Guid OperationId, string ContentHash, DateTimeOffset AppliedAtUtc,
    IReadOnlyList<CatalogRecord> Records);
public sealed record CatalogTopicIdentity(string KafkaClusterId, string TopicId)
{
    public string Fingerprint => CatalogContract.Hash(KafkaClusterId + "\n" + TopicId);
}
public interface ICatalogTopicIdentityPort
{
    Task<CatalogTopicIdentity?> ObserveAsync(string clusterId, string topicName, CancellationToken token);
}
public interface ICatalogStore
{
    string DeploymentGeneration { get; }
    Task InitializeAsync(CancellationToken token);
    Task<CatalogRecord?> GetAsync(string clusterId, CatalogEntityKind kind, string id, CancellationToken token);
    Task<CatalogRecord?> FindTopicAsync(string clusterId, string topicName, CancellationToken token);
    Task<IReadOnlyList<CatalogRecord>> ListAsync(string clusterId, CatalogEntityKind kind, string? afterId, int take, CancellationToken token);
    Task<IReadOnlyList<CatalogHistoryItem>> HistoryAsync(string clusterId, CatalogEntityKind kind, string id, long afterRevision, int take, CancellationToken token);
    Task<CatalogPlan?> GetPlanAsync(Guid planId, CancellationToken token);
    Task<CatalogPlan> StageAsync(CatalogPlan plan, CancellationToken token);
    Task ValidateAsync(CatalogPlan plan, CancellationToken token);
    Task<CatalogCommitReceipt?> GetReceiptAsync(Guid operationId, CancellationToken token);
    Task<CatalogCommitReceipt?> CommitAsync(CatalogPlan plan, Func<bool> currentAuthority, CancellationToken token);
}
public sealed class CatalogConflictException(string code) : InvalidOperationException(code)
{
    public string Code { get; } = code;
}

public static class CatalogContract
{
    public static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter() }, MaxDepth = 12,
    };
    public static string Hash(string value) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value))).ToLowerInvariant();
    public static string Resource(CatalogPlannedChange item) => item.Change.Kind == CatalogEntityKind.Topic ? item.Change.TopicName! : "catalog:" + item.Change.Kind.ToString().ToLowerInvariant() + ":" + item.Id;
    public static string Resource(CatalogRecord item) => item.Kind == CatalogEntityKind.Topic ? item.TopicName! : "catalog:" + item.Kind.ToString().ToLowerInvariant() + ":" + item.Id;
    public static string MaterialHash(string clusterId, IReadOnlyList<CatalogPlannedChange> items) => Hash(JsonSerializer.Serialize(new { clusterId, changes = items }, Json));
    public static string Bound(string value, string name, int max, bool allowWhitespace = true)
    {
        if (string.IsNullOrWhiteSpace(value) || value.Length > max || value.Any(char.IsControl) ||
            (!allowWhitespace && value.Any(char.IsWhiteSpace)) || value != value.Trim()) throw new ArgumentException("Invalid catalog " + name);
        return value;
    }
    public static string Id(string value) => Guid.TryParseExact(value, "N", out var id) && id != Guid.Empty && value == value.ToLowerInvariant()
        ? value : throw new ArgumentException("Catalog ID must be a server-issued lowercase UUID.");
    public static CatalogChange Validate(CatalogChange change)
    {
        ArgumentNullException.ThrowIfNull(change);
        if (!Enum.IsDefined(change.Kind) || change.ExpectedRevision is < 0 or long.MaxValue ||
            (change.Id is null) != (change.ExpectedRevision == 0) || change.Id is not null && Id(change.Id) != change.Id)
            throw new ArgumentException("Invalid catalog identity/revision.");
        Bound(change.Name, "name", 128);
        if (change.Description is not null) Bound(change.Description, "description", 2048);
        foreach (var text in new[] { change.Owner, change.Domain, change.Classification })
            if (text is not null) Bound(text, "text", 128);
        if (change.TeamId is not null) Id(change.TeamId);
        if (change.ApplicationId is not null) Id(change.ApplicationId);
        if (change.Kind == CatalogEntityKind.Team && (change.TeamId is not null || change.ApplicationId is not null) ||
            change.Kind == CatalogEntityKind.Application && change.ApplicationId is not null ||
            change.Kind != CatalogEntityKind.Topic && (change.TopicName is not null || change.RebindTopicIdentity)) throw new ArgumentException("Invalid catalog references.");
        if (change.Kind == CatalogEntityKind.Topic)
        {
            Bound(change.TopicName!, "topic", 249, false);
            if (change.TopicName is "." or ".." || change.TopicName!.Any(c => !char.IsAsciiLetterOrDigit(c) && c is not '.' and not '_' and not '-'))
                throw new ArgumentException("Invalid topic name.");
        }
        if (change.DocumentationReference is not null && (!Uri.TryCreate(change.DocumentationReference, UriKind.Absolute, out var uri) ||
            uri.Scheme != Uri.UriSchemeHttps || !string.IsNullOrEmpty(uri.UserInfo) || change.DocumentationReference.Length > 2048 || change.DocumentationReference.Any(char.IsControl)))
            throw new ArgumentException("Documentation reference must be safe HTTPS text.");
        var tags = change.Tags ?? [];
        if (tags.Count > 20 || tags.Distinct(StringComparer.Ordinal).Count() != tags.Count) throw new ArgumentException("Invalid catalog tags.");
        foreach (var tag in tags) Bound(tag, "tag", 64);
        return change with { Tags = tags.OrderBy(t => t, StringComparer.Ordinal).ToArray() };
    }
    public static void ValidatePlan(CatalogPlan plan)
    {
        if (plan.PlanId == Guid.Empty || plan.Changes.Count is < 1 or > 500 || plan.ExpiresAtUtc <= plan.CreatedAtUtc ||
            plan.ExpiresAtUtc - plan.CreatedAtUtc > TimeSpan.FromMinutes(15)) throw new ArgumentException("Invalid catalog plan.");
        Bound(plan.ClusterId, "cluster", 256, false); Bound(plan.RequesterPrincipalId, "actor", 2048); Bound(plan.DeploymentGeneration, "generation", 128, false);
        if (plan.AuthorityFingerprint.Length != 64 || plan.AuthorityFingerprint.Any(c => !char.IsAsciiHexDigit(c))) throw new ArgumentException("Invalid authority fingerprint.");
        foreach (var item in plan.Changes)
        {
            Validate(item.Change); Id(item.Id);
            if (item.Change.Id is not null && item.Change.Id != item.Id || item.Change.Kind == CatalogEntityKind.Topic &&
                (item.TopicIdentity is not { Length: 64 } || item.TopicIdentity.Any(c => !char.IsAsciiHexDigit(c)))) throw new ArgumentException("Invalid planned catalog identity.");
        }
        if (plan.Changes.Select(i => (i.Change.Kind, i.Id)).Distinct().Count() != plan.Changes.Count ||
            plan.ContentHash != MaterialHash(plan.ClusterId, plan.Changes)) throw new ArgumentException("Catalog material hash mismatch.");
        if (Encoding.UTF8.GetByteCount(JsonSerializer.Serialize(plan.Changes, Json)) > 1024 * 1024) throw new ArgumentException("Catalog import exceeds bound.");
    }
}
