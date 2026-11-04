using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Kafdeck.Core.Notifications;

namespace Kafdeck.Infrastructure.Persistence;

public enum W68RuntimeRegistrationKind { Owner, Source, Controller }
public enum W68RuntimePlanCommand { Adopt, Apply, Relinquish }

// Server-owned registrations; existence is never authentication or RBAC proof.
public sealed record W68RuntimeRegistration(W68RuntimeRegistrationKind Kind, string DeploymentId,
    string RegistrationId, Guid GenerationId, long Revision, string OwnerId, string PrincipalId,
    string RepositoryId, string Path, string PinnedRevision, bool Enabled);

public sealed record W68RuntimeLease(string DeploymentId, string LeaseScopeId, string HolderId,
    long Epoch, DateTimeOffset ExpiresAtUtc);

public sealed record W68RuntimeResourceKey(string DeploymentId, string ScopeKind, string ScopeId,
    string ResourceKind, string ResourceId);

public sealed record W68RuntimeOwnership(W68RuntimeResourceKey Key, string OwnerId, string SourceId,
    Guid SourceGeneration, string SourceRevisionHash, Guid GenerationId, long Revision,
    long LeaseEpoch, string State, string PrincipalId, string PolicyFingerprint);

// This envelope is created by the authenticated backend after exact resource
// reads. It is not bound from an HTTP request. PlanId is the existing governed
// MutationOperationId; approval/lifecycle remains in that mutation repository.
public sealed record W68StoredRuntimePlan(Guid PlanId, W68RuntimePlanCommand Command,
    W68RuntimeResourceKey Key, string OwnerId, string SourceId, Guid SourceGeneration,
    string SourceRevisionHash, string RequesterPrincipalId, long ExpectedOwnershipRevision,
    Guid? ExpectedOwnershipGeneration, long? ExpectedSubscriptionRevision,
    string ObservedSubscriptionFingerprint, string? ObservedDestinationId, NotificationSubscriptionDefinition DesiredSubscription,
    NotificationSubscriptionState DesiredState, string DesiredCanonicalJson, string DesiredFingerprint,
    string PolicyFingerprint, string ControllerId, string LeaseScopeId, string LeaseHolderId, long LeaseEpoch,
    DateTimeOffset ExpiresAtUtc, string PlanFingerprint, string? ExpectedOwnedSourceRevisionHash = null,
    Guid DeploymentGeneration = default, long SourceRegistrationRevision = 0,
    NotificationSubscriptionSnapshot? ObservedSubscription = null);

public sealed record W68RuntimeCommitReceipt(Guid OperationId, string PlanFingerprint,
    long OwnershipRevision, Guid OwnershipGeneration, long SubscriptionRevision,
    DateTimeOffset CommittedAtUtc);

public interface IW68RuntimeStore
{
    // Verification only. Schema installation/adoption is an explicit operator action.
    Task InitializeAsync(CancellationToken token = default);
    Task<DateTimeOffset> ReadDatabaseUtcNowAsync(CancellationToken token = default);
    Task<Guid?> GetDeploymentGenerationAsync(string deploymentId, CancellationToken token = default);
    Task<W68RuntimeRegistration?> GetRegistrationAsync(W68RuntimeRegistrationKind kind, string deploymentId, string registrationId, CancellationToken token = default);
    Task<W68RuntimeRegistration?> UpsertRegistrationAsync(W68RuntimeRegistration registration, long expectedRevision,
        Func<CancellationToken, Task<bool>> currentAuthority, CancellationToken token = default);
    Task<W68RuntimeOwnership?> GetOwnershipAsync(W68RuntimeResourceKey key, CancellationToken token = default);
    Task<W68StoredRuntimePlan?> GetPlanAsync(Guid planId, CancellationToken token = default);
    Task<W68StoredRuntimePlan?> LoadPlanAsync(Guid planId, CancellationToken token = default);
    Task<W68StoredRuntimePlan?> SavePlanAsync(W68StoredRuntimePlan plan,
        Func<W68StoredRuntimePlan, CancellationToken, Task<bool>> currentAuthority, CancellationToken token = default);
    Task<W68RuntimeLease?> AcquireLeaseAsync(string deploymentId, string leaseScopeId, string controllerId,
        string ownerId, string authenticatedPrincipalId, string holderId, int ttlSeconds,
        Func<CancellationToken, Task<bool>> currentAuthority, CancellationToken token = default);
    Task<W68RuntimeLease?> RenewLeaseAsync(W68RuntimeLease lease, string controllerId, string ownerId,
        string authenticatedPrincipalId, int ttlSeconds, Func<CancellationToken, Task<bool>> currentAuthority,
        CancellationToken token = default);
    Task<bool> ReleaseLeaseAsync(W68RuntimeLease lease, string controllerId, string ownerId,
        string authenticatedPrincipalId, Func<CancellationToken, Task<bool>> currentAuthority, CancellationToken token = default);
    Task<W68RuntimeCommitReceipt?> GetCommitReceiptAsync(Guid operationId, CancellationToken token = default);
    Task<W68RuntimeCommitReceipt?> GetVerifiedCommitReceiptAsync(Guid planId, string expectedPlanHash, string authenticatedPrincipalId,
        Func<W68StoredRuntimePlan, CancellationToken, Task<bool>> currentAuthority, CancellationToken token = default);
    Task<bool> IsPlanCurrentAsync(Guid planId, string expectedPlanHash,
        Func<W68StoredRuntimePlan, CancellationToken, Task<bool>> currentAuthority, CancellationToken token = default);
    Task<W68RuntimeCommitReceipt?> ApplySubscriptionAsync(Guid planId, string expectedPlanHash, string authenticatedPrincipalId,
        Func<W68StoredRuntimePlan, CancellationToken, Task<bool>> currentAuthority, CancellationToken token = default);
}

public static class W68RuntimeFingerprints
{
    public static string MissingSubscription { get; } = Hash("W68/NotificationSubscription/Missing/v1");
    public static string LeaseScope(string subscriptionId) => "subscription-" + Hash(subscriptionId);
    public static string Plan(W68StoredRuntimePlan plan) => Hash(JsonSerializer.Serialize(plan with { PlanFingerprint = "" }));
    public static string Desired(string canonicalJson) => Hash(canonicalJson);
    public static string Subscription(NotificationSubscriptionSnapshot snapshot) => Hash(JsonSerializer.Serialize(new
    {
        snapshot.Definition.SubscriptionId, snapshot.Definition.DestinationId,
        EventClasses = snapshot.Definition.EventClasses.Select(x => x.ToString()).ToArray(),
        snapshot.Definition.EventTypes, State = snapshot.State.ToString(), snapshot.Revision,
        UpdatedAtUtc = snapshot.UpdatedAtUtc.ToUniversalTime().ToString("O"),
    }));
    private static string Hash(string value) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value))).ToLowerInvariant();
}
